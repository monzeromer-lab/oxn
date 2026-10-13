//! Redis backend.
//!
//! Uses [`redis-rs`](https://docs.rs/redis) via
//! [`deadpool-redis`](https://docs.rs/deadpool-redis) for a pooled async
//! client. Blocking reads (`BZPOPMIN`) take a dedicated pool slot rather
//! than sharing a multiplexed connection, so multiple concurrent workers
//! never serialize on each other's blocking calls. All state transitions
//! are Lua scripts registered once (content-addressed via `EVALSHA`).

mod keys;
mod scripts;

pub use keys::KeyBuilder;
pub use scripts::script_hashes;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use deadpool_redis::{Config as PoolConfig, Connection as PooledConn, Pool, Runtime};
use futures::stream::Stream;
use redis::AsyncCommands;
use tracing::Instrument;

use crate::backend::{
    Backend, EventRecord, Fetched, JobInsert, JobRange, RawJob, RecordStream, StateCounts,
    StreamStart,
};
use crate::error::{Error, Result};
use crate::events::Event;
use crate::job::{JobId, JobState};
use crate::options::{Removal, ResolvedJobOptions};

/// Construction config for [`RedisBackend`].
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL, e.g. `redis://127.0.0.1:6379/0` or
    /// `rediss://host:6380` for TLS.
    pub url: String,
    /// Key prefix applied to every Redis key. Default: `"oxn"`.
    pub prefix: String,
    /// Max pool size. The pool is shared between non-blocking commands
    /// and blocking `BZPOPMIN` fetches; size it >= max concurrent workers
    /// + a few slots for producers and dashboard.
    pub pool_size: usize,
    /// Cap on the events Redis Stream length. Stream is trimmed on
    /// every XADD.
    pub max_stream_length: usize,
    /// How often a background task PINGs every pool slot to keep the TCP
    /// connection alive. Managed-Redis providers (DigitalOcean, AWS
    /// ElastiCache Serverless, Upstash, Redis Cloud) close idle TCP
    /// connections at the proxy after 60–300s; without keepalive, the
    /// first command on a stale slot hangs for seconds while the kernel
    /// detects the dead socket and a fresh TLS handshake is paid. Set
    /// to `None` to disable. Default: `Some(Duration::from_secs(25))`.
    pub keepalive_interval: Option<Duration>,
    /// Per-checkout health-check timeout for [`Self::keepalive_interval`].
    /// If a slot's PING does not return within this window, the connection
    /// is detached from the pool and a fresh one is minted. Default 2s.
    pub health_check_timeout: Duration,
}

impl RedisConfig {
    /// Build a config with sane defaults (prefix `"oxn"`, pool 16,
    /// max stream 10 000, keepalive every 25s).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: "oxn".to_string(),
            pool_size: 16,
            max_stream_length: 10_000,
            keepalive_interval: Some(Duration::from_secs(25)),
            health_check_timeout: Duration::from_secs(2),
        }
    }

    /// Override the Redis key prefix.
    #[must_use]
    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }

    /// Override the connection pool size (floored at 1).
    #[must_use]
    pub fn pool_size(mut self, n: usize) -> Self {
        self.pool_size = n.max(1);
        self
    }

    /// Override the events stream length cap.
    #[must_use]
    pub fn max_stream_length(mut self, n: usize) -> Self {
        self.max_stream_length = n;
        self
    }

    /// Override the keepalive cadence. Pass `None` to disable.
    #[must_use]
    pub fn keepalive_interval(mut self, interval: Option<Duration>) -> Self {
        self.keepalive_interval = interval;
        self
    }

    /// Override the per-checkout PING timeout.
    #[must_use]
    pub fn health_check_timeout(mut self, d: Duration) -> Self {
        self.health_check_timeout = d;
        self
    }
}

/// Redis-backed storage implementation.
#[derive(Clone)]
pub struct RedisBackend {
    pool: Pool,
    /// For connections that must not come from the pool: each event
    /// subscriber blocks in `XREAD` on its own connection.
    client: redis::Client,
    prefix: String,
    max_stream_length: usize,
    scripts: Arc<scripts::Scripts>,
    /// Process-local set of queue names we've already SADD'd into the
    /// `{prefix}:queues` registry. Eliminates an unconditional `SADD` round
    /// trip on every `add()` — particularly painful against managed Redis
    /// where it's a ~150 ms tax per submission.
    registered_queues: Arc<dashmap::DashSet<String>>,
    /// Holds a [`tokio_util::sync::CancellationToken`] inside an `Arc`. When
    /// the last clone of the backend drops, the inner [`Drop`] fires the
    /// token, which signals the keepalive task to exit.
    _keepalive: Option<Arc<KeepaliveGuard>>,
}

/// Drop-cancels the keepalive task when the last `RedisBackend` clone goes
/// out of scope. Held inside `Arc` so cloning the backend doesn't drop the
/// task.
#[derive(Debug)]
struct KeepaliveGuard {
    cancel: tokio_util::sync::CancellationToken,
}

impl Drop for KeepaliveGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl fmt::Debug for RedisBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisBackend")
            .field("prefix", &self.prefix)
            .field("max_stream_length", &self.max_stream_length)
            .finish_non_exhaustive()
    }
}

impl RedisBackend {
    /// Connect to Redis with the given config.
    ///
    /// Eagerly issues `SCRIPT LOAD` for every embedded Lua script before
    /// returning, so the first user-facing call (e.g. `Queue::add`) does
    /// not pay a cold-start `NOSCRIPT` → `SCRIPT LOAD` round-trip. On a
    /// managed Redis behind a TLS proxy that round-trip can stall for
    /// seconds; preloading folds the cost into normal startup.
    ///
    /// Re-call [`Self::warmup`] after a `SCRIPT FLUSH` or Redis restart
    /// to refill the cache without dropping the pool.
    pub async fn connect(cfg: RedisConfig) -> Result<Self> {
        // Honour `cfg.pool_size`: `Config::from_url` alone leaves the pool
        // at deadpool's default (`num_cpus * 4`), silently ignoring whatever
        // the caller asked for.
        let mut pool_cfg = PoolConfig::from_url(cfg.url.clone());
        // `PoolConfig::from_url` ignores `pool_size`; set it explicitly.
        pool_cfg.pool = Some(deadpool_redis::PoolConfig::new(cfg.pool_size));
        let pool = pool_cfg.create_pool(Some(Runtime::Tokio1))?;

        // Keepalive task — spawned only if enabled via config. The task
        // holds a clone of the pool plus a CancellationToken, both of
        // which the backend itself drops references to via [`KeepaliveGuard`].
        let keepalive = cfg.keepalive_interval.map(|interval| {
            let cancel = tokio_util::sync::CancellationToken::new();
            spawn_keepalive(
                pool.clone(),
                cfg.pool_size,
                interval,
                cfg.health_check_timeout,
                cancel.clone(),
            );
            Arc::new(KeepaliveGuard { cancel })
        });

        let client = redis::Client::open(cfg.url.as_str())?;
        let backend = Self {
            pool,
            client,
            prefix: cfg.prefix,
            max_stream_length: cfg.max_stream_length,
            scripts: Arc::new(scripts::Scripts::new()),
            registered_queues: Arc::new(dashmap::DashSet::new()),
            _keepalive: keepalive,
        };
        backend.warmup().await?;
        Ok(backend)
    }

    /// Re-issue `SCRIPT LOAD` for every embedded Lua script on **every
    /// connection in the pool**.
    ///
    /// Called automatically by [`Self::connect`]. Call manually after a
    /// `SCRIPT FLUSH`, after a Redis failover that wiped the script cache,
    /// or as a periodic safety net. The operation is idempotent.
    ///
    /// # Why every connection matters
    ///
    /// On standalone Redis, `SCRIPT LOAD` populates a server-wide cache, so
    /// preloading on one connection is enough. On managed Redis services
    /// that front multiple backend nodes through a proxy
    /// (DigitalOcean Managed Redis, AWS ElastiCache Serverless, Upstash,
    /// Redis Cloud, …), each TCP connection may land on a different
    /// backend node and the script cache is **per-node**. Preloading on a
    /// single connection leaves the rest of the pool cold; the first
    /// `Queue::add()` that lands on an unprimed node still pays the
    /// `EVALSHA` → `NOSCRIPT` → `SCRIPT LOAD` → retry round-trip — which
    /// can stall for tens of seconds against managed Redis.
    ///
    /// This implementation acquires `pool.status().max_size` connections
    /// simultaneously so each pool slot ends up holding a distinct TCP
    /// connection, then runs `SCRIPT LOAD` on each in parallel.
    ///
    /// # Concurrency note
    ///
    /// While `warmup` runs it temporarily holds every pool slot, so any
    /// other operations on the same backend will queue until preload
    /// finishes. Inside [`Self::connect`] this is harmless (no other
    /// callers exist yet); when calling manually under load, expect a
    /// short stall on concurrent calls.
    pub async fn warmup(&self) -> Result<()> {
        let pool_size = self.pool.status().max_size;
        // Acquire pool_size connections **in parallel**. Each acquisition
        // includes a fresh TCP + TLS handshake on managed Redis (~250–
        // 500 ms each), so doing them sequentially turns a 16-slot pool
        // into a several-second cliff during connect(). With try_join_all,
        // every getter is polled simultaneously and deadpool mints all
        // slots concurrently.
        let conn_futures =
            (0..pool_size).map(|_| async { self.pool.get().await.map_err(crate::Error::from) });
        let conns = futures::future::try_join_all(conn_futures).await?;
        // Preload on every slot in parallel; each future owns its conn,
        // and dropping it returns the slot to the pool.
        let preloads = conns
            .into_iter()
            .map(|mut conn| async move { scripts::Scripts::preload(&mut *conn).await });
        futures::future::try_join_all(preloads).await?;
        Ok(())
    }

    fn keys(&self, queue: &str) -> KeyBuilder {
        KeyBuilder::new(&self.prefix, queue)
    }

    async fn conn(&self) -> Result<deadpool_redis::Connection> {
        Ok(self.pool.get().await?)
    }

    async fn register_queue(&self, queue: &str) -> Result<()> {
        // Cheap fast-path — once we've SADD'd a queue name from this process
        // there's no reason to re-do it on every subsequent add().
        if self.registered_queues.contains(queue) {
            return Ok(());
        }
        let key = format!("{}:queues", self.prefix);
        let mut conn = self.conn().await?;
        let _: () = conn.sadd(key, queue).await?;
        self.registered_queues.insert(queue.to_string());
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Spawn a background task that periodically PINGs every pool slot to
/// prevent the managed-Redis proxy from FIN-ing idle TCP connections.
///
/// On every tick the task acquires every pool slot in parallel and issues
/// a `PING` against each one with a `health_check_timeout` ceiling. Slots
/// that fail or time out are detached from the pool via [`deadpool::managed::Object::take`]
/// so the next user call mints a fresh connection rather than blocking on
/// a dead socket.
///
/// The task exits when `cancel` fires, which happens automatically when
/// the last [`RedisBackend`] clone is dropped (see [`KeepaliveGuard`]).
fn spawn_keepalive(
    pool: Pool,
    pool_size: usize,
    interval: Duration,
    health_check_timeout: Duration,
    cancel: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the immediate first tick — the warmup we just ran already
        // touched every slot, so no need to PING them again right away.
        tick.tick().await;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {
                    keepalive_round(&pool, pool_size, health_check_timeout).await;
                }
            }
        }
    });
}

/// One pass: attempt a bounded PING on every pool slot in parallel.
async fn keepalive_round(pool: &Pool, pool_size: usize, timeout: Duration) {
    let futs = (0..pool_size).map(|_| async {
        let conn = match pool.get().await {
            Ok(c) => c,
            Err(_) => return, // pool gone or contended; try again next tick
        };
        // Bound the PING. If the connection is stale (managed-Redis proxy
        // FIN'd it during idle), the response will never arrive — without
        // a timeout we'd block here for tens of seconds.
        let mut conn = conn;
        let cmd = redis::cmd("PING");
        let ping = cmd.query_async::<String>(&mut *conn);
        match tokio::time::timeout(timeout, ping).await {
            Ok(Ok(_)) => { /* slot is healthy; drop returns it to pool */ }
            _ => {
                // Detach the broken connection so the pool mints a fresh
                // one next time someone asks for this slot.
                let _ = PooledConn::take(conn);
                tracing::debug!("oxn keepalive: detached a stale pool slot");
            }
        }
    });
    futures::future::join_all(futs).await;
}

fn encode_removal(r: &Removal) -> String {
    match r {
        Removal::Keep => "keep".to_string(),
        Removal::Remove => "remove".to_string(),
        Removal::KeepLast(n) => format!("last:{n}"),
        Removal::KeepFor(d) => format!("for:{}", d.as_millis()),
    }
}

fn opts_to_json(opts: &ResolvedJobOptions) -> Result<String> {
    Ok(serde_json::to_string(opts)?)
}

fn opts_from_json(s: &str) -> Result<ResolvedJobOptions> {
    Ok(serde_json::from_str(s)?)
}

impl RedisBackend {
    /// Run one Lua script inside an `oxn.script` tracing span.
    ///
    /// Every state transition funnels through here, so a single
    /// `RUST_LOG=oxn=debug` (or the test kit's capture layer) shows which
    /// script ran, for which job, how long it took and what it returned.
    ///
    /// A negative integer reply is the script refusing the call; it is
    /// turned into a typed error here, in one place (see [`script_error`]).
    /// Scripts that return counts never return negatives.
    async fn eval(
        &self,
        conn: &mut PooledConn,
        script: &'static str,
        queue: &str,
        job_id: Option<&str>,
        inv: &redis::ScriptInvocation<'_>,
    ) -> Result<redis::Value> {
        let span = tracing::debug_span!(
            "oxn.script",
            script,
            queue,
            job_id = job_id.unwrap_or(""),
            lua_rc = tracing::field::Empty,
            reply = tracing::field::Empty,
            elapsed_us = tracing::field::Empty,
        );
        let start = std::time::Instant::now();
        let res: redis::RedisResult<redis::Value> =
            inv.invoke_async(&mut **conn).instrument(span.clone()).await;
        span.record("elapsed_us", start.elapsed().as_micros() as u64);
        match &res {
            Ok(redis::Value::Int(n)) => {
                span.record("lua_rc", *n);
                span.record("reply", "int");
                if *n < 0 {
                    let err = script_error(*n, script, queue, job_id);
                    span.in_scope(|| tracing::debug!(error = %err, "script rejected the call"));
                    return Err(err);
                }
            }
            Ok(v) => {
                span.record("reply", reply_kind(v));
            }
            Err(e) => {
                span.in_scope(|| tracing::warn!(error = %e, "script call failed"));
            }
        }
        Ok(res?)
    }

    async fn move_to_active(
        &self,
        queue: &str,
        worker_token: &str,
        lock_duration: Duration,
    ) -> Result<Moved> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.move_to_active.prepare_invoke();
        inv.key(keys.wait())
            .key(keys.paused())
            .key(keys.active())
            .key(keys.prioritized())
            .key(keys.delayed())
            .key(keys.events())
            .key(keys.meta())
            .key(keys.marker())
            .key(keys.job_hash_prefix())
            .key(keys.priority_counter())
            .arg(worker_token)
            .arg(lock_duration.as_millis() as i64)
            .arg(now_ms())
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        let reply = self
            .eval(&mut conn, "move_to_active", queue, None, &inv)
            .await?;
        parse_fetched(reply, worker_token)
    }
}

/// Outcome of one `move_to_active` call.
#[derive(Debug)]
enum Moved {
    /// A job was moved to `active` and locked.
    Job(Box<Fetched>),
    /// Nothing ready. `next_due` is how long until the earliest delayed
    /// job becomes eligible, if there is one.
    Empty { next_due: Option<Duration> },
}

/// Job deletions per script call for batched operations; must match
/// `OXN_BATCH` in `lua/includes/common.lua`.
const SCRIPT_BATCH: u64 = 1000;

/// Smallest block time handed to `BZPOPMIN`. A timeout of `0` means
/// "block forever" in Redis, so a configured `drain_delay` of zero (or a
/// delayed job due right now) must never reach the server as 0.
const MIN_BLOCK: Duration = Duration::from_millis(1);

/// How long a worker that found nothing to do should block: until the
/// next delayed job is due, but never longer than `drain_delay`.
fn block_time(drain_delay: Duration, next_due: Option<Duration>) -> Duration {
    let t = match next_due {
        Some(due) => drain_delay.min(due),
        None => drain_delay,
    };
    t.max(MIN_BLOCK)
}

/// Translate a script's negative return code into an [`Error`].
///
/// `-1` ("the job does not exist") becomes [`Error::NotFound`] so callers
/// and HTTP layers treat it like any other missing resource; every other
/// code becomes the matching [`crate::error::ScriptError`].
fn script_error(code: i64, script: &str, queue: &str, job_id: Option<&str>) -> Error {
    match code {
        -1 => Error::NotFound(match job_id {
            Some(id) => format!("job `{id}` in queue `{queue}`"),
            None => format!("job in queue `{queue}` ({script})"),
        }),
        other => Error::Script(crate::error::ScriptError::from_code(other)),
    }
}

fn reply_kind(v: &redis::Value) -> &'static str {
    match v {
        redis::Value::Nil => "nil",
        redis::Value::Int(_) => "int",
        redis::Value::BulkString(_) => "bulk",
        redis::Value::Array(a) if a.is_empty() => "empty-array",
        redis::Value::Array(_) => "array",
        redis::Value::SimpleString(_) => "simple",
        redis::Value::Okay => "ok",
        _ => "other",
    }
}

#[async_trait]
impl Backend for RedisBackend {
    async fn add(&self, queue: &str, insert: JobInsert) -> Result<JobId> {
        self.register_queue(queue).await?;
        let keys = self.keys(queue);
        let opts_json = opts_to_json(&insert.opts)?;
        let delay_ms = insert.opts.delay.map(|d| d.as_millis() as i64).unwrap_or(0);
        let priority = insert.opts.priority as i64;
        let dedup_id = insert
            .opts
            .deduplication
            .as_ref()
            .map(|d| d.id.clone())
            .unwrap_or_default();
        let dedup_ttl_ms = insert
            .opts
            .deduplication
            .as_ref()
            .map(|d| d.ttl.as_millis() as i64)
            .unwrap_or(0);
        let explicit_id = insert.opts.id.clone().unwrap_or_default();
        let lifo = if insert.opts.lifo { 1 } else { 0 };

        let mut inv = self.scripts.add_job.prepare_invoke();
        inv.key(keys.meta())
            .key(keys.id_counter())
            .key(keys.wait())
            .key(keys.paused())
            .key(keys.delayed())
            .key(keys.prioritized())
            .key(keys.priority_counter())
            .key(keys.events())
            .key(keys.marker())
            .key(keys.dedup(&dedup_id))
            .key(keys.job_hash_prefix())
            .arg(insert.name.as_str())
            .arg(insert.data.as_str())
            .arg(opts_json)
            .arg(insert.timestamp_ms)
            .arg(delay_ms)
            .arg(priority)
            .arg(dedup_id)
            .arg(dedup_ttl_ms)
            .arg(explicit_id.as_str())
            .arg(lifo)
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        let explicit = (!explicit_id.is_empty()).then_some(explicit_id.as_str());
        let reply = self
            .eval(&mut conn, "add_job", queue, explicit, &inv)
            .await?;
        let id: String = redis::from_redis_value(&reply)?;
        Ok(JobId::new(id))
    }

    async fn fetch_next(
        &self,
        queue: &str,
        worker_token: &str,
        lock_duration: Duration,
        block_for: Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<Fetched>> {
        if cancel.is_cancelled() {
            return Ok(None);
        }
        // First: try non-blocking move-to-active via Lua.
        let next_due = match self
            .move_to_active(queue, worker_token, lock_duration)
            .await?
        {
            Moved::Job(f) => return Ok(Some(*f)),
            Moved::Empty { next_due } => next_due,
        };
        let block_for = block_time(block_for, next_due);

        // BZPOPMIN(marker) for up to `block_for`. Crucially, this is done
        // on a **dedicated pool slot** rather than a shared multiplexed
        // connection. Redis is single-threaded, so a blocking command
        // holds the connection head-of-line — multiple workers sharing one
        // multiplexed conn would serialize on each other and every
        // non-blocking command would queue behind them. Holding one pool
        // slot for at most `block_for` is the right trade.
        let keys = self.keys(queue);
        let span = tracing::debug_span!(
            "oxn.block",
            queue,
            block_ms = block_for.as_millis() as u64,
            next_due_ms = next_due.map(|d| d.as_millis() as u64),
            woke = tracing::field::Empty,
            elapsed_us = tracing::field::Empty,
        );
        let start = std::time::Instant::now();
        let mut blocking = self.conn().await?;
        let mut bzpopmin = redis::cmd("BZPOPMIN");
        bzpopmin.arg(keys.marker()).arg(block_for.as_secs_f64());
        // Cancellation is honored here, while only *waiting*: no job has
        // been taken yet, so giving up loses nothing.
        let popped = tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            r = bzpopmin
                .query_async::<Option<(String, String, f64)>>(&mut *blocking)
                .instrument(span.clone()) => Some(r),
        };
        span.record("elapsed_us", start.elapsed().as_micros() as u64);
        let Some(popped) = popped else {
            // The connection still has a BZPOPMIN in flight; returning it
            // to the pool would make its next user wait behind the block.
            // Detach it instead (dropping it closes the socket, which also
            // unblocks the client server-side).
            span.record("woke", "cancelled");
            let _ = PooledConn::take(blocking);
            return Ok(None);
        };
        drop(blocking);
        span.record("woke", popped.ok().flatten().is_some());
        drop(span);

        // Not cancellable: if this takes a job we must hand it to the
        // caller, who releases it if shutdown began meanwhile.
        match self
            .move_to_active(queue, worker_token, lock_duration)
            .await?
        {
            Moved::Job(f) => Ok(Some(*f)),
            Moved::Empty { .. } => Ok(None),
        }
    }

    async fn delay(&self, queue: &str, id: &JobId, token: &str, until_ms: i64) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.move_to_delayed.prepare_invoke();
        inv.key(keys.active())
            .key(keys.delayed())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .key(keys.events())
            .key(keys.marker())
            .arg(id.as_str())
            .arg(token)
            .arg(until_ms)
            .arg("")
            .arg(self.max_stream_length as i64)
            .arg(1);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "move_to_delayed", queue, Some(id.as_str()), &inv)
            .await?;
        Ok(())
    }

    async fn release(&self, queue: &str, id: &JobId, token: &str) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.release.prepare_invoke();
        inv.key(keys.active())
            .key(keys.wait())
            .key(keys.paused())
            .key(keys.prioritized())
            .key(keys.priority_counter())
            .key(keys.meta())
            .key(keys.marker())
            .key(keys.events())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .arg(id.as_str())
            .arg(token)
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "release", queue, Some(id.as_str()), &inv)
            .await?;
        Ok(())
    }

    async fn complete(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        return_value_json: Option<String>,
        removal: crate::options::Removal,
    ) -> Result<()> {
        let keys = self.keys(queue);
        let remove = encode_removal(&removal);
        let mut inv = self.scripts.move_to_finished.prepare_invoke();
        inv.key(keys.active())
            .key(keys.completed())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .key(keys.events())
            .key(keys.job_hash_prefix())
            .arg(id.as_str())
            .arg(token)
            .arg("completed")
            .arg(return_value_json.unwrap_or_default())
            .arg("")
            .arg(now_ms())
            .arg(remove)
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        self.eval(
            &mut conn,
            "move_to_finished",
            queue,
            Some(id.as_str()),
            &inv,
        )
        .await?;
        Ok(())
    }

    async fn fail(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        reason: &str,
        retry_at_ms: Option<i64>,
        removal: crate::options::Removal,
    ) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;

        if let Some(ts) = retry_at_ms {
            let mut inv = self.scripts.move_to_delayed.prepare_invoke();
            inv.key(keys.active())
                .key(keys.delayed())
                .key(keys.job_hash(id.as_str()))
                .key(keys.job_lock(id.as_str()))
                .key(keys.events())
                .key(keys.marker())
                .arg(id.as_str())
                .arg(token)
                .arg(ts)
                .arg(reason)
                .arg(self.max_stream_length as i64)
                .arg(0);
            self.eval(&mut conn, "move_to_delayed", queue, Some(id.as_str()), &inv)
                .await?;
            return Ok(());
        }

        let remove = encode_removal(&removal);
        let mut inv = self.scripts.move_to_finished.prepare_invoke();
        inv.key(keys.active())
            .key(keys.failed())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .key(keys.events())
            .key(keys.job_hash_prefix())
            .arg(id.as_str())
            .arg(token)
            .arg("failed")
            .arg("")
            .arg(reason)
            .arg(now_ms())
            .arg(remove)
            .arg(self.max_stream_length as i64);
        self.eval(
            &mut conn,
            "move_to_finished",
            queue,
            Some(id.as_str()),
            &inv,
        )
        .await?;
        Ok(())
    }

    async fn extend_lock(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        duration: Duration,
    ) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.extend_lock.prepare_invoke();
        inv.key(keys.job_lock(id.as_str()))
            .arg(token)
            .arg(duration.as_millis() as i64);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "extend_lock", queue, Some(id.as_str()), &inv)
            .await?;
        Ok(())
    }

    async fn scan_stalled(&self, queue: &str, max_stalled: u32) -> Result<Vec<JobId>> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.scan_stalled.prepare_invoke();
        inv.key(keys.active())
            .key(keys.wait())
            .key(keys.stalled())
            .key(keys.stalled_check())
            .key(keys.events())
            .key(keys.job_hash_prefix())
            .key(keys.marker())
            .key(keys.paused())
            .key(keys.prioritized())
            .key(keys.priority_counter())
            .key(keys.meta())
            .key(keys.failed())
            .arg(now_ms())
            .arg(max_stalled as i64)
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        let reply = self
            .eval(&mut conn, "scan_stalled", queue, None, &inv)
            .await?;
        let moved: Vec<String> = redis::from_redis_value(&reply)?;
        Ok(moved.into_iter().map(JobId::new).collect())
    }

    async fn pause(&self, queue: &str) -> Result<()> {
        self.pause_or_resume(queue, "pause").await
    }

    async fn resume(&self, queue: &str) -> Result<()> {
        self.pause_or_resume(queue, "resume").await
    }

    async fn is_paused(&self, queue: &str) -> Result<bool> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let v: Option<String> = conn.hget(keys.meta(), "paused").await?;
        Ok(matches!(v.as_deref(), Some("1")))
    }

    async fn drain(&self, queue: &str, include_delayed: bool) -> Result<()> {
        // One batch per script call so a huge backlog never blocks Redis;
        // each batch deletes the jobs' hashes and keys, not just the ids.
        let keys = self.keys(queue);
        let mut inv = self.scripts.drain.prepare_invoke();
        inv.key(keys.wait())
            .key(keys.paused())
            .key(keys.prioritized())
            .key(keys.delayed())
            .key(keys.marker())
            .key(keys.job_hash_prefix())
            .arg(if include_delayed { 1 } else { 0 });
        let mut total = 0u64;
        loop {
            let mut conn = self.conn().await?;
            let reply = self.eval(&mut conn, "drain", queue, None, &inv).await?;
            let n: u64 = redis::from_redis_value(&reply)?;
            total += n;
            if n < SCRIPT_BATCH {
                break;
            }
        }
        self.emit(
            queue,
            &[("event", "drained"), ("count", &total.to_string())],
        )
        .await
    }

    async fn obliterate(&self, queue: &str, force: bool) -> Result<()> {
        let span = tracing::debug_span!(
            "oxn.obliterate",
            queue,
            force,
            deleted = tracing::field::Empty
        );
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        if !force {
            let active: usize = conn.llen(keys.active()).await?;
            if active > 0 {
                return Err(Error::Config(format!(
                    "obliterate: {active} active jobs — pass force=true to proceed"
                )));
            }
        }
        let pattern = keys.scan_pattern();
        let mut cursor: u64 = 0;
        let mut deleted = 0usize;
        loop {
            let (next, page): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut *conn)
                .await?;
            if !page.is_empty() {
                deleted += page.len();
                let _: () = conn.del(page).await?;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let _: () = conn.srem(format!("{}:queues", self.prefix), queue).await?;
        // Drop the in-process cache entry so a future `add()` re-SADDs the
        // queue back into the registry. Without this, the cache lies and the
        // dashboard's queue listing won't show a re-created queue.
        self.registered_queues.remove(queue);
        span.record("deleted", deleted);
        Ok(())
    }

    async fn promote(&self, queue: &str, id: &JobId) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.promote.prepare_invoke();
        inv.key(keys.delayed())
            .key(keys.wait())
            .key(keys.events())
            .key(keys.marker())
            .key(keys.job_hash(id.as_str()))
            .key(keys.paused())
            .key(keys.prioritized())
            .key(keys.priority_counter())
            .key(keys.meta())
            .arg(id.as_str())
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "promote", queue, Some(id.as_str()), &inv)
            .await?;
        Ok(())
    }

    async fn promote_all(&self, queue: &str) -> Result<u64> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.promote_all.prepare_invoke();
        inv.key(keys.delayed())
            .key(keys.wait())
            .key(keys.events())
            .key(keys.marker())
            .key(keys.meta())
            .key(keys.paused())
            .key(keys.prioritized())
            .key(keys.priority_counter())
            .key(keys.job_hash_prefix())
            .arg(self.max_stream_length as i64);
        // One batch per script call so a huge delayed set never blocks Redis.
        let mut total = 0u64;
        loop {
            let mut conn = self.conn().await?;
            let reply = self
                .eval(&mut conn, "promote_all", queue, None, &inv)
                .await?;
            let n: u64 = redis::from_redis_value(&reply)?;
            total += n;
            if n < SCRIPT_BATCH {
                return Ok(total);
            }
        }
    }

    async fn clean(&self, queue: &str, state: JobState, limit: u64) -> Result<u64> {
        let keys = self.keys(queue);
        let zset = match state {
            JobState::Completed => keys.completed(),
            JobState::Failed => keys.failed(),
            JobState::Delayed => keys.delayed(),
            JobState::Prioritized => keys.prioritized(),
            JobState::WaitingChildren => keys.waiting_children(),
            JobState::Waiting | JobState::Paused | JobState::Active => {
                return Err(Error::Config(format!(
                    "clean({state}): list-backed states cannot be cleaned via this API; \
                     use drain() or obliterate() instead"
                )));
            }
        };
        // `limit == 0` means everything; either way work in batches of at
        // most SCRIPT_BATCH so no single script call runs long.
        let mut removed = 0u64;
        loop {
            let want = if limit == 0 {
                SCRIPT_BATCH
            } else {
                (limit - removed).min(SCRIPT_BATCH)
            };
            if want == 0 {
                break;
            }
            let mut inv = self.scripts.clean.prepare_invoke();
            inv.key(&zset)
                .key(keys.events())
                .key(keys.job_hash_prefix())
                .arg(want);
            let mut conn = self.conn().await?;
            let reply = self.eval(&mut conn, "clean", queue, None, &inv).await?;
            let n: u64 = redis::from_redis_value(&reply)?;
            removed += n;
            if n < want {
                break;
            }
        }
        if removed > 0 {
            self.emit(
                queue,
                &[("event", "cleaned"), ("count", &removed.to_string())],
            )
            .await?;
        }
        Ok(removed)
    }

    async fn retry(&self, queue: &str, id: &JobId) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.retry.prepare_invoke();
        inv.key(keys.failed())
            .key(keys.wait())
            .key(keys.job_hash(id.as_str()))
            .key(keys.events())
            .key(keys.marker())
            .key(keys.paused())
            .key(keys.prioritized())
            .key(keys.priority_counter())
            .key(keys.meta())
            .arg(id.as_str())
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "retry", queue, Some(id.as_str()), &inv)
            .await?;
        Ok(())
    }

    async fn remove(&self, queue: &str, id: &JobId) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.remove.prepare_invoke();
        inv.key(keys.wait())
            .key(keys.paused())
            .key(keys.active())
            .key(keys.delayed())
            .key(keys.prioritized())
            .key(keys.completed())
            .key(keys.failed())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .key(keys.job_logs(id.as_str()))
            .key(keys.events())
            .key(keys.waiting_children())
            .key(keys.job_hash_prefix())
            .arg(id.as_str())
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "remove", queue, Some(id.as_str()), &inv)
            .await?;
        Ok(())
    }

    async fn get(&self, queue: &str, id: &JobId) -> Result<Option<RawJob>> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let map: std::collections::HashMap<String, String> =
            conn.hgetall(keys.job_hash(id.as_str())).await?;
        if map.is_empty() {
            return Ok(None);
        }
        let state = find_state(&mut *conn, &keys, id).await?;
        Ok(Some(hash_to_raw(id.clone(), map, state)?))
    }

    async fn counts(&self, queue: &str) -> Result<StateCounts> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let (wait, paused, active, delayed, prio, wchild, completed, failed): (
            u64,
            u64,
            u64,
            u64,
            u64,
            u64,
            u64,
            u64,
        ) = redis::pipe()
            .llen(keys.wait())
            .llen(keys.paused())
            .llen(keys.active())
            .zcard(keys.delayed())
            .zcard(keys.prioritized())
            .zcard(keys.waiting_children())
            .zcard(keys.completed())
            .zcard(keys.failed())
            .query_async(&mut *conn)
            .await?;
        Ok(StateCounts {
            waiting: wait,
            paused,
            active,
            delayed,
            prioritized: prio,
            waiting_children: wchild,
            completed,
            failed,
        })
    }

    async fn list(&self, queue: &str, state: JobState, range: JobRange) -> Result<Vec<RawJob>> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let ids: Vec<String> = match state {
            JobState::Waiting => list_ids(&mut *conn, keys.wait(), range).await?,
            JobState::Paused => list_ids(&mut *conn, keys.paused(), range).await?,
            JobState::Active => list_ids(&mut *conn, keys.active(), range).await?,
            JobState::Delayed => zrange(&mut *conn, keys.delayed(), range).await?,
            JobState::Prioritized => zrange(&mut *conn, keys.prioritized(), range).await?,
            JobState::WaitingChildren => zrange(&mut *conn, keys.waiting_children(), range).await?,
            JobState::Completed => zrange(&mut *conn, keys.completed(), range).await?,
            JobState::Failed => zrange(&mut *conn, keys.failed(), range).await?,
        };
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // One pipelined round trip for every hash. The state is the
        // container we listed; no per-job lookup needed.
        let mut pipe = redis::pipe();
        for id in &ids {
            pipe.hgetall(keys.job_hash(id));
        }
        let maps: Vec<std::collections::HashMap<String, String>> =
            pipe.query_async(&mut *conn).await?;
        let mut out = Vec::with_capacity(ids.len());
        for (id, map) in ids.into_iter().zip(maps) {
            // A job can finish between reading the container and its hash.
            if !map.is_empty() {
                out.push(hash_to_raw(JobId::new(id), map, Some(state))?);
            }
        }
        Ok(out)
    }

    async fn log(&self, queue: &str, id: &JobId, line: &str) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: () = conn.rpush(keys.job_logs(id.as_str()), line).await?;
        Ok(())
    }

    async fn get_logs(&self, queue: &str, id: &JobId, range: JobRange) -> Result<Vec<String>> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let v: Vec<String> = conn
            .lrange(
                keys.job_logs(id.as_str()),
                range.start as isize,
                range.end as isize,
            )
            .await?;
        Ok(v)
    }

    async fn update_progress(&self, queue: &str, id: &JobId, progress_json: &str) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: () = conn
            .hset(keys.job_hash(id.as_str()), "progress", progress_json)
            .await?;
        // also emit a progress event
        let _: () = redis::cmd("XADD")
            .arg(keys.events())
            .arg("MAXLEN")
            .arg("~")
            .arg(self.max_stream_length)
            .arg("*")
            .arg("event")
            .arg("progress")
            .arg("jobId")
            .arg(id.as_str())
            .arg("data")
            .arg(progress_json)
            .query_async(&mut *conn)
            .await?;
        Ok(())
    }

    async fn subscribe(&self, queue: &str, from: StreamStart) -> Result<RecordStream> {
        let key = self.keys(queue).events();
        // Resolve `Now` to a concrete id *here*, so events emitted between
        // this call and the first poll are not lost (0.2 read from `$`
        // lazily on every XREAD).
        let cursor = match from {
            StreamStart::Beginning => "0-0".to_string(),
            StreamStart::After(id) => id,
            StreamStart::Now => {
                let mut conn = self.conn().await?;
                let last: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
                    .arg(&key)
                    .arg("+")
                    .arg("-")
                    .arg("COUNT")
                    .arg(1)
                    .query_async(&mut *conn)
                    .await?;
                last.ids
                    .into_iter()
                    .next()
                    .map(|e| e.id)
                    .unwrap_or_else(|| "0-0".to_string())
            }
        };
        Ok(Box::pin(subscription(
            self.client.clone(),
            queue.to_string(),
            key,
            cursor,
        )))
    }

    async fn list_queues(&self) -> Result<Vec<String>> {
        let mut conn = self.conn().await?;
        let v: Vec<String> = conn.smembers(format!("{}:queues", self.prefix)).await?;
        Ok(v)
    }
}

impl RedisBackend {
    /// Append one event to the queue's stream from Rust (for operations
    /// that span several script calls and report once at the end).
    async fn emit(&self, queue: &str, fields: &[(&str, &str)]) -> Result<()> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(self.keys(queue).events())
            .arg("MAXLEN")
            .arg("~")
            .arg(self.max_stream_length)
            .arg("*");
        for (k, v) in fields {
            cmd.arg(*k).arg(*v);
        }
        let mut conn = self.conn().await?;
        let _: String = cmd.query_async(&mut *conn).await?;
        Ok(())
    }

    async fn pause_or_resume(&self, queue: &str, action: &'static str) -> Result<()> {
        let keys = self.keys(queue);
        let mut inv = self.scripts.pause.prepare_invoke();
        inv.key(keys.wait())
            .key(keys.paused())
            .key(keys.meta())
            .key(keys.events())
            .key(keys.marker())
            .arg(action)
            .arg(self.max_stream_length as i64);
        let mut conn = self.conn().await?;
        self.eval(&mut conn, "pause", queue, None, &inv).await?;
        Ok(())
    }
}

async fn zrange<C>(conn: &mut C, key: String, range: JobRange) -> Result<Vec<String>>
where
    C: redis::aio::ConnectionLike + Send,
{
    use redis::AsyncCommands;
    let v: Vec<String> = if range.descending {
        conn.zrevrange(key, range.start as isize, range.end as isize)
            .await?
    } else {
        conn.zrange(key, range.start as isize, range.end as isize)
            .await?
    };
    Ok(v)
}

async fn find_state<C>(conn: &mut C, keys: &KeyBuilder, id: &JobId) -> Result<Option<JobState>>
where
    C: redis::aio::ConnectionLike + Send,
{
    // Every container in one round trip; the first non-nil reply wins.
    const ORDER: [JobState; 8] = [
        JobState::Waiting,
        JobState::Paused,
        JobState::Active,
        JobState::Delayed,
        JobState::Prioritized,
        JobState::WaitingChildren,
        JobState::Completed,
        JobState::Failed,
    ];
    let id = id.as_str();
    let replies: Vec<redis::Value> = redis::pipe()
        .cmd("LPOS")
        .arg(keys.wait())
        .arg(id)
        .cmd("LPOS")
        .arg(keys.paused())
        .arg(id)
        .cmd("LPOS")
        .arg(keys.active())
        .arg(id)
        .zscore(keys.delayed(), id)
        .zscore(keys.prioritized(), id)
        .zscore(keys.waiting_children(), id)
        .zscore(keys.completed(), id)
        .zscore(keys.failed(), id)
        .query_async(conn)
        .await?;
    Ok(ORDER
        .into_iter()
        .zip(replies)
        .find_map(|(state, v)| (!matches!(v, redis::Value::Nil)).then_some(state)))
}

/// Ids from a list-backed container in **processing order**: jobs are
/// pushed on the left and popped from the right, so "first" (ascending)
/// means rightmost — the next job to run — and descending means newest.
async fn list_ids<C>(conn: &mut C, key: String, range: JobRange) -> Result<Vec<String>>
where
    C: redis::aio::ConnectionLike + Send,
{
    use redis::AsyncCommands;
    if range.descending {
        return Ok(conn
            .lrange(key, range.start as isize, range.end as isize)
            .await?);
    }
    let len: i64 = conn.llen(&key).await?;
    let (start, end) = (
        range.start.max(0),
        if range.end < 0 {
            len + range.end
        } else {
            range.end.min(len - 1)
        },
    );
    if len == 0 || start > end {
        return Ok(Vec::new());
    }
    let mut ids: Vec<String> = conn
        .lrange(key, (len - 1 - end) as isize, (len - 1 - start) as isize)
        .await?;
    ids.reverse();
    Ok(ids)
}

fn hash_to_raw(
    id: JobId,
    mut map: std::collections::HashMap<String, String>,
    state: Option<JobState>,
) -> Result<RawJob> {
    let take = |m: &mut std::collections::HashMap<String, String>, k: &str| m.remove(k);
    let opts = match take(&mut map, "opts") {
        Some(s) => opts_from_json(&s)?,
        None => ResolvedJobOptions::default(),
    };
    let stacktrace = take(&mut map, "stacktrace")
        .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .unwrap_or_default();
    Ok(RawJob {
        id,
        name: take(&mut map, "name").unwrap_or_default(),
        data: take(&mut map, "data").unwrap_or_default(),
        opts,
        timestamp_ms: take(&mut map, "timestamp")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        processed_on_ms: take(&mut map, "processedOn").and_then(|s| s.parse().ok()),
        finished_on_ms: take(&mut map, "finishedOn").and_then(|s| s.parse().ok()),
        attempts_made: take(&mut map, "attemptsMade")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        failed_reason: take(&mut map, "failedReason"),
        stacktrace,
        progress_json: take(&mut map, "progress"),
        return_value_json: take(&mut map, "returnvalue"),
        lock_token: take(&mut map, "lockToken"),
        state,
    })
}

fn parse_fetched(reply: redis::Value, token: &str) -> Result<Moved> {
    use redis::Value;
    let arr = match reply {
        Value::Nil => return Ok(Moved::Empty { next_due: None }),
        Value::Array(a) if a.is_empty() => return Ok(Moved::Empty { next_due: None }),
        // [ nextDueMs ] — nothing ready, a delayed job is due in that long.
        Value::Array(a) if a.len() == 1 => {
            let ms: u64 = redis::from_redis_value(&a[0])?;
            return Ok(Moved::Empty {
                next_due: Some(Duration::from_millis(ms)),
            });
        }
        Value::Array(a) => a,
        other => {
            return Err(Error::Config(format!(
                "moveToActive: unexpected reply shape: {other:?}"
            )));
        }
    };
    // Script shape: [ id, hashFieldsArray ]
    let mut it = arr.into_iter();
    let id: String = redis::from_redis_value(&it.next().unwrap_or(Value::Nil))?;
    let hash_arr: Vec<String> =
        redis::from_redis_value(&it.next().unwrap_or(Value::Array(vec![])))?;
    let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut iter = hash_arr.into_iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        map.insert(k, v);
    }
    let raw = hash_to_raw(JobId::new(id), map, Some(JobState::Active))?;
    Ok(Moved::Job(Box::new(Fetched {
        job: raw,
        token: token.to_string(),
    })))
}

/// Entries per `XREAD`; a burst is consumed in batches, not one per trip.
const XREAD_COUNT: usize = 500;
/// How long one `XREAD` blocks before looping (keeps dead peers detectable).
const XREAD_BLOCK_MS: u64 = 5_000;
/// Reconnect backoff bounds after a subscriber connection fails.
const BACKOFF_MIN: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

struct Sub {
    client: redis::Client,
    queue: String,
    key: String,
    cursor: String,
    conn: Option<redis::aio::MultiplexedConnection>,
    buf: std::collections::VecDeque<EventRecord>,
    backoff: Duration,
    failed: bool,
}

/// The event subscription: a dedicated connection (named
/// `oxn-events:{queue}` in `CLIENT LIST`) blocked in `XREAD` from `cursor`.
/// Reads whole batches; on a connection error yields one `Err`, waits with
/// exponential backoff, reconnects and continues from the same cursor, so
/// nothing is lost or repeated. Dropping the stream closes the connection.
fn subscription(
    client: redis::Client,
    queue: String,
    key: String,
    cursor: String,
) -> impl Stream<Item = Result<EventRecord>> + Send + 'static {
    let sub = Sub {
        client,
        queue,
        key,
        cursor,
        conn: None,
        buf: std::collections::VecDeque::new(),
        backoff: BACKOFF_MIN,
        failed: false,
    };
    futures::stream::unfold(sub, |mut sub| async move {
        loop {
            if let Some(rec) = sub.buf.pop_front() {
                return Some((Ok(rec), sub));
            }
            if sub.failed {
                tokio::time::sleep(sub.backoff).await;
                sub.backoff = (sub.backoff * 2).min(BACKOFF_MAX);
                sub.failed = false;
            }
            if sub.conn.is_none() {
                match connect_subscriber(&sub.client, &sub.queue).await {
                    Ok(c) => sub.conn = Some(c),
                    Err(e) => {
                        sub.failed = true;
                        return Some((Err(e), sub));
                    }
                }
            }
            let Some(conn) = sub.conn.as_mut() else {
                continue; // unreachable: connected just above
            };
            let reply: redis::RedisResult<Option<redis::streams::StreamReadReply>> =
                redis::cmd("XREAD")
                    .arg("BLOCK")
                    .arg(XREAD_BLOCK_MS)
                    .arg("COUNT")
                    .arg(XREAD_COUNT)
                    .arg("STREAMS")
                    .arg(&sub.key)
                    .arg(&sub.cursor)
                    .query_async(conn)
                    .await;
            match reply {
                Err(e) => {
                    tracing::warn!(queue = %sub.queue, error = %e, cursor = %sub.cursor,
                        "event subscription lost its connection; reconnecting");
                    sub.conn = None;
                    sub.failed = true;
                    return Some((Err(e.into()), sub));
                }
                Ok(None) => continue, // BLOCK timed out with nothing new
                Ok(Some(r)) => {
                    sub.backoff = BACKOFF_MIN;
                    for entry in r.keys.into_iter().flat_map(|k| k.ids) {
                        sub.cursor = entry.id.clone();
                        let event = parse_stream_entry(&entry.map);
                        if matches!(event, Event::Unknown) {
                            continue;
                        }
                        let at_ms = entry
                            .id
                            .split('-')
                            .next()
                            .and_then(|ms| ms.parse().ok())
                            .unwrap_or(0);
                        sub.buf.push_back(EventRecord {
                            id: entry.id,
                            at_ms,
                            event,
                        });
                    }
                }
            }
        }
    })
}

async fn connect_subscriber(
    client: &redis::Client,
    queue: &str,
) -> Result<redis::aio::MultiplexedConnection> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let name: String = format!("oxn-events:{queue}")
        .chars()
        .map(|c| if c.is_ascii_graphic() { c } else { '_' })
        .collect();
    let _: () = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg(name)
        .query_async(&mut conn)
        .await?;
    Ok(conn)
}

fn parse_stream_entry(map: &std::collections::HashMap<String, redis::Value>) -> Event {
    fn s(map: &std::collections::HashMap<String, redis::Value>, k: &str) -> Option<String> {
        let v = map.get(k)?;
        match v {
            redis::Value::BulkString(b) => String::from_utf8(b.clone()).ok(),
            redis::Value::SimpleString(s) => Some(s.clone()),
            _ => None,
        }
    }
    let event = s(map, "event").unwrap_or_default();
    let id = s(map, "jobId")
        .map(JobId::new)
        .unwrap_or_else(|| JobId::new(""));
    match event.as_str() {
        "added" => Event::Added {
            id,
            name: s(map, "name").unwrap_or_default(),
        },
        "waiting" => Event::Waiting {
            id,
            prev: s(map, "prev"),
        },
        "active" => Event::Active {
            id,
            prev: s(map, "prev"),
        },
        "completed" => Event::Completed {
            id,
            return_value_json: s(map, "returnvalue"),
        },
        "failed" => Event::Failed {
            id,
            reason: s(map, "failedReason").unwrap_or_default(),
            attempts_made: s(map, "attemptsMade")
                .and_then(|x| x.parse().ok())
                .unwrap_or(0),
        },
        "delayed" => Event::Delayed {
            id,
            due_at_ms: s(map, "delay").and_then(|x| x.parse().ok()).unwrap_or(0),
        },
        "progress" => Event::Progress {
            id,
            progress_json: s(map, "data").unwrap_or_default(),
        },
        "stalled" => Event::Stalled { id },
        "paused" => Event::Paused,
        "resumed" => Event::Resumed,
        "drained" => Event::Drained,
        "removed" => Event::Removed { id },
        "duplicated" => Event::Duplicated { id },
        "deduplicated" => Event::Deduplicated {
            id,
            dedup_id: s(map, "dedupId").unwrap_or_default(),
        },
        "cleaned" => Event::Cleaned {
            count: s(map, "count").and_then(|c| c.parse().ok()).unwrap_or(0),
        },
        _ => Event::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ScriptError;

    #[test]
    fn removal_json_matches_what_the_lua_prelude_parses() {
        // oxnPolicyFromOpts in common.lua reads these exact shapes out of
        // the job's stored opts; a serde change here would silently turn
        // every stall-failure policy into "keep".
        let j = |r: Removal| serde_json::to_value(r).unwrap();
        assert_eq!(j(Removal::Keep), serde_json::json!("Keep"));
        assert_eq!(j(Removal::Remove), serde_json::json!("Remove"));
        assert_eq!(j(Removal::KeepLast(5)), serde_json::json!({"KeepLast": 5}));
        assert_eq!(
            j(Removal::KeepFor(Duration::from_millis(1500))),
            serde_json::json!({"KeepFor": {"secs": 1, "nanos": 500_000_000}})
        );
    }

    #[test]
    fn batch_size_matches_the_lua_prelude() {
        let prelude = include_str!("lua/includes/common.lua");
        assert!(
            prelude.contains(&format!("local OXN_BATCH = {SCRIPT_BATCH}")),
            "SCRIPT_BATCH and OXN_BATCH diverged"
        );
    }

    #[test]
    fn block_time_waits_for_the_next_due_job_but_never_forever() {
        let s = Duration::from_secs;
        let ms = Duration::from_millis;
        assert_eq!(block_time(s(5), None), s(5));
        assert_eq!(block_time(s(5), Some(ms(400))), ms(400));
        assert_eq!(block_time(ms(50), Some(s(10))), ms(50));
        // BZPOPMIN 0 = block forever: never send it.
        assert_eq!(block_time(Duration::ZERO, None), MIN_BLOCK);
        assert_eq!(block_time(s(5), Some(Duration::ZERO)), MIN_BLOCK);
    }

    #[test]
    fn parse_fetched_shapes() {
        use redis::Value;
        assert!(matches!(
            parse_fetched(Value::Array(vec![]), "t").unwrap(),
            Moved::Empty { next_due: None }
        ));
        assert!(matches!(
            parse_fetched(Value::Array(vec![Value::Int(250)]), "t").unwrap(),
            Moved::Empty { next_due: Some(d) } if d == Duration::from_millis(250)
        ));
        let job = Value::Array(vec![
            Value::BulkString(b"7".to_vec()),
            Value::Array(vec![
                Value::BulkString(b"name".to_vec()),
                Value::BulkString(b"n".to_vec()),
                Value::BulkString(b"attemptsMade".to_vec()),
                Value::BulkString(b"2".to_vec()),
            ]),
        ]);
        match parse_fetched(job, "tok").unwrap() {
            Moved::Job(f) => {
                assert_eq!(f.job.id.as_str(), "7");
                assert_eq!(f.job.attempts_made, 2);
                assert_eq!(f.token, "tok");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn missing_job_maps_to_not_found_with_context() {
        match script_error(-1, "retry", "emails", Some("42")) {
            Error::NotFound(m) => assert_eq!(m, "job `42` in queue `emails`"),
            e => panic!("unexpected {e:?}"),
        }
        assert!(
            matches!(script_error(-1, "clean", "q", None), Error::NotFound(m) if m.contains("clean"))
        );
    }

    #[test]
    fn other_codes_map_to_script_errors() {
        for (code, want) in [
            (-2, ScriptError::LockNotFound),
            (-3, ScriptError::NotInExpectedState),
            (-6, ScriptError::LockMismatch),
            (-7, ScriptError::JobLocked),
            (-99, ScriptError::Unknown(-99)),
        ] {
            match script_error(code, "s", "q", Some("1")) {
                Error::Script(e) => assert_eq!(e, want, "code {code}"),
                e => panic!("code {code}: unexpected {e:?}"),
            }
        }
    }
}
