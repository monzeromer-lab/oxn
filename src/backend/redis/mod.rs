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

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use deadpool_redis::{Config as PoolConfig, Connection as PooledConn, Pool, Runtime};
use futures::stream::{Stream, StreamExt};
use redis::AsyncCommands;

use crate::backend::{
    Backend, EventStream, Fetched, JobInsert, JobRange, RawJob, StateCounts,
};
use crate::error::{Error, Result};
use crate::events::Event;
use crate::job::{JobId, JobState};
use crate::options::{JobOptions, Removal};

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

        let backend = Self {
            pool,
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
        let conn_futures = (0..pool_size).map(|_| async {
            self.pool.get().await.map_err(crate::Error::from)
        });
        let conns = futures::future::try_join_all(conn_futures).await?;
        // Preload on every slot in parallel; each future owns its conn,
        // and dropping it returns the slot to the pool.
        let preloads = conns.into_iter().map(|mut conn| async move {
            scripts::Scripts::preload(&mut *conn).await
        });
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

fn opts_to_json(opts: &JobOptions) -> Result<String> {
    Ok(serde_json::to_string(opts)?)
}

fn opts_from_json(s: &str) -> Result<JobOptions> {
    Ok(serde_json::from_str(s)?)
}

#[async_trait]
impl Backend for RedisBackend {
    async fn add(&self, queue: &str, insert: JobInsert) -> Result<JobId> {
        self.register_queue(queue).await?;
        let keys = self.keys(queue);
        let opts_json = opts_to_json(&insert.opts)?;
        let delay_ms = insert
            .opts
            .delay
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let priority = insert.opts.priority as i64;
        let dedup_id = insert.opts.deduplication.as_ref().map(|d| d.id.clone()).unwrap_or_default();
        let dedup_ttl_ms = insert
            .opts
            .deduplication
            .as_ref()
            .map(|d| d.ttl.as_millis() as i64)
            .unwrap_or(0);
        let explicit_id = insert.opts.id.clone().unwrap_or_default();
        let lifo = if insert.opts.lifo { 1 } else { 0 };

        let mut conn = self.conn().await?;
        let reply: redis::Value = self
            .scripts
            .add_job
            .key(keys.meta())
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
            .arg(explicit_id)
            .arg(lifo)
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
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
    ) -> Result<Option<Fetched>> {
        let keys = self.keys(queue);
        // First: try non-blocking move-to-active via Lua.
        let mut conn = self.conn().await?;
        let reply: redis::Value = self
            .scripts
            .move_to_active
            .key(keys.wait())
            .key(keys.paused())
            .key(keys.active())
            .key(keys.prioritized())
            .key(keys.delayed())
            .key(keys.events())
            .key(keys.meta())
            .key(keys.marker())
            .key(keys.job_hash_prefix())
            .arg(worker_token)
            .arg(lock_duration.as_millis() as i64)
            .arg(now_ms())
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;

        if let Some(fetched) = parse_fetched(reply, worker_token)? {
            return Ok(Some(fetched));
        }
        // Drop the slot before BZPOPMIN; we'll grab a fresh one for the
        // blocking call so the second EVALSHA below isn't competing with
        // ourselves for the same in-use connection.
        drop(conn);

        // BZPOPMIN(marker) for up to `block_for`. Crucially, this is done
        // on a **dedicated pool slot** rather than the shared
        // [`ConnectionManager`]. Redis is single-threaded, so a blocking
        // command holds the connection head-of-line — multiple workers
        // sharing one multiplexed conn would serialize on each other and
        // every non-blocking command would queue behind them. Holding one
        // pool slot for at most `block_for` is the right trade.
        let mut blocking = self.conn().await?;
        let secs = block_for.as_secs_f64().max(0.0);
        let _: Option<(String, String, f64)> = redis::cmd("BZPOPMIN")
            .arg(keys.marker())
            .arg(secs)
            .query_async(&mut *blocking)
            .await
            .ok()
            .flatten();
        drop(blocking);

        let mut conn = self.conn().await?;
        let reply: redis::Value = self
            .scripts
            .move_to_active
            .key(keys.wait())
            .key(keys.paused())
            .key(keys.active())
            .key(keys.prioritized())
            .key(keys.delayed())
            .key(keys.events())
            .key(keys.meta())
            .key(keys.marker())
            .key(keys.job_hash_prefix())
            .arg(worker_token)
            .arg(lock_duration.as_millis() as i64)
            .arg(now_ms())
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        parse_fetched(reply, worker_token)
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
        let mut conn = self.conn().await?;
        let remove = encode_removal(&removal);
        let _: redis::Value = self
            .scripts
            .move_to_finished
            .key(keys.active())
            .key(keys.completed())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .key(keys.events())
            .arg(id.as_str())
            .arg(token)
            .arg("completed")
            .arg(return_value_json.unwrap_or_default())
            .arg("")
            .arg(now_ms())
            .arg(remove)
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
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
            let _: redis::Value = self
                .scripts
                .move_to_delayed
                .key(keys.active())
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
                .invoke_async(&mut *conn)
                .await?;
            return Ok(());
        }

        let remove = encode_removal(&removal);
        let _: redis::Value = self
            .scripts
            .move_to_finished
            .key(keys.active())
            .key(keys.failed())
            .key(keys.job_hash(id.as_str()))
            .key(keys.job_lock(id.as_str()))
            .key(keys.events())
            .arg(id.as_str())
            .arg(token)
            .arg("failed")
            .arg("")
            .arg(reason)
            .arg(now_ms())
            .arg(remove)
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
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
        let mut conn = self.conn().await?;
        let reply: i64 = self
            .scripts
            .extend_lock
            .key(keys.job_lock(id.as_str()))
            .arg(token)
            .arg(duration.as_millis() as i64)
            .invoke_async(&mut *conn)
            .await?;
        if reply < 0 {
            return Err(Error::Script(crate::error::ScriptError::from_code(reply)));
        }
        Ok(())
    }

    async fn scan_stalled(&self, queue: &str, max_stalled: u32) -> Result<Vec<JobId>> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let moved: Vec<String> = self
            .scripts
            .scan_stalled
            .key(keys.active())
            .key(keys.wait())
            .key(keys.stalled())
            .key(keys.stalled_check())
            .key(keys.events())
            .key(keys.job_hash_prefix())
            .arg(now_ms())
            .arg(max_stalled as i64)
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(moved.into_iter().map(JobId::new).collect())
    }

    async fn pause(&self, queue: &str) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: redis::Value = self
            .scripts
            .pause
            .key(keys.wait())
            .key(keys.paused())
            .key(keys.meta())
            .key(keys.events())
            .arg("pause")
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(())
    }

    async fn resume(&self, queue: &str) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: redis::Value = self
            .scripts
            .pause
            .key(keys.wait())
            .key(keys.paused())
            .key(keys.meta())
            .key(keys.events())
            .arg("resume")
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(())
    }

    async fn is_paused(&self, queue: &str) -> Result<bool> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let v: Option<String> = conn.hget(keys.meta(), "paused").await?;
        Ok(matches!(v.as_deref(), Some("1")))
    }

    async fn drain(&self, queue: &str, include_delayed: bool) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .del(keys.wait())
            .del(keys.paused())
            .del(keys.prioritized());
        if include_delayed {
            pipe.del(keys.delayed());
        }
        let _: () = pipe.query_async(&mut *conn).await?;
        Ok(())
    }

    async fn obliterate(&self, queue: &str, force: bool) -> Result<()> {
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
        Ok(())
    }

    async fn promote(&self, queue: &str, id: &JobId) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: redis::Value = self
            .scripts
            .promote
            .key(keys.delayed())
            .key(keys.wait())
            .key(keys.events())
            .key(keys.marker())
            .arg(id.as_str())
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(())
    }

    async fn promote_all(&self, queue: &str) -> Result<u64> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let moved: i64 = self
            .scripts
            .promote_all
            .key(keys.delayed())
            .key(keys.wait())
            .key(keys.events())
            .key(keys.marker())
            .key(keys.meta())
            .key(keys.paused())
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(moved.max(0) as u64)
    }

    async fn clean(
        &self,
        queue: &str,
        state: JobState,
        limit: u64,
    ) -> Result<u64> {
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
                )))
            }
        };
        let mut conn = self.conn().await?;
        let removed: i64 = self
            .scripts
            .clean
            .key(zset)
            .key(keys.events())
            .key(keys.job_hash_prefix())
            .arg(limit as i64)
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(removed.max(0) as u64)
    }

    async fn retry(&self, queue: &str, id: &JobId) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: redis::Value = self
            .scripts
            .retry
            .key(keys.failed())
            .key(keys.wait())
            .key(keys.job_hash(id.as_str()))
            .key(keys.events())
            .key(keys.marker())
            .arg(id.as_str())
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
            .await?;
        Ok(())
    }

    async fn remove(&self, queue: &str, id: &JobId) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let _: redis::Value = self
            .scripts
            .remove
            .key(keys.wait())
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
            .arg(id.as_str())
            .arg(self.max_stream_length as i64)
            .invoke_async(&mut *conn)
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

    async fn list(
        &self,
        queue: &str,
        state: JobState,
        range: JobRange,
    ) -> Result<Vec<RawJob>> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let start = range.start as isize;
        let end = range.end as isize;
        let ids: Vec<String> = match state {
            JobState::Waiting => {
                if range.descending {
                    let n: isize = conn.llen(keys.wait()).await?;
                    let s = (n - end - 1).max(0);
                    let e = n - start - 1;
                    conn.lrange(keys.wait(), s, e).await?
                } else {
                    conn.lrange(keys.wait(), start, end).await?
                }
            }
            JobState::Paused => conn.lrange(keys.paused(), start, end).await?,
            JobState::Active => conn.lrange(keys.active(), start, end).await?,
            JobState::Delayed => zrange(&mut *conn, keys.delayed(), range).await?,
            JobState::Prioritized => zrange(&mut *conn, keys.prioritized(), range).await?,
            JobState::WaitingChildren => zrange(&mut *conn, keys.waiting_children(), range).await?,
            JobState::Completed => zrange(&mut *conn, keys.completed(), range).await?,
            JobState::Failed => zrange(&mut *conn, keys.failed(), range).await?,
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(raw) = self.get(queue, &JobId::new(id)).await? {
                out.push(raw);
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

    async fn update_progress(
        &self,
        queue: &str,
        id: &JobId,
        progress_json: &str,
    ) -> Result<()> {
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

    async fn subscribe(&self, queue: &str) -> Result<EventStream> {
        Ok(self.subscribe_impl(queue.to_string()).boxed())
    }

    async fn list_queues(&self) -> Result<Vec<String>> {
        let mut conn = self.conn().await?;
        let v: Vec<String> = conn.smembers(format!("{}:queues", self.prefix)).await?;
        Ok(v)
    }
}

async fn zrange<C>(conn: &mut C, key: String, range: JobRange) -> Result<Vec<String>>
where
    C: redis::aio::ConnectionLike + Send,
{
    use redis::AsyncCommands;
    let v: Vec<String> = if range.descending {
        conn.zrevrange(key, range.start as isize, range.end as isize).await?
    } else {
        conn.zrange(key, range.start as isize, range.end as isize).await?
    };
    Ok(v)
}

async fn find_state<C>(conn: &mut C, keys: &KeyBuilder, id: &JobId) -> Result<Option<JobState>>
where
    C: redis::aio::ConnectionLike + Send,
{
    use redis::AsyncCommands;
    macro_rules! lmember {
        ($k:expr) => {{
            let v: i64 = conn
                .lpos::<_, _, Option<i64>>($k, id.as_str(), redis::LposOptions::default())
                .await?
                .unwrap_or(-1);
            v >= 0
        }};
    }
    if lmember!(keys.wait()) {
        return Ok(Some(JobState::Waiting));
    }
    if lmember!(keys.paused()) {
        return Ok(Some(JobState::Paused));
    }
    if lmember!(keys.active()) {
        return Ok(Some(JobState::Active));
    }
    if conn
        .zscore::<_, _, Option<f64>>(keys.delayed(), id.as_str())
        .await?
        .is_some()
    {
        return Ok(Some(JobState::Delayed));
    }
    if conn
        .zscore::<_, _, Option<f64>>(keys.prioritized(), id.as_str())
        .await?
        .is_some()
    {
        return Ok(Some(JobState::Prioritized));
    }
    if conn
        .zscore::<_, _, Option<f64>>(keys.waiting_children(), id.as_str())
        .await?
        .is_some()
    {
        return Ok(Some(JobState::WaitingChildren));
    }
    if conn
        .zscore::<_, _, Option<f64>>(keys.completed(), id.as_str())
        .await?
        .is_some()
    {
        return Ok(Some(JobState::Completed));
    }
    if conn
        .zscore::<_, _, Option<f64>>(keys.failed(), id.as_str())
        .await?
        .is_some()
    {
        return Ok(Some(JobState::Failed));
    }
    Ok(None)
}

fn hash_to_raw(
    id: JobId,
    mut map: std::collections::HashMap<String, String>,
    state: Option<JobState>,
) -> Result<RawJob> {
    let take = |m: &mut std::collections::HashMap<String, String>, k: &str| m.remove(k);
    let opts = match take(&mut map, "opts") {
        Some(s) => opts_from_json(&s)?,
        None => JobOptions::default(),
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

fn parse_fetched(reply: redis::Value, token: &str) -> Result<Option<Fetched>> {
    use redis::Value;
    let arr = match reply {
        Value::Nil => return Ok(None),
        Value::Array(a) if a.is_empty() => return Ok(None),
        Value::Array(a) => a,
        other => {
            return Err(Error::Config(format!(
                "moveToActive: unexpected reply shape: {other:?}"
            )))
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
    Ok(Some(Fetched {
        job: raw,
        token: token.to_string(),
    }))
}

impl RedisBackend {
    fn subscribe_impl(&self, queue: String) -> impl Stream<Item = Result<Event>> + Send + 'static {
        use futures::stream;
        let backend = self.clone();
        let keys = self.keys(&queue);
        let events_key = keys.events();
        stream::unfold(
            (backend, events_key, "$".to_string()),
            move |(backend, key, last_id)| async move {
                let mut conn = match backend.conn().await {
                    Ok(c) => c,
                    Err(e) => return Some((Err(e), (backend, key, last_id))),
                };
                let reply: redis::streams::StreamReadReply = match redis::cmd("XREAD")
                    .arg("BLOCK")
                    .arg(5000)
                    .arg("COUNT")
                    .arg(100)
                    .arg("STREAMS")
                    .arg(&key)
                    .arg(&last_id)
                    .query_async(&mut *conn)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => return Some((Err(e.into()), (backend, key, last_id))),
                };
                let mut last = last_id.clone();
                if let Some(s) = reply.keys.first() {
                    if let Some(e) = s.ids.first() {
                        last = e.id.clone();
                        let evt = parse_stream_entry(&e.map);
                        return Some((Ok(evt), (backend, key, last)));
                    }
                }
                Some((Ok(Event::Unknown), (backend, key, last)))
            },
        )
        .filter(|r| {
            let keep = !matches!(r, Ok(Event::Unknown));
            futures::future::ready(keep)
        })
    }
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
    let id = s(map, "jobId").map(JobId::new).unwrap_or_else(|| JobId::new(""));
    match event.as_str() {
        "added" => Event::Added {
            id,
            name: s(map, "name").unwrap_or_default(),
        },
        "waiting" => Event::Waiting { id },
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
            attempts_made: s(map, "attemptsMade").and_then(|x| x.parse().ok()).unwrap_or(0),
        },
        "delayed" => Event::Delayed {
            id,
            delay_ms: s(map, "delay").and_then(|x| x.parse().ok()).unwrap_or(0),
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
        "duplicated" => Event::Duplicated {
            id,
            dedup_id: s(map, "dedupId").unwrap_or_default(),
        },
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
