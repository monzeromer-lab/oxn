//! Redis backend.
//!
//! Uses [`redis-rs`](https://docs.rs/redis) via [`deadpool-redis`] for a
//! pooled async client, plus a dedicated connection for blocking reads (so
//! `BZPOPMIN` cannot starve the pool). All state transitions are Lua scripts
//! registered once (content-addressed via `EVALSHA`).

mod keys;
mod scripts;

pub use keys::KeyBuilder;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use deadpool_redis::{Config as PoolConfig, Pool, Runtime};
use futures::stream::{Stream, StreamExt};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};

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
    pub url: String,
    pub prefix: String,
    /// Max pool size for non-blocking ops. Blocking fetches use a separate
    /// dedicated connection.
    pub pool_size: usize,
    pub max_stream_length: usize,
}

impl RedisConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: "oxn".to_string(),
            pool_size: 16,
            max_stream_length: 10_000,
        }
    }

    #[must_use]
    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }

    #[must_use]
    pub fn pool_size(mut self, n: usize) -> Self {
        self.pool_size = n.max(1);
        self
    }

    #[must_use]
    pub fn max_stream_length(mut self, n: usize) -> Self {
        self.max_stream_length = n;
        self
    }
}

/// Redis-backed storage implementation.
#[derive(Clone)]
pub struct RedisBackend {
    pool: Pool,
    blocking: ConnectionManager,
    prefix: String,
    max_stream_length: usize,
    scripts: Arc<scripts::Scripts>,
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
    pub async fn connect(cfg: RedisConfig) -> Result<Self> {
        let pool_cfg = PoolConfig::from_url(cfg.url.clone());
        let pool = pool_cfg.create_pool(Some(Runtime::Tokio1))?;
        // Dedicated connection for blocking pops.
        let client = Client::open(cfg.url)?;
        let blocking = client.get_connection_manager().await?;
        Ok(Self {
            pool,
            blocking,
            prefix: cfg.prefix,
            max_stream_length: cfg.max_stream_length,
            scripts: Arc::new(scripts::Scripts::new()),
        })
    }

    fn keys(&self, queue: &str) -> KeyBuilder {
        KeyBuilder::new(&self.prefix, queue)
    }

    async fn conn(&self) -> Result<deadpool_redis::Connection> {
        Ok(self.pool.get().await?)
    }

    async fn register_queue(&self, queue: &str) -> Result<()> {
        let key = format!("{}:queues", self.prefix);
        let mut conn = self.conn().await?;
        let _: () = conn.sadd(key, queue).await?;
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

        // Otherwise: block on BZPOPMIN(marker) for up to `block_for`, then
        // retry the Lua once.
        let mut blocking = self.blocking.clone();
        let secs = block_for.as_secs_f64().max(0.0);
        let _: Option<(String, String, f64)> = redis::cmd("BZPOPMIN")
            .arg(keys.marker())
            .arg(secs)
            .query_async(&mut blocking)
            .await
            .ok()
            .flatten();

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
    ) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let opts = match self.get(queue, id).await? {
            Some(j) => j.opts,
            None => return Err(Error::NotFound(id.to_string())),
        };
        let remove = encode_removal(&opts.remove_on_complete);
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
    ) -> Result<()> {
        let keys = self.keys(queue);
        let mut conn = self.conn().await?;
        let opts = match self.get(queue, id).await? {
            Some(j) => j.opts,
            None => return Err(Error::NotFound(id.to_string())),
        };

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

        let remove = encode_removal(&opts.remove_on_fail);
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
        _ => Event::Unknown,
    }
}
