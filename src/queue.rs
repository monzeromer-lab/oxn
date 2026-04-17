//! [`Queue<D>`] — the producer side.
//!
//! A queue is cheap to clone (holds an `Arc<dyn Backend>`), typed in its
//! payload, and does not keep open connections — the backend does.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::backend::{Backend, JobInsert, JobRange, RawJob, StateCounts};
use crate::error::{Error, Result};
use crate::events::QueueEvents;
use crate::job::{Job, JobId, JobState};
use crate::options::{JobOptions, QueueOptions};

/// Typed producer handle.
///
/// `D` is the job payload type; it must be round-trippable via `serde_json`.
pub struct Queue<D> {
    name: String,
    opts: QueueOptions,
    backend: Arc<dyn Backend>,
    _data: PhantomData<fn() -> D>,
}

impl<D> fmt::Debug for Queue<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("name", &self.name)
            .field("prefix", &self.opts.prefix)
            .finish()
    }
}

impl<D> Clone for Queue<D> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            opts: self.opts.clone(),
            backend: self.backend.clone(),
            _data: PhantomData,
        }
    }
}

impl<D> Queue<D>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Start a builder.
    pub fn builder(name: impl Into<String>) -> QueueBuilder<D> {
        QueueBuilder::new(name.into())
    }

    /// Construct from an already-built backend.
    pub fn from_backend(
        name: impl Into<String>,
        backend: Arc<dyn Backend>,
        opts: QueueOptions,
    ) -> Self {
        Self {
            name: name.into(),
            opts,
            backend,
            _data: PhantomData,
        }
    }

    /// Queue name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Shared backend handle.
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }

    /// Submit a job.
    pub async fn add(&self, data: D, opts: JobOptions) -> Result<JobId> {
        let opts = merge_opts(&self.opts.default_job_options, opts);
        let insert = JobInsert {
            name: opts
                .name
                .clone()
                .unwrap_or_else(|| self.name.clone()),
            data: serde_json::to_string(&data)?,
            opts,
            timestamp_ms: now_ms(),
        };
        self.backend.add(&self.name, insert).await
    }

    /// Bulk submission. For the Redis backend this is pipelined.
    pub async fn add_bulk(&self, jobs: Vec<(D, JobOptions)>) -> Result<Vec<JobId>> {
        let inserts = jobs
            .into_iter()
            .map(|(d, opts)| {
                let opts = merge_opts(&self.opts.default_job_options, opts);
                let name = opts
                    .name
                    .clone()
                    .unwrap_or_else(|| self.name.clone());
                Ok(JobInsert {
                    name,
                    data: serde_json::to_string(&d)?,
                    opts,
                    timestamp_ms: now_ms(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.backend.add_bulk(&self.name, inserts).await
    }

    /// Stop accepting new active jobs; existing ones continue.
    pub async fn pause(&self) -> Result<()> {
        self.backend.pause(&self.name).await
    }

    /// Re-enable the queue.
    pub async fn resume(&self) -> Result<()> {
        self.backend.resume(&self.name).await
    }

    /// Paused status.
    pub async fn is_paused(&self) -> Result<bool> {
        self.backend.is_paused(&self.name).await
    }

    /// Drop all waiting/prioritized jobs. Does not touch active jobs.
    pub async fn drain(&self, include_delayed: bool) -> Result<()> {
        self.backend.drain(&self.name, include_delayed).await
    }

    /// Nuke every key for the queue. Pass `force = true` to proceed while
    /// jobs are active.
    pub async fn obliterate(&self, force: bool) -> Result<()> {
        self.backend.obliterate(&self.name, force).await
    }

    /// Promote a delayed job to `wait`.
    pub async fn promote(&self, id: &JobId) -> Result<()> {
        self.backend.promote(&self.name, id).await
    }

    /// Retry a failed job.
    pub async fn retry(&self, id: &JobId) -> Result<()> {
        self.backend.retry(&self.name, id).await
    }

    /// Remove a job.
    pub async fn remove(&self, id: &JobId) -> Result<()> {
        self.backend.remove(&self.name, id).await
    }

    /// Fetch a typed job by id.
    pub async fn get(&self, id: &JobId) -> Result<Option<Job<D>>> {
        Ok(match self.backend.get(&self.name, id).await? {
            Some(raw) => Some(decode_job(raw)?),
            None => None,
        })
    }

    /// Per-state counts.
    pub async fn counts(&self) -> Result<StateCounts> {
        self.backend.counts(&self.name).await
    }

    /// List jobs in a specific state. `range.end = -1` means "to the end".
    pub async fn list(&self, state: JobState, range: JobRange) -> Result<Vec<Job<D>>> {
        let raw = self.backend.list(&self.name, state, range).await?;
        raw.into_iter().map(decode_job).collect()
    }

    /// Attach a local event subscriber.
    pub fn events(&self) -> QueueEvents {
        QueueEvents::new(self.name.clone(), self.backend.clone())
    }
}

pub(crate) fn merge_opts(default: &JobOptions, override_: JobOptions) -> JobOptions {
    JobOptions {
        id: override_.id.or_else(|| default.id.clone()),
        name: override_.name.or_else(|| default.name.clone()),
        delay: override_.delay.or(default.delay),
        priority: if override_.priority != 0 {
            override_.priority
        } else {
            default.priority
        },
        attempts: override_.attempts.max(1),
        backoff: override_.backoff.or(default.backoff),
        deduplication: override_.deduplication.or_else(|| default.deduplication.clone()),
        remove_on_complete: override_.remove_on_complete,
        remove_on_fail: override_.remove_on_fail,
        lifo: override_.lifo,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn decode_job<D>(raw: RawJob) -> Result<Job<D>>
where
    D: DeserializeOwned,
{
    let data: D = serde_json::from_str(&raw.data)?;
    let progress = match &raw.progress_json {
        Some(s) if !s.is_empty() => serde_json::from_str(s).ok(),
        _ => None,
    };
    let return_value = match &raw.return_value_json {
        Some(s) if !s.is_empty() => serde_json::from_str(s).ok(),
        _ => None,
    };
    Ok(Job {
        id: raw.id,
        name: raw.name,
        data,
        opts: raw.opts,
        timestamp_ms: raw.timestamp_ms,
        processed_on_ms: raw.processed_on_ms,
        finished_on_ms: raw.finished_on_ms,
        attempts_made: raw.attempts_made,
        failed_reason: raw.failed_reason,
        stacktrace: raw.stacktrace,
        progress,
        return_value,
        #[cfg(feature = "flow")]
        parent: None,
        lock_token: raw.lock_token,
    })
}

/// Builder returned by [`Queue::builder`].
pub struct QueueBuilder<D> {
    name: String,
    opts: QueueOptions,
    backend: Option<Arc<dyn Backend>>,
    #[cfg(feature = "redis-backend")]
    redis_url: Option<String>,
    _data: PhantomData<fn() -> D>,
}

impl<D> fmt::Debug for QueueBuilder<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueBuilder")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<D> QueueBuilder<D>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn new(name: String) -> Self {
        Self {
            name,
            opts: QueueOptions::default(),
            backend: None,
            #[cfg(feature = "redis-backend")]
            redis_url: None,
            _data: PhantomData,
        }
    }

    #[must_use]
    pub fn options(mut self, opts: QueueOptions) -> Self {
        self.opts = opts;
        self
    }

    #[must_use]
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.opts.prefix = prefix.into();
        self
    }

    /// Use an already-constructed backend.
    #[must_use]
    pub fn backend(mut self, backend: Arc<dyn Backend>) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Shortcut for wiring up a Redis backend in one call.
    #[cfg(feature = "redis-backend")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-backend")))]
    #[must_use]
    pub fn redis(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    /// Build the queue. Establishes the backend connection if needed.
    #[allow(clippy::redundant_closure_for_method_calls)]
    pub async fn build(self) -> Result<Queue<D>> {
        let backend = match self.backend {
            Some(b) => b,
            None => {
                #[cfg(feature = "redis-backend")]
                {
                    let url = self
                        .redis_url
                        .ok_or_else(|| Error::Config("no backend specified".into()))?;
                    let cfg = crate::backend::redis::RedisConfig::new(url)
                        .prefix(self.opts.prefix.clone())
                        .max_stream_length(self.opts.max_stream_length);
                    let backend = crate::backend::redis::RedisBackend::connect(cfg).await?;
                    Arc::new(backend) as Arc<dyn Backend>
                }
                #[cfg(not(feature = "redis-backend"))]
                {
                    return Err(Error::Config(
                        "no backend specified and no default compiled".into(),
                    ));
                }
            }
        };
        Ok(Queue::from_backend(self.name, backend, self.opts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Backoff;
    use std::time::Duration;

    #[test]
    fn merge_prefers_override_but_falls_back() {
        let default = JobOptions::new()
            .attempts(2)
            .priority(1)
            .backoff(Backoff::ONE_SECOND);
        let merged = merge_opts(
            &default,
            JobOptions::new()
                .id("abc")
                .delay(Duration::from_millis(300))
                .attempts(5),
        );
        assert_eq!(merged.id.as_deref(), Some("abc"));
        assert_eq!(merged.attempts, 5);
        assert_eq!(merged.delay, Some(Duration::from_millis(300)));
        // override_ priority 0 should fall back to default's 1
        assert_eq!(merged.priority, 1);
        // backoff should inherit
        assert!(merged.backoff.is_some());
    }

    #[test]
    fn merge_respects_override_priority_when_nonzero() {
        let default = JobOptions::new().priority(1);
        let merged = merge_opts(&default, JobOptions::new().priority(9));
        assert_eq!(merged.priority, 9);
    }

    #[test]
    fn merge_inherits_dedup_when_override_is_none() {
        let default =
            JobOptions::new().deduplicate("abc", std::time::Duration::from_secs(10));
        let merged = merge_opts(&default, JobOptions::new());
        let d = merged.deduplication.unwrap();
        assert_eq!(d.id, "abc");
    }
}
