//! [`Queue<D>`] — the producer side.
//!
//! A queue is cheap to clone (holds an `Arc<dyn Backend>`), typed in its
//! payload, and does not keep open connections — the backend does.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::Instrument;

use crate::backend::{Backend, JobInsert, JobRange, RawJob, StateCounts};
use crate::error::{Error, Result};
use crate::events::QueueEvents;
use crate::job::{Job, JobId, JobState};
use crate::options::{JobOptions, QueueOptions, Removal};

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
    /// Start building a queue.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxn::Queue;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Email;
    /// # async fn demo() -> oxn::Result<()> {
    /// let q: Queue<Email> = Queue::builder("emails")
    ///     .redis("redis://127.0.0.1:6379")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn builder(name: impl Into<String>) -> QueueBuilder<D> {
        QueueBuilder::new(name.into())
    }

    /// Construct from an already-built backend.
    ///
    /// Use this when you want to share a single backend (and its pool)
    /// across multiple queues, or when plugging in a custom backend.
    ///
    /// Fails with [`Error::Config`] if `name` is not a valid queue name
    /// (see [`crate::validate::queue_name`]).
    pub fn from_backend(
        name: impl Into<String>,
        backend: Arc<dyn Backend>,
        opts: QueueOptions,
    ) -> Result<Self> {
        let name = name.into();
        crate::validate::queue_name(&name)?;
        Ok(Self {
            name,
            opts,
            backend,
            _data: PhantomData,
        })
    }

    /// Name the queue was constructed with.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Shared backend handle. Clone this to build additional queues (of
    /// possibly different types) against the same connection pool.
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }

    /// Submit a job.
    ///
    /// Returns the assigned [`JobId`]. If the job was deduplicated against
    /// an existing id (see [`JobOptions::deduplicate`]), the existing id is
    /// returned instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use oxn::{Queue, JobOptions};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Email { to: String }
    /// # async fn demo(q: Queue<Email>) -> oxn::Result<()> {
    /// let id = q
    ///     .add(
    ///         Email { to: "alice@example.com".into() },
    ///         JobOptions::new()
    ///             .attempts(3)
    ///             .delay(Duration::from_secs(2)),
    ///     )
    ///     .await?;
    /// println!("queued job {id}");
    /// # Ok(()) }
    /// ```
    pub async fn add(&self, data: D, opts: JobOptions) -> Result<JobId> {
        let opts = opts.resolve_with(&self.opts.default_job_options);
        if let Some(id) = &opts.id {
            crate::validate::job_id(id)?;
        }
        let insert = JobInsert {
            name: opts.name.clone().unwrap_or_else(|| self.name.clone()),
            data: serde_json::to_string(&data)?,
            opts,
            timestamp_ms: now_ms(),
        };
        let span =
            tracing::debug_span!("oxn.add", queue = %self.name, job_id = tracing::field::Empty);
        let id = self
            .backend
            .add(&self.name, insert)
            .instrument(span.clone())
            .await?;
        span.record("job_id", id.as_str());
        Ok(id)
    }

    /// Bulk submission. Every job is validated first; then the Redis
    /// backend adds them with one script call per chunk of up to 1,000
    /// jobs. Each chunk is applied without other clients interleaving
    /// (Redis scripts are isolated, not rolled back); ids come back in
    /// submission order. Other backends may fall back to sequential `add`s.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxn::{Queue, JobOptions};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Task(u32);
    /// # async fn demo(q: Queue<Task>) -> oxn::Result<()> {
    /// let ids = q
    ///     .add_bulk((0..100).map(|i| (Task(i), JobOptions::new())).collect())
    ///     .await?;
    /// assert_eq!(ids.len(), 100);
    /// # Ok(()) }
    /// ```
    pub async fn add_bulk(&self, jobs: Vec<(D, JobOptions)>) -> Result<Vec<JobId>> {
        let inserts = jobs
            .into_iter()
            .map(|(d, opts)| {
                let opts = opts.resolve_with(&self.opts.default_job_options);
                if let Some(id) = &opts.id {
                    crate::validate::job_id(id)?;
                }
                let name = opts.name.clone().unwrap_or_else(|| self.name.clone());
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
    ///
    /// Errors with [`Error::NotFound`] for an unknown id and
    /// [`Error::Script`]`(NotInExpectedState)` if the job isn't delayed.
    pub async fn promote(&self, id: &JobId) -> Result<()> {
        self.backend.promote(&self.name, id).await
    }

    /// Retry a failed job.
    ///
    /// Errors with [`Error::NotFound`] for an unknown id and
    /// [`Error::Script`]`(NotInExpectedState)` if the job isn't failed.
    pub async fn retry(&self, id: &JobId) -> Result<()> {
        self.backend.retry(&self.name, id).await
    }

    /// Remove a job and everything attached to it.
    ///
    /// Errors with [`Error::NotFound`] for an unknown id and
    /// [`Error::Script`]`(JobLocked)` while a worker is processing it.
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
    ///
    /// Ascending ranges are in processing order for `Waiting` / `Paused` /
    /// `Active` (next job to run first) and ascending score for the zsets
    /// (next due, highest priority, oldest finished first); set
    /// `descending` for newest-first.
    pub async fn list(&self, state: JobState, range: JobRange) -> Result<Vec<Job<D>>> {
        let raw = self.backend.list(&self.name, state, range).await?;
        raw.into_iter().map(decode_job).collect()
    }

    /// Attach a local event subscriber.
    pub fn events(&self) -> QueueEvents {
        QueueEvents::new(self.name.clone(), self.backend.clone())
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

    /// Override all [`QueueOptions`] at once.
    #[must_use]
    pub fn options(mut self, opts: QueueOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Set the Redis key prefix (default `"oxn"`).
    #[must_use]
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.opts.prefix = prefix.into();
        self
    }

    /// Override the queue-wide default job options.
    ///
    /// Each individual `Queue::add` call can still override these per-job;
    /// any field not explicitly set falls back to this default.
    #[must_use]
    pub fn default_job_options(mut self, opts: JobOptions) -> Self {
        self.opts.default_job_options = opts;
        self
    }

    /// Configure the default [`Removal`] policy applied to every job in
    /// this queue when it completes successfully.
    ///
    /// Pass [`Removal::Remove`] to drop completed jobs as soon as they
    /// finish — useful for keeping Redis tidy when you don't need the
    /// completion record.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxn::{Queue, options::Removal};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Email;
    /// # async fn demo() -> oxn::Result<()> {
    /// let q: Queue<Email> = Queue::builder("emails")
    ///     .redis("redis://127.0.0.1:6379")
    ///     .remove_on_complete(Removal::Remove)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn remove_on_complete(mut self, r: Removal) -> Self {
        self.opts.default_job_options.remove_on_complete = Some(r);
        self
    }

    /// Configure the default [`Removal`] policy applied to every job in
    /// this queue when it fails permanently.
    #[must_use]
    pub fn remove_on_fail(mut self, r: Removal) -> Self {
        self.opts.default_job_options.remove_on_fail = Some(r);
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
        crate::validate::queue_name(&self.name)?;
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
        Queue::from_backend(self.name, backend, self.opts)
    }
}
