//! Storage backends.
//!
//! The [`Backend`] trait is the pluggable point: every operation the queue,
//! worker, events subsystem and dashboard need goes through it. A
//! A `RedisBackend` (under `backend::redis`, enabled by the
//! `redis-backend` feature) ships out of the box, and the trait is
//! object-safe so downstream crates can slot in alternative stores
//! (in-memory, SQL) without a source change.
//!
//! The trait speaks in **erased** job payloads (`data` is a raw JSON string).
//! Typed [`Job<D>`](crate::Job) is reconstructed by [`Queue`](crate::Queue)
//! and [`Worker`](crate::Worker) via `serde_json`.

use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::events::Event;
use crate::job::{JobId, JobState};
use crate::options::JobOptions;

#[cfg(feature = "redis-backend")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-backend")))]
pub mod redis;

/// Erased job record as it's exchanged with the backend.
///
/// `Queue<D>` converts this into a typed [`Job<D>`](crate::Job) on the way
/// out. Implementations of [`Backend`] construct these directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawJob {
    /// Job id.
    pub id: JobId,
    /// Job's logical name.
    pub name: String,
    /// JSON-encoded payload.
    pub data: String,
    /// Options originally used to submit the job.
    pub opts: JobOptions,
    /// Epoch-ms of the initial enqueue.
    pub timestamp_ms: i64,
    /// Epoch-ms of the most recent `move_to_active`, if any.
    pub processed_on_ms: Option<i64>,
    /// Epoch-ms of completion or terminal failure, if finished.
    pub finished_on_ms: Option<i64>,
    /// Attempts already made (1-based after the first fetch).
    pub attempts_made: u32,
    /// Most recent failure reason.
    pub failed_reason: Option<String>,
    /// Stack trace strings, oldest first (user-appended).
    pub stacktrace: Vec<String>,
    /// JSON-encoded progress value.
    pub progress_json: Option<String>,
    /// JSON-encoded return value.
    pub return_value_json: Option<String>,
    /// Current lock token, if held.
    pub lock_token: Option<String>,
    /// Resolved state, when the backend knows it (e.g. on `get`).
    pub state: Option<JobState>,
}

/// Input for a new job submission.
///
/// Constructed by [`Queue::add`](crate::Queue::add) and friends before
/// handing off to a [`Backend`] implementation.
#[derive(Debug, Clone)]
pub struct JobInsert {
    /// Job name used for named-handler routing.
    pub name: String,
    /// JSON-encoded payload.
    pub data: String,
    /// Options (priority, delay, attempts, …).
    pub opts: JobOptions,
    /// Epoch-ms at submission.
    pub timestamp_ms: i64,
}

/// A fetched job plus the lock token that authorizes operations on it.
///
/// The worker must pass `token` to subsequent `complete`/`fail`/`extend_lock`
/// calls — tokens are the only authorization on a job's state transitions.
#[derive(Debug, Clone)]
pub struct Fetched {
    /// The job record as it lives in storage.
    pub job: RawJob,
    /// Lock token this fetch acquired.
    pub token: String,
}

/// Counts of jobs per state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateCounts {
    /// Jobs in the `wait` list.
    pub waiting: u64,
    /// Jobs in the `paused` list.
    pub paused: u64,
    /// Jobs currently held in `active` by a worker.
    pub active: u64,
    /// Jobs scheduled for a future timestamp.
    pub delayed: u64,
    /// Jobs in the prioritized zset.
    pub prioritized: u64,
    /// Parent jobs blocked on pending child dependencies.
    pub waiting_children: u64,
    /// Completed jobs retained per `Removal` policy.
    pub completed: u64,
    /// Permanently-failed jobs retained per `Removal` policy.
    pub failed: u64,
}

/// A half-open range into one of the job state lists/zsets.
///
/// Follows Redis `LRANGE`/`ZRANGE` semantics: `start` and `end` are
/// inclusive, and `-1` means "the last element".
#[derive(Debug, Clone, Copy)]
pub struct JobRange {
    /// Zero-based start index (inclusive).
    pub start: i64,
    /// End index (inclusive). Use `-1` for "to the end".
    pub end: i64,
    /// If true, iterate newest-first; else oldest-first.
    pub descending: bool,
}

impl JobRange {
    /// First `n` entries, oldest-first.
    pub const fn first(n: i64) -> Self {
        Self {
            start: 0,
            end: n - 1,
            descending: false,
        }
    }
    /// Last `n` entries, newest-first.
    pub const fn last(n: i64) -> Self {
        Self {
            start: 0,
            end: n - 1,
            descending: true,
        }
    }
}

/// Event stream type alias to avoid `Pin<Box<...>>` noise at call sites.
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event>> + Send + 'static>>;

/// Pluggable storage backend.
///
/// All operations are `async` and fail with [`crate::Error`]. A single
/// `Backend` instance is shared across a `Queue`, any number of `Worker`s
/// and a `QueueEvents` via `Arc<dyn Backend>` — construct it once and clone
/// the `Arc`.
#[async_trait]
pub trait Backend: Send + Sync + fmt::Debug + 'static {
    /// Submit a new job. Returns the assigned id.
    async fn add(&self, queue: &str, insert: JobInsert) -> Result<JobId>;

    /// Submit a batch of jobs atomically where the backend supports it.
    async fn add_bulk(&self, queue: &str, inserts: Vec<JobInsert>) -> Result<Vec<JobId>> {
        // Default: sequential. Redis backend overrides with a pipelined call.
        let mut ids = Vec::with_capacity(inserts.len());
        for i in inserts {
            ids.push(self.add(queue, i).await?);
        }
        Ok(ids)
    }

    /// Block up to `block_for` waiting for the next job. Returns `None` on
    /// timeout.
    async fn fetch_next(
        &self,
        queue: &str,
        worker_token: &str,
        lock_duration: Duration,
        block_for: Duration,
    ) -> Result<Option<Fetched>>;

    /// Mark a job completed and record `return_value_json`.
    ///
    /// `removal` is the job's `JobOptions::remove_on_complete` policy; the
    /// caller passes it from the in-memory job record so the backend
    /// doesn't need a round trip to re-read it from storage.
    async fn complete(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        return_value_json: Option<String>,
        removal: crate::options::Removal,
    ) -> Result<()>;

    /// Mark a job failed. If `retry_at_ms` is `Some`, the job goes to
    /// `delayed` for re-execution at that timestamp; otherwise it's moved
    /// to `failed` with `removal` applied.
    async fn fail(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        reason: &str,
        retry_at_ms: Option<i64>,
        removal: crate::options::Removal,
    ) -> Result<()>;

    /// Renew the lock TTL.
    async fn extend_lock(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        duration: Duration,
    ) -> Result<()>;

    /// Scan `active` for jobs whose lock has expired, marking them stalled
    /// and pushing them back to `wait`. Returns the ids that moved.
    async fn scan_stalled(&self, queue: &str, max_stalled: u32) -> Result<Vec<JobId>>;

    /// Pause `wait` → `paused`. Already-active jobs continue.
    async fn pause(&self, queue: &str) -> Result<()>;

    /// Resume a paused queue.
    async fn resume(&self, queue: &str) -> Result<()>;

    /// True if the queue is paused.
    async fn is_paused(&self, queue: &str) -> Result<bool>;

    /// Remove every pending job. If `include_delayed` is true, also drops
    /// the `delayed` zset.
    async fn drain(&self, queue: &str, include_delayed: bool) -> Result<()>;

    /// Nuke everything for the queue.
    async fn obliterate(&self, queue: &str, force: bool) -> Result<()>;

    /// Move a delayed job into `wait` ahead of schedule.
    async fn promote(&self, queue: &str, id: &JobId) -> Result<()>;

    /// Move **every** delayed job into `wait` (or `paused`, if the queue
    /// is paused) immediately. Returns the number of jobs promoted.
    ///
    /// Useful for operators clearing a backlog of retry-delayed jobs after
    /// fixing a downstream issue.
    async fn promote_all(&self, queue: &str) -> Result<u64>;

    /// Bulk-delete jobs sitting in a finished/scheduled state.
    ///
    /// Supported states: `Completed`, `Failed`, `Delayed`, `Prioritized`,
    /// `WaitingChildren`. Calling with `Waiting`, `Paused`, or `Active`
    /// returns [`crate::Error::Config`] — those are list-backed and you
    /// should use [`Self::drain`] / [`Self::obliterate`] instead.
    ///
    /// `limit = 0` means "every job in that state". Returns the number of
    /// jobs removed. Job hash + auxiliary keys (logs, dependencies, etc.)
    /// are deleted along with the zset entries.
    async fn clean(
        &self,
        queue: &str,
        state: JobState,
        limit: u64,
    ) -> Result<u64>;

    /// Retry a failed job by moving it back to `wait`.
    async fn retry(&self, queue: &str, id: &JobId) -> Result<()>;

    /// Remove a job and its auxiliary keys.
    async fn remove(&self, queue: &str, id: &JobId) -> Result<()>;

    /// Fetch a job record by id.
    async fn get(&self, queue: &str, id: &JobId) -> Result<Option<RawJob>>;

    /// Counts of jobs per state.
    async fn counts(&self, queue: &str) -> Result<StateCounts>;

    /// Iterate jobs in a particular state.
    async fn list(
        &self,
        queue: &str,
        state: JobState,
        range: JobRange,
    ) -> Result<Vec<RawJob>>;

    /// Append a log line attached to a job.
    async fn log(&self, queue: &str, id: &JobId, line: &str) -> Result<()>;

    /// Fetch the log lines for a job.
    async fn get_logs(&self, queue: &str, id: &JobId, range: JobRange) -> Result<Vec<String>>;

    /// Update in-progress progress value.
    async fn update_progress(
        &self,
        queue: &str,
        id: &JobId,
        progress_json: &str,
    ) -> Result<()>;

    /// Subscribe to the queue's event stream. Each call yields an
    /// independent cursor; backpressure-friendly.
    async fn subscribe(&self, queue: &str) -> Result<EventStream>;

    /// List queues known to this backend (used by the dashboard).
    async fn list_queues(&self) -> Result<Vec<String>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_range_helpers() {
        let f = JobRange::first(10);
        assert_eq!(f.start, 0);
        assert_eq!(f.end, 9);
        assert!(!f.descending);
        let l = JobRange::last(5);
        assert_eq!(l.start, 0);
        assert_eq!(l.end, 4);
        assert!(l.descending);
    }

    #[test]
    fn state_counts_default_zero() {
        let c = StateCounts::default();
        assert_eq!(c.waiting, 0);
        assert_eq!(c.paused, 0);
        assert_eq!(c.active, 0);
        assert_eq!(c.completed, 0);
        assert_eq!(c.failed, 0);
    }
}
