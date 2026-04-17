//! Storage backends.
//!
//! The [`Backend`] trait is the pluggable point: every operation the queue,
//! worker, events subsystem and dashboard need goes through it. A
//! [`RedisBackend`](redis::RedisBackend) ships out of the box, and the trait
//! is object-safe so downstream crates can slot in alternative stores
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawJob {
    pub id: JobId,
    pub name: String,
    /// JSON-encoded payload.
    pub data: String,
    pub opts: JobOptions,
    pub timestamp_ms: i64,
    pub processed_on_ms: Option<i64>,
    pub finished_on_ms: Option<i64>,
    pub attempts_made: u32,
    pub failed_reason: Option<String>,
    pub stacktrace: Vec<String>,
    /// JSON-encoded progress value.
    pub progress_json: Option<String>,
    /// JSON-encoded return value.
    pub return_value_json: Option<String>,
    pub lock_token: Option<String>,
    pub state: Option<JobState>,
}

/// Input for a new job submission.
#[derive(Debug, Clone)]
pub struct JobInsert {
    pub name: String,
    /// JSON-encoded payload.
    pub data: String,
    pub opts: JobOptions,
    pub timestamp_ms: i64,
}

/// A fetched job plus the lock token that authorizes operations on it.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub job: RawJob,
    pub token: String,
}

/// Counts of jobs per state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateCounts {
    pub waiting: u64,
    pub paused: u64,
    pub active: u64,
    pub delayed: u64,
    pub prioritized: u64,
    pub waiting_children: u64,
    pub completed: u64,
    pub failed: u64,
}

/// A half-open range into one of the job state lists/zsets.
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
    pub const fn first(n: i64) -> Self {
        Self {
            start: 0,
            end: n - 1,
            descending: false,
        }
    }
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
    async fn complete(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        return_value_json: Option<String>,
    ) -> Result<()>;

    /// Mark a job failed. If `retry_at_ms` is `Some`, the job goes to
    /// `delayed` for re-execution at that timestamp.
    async fn fail(
        &self,
        queue: &str,
        id: &JobId,
        token: &str,
        reason: &str,
        retry_at_ms: Option<i64>,
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
