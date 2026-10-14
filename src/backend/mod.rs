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
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::job::{JobId, JobState};
use crate::options::ResolvedJobOptions;

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
    /// The options the job runs with (per-job, queue defaults and
    /// built-ins merged at submission).
    pub opts: ResolvedJobOptions,
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
    /// The parent this job reports to (flow children).
    #[serde(default)]
    pub parent: Option<crate::job::JobRef>,
}

/// One node of a flow, flattened for [`Backend::add_flow`]: parents come
/// before their children.
#[derive(Debug, Clone)]
pub struct FlowJobInsert {
    /// The node's queue.
    pub queue: String,
    /// The job itself.
    pub insert: JobInsert,
    /// Index of this node's parent in the same list; `None` for the root.
    pub parent: Option<usize>,
    /// Number of direct children. A node with children waits in
    /// `waiting-children` until they resolve.
    pub children: usize,
    /// If this job fails for good, fail its parent too.
    pub fail_parent_on_failure: bool,
    /// If this job fails for good, drop the dependency and let the parent
    /// run (the failure is listed in [`Dependencies::failed`]).
    pub ignore_dependency_on_failure: bool,
}

/// How a scheduler repeats, as stored with its record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Repeat {
    /// A cron expression (seconds field first, as the `cron` crate parses).
    Cron(String),
    /// A fixed interval in ms; ticks are aligned to the Unix epoch.
    EveryMs(u64),
}

/// A scheduler registered on a queue (see [`Backend::schedulers`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerInfo {
    /// The scheduler's id.
    pub id: String,
    /// Its cadence.
    pub repeat: Repeat,
    /// Name of the jobs it adds.
    pub name: String,
    /// Fire time (epoch-ms) of the latest tick enqueued — the next one to
    /// run, until it does.
    #[serde(rename = "next_ms")]
    pub next_fire_ms: i64,
    /// Epoch-ms the record was last written.
    pub updated_ms: i64,
}

/// One scheduler tick to enqueue (see [`Backend::schedule_tick`]).
#[derive(Debug, Clone)]
pub struct SchedulerTick {
    /// The scheduler's id.
    pub scheduler_id: String,
    /// The tick's fire time, epoch-ms. The job is `sched-{id}-{fire_at_ms}`.
    pub fire_at_ms: i64,
    /// The scheduler's cadence, stored with its record.
    pub repeat: Repeat,
    /// `true` when a scheduler (re)starts: revives a removed one. A running
    /// loop passes `false`, so a removed scheduler stops.
    pub register: bool,
    /// The job (its id and delay are set from the tick).
    pub insert: JobInsert,
}

/// What [`Backend::schedule_tick`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickOutcome {
    /// This call enqueued the tick.
    Added(JobId),
    /// The tick (or a later one) was already enqueued — by another
    /// instance, or earlier; carries the latest tick's job id.
    Exists(JobId),
    /// The scheduler was removed; a running loop should stop.
    Removed,
}

/// A parent's children by outcome (see [`Backend::dependencies`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dependencies {
    /// Children not resolved yet (including failed ones the parent still
    /// waits on).
    pub pending: Vec<crate::job::JobRef>,
    /// Completed children and their JSON return values (`null` for none).
    pub processed: Vec<(crate::job::JobRef, String)>,
    /// Children whose failure was ignored (`ignore_dependency_on_failure`),
    /// with their failure reasons.
    pub failed: Vec<(crate::job::JobRef, String)>,
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
    /// Fully-resolved options (priority, delay, attempts, …).
    pub opts: ResolvedJobOptions,
    /// Epoch-ms at submission.
    pub timestamp_ms: i64,
}

/// What went wrong with an attempt, for [`Backend::fail`].
///
/// ```
/// use oxn::backend::Failure;
/// use oxn::options::Removal;
///
/// let retry = Failure::new("timeout").retry_at(1_700_000_000_000);
/// let fatal = Failure::new("bad input").removal(Removal::KeepLast(100));
/// # let _ = (retry, fatal);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Failure<'a> {
    /// Human-readable reason, stored as `failedReason`.
    pub reason: &'a str,
    /// Retry at this epoch-ms (backoff); `None` fails the job for good.
    pub retry_at_ms: Option<i64>,
    /// Retention applied when the job fails for good.
    pub removal: crate::options::Removal,
    /// Stack-trace entry for this attempt.
    pub trace: Option<&'a str>,
}

impl<'a> Failure<'a> {
    /// A permanent failure with `reason`, kept ([`Removal::Keep`](crate::options::Removal)), no trace.
    pub fn new(reason: &'a str) -> Self {
        Self {
            reason,
            retry_at_ms: None,
            removal: crate::options::Removal::Keep,
            trace: None,
        }
    }
    /// Retry at `ms` (epoch-ms) instead of failing for good.
    #[must_use]
    pub fn retry_at(mut self, ms: i64) -> Self {
        self.retry_at_ms = Some(ms);
        self
    }
    /// Retention applied if the job fails for good.
    #[must_use]
    pub fn removal(mut self, removal: crate::options::Removal) -> Self {
        self.removal = removal;
        self
    }
    /// Stack-trace entry for this attempt.
    #[must_use]
    pub fn trace(mut self, trace: &'a str) -> Self {
        self.trace = Some(trace);
        self
    }
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

/// A range into one of the job state lists/zsets.
///
/// `start` and `end` are inclusive positions, `-1` meaning the last one.
/// Positions count in **ascending** order unless `descending` is set:
/// for `wait` / `paused` / `active` that is processing order (next job to
/// run first); for zsets it is ascending score (next due / highest
/// priority / oldest finished first).
#[derive(Debug, Clone, Copy)]
pub struct JobRange {
    /// Zero-based start index (inclusive).
    pub start: i64,
    /// End index (inclusive). Use `-1` for "to the end".
    pub end: i64,
    /// Iterate newest-first instead of ascending order.
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
pub use crate::events::{EventRecord, EventStream, RecordStream, StreamStart};

/// Pluggable storage backend.
///
/// All operations are `async` and fail with [`crate::Error`]. A single
/// `Backend` instance is shared across a `Queue`, any number of `Worker`s
/// and a `QueueEvents` via `Arc<dyn Backend>` — construct it once and clone
/// the `Arc`.
// `async_trait` marks the generated methods `#[must_use]` on top of the
// boxed future they return, which newer clippy flags as double_must_use.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Backend: Send + Sync + fmt::Debug + 'static {
    /// Submit a new job. Returns the assigned id.
    async fn add(&self, queue: &str, insert: JobInsert) -> Result<JobId>;

    /// Submit a batch of jobs atomically where the backend supports it.
    async fn add_bulk(&self, queue: &str, inserts: Vec<JobInsert>) -> Result<Vec<JobId>> {
        // Default: sequential `add`s. The Redis backend overrides this with
        // one script call per chunk of up to 1,000 jobs.
        let mut ids = Vec::with_capacity(inserts.len());
        for i in inserts {
            ids.push(self.add(queue, i).await?);
        }
        Ok(ids)
    }

    /// Take the next ready job, blocking up to `block_for` when nothing is
    /// ready. Returns `None` on timeout, or as soon as `cancel` fires.
    ///
    /// Cancellation must only be honored while **waiting**, never while a
    /// job is being moved to `active`: a fetch that already took a job
    /// returns it even if `cancel` fired meanwhile, so the caller can
    /// [`Self::release`] it instead of leaving it locked until it stalls.
    ///
    /// `rate_limit`, when set, caps how many jobs move to active per window
    /// across every worker on the queue; a limited fetch waits for the
    /// window to reopen (interrupted by `cancel`) and returns `None`.
    async fn fetch_next(
        &self,
        queue: &str,
        worker_token: &str,
        lock_duration: Duration,
        block_for: Duration,
        cancel: &tokio_util::sync::CancellationToken,
        rate_limit: Option<crate::options::RateLimit>,
    ) -> Result<Option<Fetched>>;

    /// Mark a job completed and record `return_value_json`.
    ///
    /// Fails with [`ScriptError::LockMismatch`](crate::error::ScriptError) /
    /// `LockNotFound` when `token` no longer owns the job (the worker's
    /// outcome is then discarded), or [`crate::Error::NotFound`] if the job
    /// was deleted while active.
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

    /// Record a failed attempt. With [`Failure::retry_at`] the job goes to
    /// `delayed` for re-execution at that time; otherwise it fails for good
    /// into `failed` with [`Failure::removal`] applied. [`Failure::trace`]
    /// is appended to the job's `stacktrace` (10 most recent kept).
    ///
    /// Same lock errors as [`Self::complete`].
    async fn fail(&self, queue: &str, id: &JobId, token: &str, failure: Failure<'_>) -> Result<()>;

    /// Postpone an active job until `until_ms` without counting it as a
    /// failure: the attempt is refunded and no `failedReason` is recorded.
    /// Backs `Error::Delayed` / `Error::RateLimited` from handlers. Same
    /// lock errors as [`Self::complete`].
    async fn delay(&self, queue: &str, id: &JobId, token: &str, until_ms: i64) -> Result<()>;

    /// Hand an active job back without recording an outcome: drop the lock,
    /// refund the attempt the fetch counted, and put the job back at the
    /// head of its ready container (respecting priority and pause).
    ///
    /// Used by a worker shutting down when a handler outlives
    /// `shutdown_timeout`, and for jobs fetched after shutdown began. Same
    /// lock errors as [`Self::complete`].
    async fn release(&self, queue: &str, id: &JobId, token: &str) -> Result<()>;

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
    ///
    /// Fails with [`crate::Error::NotFound`] if the job doesn't exist and
    /// [`ScriptError::NotInExpectedState`](crate::error::ScriptError) if it
    /// isn't delayed.
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
    async fn clean(&self, queue: &str, state: JobState, limit: u64) -> Result<u64>;

    /// Retry a failed job by moving it back to `wait`.
    ///
    /// Fails with [`crate::Error::NotFound`] if the job doesn't exist and
    /// [`ScriptError::NotInExpectedState`](crate::error::ScriptError) if it
    /// isn't in `failed`.
    async fn retry(&self, queue: &str, id: &JobId) -> Result<()>;

    /// Remove a job and its auxiliary keys.
    ///
    /// Fails with [`crate::Error::NotFound`] if the job doesn't exist and
    /// [`ScriptError::JobLocked`](crate::error::ScriptError) if it is
    /// active and a worker still holds its lock.
    async fn remove(&self, queue: &str, id: &JobId) -> Result<()>;

    /// Fetch a job record by id.
    async fn get(&self, queue: &str, id: &JobId) -> Result<Option<RawJob>>;

    /// Counts of jobs per state.
    async fn counts(&self, queue: &str) -> Result<StateCounts>;

    /// Iterate jobs in a particular state.
    async fn list(&self, queue: &str, state: JobState, range: JobRange) -> Result<Vec<RawJob>>;

    /// Append a log line attached to a job.
    async fn log(&self, queue: &str, id: &JobId, line: &str) -> Result<()>;

    /// Fetch the log lines for a job.
    async fn get_logs(&self, queue: &str, id: &JobId, range: JobRange) -> Result<Vec<String>>;

    /// Update in-progress progress value.
    async fn update_progress(&self, queue: &str, id: &JobId, progress_json: &str) -> Result<()>;

    /// Subscribe to the queue's event stream from `from`. Each call yields
    /// an independent cursor; backpressure-friendly.
    ///
    /// Implementations must resolve [`StreamStart::Now`] to a concrete
    /// position **before returning** (events emitted between this call and
    /// the first poll must be delivered), must not lose or duplicate
    /// entries across reconnects, and should surface transport errors as
    /// `Err` items rather than ending the stream.
    async fn subscribe(&self, queue: &str, from: StreamStart) -> Result<RecordStream>;

    /// List queues known to this backend (used by the dashboard).
    async fn list_queues(&self) -> Result<Vec<String>>;

    /// Add a flow tree atomically (see the `flow` module). With `parent`, the
    /// root becomes a child of that job, which must be active or waiting
    /// for children. Returns the ids in input order.
    async fn add_flow(
        &self,
        nodes: Vec<FlowJobInsert>,
        parent: Option<&crate::job::JobRef>,
    ) -> Result<Vec<JobId>> {
        let _ = (nodes, parent);
        Err(Error::Config("this backend does not support flows".into()))
    }

    /// Park an active job until its children resolve (the handler returned
    /// [`Error::WaitingChildren`]). Returns `false` when none was pending
    /// and the job is ready again at once.
    async fn wait_for_children(&self, queue: &str, id: &JobId, token: &str) -> Result<bool> {
        let _ = (queue, id, token);
        Err(Error::Config("this backend does not support flows".into()))
    }

    /// Enqueue one scheduler tick, at most once across every instance of
    /// the scheduler.
    async fn schedule_tick(&self, queue: &str, tick: SchedulerTick) -> Result<TickOutcome> {
        let _ = (queue, tick);
        Err(Error::Config(
            "this backend does not support schedulers".into(),
        ))
    }

    /// The schedulers registered on `queue` (removed ones excluded).
    async fn schedulers(&self, queue: &str) -> Result<Vec<SchedulerInfo>> {
        let _ = queue;
        Ok(Vec::new())
    }

    /// Remove a scheduler and its pending tick; every running instance
    /// stops at its next tick. Returns whether it existed.
    async fn remove_scheduler(&self, queue: &str, id: &str) -> Result<bool> {
        let _ = (queue, id);
        Err(Error::Config(
            "this backend does not support schedulers".into(),
        ))
    }

    /// A job's children by outcome. Empty for a job that has none.
    async fn dependencies(&self, queue: &str, id: &JobId) -> Result<Dependencies> {
        let _ = (queue, id);
        Err(Error::Config("this backend does not support flows".into()))
    }
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
