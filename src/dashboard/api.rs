//! Framework-agnostic dashboard endpoints.
//!
//! Each public fn takes a [`DashboardState`] and plain argument values, and
//! returns something serializable. Both the axum and actix scaffolds call
//! into these functions — the HTTP glue is kept as a thin adapter so the
//! business logic isn't duplicated per framework.
//!
//! Consumers that roll their own HTTP stack (e.g. rocket, warp, hyper,
//! gRPC) can call these directly.

use serde::{Deserialize, Serialize};

use crate::backend::{JobRange, RawJob, StateCounts};
use crate::dashboard::DashboardState;
use crate::error::Result;
use crate::job::{JobId, JobState};

/// Lightweight summary of a queue — name, pause flag, and state counts.
///
/// Returned as JSON from `GET /api/queues`.
#[derive(Debug, Serialize)]
pub struct QueueSummary {
    /// The queue's name (second segment of the Redis key layout).
    pub name: String,
    /// `true` if the queue is currently paused.
    pub paused: bool,
    /// Per-state job counts.
    pub counts: StateCounts,
}

/// Query string for `GET /api/queues/{name}/jobs`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Job state to list. Parsed by [`JobState::parse`]; defaults to
    /// `"waiting"`.
    #[serde(default = "default_state")]
    pub state: String,
    /// Inclusive zero-based start index. Defaults to 0.
    #[serde(default)]
    pub start: i64,
    /// Inclusive end index. Use `-1` for "to the end". Defaults to 49.
    #[serde(default = "default_end")]
    pub end: i64,
    /// When true, iterate newest-first. Defaults to false.
    #[serde(default)]
    pub desc: bool,
}

fn default_state() -> String {
    "waiting".into()
}
fn default_end() -> i64 {
    49
}

/// Response body of `GET /api/queues/{name}/jobs`.
#[derive(Debug, Serialize)]
pub struct ListResponse {
    /// Page of [`JobSummary`] records, in the requested order.
    pub jobs: Vec<JobSummary>,
    /// Total jobs currently in the requested state (not just this page).
    pub total: u64,
}

/// Compact projection of a [`RawJob`] for dashboard rendering.
#[derive(Debug, Serialize)]
pub struct JobSummary {
    /// Job id.
    pub id: JobId,
    /// Job's logical name.
    pub name: String,
    /// Lifecycle state, when known.
    pub state: Option<JobState>,
    /// Number of attempts made so far.
    pub attempts_made: u32,
    /// Configured total attempts cap.
    pub attempts_total: u32,
    /// Epoch-ms the job was first accepted.
    pub timestamp_ms: i64,
    /// Epoch-ms of the most recent `move_to_active`, if ever active.
    pub processed_on_ms: Option<i64>,
    /// Epoch-ms of completion or terminal failure, if finished.
    pub finished_on_ms: Option<i64>,
    /// Last-captured failure reason, if any.
    pub failed_reason: Option<String>,
}

impl From<RawJob> for JobSummary {
    fn from(raw: RawJob) -> Self {
        Self {
            id: raw.id,
            name: raw.name,
            state: raw.state,
            attempts_made: raw.attempts_made,
            attempts_total: raw.opts.attempts,
            timestamp_ms: raw.timestamp_ms,
            processed_on_ms: raw.processed_on_ms,
            finished_on_ms: raw.finished_on_ms,
            failed_reason: raw.failed_reason,
        }
    }
}

/// Backs `GET /api/queues` — list every known queue with pause flag and counts.
pub async fn list_queues(state: &DashboardState) -> Result<Vec<QueueSummary>> {
    let names = state.backend.list_queues().await?;
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        let counts = state.backend.counts(&n).await.unwrap_or_default();
        let paused = state.backend.is_paused(&n).await.unwrap_or(false);
        out.push(QueueSummary {
            name: n,
            paused,
            counts,
        });
    }
    Ok(out)
}

/// Backs `GET /api/queues/{name}/counts` — per-state job counts.
pub async fn queue_counts(state: &DashboardState, queue: &str) -> Result<StateCounts> {
    state.backend.counts(queue).await
}

/// Backs `GET /api/queues/{name}/jobs` — paginated listing of jobs in a state.
pub async fn list_jobs(
    state: &DashboardState,
    queue: &str,
    q: ListQuery,
) -> Result<ListResponse> {
    let js = JobState::parse(&q.state).unwrap_or(JobState::Waiting);
    let range = JobRange {
        start: q.start,
        end: q.end,
        descending: q.desc,
    };
    let raw = state.backend.list(queue, js, range).await?;
    let counts = state.backend.counts(queue).await.unwrap_or_default();
    let total = match js {
        JobState::Waiting => counts.waiting,
        JobState::Paused => counts.paused,
        JobState::Active => counts.active,
        JobState::Delayed => counts.delayed,
        JobState::Prioritized => counts.prioritized,
        JobState::WaitingChildren => counts.waiting_children,
        JobState::Completed => counts.completed,
        JobState::Failed => counts.failed,
    };
    Ok(ListResponse {
        jobs: raw.into_iter().map(JobSummary::from).collect(),
        total,
    })
}

/// Backs `GET /api/queues/{name}/jobs/{id}` — full [`RawJob`] record.
pub async fn get_job(
    state: &DashboardState,
    queue: &str,
    id: &JobId,
) -> Result<Option<RawJob>> {
    state.backend.get(queue, id).await
}

/// Query string for `GET /api/queues/{name}/jobs/{id}/logs`.
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    /// Inclusive zero-based start index. Defaults to 0.
    #[serde(default)]
    pub start: i64,
    /// Inclusive end index. Use `-1` for "to the end". Defaults to `-1`.
    #[serde(default = "default_logs_end")]
    pub end: i64,
    /// When true, iterate newest-first. Defaults to false.
    #[serde(default)]
    pub desc: bool,
}

fn default_logs_end() -> i64 {
    -1
}

/// Response body of `GET /api/queues/{name}/jobs/{id}/logs`.
#[derive(Debug, Serialize)]
pub struct LogsResponse {
    /// Log lines in the requested order.
    pub logs: Vec<String>,
    /// Number of lines returned.
    pub count: usize,
}

/// Backs `GET /api/queues/{name}/jobs/{id}/logs` — fetch the log lines
/// attached to a job.
pub async fn get_job_logs(
    state: &DashboardState,
    queue: &str,
    id: &JobId,
    q: LogsQuery,
) -> Result<LogsResponse> {
    let range = JobRange {
        start: q.start,
        end: q.end,
        descending: q.desc,
    };
    let logs = state.backend.get_logs(queue, id, range).await?;
    Ok(LogsResponse {
        count: logs.len(),
        logs,
    })
}

/// Backs `POST /api/queues/{name}/jobs/{id}/retry` — move a failed job back
/// to `wait`, clearing failure state.
pub async fn retry_job(state: &DashboardState, queue: &str, id: &JobId) -> Result<()> {
    state.backend.retry(queue, id).await
}

/// Backs `POST /api/queues/{name}/jobs/{id}/promote` — move a delayed job
/// to `wait` immediately.
pub async fn promote_job(state: &DashboardState, queue: &str, id: &JobId) -> Result<()> {
    state.backend.promote(queue, id).await
}

/// Backs `POST /api/queues/{name}/jobs/{id}/remove` — delete a job and
/// all its auxiliary keys.
pub async fn remove_job(state: &DashboardState, queue: &str, id: &JobId) -> Result<()> {
    state.backend.remove(queue, id).await
}

/// Backs `POST /api/queues/{name}/pause` — pause the queue.
pub async fn pause_queue(state: &DashboardState, queue: &str) -> Result<()> {
    state.backend.pause(queue).await
}

/// Backs `POST /api/queues/{name}/resume` — resume a paused queue.
pub async fn resume_queue(state: &DashboardState, queue: &str) -> Result<()> {
    state.backend.resume(queue).await
}

/// Backs `POST /api/queues/{name}/drain` — remove every pending job.
/// When `include_delayed` is true, the `delayed` zset is wiped as well.
pub async fn drain_queue(
    state: &DashboardState,
    queue: &str,
    include_delayed: bool,
) -> Result<()> {
    state.backend.drain(queue, include_delayed).await
}
