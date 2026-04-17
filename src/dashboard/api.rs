//! Framework-agnostic dashboard endpoints.
//!
//! Each public fn takes a [`DashboardState`] and plain argument values, and
//! returns something serializable. Both the axum and actix scaffolds call
//! these; the HTTP glue is kept very thin.

use serde::{Deserialize, Serialize};

use crate::backend::{JobRange, RawJob, StateCounts};
use crate::dashboard::DashboardState;
use crate::error::Result;
use crate::job::{JobId, JobState};

#[derive(Debug, Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub paused: bool,
    pub counts: StateCounts,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default = "default_state")]
    pub state: String,
    #[serde(default)]
    pub start: i64,
    #[serde(default = "default_end")]
    pub end: i64,
    #[serde(default)]
    pub desc: bool,
}

fn default_state() -> String {
    "waiting".into()
}
fn default_end() -> i64 {
    49
}

#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub jobs: Vec<JobSummary>,
    pub total: u64,
}

#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub id: JobId,
    pub name: String,
    pub state: Option<JobState>,
    pub attempts_made: u32,
    pub attempts_total: u32,
    pub timestamp_ms: i64,
    pub processed_on_ms: Option<i64>,
    pub finished_on_ms: Option<i64>,
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

pub async fn queue_counts(state: &DashboardState, queue: &str) -> Result<StateCounts> {
    state.backend.counts(queue).await
}

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

pub async fn get_job(
    state: &DashboardState,
    queue: &str,
    id: &JobId,
) -> Result<Option<RawJob>> {
    state.backend.get(queue, id).await
}

pub async fn retry_job(state: &DashboardState, queue: &str, id: &JobId) -> Result<()> {
    state.backend.retry(queue, id).await
}

pub async fn promote_job(state: &DashboardState, queue: &str, id: &JobId) -> Result<()> {
    state.backend.promote(queue, id).await
}

pub async fn remove_job(state: &DashboardState, queue: &str, id: &JobId) -> Result<()> {
    state.backend.remove(queue, id).await
}

pub async fn pause_queue(state: &DashboardState, queue: &str) -> Result<()> {
    state.backend.pause(queue).await
}

pub async fn resume_queue(state: &DashboardState, queue: &str) -> Result<()> {
    state.backend.resume(queue).await
}

pub async fn drain_queue(
    state: &DashboardState,
    queue: &str,
    include_delayed: bool,
) -> Result<()> {
    state.backend.drain(queue, include_delayed).await
}
