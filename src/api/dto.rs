//! JSON shapes of the `/v1` API.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backend::{RawJob, StateCounts};
use crate::job::{Backoff, JobId, JobRef, JobState};
use crate::options::{JobOptions, Removal, ResolvedJobOptions};

/// A queue: `GET /v1/queues`, `GET /v1/queues/{queue}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDto {
    /// The queue's name.
    pub name: String,
    /// Whether the queue is paused.
    pub paused: bool,
    /// Jobs per state.
    pub counts: StateCounts,
}

/// A job, in full: `GET /v1/queues/{queue}/jobs/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDto {
    /// Job id.
    pub id: JobId,
    /// The job's queue.
    pub queue: String,
    /// Logical name.
    pub name: String,
    /// Where the job is now.
    pub state: Option<JobState>,
    /// The payload (JSON; a string if it isn't valid JSON).
    pub data: Value,
    /// The options it runs with.
    pub opts: ResolvedJobOptions,
    /// Last reported progress.
    pub progress: Option<Value>,
    /// Return value, once completed.
    pub return_value: Option<Value>,
    /// Last failure reason.
    pub failed_reason: Option<String>,
    /// The most recent failed attempts' errors, oldest first.
    pub stacktrace: Vec<String>,
    /// Attempts made.
    pub attempts_made: u32,
    /// Attempts allowed.
    pub attempts_total: u32,
    /// Epoch-ms added.
    pub timestamp_ms: i64,
    /// Epoch-ms its current or last run started.
    pub processed_on_ms: Option<i64>,
    /// Epoch-ms finished.
    pub finished_on_ms: Option<i64>,
    /// The parent it reports to (flow children).
    pub parent: Option<JobRef>,
    /// A worker holds it.
    pub locked: bool,
}

fn json_or_string(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
}

impl JobDto {
    pub(crate) fn from_raw(queue: &str, raw: RawJob) -> Self {
        Self {
            queue: queue.to_string(),
            data: json_or_string(&raw.data),
            progress: raw
                .progress_json
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(json_or_string),
            return_value: raw
                .return_value_json
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(json_or_string),
            attempts_total: raw.opts.attempts,
            locked: raw.lock_token.is_some(),
            id: raw.id,
            name: raw.name,
            state: raw.state,
            opts: raw.opts,
            failed_reason: raw.failed_reason,
            stacktrace: raw.stacktrace,
            attempts_made: raw.attempts_made,
            timestamp_ms: raw.timestamp_ms,
            processed_on_ms: raw.processed_on_ms,
            finished_on_ms: raw.finished_on_ms,
            parent: raw.parent,
        }
    }
}

/// A job in a list: `GET /v1/queues/{queue}/jobs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummaryDto {
    /// Job id.
    pub id: JobId,
    /// Logical name.
    pub name: String,
    /// The listed state.
    pub state: JobState,
    /// Attempts made.
    pub attempts_made: u32,
    /// Attempts allowed.
    pub attempts_total: u32,
    /// Epoch-ms added.
    pub timestamp_ms: i64,
    /// Epoch-ms its current or last run started.
    pub processed_on_ms: Option<i64>,
    /// Epoch-ms finished.
    pub finished_on_ms: Option<i64>,
    /// Last failure reason.
    pub failed_reason: Option<String>,
    /// Last reported progress.
    pub progress: Option<Value>,
    /// The parent it reports to.
    pub parent: Option<JobRef>,
}

impl JobSummaryDto {
    pub(crate) fn from_raw(state: JobState, raw: RawJob) -> Self {
        Self {
            progress: raw
                .progress_json
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(json_or_string),
            attempts_total: raw.opts.attempts,
            id: raw.id,
            name: raw.name,
            state,
            attempts_made: raw.attempts_made,
            timestamp_ms: raw.timestamp_ms,
            processed_on_ms: raw.processed_on_ms,
            finished_on_ms: raw.finished_on_ms,
            failed_reason: raw.failed_reason,
            parent: raw.parent,
        }
    }
}

/// A page of jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPage {
    /// The jobs, in the requested order.
    pub jobs: Vec<JobSummaryDto>,
    /// Jobs in the state in all.
    pub total: u64,
    /// Index of the first job returned.
    pub offset: u64,
    /// Requested page size.
    pub limit: u64,
}

/// A node of `GET /v1/queues/{queue}/jobs/{id}/flow`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNodeDto {
    /// The job's queue.
    pub queue: String,
    /// Job id.
    pub id: JobId,
    /// Logical name (`null` if the job no longer exists).
    pub name: Option<String>,
    /// Where it is now (`null` if it no longer exists).
    pub state: Option<JobState>,
    /// How it stands with its parent: `pending`, `processed` or
    /// `ignored-failure`; `null` for the requested job.
    pub dependency: Option<String>,
    /// Its children (cut off at the requested depth).
    pub children: Vec<FlowNodeDto>,
    /// True when it has children beyond the requested depth.
    pub truncated: bool,
}

/// A retention policy in a request: `true` (remove), `false` (keep),
/// `{"keep_last": n}` or `{"keep_for_ms": ms}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HttpRemoval {
    /// `true` removes on finish, `false` keeps.
    Flag(bool),
    /// Keep the most recent `n`.
    KeepLast {
        /// How many to keep.
        keep_last: usize,
    },
    /// Keep for this long.
    KeepFor {
        /// How long, ms.
        keep_for_ms: u64,
    },
}

impl From<HttpRemoval> for Removal {
    fn from(r: HttpRemoval) -> Self {
        match r {
            HttpRemoval::Flag(true) => Removal::Remove,
            HttpRemoval::Flag(false) => Removal::Keep,
            HttpRemoval::KeepLast { keep_last } => Removal::KeepLast(keep_last),
            HttpRemoval::KeepFor { keep_for_ms } => {
                Removal::KeepFor(Duration::from_millis(keep_for_ms))
            }
        }
    }
}

/// A backoff in a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpBackoff {
    /// The same delay before every retry.
    Fixed {
        /// Delay, ms.
        delay_ms: u64,
    },
    /// `delay_ms × 2^(attempt-1)`, capped at `max_ms`.
    Exponential {
        /// First delay, ms.
        delay_ms: u64,
        /// Cap, ms.
        #[serde(default)]
        max_ms: Option<u64>,
    },
}

/// Deduplication in a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpDedup {
    /// Submissions with the same id collapse into one job…
    pub id: String,
    /// …for this long, ms.
    pub ttl_ms: u64,
}

/// Job options in a request. Unset fields take the built-in defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpJobOptions {
    /// Explicit job id (re-adding an existing id returns that job).
    pub id: Option<String>,
    /// Run no earlier than this many ms from now.
    pub delay_ms: Option<u64>,
    /// Priority, 1 = highest; 0 = none.
    pub priority: Option<u32>,
    /// Attempts allowed.
    pub attempts: Option<u32>,
    /// Delay between attempts.
    pub backoff: Option<HttpBackoff>,
    /// Push to the front of the queue.
    pub lifo: Option<bool>,
    /// Retention once completed.
    pub remove_on_complete: Option<HttpRemoval>,
    /// Retention once failed for good.
    pub remove_on_fail: Option<HttpRemoval>,
    /// Deduplication.
    pub deduplicate: Option<HttpDedup>,
}

impl From<HttpJobOptions> for JobOptions {
    fn from(o: HttpJobOptions) -> Self {
        let mut j = JobOptions::new();
        if let Some(v) = o.id {
            j = j.id(v);
        }
        if let Some(v) = o.delay_ms {
            j = j.delay(Duration::from_millis(v));
        }
        if let Some(v) = o.priority {
            j = j.priority(v);
        }
        if let Some(v) = o.attempts {
            j = j.attempts(v);
        }
        if let Some(b) = o.backoff {
            j = j.backoff(match b {
                HttpBackoff::Fixed { delay_ms } => Backoff::Fixed {
                    delay: Duration::from_millis(delay_ms),
                },
                HttpBackoff::Exponential { delay_ms, max_ms } => Backoff::Exponential {
                    initial: Duration::from_millis(delay_ms),
                    max: max_ms.map(Duration::from_millis),
                },
            });
        }
        if let Some(v) = o.lifo {
            j = j.lifo(v);
        }
        if let Some(r) = o.remove_on_complete {
            j = j.remove_on_complete(r.into());
        }
        if let Some(r) = o.remove_on_fail {
            j = j.remove_on_fail(r.into());
        }
        if let Some(d) = o.deduplicate {
            j = j.deduplicate(d.id, Duration::from_millis(d.ttl_ms));
        }
        j
    }
}

/// Body of `POST /v1/queues/{queue}/jobs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddJob {
    /// The payload.
    pub data: Value,
    /// Logical name (defaults to the queue's name).
    #[serde(default)]
    pub name: Option<String>,
    /// Options.
    #[serde(default)]
    pub opts: HttpJobOptions,
}

/// Body of `POST /v1/queues/{queue}/jobs/bulk`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BulkAdd {
    /// The jobs.
    pub jobs: Vec<AddJob>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_options_map_onto_the_builder() {
        let o: HttpJobOptions = serde_json::from_value(serde_json::json!({
            "id": "x", "delay_ms": 1500, "priority": 2, "attempts": 4,
            "backoff": {"type": "exponential", "delay_ms": 100, "max_ms": 5000},
            "lifo": true, "remove_on_complete": true, "remove_on_fail": {"keep_last": 10},
            "deduplicate": {"id": "d", "ttl_ms": 60000}
        }))
        .unwrap();
        let r = JobOptions::from(o).resolve();
        assert_eq!(r.id.as_deref(), Some("x"));
        assert_eq!(r.delay, Some(Duration::from_millis(1500)));
        assert_eq!((r.priority, r.attempts, r.lifo), (2, 4, true));
        assert_eq!(r.remove_on_complete, Removal::Remove);
        assert_eq!(r.remove_on_fail, Removal::KeepLast(10));
        assert!(
            matches!(r.backoff, Some(Backoff::Exponential { max: Some(m), .. }) if m == Duration::from_secs(5))
        );
        assert_eq!(r.deduplication.unwrap().ttl, Duration::from_secs(60));
    }

    #[test]
    fn unknown_option_fields_are_rejected() {
        let r: Result<AddJob, _> =
            serde_json::from_value(serde_json::json!({"data": 1, "opts": {"atempts": 3}}));
        assert!(r.is_err());
        let r: Result<HttpRemoval, _> =
            serde_json::from_value(serde_json::json!({"keep_for_ms": 30000}));
        assert!(matches!(r, Ok(HttpRemoval::KeepFor { keep_for_ms: 30000 })));
    }
}
