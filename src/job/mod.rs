//! Job domain types: [`Job<D>`], [`JobState`], [`Backoff`], [`Progress`].

mod backoff;
mod progress;

pub use backoff::Backoff;
pub use progress::Progress;

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::options::JobOptions;

/// A job id. Wraps `String` so the rest of the crate doesn't pass untyped
/// strings around.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub String);

impl JobId {
    /// Wrap an arbitrary string as a job id.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the inner id as a `&str`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for JobId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for JobId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Lifecycle state of a job.
///
/// Maps directly onto the Redis list/zset the job currently lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobState {
    /// In the `wait` list, ready to be picked up.
    Waiting,
    /// In the `paused` list; resumes when the queue is unpaused.
    Paused,
    /// Held by a worker with an active lock.
    Active,
    /// In the `delayed` zset; eligible once its scheduled time arrives.
    Delayed,
    /// In the `prioritized` zset.
    Prioritized,
    /// Parent job waiting on pending children.
    WaitingChildren,
    /// Finished successfully.
    Completed,
    /// Finished with a terminal error.
    Failed,
}

impl JobState {
    /// Canonical string name, matching Redis key suffixes and stream event
    /// tags (`waiting`, `active`, etc.).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Active => "active",
            Self::Delayed => "delayed",
            Self::Prioritized => "prioritized",
            Self::WaitingChildren => "waiting-children",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// Parse a state name. Accepts the canonical form as well as `"wait"`
    /// (the legacy BullMQ alias for `waiting`).
    ///
    /// Returns `None` for unrecognized input.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "waiting" | "wait" => Self::Waiting,
            "paused" => Self::Paused,
            "active" => Self::Active,
            "delayed" => Self::Delayed,
            "prioritized" => Self::Prioritized,
            "waiting-children" => Self::WaitingChildren,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => return None,
        })
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A typed job record.
///
/// `D` is the user-defined payload type; `R` is the return value captured
/// on success. Both are `serde`-round-trippable.
///
/// Handlers receive `Job<D>` (with `R = ()`) by default. The full record
/// (including `progress`, `return_value`, `failed_reason`, `stacktrace`)
/// is available via [`Queue::get`](crate::Queue::get).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job<D, R = ()> {
    /// Backend-assigned or user-provided id.
    pub id: JobId,
    /// Logical job name (used for named-handler dispatch). Defaults to the
    /// queue name when unspecified.
    pub name: String,
    /// The user payload.
    pub data: D,
    /// Options originally used to submit the job.
    pub opts: JobOptions,
    /// Epoch-ms the job was accepted by the queue.
    pub timestamp_ms: i64,
    /// Epoch-ms the worker started processing, if started.
    pub processed_on_ms: Option<i64>,
    /// Epoch-ms the job finished (either completed or failed).
    pub finished_on_ms: Option<i64>,
    /// Number of attempts made so far.
    pub attempts_made: u32,
    /// Last failure reason, if any.
    pub failed_reason: Option<String>,
    /// Stack trace strings, oldest first.
    pub stacktrace: Vec<String>,
    /// Latest progress marker.
    pub progress: Option<Progress>,
    /// Return value, set on completion.
    pub return_value: Option<R>,
    /// Parent reference, for flow-produced jobs.
    #[cfg(feature = "flow")]
    pub parent: Option<ParentRef>,
    /// Lock token currently held. `None` when not active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_token: Option<String>,
}

impl<D, R> Job<D, R> {
    /// `true` if the job has attempts remaining (i.e. a retry is still
    /// possible).
    pub fn can_retry(&self) -> bool {
        self.attempts_made < self.opts.attempts
    }
}

/// Reference to a parent job for flow-produced children.
///
/// Stored on a child's [`Job::parent`] when the job is enqueued via
/// [`FlowProducer`](crate::FlowProducer). Only available with the `flow`
/// feature.
#[cfg(feature = "flow")]
#[cfg_attr(docsrs, doc(cfg(feature = "flow")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentRef {
    /// Parent's job id.
    pub id: JobId,
    /// Parent's queue name.
    pub queue: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_id_display_and_ctors() {
        assert_eq!(JobId::new("7").to_string(), "7");
        assert_eq!(JobId::from("8").as_str(), "8");
        assert_eq!(JobId::from(String::from("9")).as_str(), "9");
    }

    #[test]
    fn job_id_serde_transparent() {
        let id = JobId::new("42");
        let j = serde_json::to_string(&id).unwrap();
        assert_eq!(j, "\"42\"");
        let back: JobId = serde_json::from_str(&j).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn state_names_roundtrip() {
        for s in [
            JobState::Waiting,
            JobState::Paused,
            JobState::Active,
            JobState::Delayed,
            JobState::Prioritized,
            JobState::WaitingChildren,
            JobState::Completed,
            JobState::Failed,
        ] {
            let name = s.as_str();
            let back = JobState::parse(name).unwrap();
            assert_eq!(back, s, "roundtrip failed for {name}");
        }
    }

    #[test]
    fn state_aliases() {
        assert_eq!(JobState::parse("wait"), Some(JobState::Waiting));
        assert_eq!(JobState::parse("mystery"), None);
    }
}
