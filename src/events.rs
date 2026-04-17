//! Queue events.
//!
//! BullMQ exposes events two ways: EventEmitter for local subscribers, and a
//! Redis Stream for cross-process subscribers. `oxn` mirrors both, but the
//! local side is a [`tokio::sync::broadcast`] and the cross-process side is a
//! [`futures::Stream`] of [`Event`] values — not the dynamic name dispatch
//! of JavaScript EventEmitters.

use std::pin::Pin;
use std::sync::Arc;

use futures::stream::Stream;
use serde::{Deserialize, Serialize};

use crate::backend::Backend;
use crate::error::Result;
use crate::job::JobId;

/// A lifecycle event emitted on the queue's Redis Stream.
///
/// Variant names match BullMQ's stream entries so mixed-language deployments
/// can subscribe from either side. Use [`QueueEvents::stream`] to consume
/// these.
///
/// # Examples
///
/// ```no_run
/// use futures::StreamExt;
/// use oxn::{Queue, Event};
///
/// # async fn run(queue: Queue<serde_json::Value>) -> oxn::Result<()> {
/// let mut events = queue.events().stream().await?;
/// while let Some(Ok(event)) = events.next().await {
///     if let Event::Failed { id, reason, .. } = event {
///         eprintln!("{id} failed: {reason}");
///     }
/// }
/// # Ok(()) }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Event {
    /// Emitted immediately after a job hash is written.
    Added {
        /// The job id assigned by the backend (or the user, if explicit).
        id: JobId,
        /// The job's logical name.
        name: String,
    },
    /// Job is in `wait` (or `paused`) and ready for a worker.
    Waiting {
        /// The affected job id.
        id: JobId,
    },
    /// A worker picked up the job and acquired its lock.
    Active {
        /// The affected job id.
        id: JobId,
        /// The state the job came from (e.g. `"waiting"`, `"delayed"`).
        prev: Option<String>,
    },
    /// Handler returned `Ok`; job moved to the `completed` zset.
    Completed {
        /// The affected job id.
        id: JobId,
        /// JSON-encoded return value, if any.
        return_value_json: Option<String>,
    },
    /// Handler failed permanently (retries exhausted or `Unrecoverable`).
    Failed {
        /// The affected job id.
        id: JobId,
        /// Human-readable reason from the handler's `Error::Display`.
        reason: String,
        /// How many attempts had been made at failure time.
        attempts_made: u32,
    },
    /// Job was scheduled (newly, or via retry backoff, or `Error::Delayed`).
    Delayed {
        /// The affected job id.
        id: JobId,
        /// Absolute epoch-ms when the job becomes eligible.
        delay_ms: i64,
    },
    /// Handler reported progress via [`crate::Progress`].
    Progress {
        /// The affected job id.
        id: JobId,
        /// JSON-encoded progress value (percentage or arbitrary object).
        progress_json: String,
    },
    /// Lock was lost before renewal; the scanner requeued the job.
    Stalled {
        /// The affected job id.
        id: JobId,
    },
    /// [`crate::Queue::pause`] was called.
    Paused,
    /// [`crate::Queue::resume`] was called.
    Resumed,
    /// Queue was drained.
    Drained,
    /// Job hash and auxiliary keys were removed.
    Removed {
        /// The affected job id.
        id: JobId,
    },
    /// A second add for an existing dedup id was rejected.
    Duplicated {
        /// The affected (existing) job id.
        id: JobId,
        /// The dedup key that collided.
        dedup_id: String,
    },
    /// The backend swallowed a duplicate submission (equivalent to
    /// [`Event::Duplicated`] but emitted before a job is materialized).
    Deduplicated {
        /// The existing job id the new submission collapsed into.
        id: JobId,
        /// The dedup key.
        dedup_id: String,
    },
    /// A cleanup operation removed this many jobs.
    Cleaned {
        /// Number of jobs removed.
        count: u64,
    },
    /// Unrecognized event tag — forward compatibility for new variants.
    #[serde(other)]
    Unknown,
}

/// Subscriber handle for queue-level events.
///
/// Backed by [`Backend::subscribe`]. A single `QueueEvents` can be cloned
/// cheaply (`Arc`-cheap); each call to [`Self::stream`] returns an independent
/// cursor.
#[derive(Clone, Debug)]
pub struct QueueEvents {
    queue: String,
    backend: Arc<dyn Backend>,
}

impl QueueEvents {
    #[doc(hidden)]
    pub fn new(queue: impl Into<String>, backend: Arc<dyn Backend>) -> Self {
        Self {
            queue: queue.into(),
            backend,
        }
    }

    /// Name of the queue being observed.
    pub fn queue_name(&self) -> &str {
        &self.queue
    }

    /// Open a new event stream.
    pub async fn stream(&self) -> Result<EventStream> {
        self.backend.subscribe(&self.queue).await
    }
}

/// Stream type yielded by [`QueueEvents::stream`].
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event>> + Send + 'static>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(e: &Event) -> Event {
        let j = serde_json::to_string(e).unwrap();
        serde_json::from_str(&j).unwrap()
    }

    #[test]
    fn added_roundtrips() {
        let e = Event::Added {
            id: JobId::new("42"),
            name: "send-email".into(),
        };
        assert_eq!(roundtrip(&e), e);
    }

    #[test]
    fn failed_roundtrips_with_reason() {
        let e = Event::Failed {
            id: JobId::new("7"),
            reason: "handler panicked".into(),
            attempts_made: 3,
        };
        assert_eq!(roundtrip(&e), e);
    }

    #[test]
    fn progress_roundtrips() {
        let e = Event::Progress {
            id: JobId::new("1"),
            progress_json: "42.5".into(),
        };
        assert_eq!(roundtrip(&e), e);
    }

    #[test]
    fn unknown_tag_maps_to_unknown_variant() {
        let raw = r#"{"type":"freshly-invented-event"}"#;
        let e: Event = serde_json::from_str(raw).unwrap();
        assert_eq!(e, Event::Unknown);
    }
}
