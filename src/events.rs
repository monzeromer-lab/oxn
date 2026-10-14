//! Queue events.
//!
//! Every lifecycle event is written to the queue's Redis Stream, so any
//! process can follow it. [`QueueEvents::stream`] yields typed [`Event`]
//! values as a [`futures::Stream`] (and [`QueueEvents::records`] adds each
//! entry's id, for consumers that resume), instead of the dynamic name
//! dispatch of BullMQ's EventEmitter.

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
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
        /// The state it came from when it re-entered the queue — `"active"`
        /// for a job a shutting-down worker released. `None` for new jobs.
        prev: Option<String>,
    },
    /// A parent is waiting for its children: added by a flow, parked by a
    /// handler that returned `Error::WaitingChildren`, or retried while
    /// children are still pending.
    WaitingChildren {
        /// The affected job id.
        id: JobId,
        /// The state it came from (`"active"`, `"failed"`), `None` when new.
        prev: Option<String>,
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
        /// Epoch-ms when the job becomes eligible (absolute, not a delay).
        due_at_ms: i64,
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
    /// An add with an explicit job id that already exists returned the
    /// existing job instead of creating or overwriting one.
    Duplicated {
        /// The existing job id.
        id: JobId,
    },
    /// An add carrying a deduplication id that is still live collapsed
    /// into the job that holds it (see `JobOptions::deduplicate`).
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

    /// Every event emitted from **this call** on.
    ///
    /// The cursor is pinned when `stream()` returns, not on first poll, so
    /// events emitted in between are delivered. Connection errors surface
    /// as `Err` items; the stream keeps going (reconnecting with backoff,
    /// from where it left off) until it is dropped.
    pub async fn stream(&self) -> Result<EventStream> {
        let records = self.records(StreamStart::Now).await?;
        Ok(Box::pin(records.map(|r| r.map(|r| r.event))))
    }

    /// Events with their stream ids, starting at `from`. Persist the last
    /// [`EventRecord::id`] you processed and resume later with
    /// [`StreamStart::After`] to get exactly the events you missed (as long
    /// as they're still within the stream's `max_stream_length`).
    pub async fn records(&self, from: StreamStart) -> Result<RecordStream> {
        self.backend.subscribe(&self.queue, from).await
    }
}

/// Where a subscription starts reading the queue's event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamStart {
    /// Only events emitted after the subscription is created.
    Now,
    /// Everything still in the stream (it is capped at `max_stream_length`).
    Beginning,
    /// Everything after this stream entry id (exclusive) — resume point.
    After(String),
}

/// One event plus its position in the stream.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    /// Stream entry id (`<ms>-<seq>`), strictly increasing. Pass it to
    /// [`StreamStart::After`] to resume.
    pub id: String,
    /// Epoch-ms the event was recorded (the id's time component).
    pub at_ms: i64,
    /// The event.
    pub event: Event,
}

/// Stream type yielded by [`QueueEvents::stream`].
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event>> + Send + 'static>>;

/// Stream type yielded by [`QueueEvents::records`].
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<EventRecord>> + Send + 'static>>;

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
