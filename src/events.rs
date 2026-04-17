//! Queue events.
//!
//! BullMQ exposes events two ways: EventEmitter for local subscribers, and a
//! Redis Stream for cross-process subscribers. `oxn` mirrors both, but the
//! local side is a [`tokio::sync::broadcast`] and the cross-process side is a
//! [`Stream`](futures::Stream) of [`Event`] values — not the dynamic name
//! dispatch of JavaScript EventEmitters.

use std::pin::Pin;
use std::sync::Arc;

use futures::stream::Stream;
use serde::{Deserialize, Serialize};

use crate::backend::Backend;
use crate::error::Result;
use crate::job::JobId;

/// Discrete event variants. Names match BullMQ's stream entries so mixed
/// deployments can subscribe from either side.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Event {
    Added {
        id: JobId,
        name: String,
    },
    Waiting {
        id: JobId,
    },
    Active {
        id: JobId,
        prev: Option<String>,
    },
    Completed {
        id: JobId,
        return_value_json: Option<String>,
    },
    Failed {
        id: JobId,
        reason: String,
        attempts_made: u32,
    },
    Delayed {
        id: JobId,
        delay_ms: i64,
    },
    Progress {
        id: JobId,
        progress_json: String,
    },
    Stalled {
        id: JobId,
    },
    Paused,
    Resumed,
    Drained,
    Removed {
        id: JobId,
    },
    Duplicated {
        id: JobId,
        dedup_id: String,
    },
    Deduplicated {
        id: JobId,
        dedup_id: String,
    },
    Cleaned {
        count: u64,
    },
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
