//! # oxn-testkit
//!
//! Test harness for `oxn` that watches **every step** of a test, not just
//! the final assertion:
//!
//! - [`Recorder`] — run an action, then snapshot the queue's whole Redis
//!   footprint, check it against [`invariants`], and collect the stream
//!   events, oxn tracing spans and (optionally) raw Redis commands it
//!   caused. Writes a `timeline.md` report on failure or with `OXN_TRACE=1`.
//! - [`snapshot::Snapshot`] — point-in-time dump of every key a queue owns.
//! - [`invariants::check`] — the structural rules oxn's state must obey.
//! - [`capture`] — the `tracing` layer that buckets oxn spans per queue.
//! - [`monitor::MonitorTap`] — a `MONITOR` connection filtered to one queue,
//!   including commands executed inside Lua scripts.
//! - [`env`] — connection helpers (`fresh_queue`, `wait_for`, `teardown`, …).
//!
//! Environment knobs:
//!
//! | Variable | Effect |
//! | --- | --- |
//! | `TEST_REDIS_URL` | Redis to test against (default `redis://127.0.0.1:6379`) |
//! | `OXN_TRACE=1` | Always write timeline reports, not just on failure |
//! | `OXN_MONITOR=1` | Attach a `MONITOR` tap to every recorder |
//! | `OXN_LOG=<filter>` | Also print oxn traces to stderr (`EnvFilter` syntax) |
//! | `OXN_REPORT_DIR` | Where reports go (default `<cargo target dir>/oxn-reports`) |

#![warn(missing_docs)]

pub mod capture;
pub mod env;
pub mod invariants;
pub mod metrics;
pub mod monitor;
pub mod recorder;
pub mod report;
pub mod snapshot;

pub use env::*;
pub use invariants::{Severity, Violation};
pub use recorder::{Recorder, Step, StreamEvent};
pub use snapshot::Snapshot;

/// `Duration::from_millis` shorthand for test readability.
pub fn ms(n: u64) -> std::time::Duration {
    std::time::Duration::from_millis(n)
}

/// `Duration::from_secs` shorthand for test readability.
pub fn secs(n: u64) -> std::time::Duration {
    std::time::Duration::from_secs(n)
}
