//! Internal metric emitters. They compile to nothing without the `metrics`
//! feature, so call sites need no `cfg`. Names: [`crate::metrics`].
#![cfg_attr(not(feature = "metrics"), allow(unused_variables))]

use std::time::Duration;

use crate::Result;

#[cfg(feature = "metrics")]
use crate::metrics as m;

#[cfg(feature = "metrics")]
fn count(name: &'static str, queue: &str, n: u64) {
    ::metrics::counter!(name, "queue" => queue.to_string()).increment(n);
}

pub(crate) fn describe() {
    #[cfg(feature = "metrics")]
    m::describe();
}

pub(crate) fn added(queue: &str, n: usize) {
    #[cfg(feature = "metrics")]
    count(m::ADDED, queue, n as u64);
}

/// A handler run starts. `wait` is set for a first attempt.
pub(crate) fn started(queue: &str, wait: Option<Duration>) {
    #[cfg(feature = "metrics")]
    {
        count(m::STARTED, queue, 1);
        ::metrics::gauge!(m::ACTIVE, "queue" => queue.to_string()).increment(1.0);
        if let Some(w) = wait {
            ::metrics::histogram!(m::JOB_WAIT, "queue" => queue.to_string())
                .record(w.as_secs_f64());
        }
    }
}

/// The handler returned (or was aborted) after `ran`.
pub(crate) fn ran(queue: &str, outcome: &'static str, ran: Duration) {
    #[cfg(feature = "metrics")]
    ::metrics::histogram!(m::JOB_DURATION, "queue" => queue.to_string(), "outcome" => outcome)
        .record(ran.as_secs_f64());
}

/// The job is no longer this process's (whatever its outcome).
pub(crate) fn finished(queue: &str) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!(m::ACTIVE, "queue" => queue.to_string()).decrement(1.0);
}

/// How Redis answered an outcome transition (`to` as logged: `completed`,
/// `failed`, `retry-delayed`, `postponed`, `released`).
pub(crate) fn transition(queue: &str, to: &'static str, r: &Result<()>) {
    #[cfg(feature = "metrics")]
    match r {
        Ok(()) => {
            let name = match to {
                "completed" => m::COMPLETED,
                "failed" => m::FAILED,
                "retry-delayed" => m::RETRIED,
                "postponed" => m::POSTPONED,
                "released" => m::RELEASED,
                _ => return,
            };
            count(name, queue, 1);
        }
        Err(e) if e.is_lock_lost() => count(m::LOCK_LOST, queue, 1),
        Err(_) => {
            ::metrics::counter!(m::TRANSITION_ERRORS, "queue" => queue.to_string(), "to" => to)
                .increment(1)
        }
    }
}

pub(crate) fn stalled(queue: &str, n: usize) {
    #[cfg(feature = "metrics")]
    if n > 0 {
        count(m::STALLED, queue, n as u64);
    }
}

pub(crate) fn transition_retried(queue: &str, script: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(m::TRANSITION_RETRIES, "queue" => queue.to_string(), "script" => script)
        .increment(1);
}

pub(crate) fn fetch_error(queue: &str) {
    #[cfg(feature = "metrics")]
    count(m::FETCH_ERRORS, queue, 1);
}

pub(crate) fn rate_limited(queue: &str) {
    #[cfg(feature = "metrics")]
    count(m::RATE_LIMITED, queue, 1);
}
