//! Report job failures to [Sentry](https://sentry.io) (feature `sentry`).
//!
//! With the feature on, every worker reports to the Sentry client your
//! application set up with `sentry::init`. Nothing else is needed:
//!
//! ```toml
//! [dependencies]
//! oxn = { version = "0.3", features = ["sentry"] }
//! sentry = "0.46"
//! ```
//!
//! ```no_run
//! # use oxn::{Queue, Worker, Job, Error};
//! # async fn demo(queue: Queue<String>) -> oxn::Result<()> {
//! // let _guard = sentry::init("https://key@sentry.example.com/1");  // in main, before workers start
//! let worker = Worker::builder(queue, |job: Job<String>| async move { Ok::<_, Error>(()) })
//!     // optional: the defaults are shown
//!     .sentry(oxn::sentry::SentryOptions::default().report_retries(false))
//!     .build();
//! # worker.run().await }
//! ```
//!
//! # What is reported
//!
//! - **A job that failed for good** (attempts used up, `Error::Unrecoverable`,
//!   a panic on its last attempt, an undecodable payload, `Ok` with children
//!   still pending, or more stalls than `max_stalled`): an event at level
//!   `error`. Failed attempts that will be retried are reported only with
//!   [`SentryOptions::report_retries`], at level `warning`.
//! - **An outcome Redis did not record** (the connection stayed down for
//!   longer than `RedisConfig::transition_retry_for`): an event at level
//!   `error`. The job runs again once its lock expires. Turn this off with
//!   [`SentryOptions::report_lost_outcomes`].
//!
//! The event's exception chain is the handler's error and its `source()`s.
//! For `Error::handler(e)`, the chain starts at `e`, so the issue is titled
//! with your error type. Events carry the tags `oxn.queue`, `oxn.job_name`,
//! `oxn.failure` (`error`, `panicked`, `unrecoverable`, `decode`,
//! `pending-children`, `stalled`) and `oxn.final`, plus an `oxn job`
//! context (id, attempt, attempts, reason, retry time). Issues are grouped
//! per queue and job name: the same error in two job types is two issues.
//!
//! # Handlers run in their own Sentry scope
//!
//! Each job's handler runs with its own Sentry hub, forked from the
//! worker's, with the `oxn.queue` and `oxn.job_name` tags and the `oxn job`
//! context set. Anything the handler reports (`sentry::capture_error`,
//! breadcrumbs, `sentry-tracing` events) carries them. Panics caught by
//! Sentry's panic integration carry them too. Concurrent jobs never see
//! each other's scope.
//!
//! The panic integration reports a panic when it happens, with a backtrace.
//! oxn reports the job failure separately once the job has failed for good,
//! so a job that panics on its last attempt produces two events.
//!
//! # One `sentry-core`
//!
//! oxn depends on `sentry-core` (any version from 0.36 up to, but not
//! including, 0.47), which your `sentry` crate also uses. Both must resolve
//! to **one** copy, or oxn reports into a hub that has no client:
//! `cargo tree -i sentry-core` should print a single version. If it prints
//! two, align them, for example with
//! `cargo update -p sentry-core@<newer> --precise <your version>`. A worker
//! that starts with no Sentry client bound logs a warning saying this.

use std::collections::BTreeMap;
use std::sync::Arc;

use sentry_core::Hub;
use sentry_core::protocol::{Context, Event, Exception, Level, Mechanism, Value};

use crate::error::Error;
use crate::report::{FailureKind, JobFailure, JobInfo};

/// What a worker sends to Sentry. Set with
/// [`WorkerBuilder::sentry`](crate::worker::WorkerBuilder::sentry).
#[derive(Debug, Clone)]
pub struct SentryOptions {
    enabled: bool,
    report_retries: bool,
    report_lost_outcomes: bool,
}

impl Default for SentryOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            report_retries: false,
            report_lost_outcomes: true,
        }
    }
}

impl SentryOptions {
    /// Report nothing from this worker.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Also report failed attempts that will be retried, at level
    /// `warning`. Default `false`: only final failures are reported.
    #[must_use]
    pub fn report_retries(mut self, yes: bool) -> Self {
        self.report_retries = yes;
        self
    }

    /// Report outcomes Redis did not record (default `true`). If you send
    /// oxn's `ERROR` logs to Sentry through `sentry-tracing`, you may turn
    /// this off: the worker logs those at `ERROR` too.
    #[must_use]
    pub fn report_lost_outcomes(mut self, yes: bool) -> Self {
        self.report_lost_outcomes = yes;
        self
    }
}

/// The hub a starting worker reports to: the caller's current hub, if
/// Sentry reporting is on and a client is bound to it.
pub(crate) fn worker_hub(opts: &SentryOptions) -> Option<Arc<Hub>> {
    if !opts.enabled {
        return None;
    }
    let hub = Hub::current();
    if hub.client().is_none() {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                "oxn's `sentry` feature is on but no Sentry client is bound: call \
                 `sentry::init` before starting workers. If you do, your `sentry` crate \
                 uses a different `sentry-core` than oxn (check `cargo tree -i sentry-core`)."
            );
        });
        return None;
    }
    Some(hub)
}

/// A hub for one job, forked from the worker's, with the job's tags.
pub(crate) fn job_hub(worker: &Arc<Hub>, job: &JobInfo<'_>) -> Arc<Hub> {
    let hub = Arc::new(Hub::new_from_top(worker));
    hub.configure_scope(|scope| {
        scope.set_tag("oxn.queue", job.queue);
        scope.set_tag("oxn.job_name", job.name);
        scope.set_context("oxn job", job_context(job, None, None));
    });
    hub
}

fn job_context(job: &JobInfo<'_>, reason: Option<&str>, retry_at_ms: Option<i64>) -> Context {
    let mut m = BTreeMap::new();
    m.insert("queue".to_string(), Value::from(job.queue));
    m.insert("id".to_string(), Value::from(job.id.as_str()));
    m.insert("name".to_string(), Value::from(job.name));
    m.insert("attempt".to_string(), Value::from(job.attempt));
    m.insert("attempts".to_string(), Value::from(job.attempts));
    if let Some(r) = reason {
        m.insert("reason".to_string(), Value::from(r));
    }
    if let Some(t) = retry_at_ms {
        m.insert("retry_at_ms".to_string(), Value::from(t));
    }
    Context::Other(m)
}

/// The exception chain for `err`, starting at the user's own error for
/// `Error::Handler`.
fn error_event(err: &Error) -> Event<'static> {
    match err {
        Error::Handler(inner) => sentry_core::event_from_error(inner.as_ref()),
        other => sentry_core::event_from_error(other),
    }
}

/// Report a failed attempt.
pub(crate) fn capture_failure(hub: &Hub, opts: &SentryOptions, f: &JobFailure<'_>) {
    if !f.is_final() && !opts.report_retries {
        return;
    }
    let mut event = match f.error {
        Some(err) => error_event(err),
        None => Event {
            exception: vec![Exception {
                ty: match f.kind {
                    FailureKind::Stalled => "JobStalled".into(),
                    FailureKind::PendingChildren => "ChildrenPending".into(),
                    other => other.as_str().into(),
                },
                value: Some(f.reason.to_string()),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        },
    };
    if let Some(top) = event.exception.values.last_mut() {
        top.mechanism = Some(Mechanism {
            ty: "oxn".into(),
            handled: Some(f.kind != FailureKind::Panicked),
            ..Default::default()
        });
    }
    let job = JobInfo {
        queue: f.queue,
        id: f.job_id,
        name: f.job_name,
        attempt: f.attempt,
        attempts: f.attempts,
    };
    event.level = if f.is_final() {
        Level::Error
    } else {
        Level::Warning
    };
    event.message = Some(if f.is_final() {
        format!("job {}/{} failed: {}", f.queue, f.job_name, f.reason)
    } else {
        format!(
            "job {}/{} failed attempt {} of {}, will retry: {}",
            f.queue, f.job_name, f.attempt, f.attempts, f.reason
        )
    });
    event.transaction = Some(format!("{}/{}", f.queue, f.job_name));
    event.fingerprint = vec![
        "{{ default }}".into(),
        f.queue.to_string().into(),
        f.job_name.to_string().into(),
    ]
    .into();
    event.tags.insert("oxn.queue".into(), f.queue.into());
    event.tags.insert("oxn.job_name".into(), f.job_name.into());
    event
        .tags
        .insert("oxn.failure".into(), f.kind.as_str().into());
    event
        .tags
        .insert("oxn.final".into(), f.is_final().to_string());
    // The job's scope already holds an `oxn job` context, and a scope's
    // contexts win over an event's: set the full one on the scope.
    let ctx = job_context(&job, Some(f.reason), f.retry_at_ms);
    hub.with_scope(
        |scope| scope.set_context("oxn job", ctx),
        || hub.capture_event(event),
    );
}

/// Report an outcome Redis did not record.
pub(crate) fn capture_lost_outcome(
    hub: &Hub,
    opts: &SentryOptions,
    job: &JobInfo<'_>,
    to: &str,
    err: &Error,
) {
    if !opts.report_lost_outcomes {
        return;
    }
    let mut event = sentry_core::event_from_error(err);
    event.level = Level::Error;
    event.message = Some(format!(
        "oxn could not record job {}/{} as {to}: {err}",
        job.queue, job.name
    ));
    event.transaction = Some(format!("{}/{}", job.queue, job.name));
    event.fingerprint = vec![
        "oxn-lost-outcome".into(),
        job.queue.to_string().into(),
        to.to_string().into(),
    ]
    .into();
    event.tags.insert("oxn.queue".into(), job.queue.into());
    event.tags.insert("oxn.job_name".into(), job.name.into());
    event.tags.insert("oxn.lost_outcome".into(), to.into());
    hub.capture_event(event);
}
