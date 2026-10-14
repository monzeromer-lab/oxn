//! Reporting failed jobs to error trackers.
//!
//! [`WorkerBuilder::on_failure`](crate::worker::WorkerBuilder::on_failure)
//! registers a hook that receives a [`JobFailure`] for every failed
//! attempt, once Redis has recorded it: hand it to Sentry, Bugsnag,
//! Rollbar, a pager, a log pipeline. With the `sentry` feature, workers
//! also report to Sentry on their own (see [`crate::sentry`]).
//!
//! ```no_run
//! # use oxn::{Queue, Worker, Job, Error};
//! # async fn demo(queue: Queue<String>) -> oxn::Result<()> {
//! let worker = Worker::builder(queue, |job: Job<String>| async move { Ok::<_, Error>(()) })
//!     .on_failure(|f| {
//!         if f.is_final() {
//!             eprintln!("{}/{} ({}) failed for good: {}", f.queue, f.job_id, f.job_name, f.reason);
//!         }
//!     })
//!     .build();
//! # worker.run().await }
//! ```

use std::sync::Arc;

use crate::error::Error;
use crate::job::JobId;

/// Why a job attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FailureKind {
    /// The handler returned an error.
    Error,
    /// The handler panicked.
    Panicked,
    /// The handler returned [`Error::Unrecoverable`]: no retries.
    Unrecoverable,
    /// The payload did not deserialize into the worker's job type.
    Decode,
    /// The handler returned `Ok` while child jobs it added were still
    /// pending (it should return [`Error::WaitingChildren`]).
    PendingChildren,
    /// The job stalled more than `max_stalled` times: its worker died, hung
    /// past its lock, or lost Redis while running it.
    Stalled,
}

impl FailureKind {
    /// A short stable name: `error`, `panicked`, `unrecoverable`, `decode`,
    /// `pending-children` or `stalled`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Panicked => "panicked",
            Self::Unrecoverable => "unrecoverable",
            Self::Decode => "decode",
            Self::PendingChildren => "pending-children",
            Self::Stalled => "stalled",
        }
    }
}

impl std::fmt::Display for FailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failed job attempt, as Redis recorded it.
///
/// Postponements ([`Error::Delayed`], [`Error::RateLimited`],
/// [`Error::WaitingChildren`]) are not failures and are never reported.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct JobFailure<'a> {
    /// The job's queue.
    pub queue: &'a str,
    /// The job's id.
    pub job_id: &'a JobId,
    /// The job's name.
    pub job_name: &'a str,
    /// The attempt that failed, counting from 1.
    pub attempt: u32,
    /// Attempts the job is allowed.
    pub attempts: u32,
    /// What went wrong.
    pub kind: FailureKind,
    /// The failure reason stored with the job.
    pub reason: &'a str,
    /// The error the handler returned (or the decode error). Walk
    /// [`std::error::Error::source`] for its causes. `None` for
    /// [`FailureKind::PendingChildren`] and [`FailureKind::Stalled`].
    pub error: Option<&'a Error>,
    /// When the job runs again, in epoch ms; `None` when it failed for good.
    pub retry_at_ms: Option<i64>,
}

impl JobFailure<'_> {
    /// `true` when the job will not be retried.
    pub fn is_final(&self) -> bool {
        self.retry_at_ms.is_none()
    }
}

/// A registered [`WorkerBuilder::on_failure`](crate::worker::WorkerBuilder::on_failure) hook.
pub(crate) type FailureHook = Arc<dyn Fn(&JobFailure<'_>) + Send + Sync>;

/// What a worker reports, as configured on its builder.
#[derive(Clone, Default)]
pub(crate) struct ReportConfig {
    pub hooks: Vec<FailureHook>,
    #[cfg(feature = "sentry")]
    pub sentry: crate::sentry::SentryOptions,
}

/// A running worker's reporter.
pub(crate) struct Reporter {
    hooks: Vec<FailureHook>,
    /// Set when Sentry reporting is on and a client is bound.
    #[cfg(feature = "sentry")]
    sentry: Option<(crate::sentry::SentryOptions, Arc<sentry_core::Hub>)>,
}

impl Reporter {
    /// Called when the worker starts, in the caller's context: that is
    /// where the application's Sentry hub is found.
    pub(crate) fn new(cfg: ReportConfig) -> Self {
        Self {
            hooks: cfg.hooks,
            #[cfg(feature = "sentry")]
            sentry: crate::sentry::worker_hub(&cfg.sentry).map(|hub| (cfg.sentry, hub)),
        }
    }

    /// Context for one job's run: a Sentry hub carrying the job's tags.
    pub(crate) fn job<'r>(&'r self, job: JobInfo<'r>) -> JobReport<'r> {
        JobReport {
            #[cfg(feature = "sentry")]
            hub: self
                .sentry
                .as_ref()
                .map(|(_, hub)| crate::sentry::job_hub(hub, &job)),
            reporter: self,
            job,
        }
    }

    fn call_hooks(&self, f: &JobFailure<'_>) {
        for hook in &self.hooks {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(f)));
            if r.is_err() {
                tracing::warn!(queue = f.queue, job_id = %f.job_id, "on_failure hook panicked");
            }
        }
    }

    /// A job the stalled scanner failed for good.
    pub(crate) fn stalled(&self, queue: &str, job: &crate::backend::StalledJob) {
        let r = self.job(JobInfo {
            queue,
            id: &job.id,
            name: &job.name,
            attempt: job.attempts_made,
            attempts: job.attempts,
        });
        r.failed(
            FailureKind::Stalled,
            "job stalled more than allowable limit",
            None,
            None,
        );
    }
}

/// The job a [`JobReport`] is about.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JobInfo<'a> {
    pub queue: &'a str,
    pub id: &'a JobId,
    pub name: &'a str,
    pub attempt: u32,
    pub attempts: u32,
}

/// Reporting for one job's run.
pub(crate) struct JobReport<'r> {
    reporter: &'r Reporter,
    job: JobInfo<'r>,
    #[cfg(feature = "sentry")]
    hub: Option<Arc<sentry_core::Hub>>,
}

impl JobReport<'_> {
    /// Run `f` (building the handler's future) with the job's Sentry hub
    /// current, so a panic in it carries the job's tags.
    pub(crate) fn run_sync<T>(&self, f: impl FnOnce() -> T) -> T {
        #[cfg(feature = "sentry")]
        if let Some(hub) = &self.hub {
            return sentry_core::Hub::run(hub.clone(), f);
        }
        f()
    }

    /// The handler's future, polled with the job's Sentry hub current:
    /// panics and anything the handler sends to Sentry carry the job's tags,
    /// and concurrent jobs' scopes never mix.
    pub(crate) fn bind<T: Send + 'static>(
        &self,
        fut: futures::future::BoxFuture<'static, T>,
    ) -> futures::future::BoxFuture<'static, T> {
        #[cfg(feature = "sentry")]
        if let Some(hub) = &self.hub {
            use futures::FutureExt;
            use sentry_core::SentryFutureExt;
            return fut.bind_hub(hub.clone()).boxed();
        }
        fut
    }

    /// Report a failed attempt that Redis recorded.
    pub(crate) fn failed(
        &self,
        kind: FailureKind,
        reason: &str,
        error: Option<&Error>,
        retry_at_ms: Option<i64>,
    ) {
        let f = JobFailure {
            queue: self.job.queue,
            job_id: self.job.id,
            job_name: self.job.name,
            attempt: self.job.attempt,
            attempts: self.job.attempts,
            kind,
            reason,
            error,
            retry_at_ms,
        };
        self.reporter.call_hooks(&f);
        #[cfg(feature = "sentry")]
        if let (Some(hub), Some((opts, _))) = (&self.hub, &self.reporter.sentry) {
            crate::sentry::capture_failure(hub, opts, &f);
        }
    }

    /// Log how a state transition went; report one Redis did not record
    /// for a reason other than a lost lock (the job will run again once its
    /// lock expires).
    pub(crate) fn transition(&self, to: &'static str, r: &crate::Result<()>) {
        crate::worker::log_transition(self.job.queue, to, r);
        #[cfg(feature = "sentry")]
        if let (Err(e), Some(hub), Some((opts, _))) = (r, &self.hub, &self.reporter.sentry) {
            if !e.is_lock_lost() {
                crate::sentry::capture_lost_outcome(hub, opts, &self.job, to, e);
            }
        }
    }
}
