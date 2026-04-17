//! Repeatable-job scheduler.
//!
//! A thin wrapper around a [`Queue`] that re-enqueues a template job on a
//! schedule (either a cron expression or a fixed interval). Each run
//! computes the next fire time from the current wall clock and uses
//! [`JobOptions::delay`](crate::JobOptions::delay) to hand the job off to
//! the queue — no separate "scheduler process" is required, workers just
//! pick up delayed jobs as they mature.

use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{Error, Result};
use crate::job::JobId;
use crate::options::JobOptions;
use crate::queue::Queue;

/// How often (or when) a scheduled job fires.
#[derive(Debug, Clone)]
pub enum RepeatOptions {
    /// Cron expression. Uses the `cron` crate (extended syntax).
    Cron(Box<Schedule>),
    /// Fixed interval between runs.
    Every(Duration),
}

impl RepeatOptions {
    /// Parse a cron expression.
    pub fn cron(expr: &str) -> Result<Self> {
        expr.parse::<Schedule>()
            .map(|s| Self::Cron(Box::new(s)))
            .map_err(|e| Error::Config(format!("cron: {e}")))
    }

    /// Fixed interval.
    pub fn every(d: Duration) -> Self {
        Self::Every(d)
    }

    /// Compute the delay from `now` until the next fire.
    pub fn next_delay(&self) -> Duration {
        match self {
            Self::Every(d) => *d,
            Self::Cron(schedule) => {
                let now = Utc::now();
                match schedule.upcoming(Utc).next() {
                    Some(ts) => ts
                        .signed_duration_since(now)
                        .to_std()
                        .unwrap_or(Duration::ZERO),
                    None => Duration::from_secs(60),
                }
            }
        }
    }
}

/// Job scheduler: re-enqueues `template` jobs on `repeat` cadence.
#[derive(Debug)]
pub struct JobScheduler<D>
where
    D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    queue: Queue<D>,
    id: String,
    repeat: RepeatOptions,
    template: D,
    opts: JobOptions,
}

impl<D> JobScheduler<D>
where
    D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Create a scheduler (does not enqueue anything yet).
    pub fn new(
        queue: Queue<D>,
        id: impl Into<String>,
        repeat: RepeatOptions,
        template: D,
    ) -> Self {
        Self {
            queue,
            id: id.into(),
            repeat,
            template,
            opts: JobOptions::default(),
        }
    }

    /// Override the default [`JobOptions`] used when enqueueing each tick.
    #[must_use]
    pub fn with_options(mut self, opts: JobOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Enqueue a single occurrence (the one due next).
    pub async fn enqueue_next(&self) -> Result<JobId> {
        let delay = self.repeat.next_delay();
        let mut opts = self.opts.clone();
        opts.delay = Some(delay);
        opts.id = Some(format!("sched:{}:{}", self.id, chrono_now_ms()));
        self.queue.add(self.template.clone(), opts).await
    }

    /// Run forever, enqueueing on each tick. Consume in a `tokio::spawn`.
    pub async fn run(
        self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        loop {
            let delay = self.repeat.next_delay();
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(delay) => {
                    let mut opts = self.opts.clone();
                    opts.id = Some(format!("sched:{}:{}", self.id, chrono_now_ms()));
                    if let Err(e) = self.queue.add(self.template.clone(), opts).await {
                        tracing::error!(scheduler = %self.id, error = %e, "enqueue failed");
                    }
                }
            }
        }
    }
}

fn chrono_now_ms() -> i64 {
    Utc::now().timestamp_millis()
}
