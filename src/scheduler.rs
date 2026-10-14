//! Repeatable jobs: a [`JobScheduler`] adds a job on a cron schedule or a
//! fixed interval.
//!
//! Safe to run in every process of a deployment: each tick is added
//! **once**, however many instances of the same scheduler run. Every
//! instance computes the same fire times — cron times are absolute, and
//! [`RepeatOptions::every`] ticks are aligned to the Unix epoch (every hour
//! fires on the hour, UTC) — and tick `T` of scheduler `id` is the job
//! `sched-{id}-{T}`. Redis keeps each scheduler's latest enqueued tick, so a
//! tick is never added twice, even after its job was deleted by retention.
//!
//! The next tick is always queued ahead as a delayed job (enqueued halfway
//! through the previous interval), so it fires even if every scheduler
//! process stops. Ticks missed while no instance ran are
//! skipped (no burst on restart).
//!
//! Schedulers are recorded in Redis: list them with
//! [`Queue::schedulers`](crate::Queue::schedulers) and remove one with
//! [`Queue::remove_scheduler`](crate::Queue::remove_scheduler) or
//! [`JobScheduler::remove`]. A removed scheduler stops in every running
//! instance; one started afterwards registers it again.

use std::time::Duration;

use chrono::{TimeZone, Utc};
use cron::Schedule;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::backend::{Repeat, SchedulerTick, TickOutcome};
use crate::error::{Error, Result};
use crate::job::JobId;
use crate::options::JobOptions;
use crate::queue::Queue;

/// How often (or when) a scheduled job fires.
#[derive(Debug, Clone)]
pub enum RepeatOptions {
    /// A cron expression (`cron` crate syntax: the first field is seconds).
    Cron {
        /// The expression as given.
        expr: String,
        /// The parsed schedule.
        schedule: Box<Schedule>,
    },
    /// A fixed interval, aligned to the Unix epoch.
    Every(Duration),
}

impl RepeatOptions {
    /// Parse a cron expression, e.g. `"0 */5 * * * *"` (every 5 minutes).
    pub fn cron(expr: &str) -> Result<Self> {
        let schedule = expr
            .parse::<Schedule>()
            .map_err(|e| Error::Config(format!("cron: {e}")))?;
        Ok(Self::Cron {
            expr: expr.to_string(),
            schedule: Box::new(schedule),
        })
    }

    /// A fixed interval (at least 1ms). Ticks fall on multiples of it since
    /// the Unix epoch.
    pub fn every(d: Duration) -> Self {
        Self::Every(d)
    }

    /// The first fire time strictly after `after_ms` (epoch-ms), or `None`
    /// when a cron schedule has no further occurrence.
    pub fn next_after(&self, after_ms: i64) -> Option<i64> {
        match self {
            Self::Every(d) => {
                let every = d.as_millis().max(1) as i64;
                Some((after_ms.div_euclid(every) + 1) * every)
            }
            Self::Cron { schedule, .. } => {
                let after = Utc.timestamp_millis_opt(after_ms).single()?;
                schedule.after(&after).next().map(|t| t.timestamp_millis())
            }
        }
    }

    /// Time from now until the next fire.
    pub fn next_delay(&self) -> Duration {
        let now = now_ms();
        self.next_after(now)
            .map(|t| Duration::from_millis((t - now).max(0) as u64))
            .unwrap_or(Duration::MAX)
    }

    fn validate(&self) -> Result<()> {
        if matches!(self, Self::Every(d) if d.as_millis() == 0) {
            return Err(Error::Config(
                "scheduler interval must be at least 1ms".into(),
            ));
        }
        Ok(())
    }

    fn spec(&self) -> Repeat {
        match self {
            Self::Cron { expr, .. } => Repeat::Cron(expr.clone()),
            Self::Every(d) => Repeat::EveryMs(d.as_millis().max(1) as u64),
        }
    }
}

/// Adds `template` jobs to a queue on a [`RepeatOptions`] cadence.
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

/// After a failed enqueue, how long to wait before trying the same tick
/// again.
const RETRY_AFTER: Duration = Duration::from_millis(200);

impl<D> JobScheduler<D>
where
    D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// A scheduler (nothing is enqueued yet). Instances in different
    /// processes with the same `id` on the same queue are one scheduler.
    pub fn new(queue: Queue<D>, id: impl Into<String>, repeat: RepeatOptions, template: D) -> Self {
        Self {
            queue,
            id: id.into(),
            repeat,
            template,
            opts: JobOptions::default(),
        }
    }

    /// Options for every tick's job (merged with the queue's defaults). Its
    /// id, delay and deduplication are set by the scheduler.
    #[must_use]
    pub fn with_options(mut self, opts: JobOptions) -> Self {
        self.opts = opts;
        self
    }

    /// This scheduler's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Enqueue the next tick (a delayed job, due at the next fire time) and
    /// return its id. Idempotent: another instance having enqueued it
    /// already returns the same id.
    pub async fn enqueue_next(&self) -> Result<JobId> {
        self.validate()?;
        let Some(t) = self.repeat.next_after(now_ms()) else {
            return Err(Error::Config(format!(
                "scheduler `{}`: the cron schedule has no further occurrence",
                self.id
            )));
        };
        match self.tick(t, true).await? {
            TickOutcome::Added(id) | TickOutcome::Exists(id) => Ok(id),
            TickOutcome::Removed => Err(Error::NotFound(format!("scheduler `{}`", self.id))),
        }
    }

    /// Keep the next tick enqueued until `cancel` fires, the scheduler is
    /// removed, or a cron schedule runs out. Run it in a `tokio::spawn` in
    /// as many processes as you like.
    pub async fn run(self, cancel: tokio_util::sync::CancellationToken) -> Result<()> {
        self.validate()?;
        let mut register = true;
        let Some(mut t) = self.repeat.next_after(now_ms()) else {
            tracing::info!(scheduler = %self.id, "cron schedule has no further occurrence");
            return Ok(());
        };
        loop {
            // Enqueue tick t, retrying a transient failure for the same
            // tick: a late tick still runs (at once) rather than vanish.
            loop {
                match self.tick(t, register).await {
                    Ok(TickOutcome::Removed) => {
                        tracing::info!(scheduler = %self.id, "scheduler removed; stopping");
                        return Ok(());
                    }
                    Ok(_) => break,
                    Err(e) => {
                        tracing::warn!(scheduler = %self.id, fire_at_ms = t, error = %e, "enqueue failed; retrying");
                        tokio::select! {
                            _ = cancel.cancelled() => return Ok(()),
                            _ = tokio::time::sleep(RETRY_AFTER) => {}
                        }
                    }
                }
            }
            register = false;
            let Some(next) = self.repeat.next_after(t) else {
                tracing::info!(scheduler = %self.id, "cron schedule has no further occurrence");
                return Ok(());
            };
            // Enqueue the next tick halfway between ticks, not at the
            // instant t fires: many schedulers on one grid (every minute,
            // on the minute) would otherwise all hit Redis exactly when
            // their ticks fall due and workers need it most.
            let mid = t + (next - t) / 2;
            let wait = Duration::from_millis((mid - now_ms()).max(0) as u64);
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(wait) => {}
            }
            // Fell behind (retries during an outage)? Skip ticks already
            // past rather than fire them in a burst.
            let now = now_ms();
            t = if next > now {
                next
            } else {
                match self.repeat.next_after(now) {
                    Some(n) => n,
                    None => return Ok(()),
                }
            };
        }
    }

    /// Remove this scheduler and its pending tick (see
    /// [`Queue::remove_scheduler`]).
    pub async fn remove(&self) -> Result<bool> {
        self.queue.remove_scheduler(&self.id).await
    }

    fn validate(&self) -> Result<()> {
        if self.id.is_empty() {
            return Err(Error::Config("scheduler id must not be empty".into()));
        }
        crate::validate::job_id(&format!("sched-{}-0", self.id))
            .map_err(|e| Error::Config(format!("scheduler id `{}`: {e}", self.id)))?;
        self.repeat.validate()
    }

    async fn tick(&self, fire_at_ms: i64, register: bool) -> Result<TickOutcome> {
        let insert = self.queue.insert(&self.template, self.opts.clone())?;
        let outcome = self
            .queue
            .backend()
            .schedule_tick(
                self.queue.name(),
                SchedulerTick {
                    scheduler_id: self.id.clone(),
                    fire_at_ms,
                    repeat: self.repeat.spec(),
                    register,
                    insert,
                },
            )
            .await?;
        if matches!(outcome, TickOutcome::Added(_)) {
            crate::telemetry::added(self.queue.name(), 1);
        }
        tracing::debug!(scheduler = %self.id, fire_at_ms, ?outcome, "scheduler tick");
        Ok(outcome)
    }
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_are_aligned_to_the_epoch() {
        let r = RepeatOptions::every(Duration::from_millis(200));
        assert_eq!(r.next_after(1_000), Some(1_200), "strictly after");
        assert_eq!(r.next_after(1_199), Some(1_200));
        assert_eq!(r.next_after(1_200), Some(1_400));
        let hourly = RepeatOptions::every(Duration::from_secs(3600));
        let t = hourly.next_after(1_791_939_026_044).unwrap();
        assert_eq!(t % 3_600_000, 0, "on the hour");
        assert!(RepeatOptions::every(Duration::ZERO).validate().is_err());
    }

    #[test]
    fn cron_ticks_are_absolute_and_strictly_after() {
        let r = RepeatOptions::cron("*/5 * * * * *").unwrap();
        let t = r.next_after(1_791_939_026_044).unwrap();
        assert_eq!(t, 1_791_939_030_000);
        assert_eq!(r.next_after(t), Some(t + 5_000));
        assert!(matches!(r.spec(), Repeat::Cron(e) if e == "*/5 * * * * *"));
        // a schedule that is over
        let past = RepeatOptions::cron("0 0 0 1 1 * 2001").unwrap();
        assert_eq!(past.next_after(1_791_939_026_044), None);
    }
}
