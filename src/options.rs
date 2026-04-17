//! Typed configuration structs with builder methods.
//!
//! Every knob a BullMQ user reaches for via an untyped options bag lives
//! here as a dedicated field. Builders consume `self` so chains are
//! non-owning-reference free and construction is `const`-friendly where
//! possible.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::job::Backoff;

/// Construction-time options for a [`Queue`](crate::Queue).
///
/// Start with [`QueueOptions::default`] and chain the `with_*` methods,
/// or use [`crate::queue::QueueBuilder::options`] to pass an existing bag.
#[derive(Debug, Clone)]
pub struct QueueOptions {
    /// Redis key prefix. BullMQ defaults to `"bull"`; we default to `"oxn"`.
    pub prefix: String,
    /// Default job options, applied when [`JobOptions::default()`] is used.
    pub default_job_options: JobOptions,
    /// Max length of the `events` Redis Stream (trimmed on every XADD).
    pub max_stream_length: usize,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            prefix: "oxn".to_string(),
            default_job_options: JobOptions::default(),
            max_stream_length: 10_000,
        }
    }
}

impl QueueOptions {
    /// Set the Redis key prefix.
    #[must_use]
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Override the default job options.
    #[must_use]
    pub fn default_job_options(mut self, opts: JobOptions) -> Self {
        self.default_job_options = opts;
        self
    }

    /// Cap on the events stream length.
    #[must_use]
    pub fn max_stream_length(mut self, n: usize) -> Self {
        self.max_stream_length = n;
        self
    }
}

/// Per-job submission options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobOptions {
    /// Optional explicit job id. When omitted, the backend assigns one.
    pub id: Option<String>,
    /// Optional human-readable name for the job (used for routing handlers
    /// when the worker is configured with named handlers).
    pub name: Option<String>,
    /// Delay before the job becomes eligible for execution.
    pub delay: Option<Duration>,
    /// Priority — lower number = higher priority. `0` means unprioritized
    /// (FIFO via `wait`).
    pub priority: u32,
    /// Total number of attempts before the job is marked failed.
    pub attempts: u32,
    /// Backoff strategy applied between attempts.
    pub backoff: Option<Backoff>,
    /// If `Some`, the job is deduplicated against this id for the given TTL.
    pub deduplication: Option<Dedup>,
    /// Remove the job from Redis after it completes.
    pub remove_on_complete: Removal,
    /// Remove the job from Redis after it fails permanently.
    pub remove_on_fail: Removal,
    /// Use LIFO ordering when pushed to `wait` (default: false = FIFO).
    pub lifo: bool,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            delay: None,
            priority: 0,
            attempts: 1,
            backoff: None,
            deduplication: None,
            remove_on_complete: Removal::Keep,
            remove_on_fail: Removal::Keep,
            lifo: false,
        }
    }
}

impl JobOptions {
    /// Equivalent to [`Self::default`]. Useful for chainable construction:
    ///
    /// ```
    /// use oxn::JobOptions;
    /// let opts = JobOptions::new().attempts(5);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign an explicit id. If omitted, the backend auto-increments one.
    ///
    /// Explicit ids are useful for idempotent producers — adding the same
    /// id twice without [`Self::deduplicate`] still overwrites the job
    /// hash, so prefer deduplication when collisions are possible.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Job name used for named-handler routing.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Postpone execution by `delay`.
    #[must_use]
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set priority (lower = higher priority).
    #[must_use]
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Number of attempts before giving up.
    #[must_use]
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts.max(1);
        self
    }

    /// Configure retry backoff.
    #[must_use]
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = Some(backoff);
        self
    }

    /// Deduplicate adds within `ttl`.
    #[must_use]
    pub fn deduplicate(mut self, id: impl Into<String>, ttl: Duration) -> Self {
        self.deduplication = Some(Dedup { id: id.into(), ttl });
        self
    }

    /// Clean up the job hash when it completes.
    #[must_use]
    pub fn remove_on_complete(mut self, r: Removal) -> Self {
        self.remove_on_complete = r;
        self
    }

    /// Clean up the job hash when it fails permanently.
    #[must_use]
    pub fn remove_on_fail(mut self, r: Removal) -> Self {
        self.remove_on_fail = r;
        self
    }

    /// Use LIFO ordering.
    #[must_use]
    pub fn lifo(mut self, yes: bool) -> Self {
        self.lifo = yes;
        self
    }
}

/// Deduplication configuration.
///
/// Attached to a [`JobOptions`] via [`JobOptions::deduplicate`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dedup {
    /// Dedup key — multiple submissions with the same id collapse to one.
    pub id: String,
    /// TTL for the dedup flag.
    pub ttl: Duration,
}

/// Retention policy for finished jobs.
///
/// Applied after completion or terminal failure via
/// [`JobOptions::remove_on_complete`] / [`JobOptions::remove_on_fail`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Removal {
    /// Keep the job hash indefinitely.
    Keep,
    /// Remove immediately.
    Remove,
    /// Keep the most recent `n` jobs; remove the rest.
    KeepLast(usize),
    /// Keep jobs younger than the given duration.
    KeepFor(Duration),
}

/// Construction-time options for a [`Worker`](crate::Worker).
///
/// Most users start with [`WorkerOptions::default`] and tune only
/// `concurrency` + `lock_duration`. See the individual field docs for the
/// full knob list.
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Max concurrent in-flight jobs for this worker.
    pub concurrency: usize,
    /// Job lock duration. Must be renewed before expiry.
    pub lock_duration: Duration,
    /// How often to renew locks (typically half `lock_duration`).
    pub lock_renew_interval: Duration,
    /// How often to scan for stalled jobs.
    pub stalled_interval: Duration,
    /// Max number of times a job can be marked stalled before being failed.
    pub max_stalled: u32,
    /// How long to BLOCK a blocking pop before falling back to non-blocking.
    pub drain_delay: Duration,
    /// Run the stalled-job scanner in this worker. Set to `false` if you run
    /// a dedicated housekeeper.
    pub run_stalled_scanner: bool,
    /// Optional rate limit: at most `max` jobs per `duration`.
    pub rate_limit: Option<RateLimit>,
    /// How long to wait for in-flight jobs to finish on shutdown.
    pub shutdown_timeout: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            concurrency: 1,
            lock_duration: Duration::from_secs(30),
            lock_renew_interval: Duration::from_secs(15),
            stalled_interval: Duration::from_secs(30),
            max_stalled: 1,
            drain_delay: Duration::from_secs(5),
            run_stalled_scanner: true,
            rate_limit: None,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl WorkerOptions {
    /// Equivalent to [`Self::default`]; chainable.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set concurrency (floored at 1).
    #[must_use]
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Set the lock duration.
    #[must_use]
    pub fn lock_duration(mut self, d: Duration) -> Self {
        self.lock_duration = d;
        self
    }

    /// Set the lock renewal interval (half of `lock_duration` is typical).
    #[must_use]
    pub fn lock_renew_interval(mut self, d: Duration) -> Self {
        self.lock_renew_interval = d;
        self
    }

    /// Set the stalled-scanner cadence.
    #[must_use]
    pub fn stalled_interval(mut self, d: Duration) -> Self {
        self.stalled_interval = d;
        self
    }

    /// Set the max stall count before a job is force-failed.
    #[must_use]
    pub fn max_stalled(mut self, n: u32) -> Self {
        self.max_stalled = n;
        self
    }

    /// Set the blocking fetch drain delay.
    #[must_use]
    pub fn drain_delay(mut self, d: Duration) -> Self {
        self.drain_delay = d;
        self
    }

    /// Toggle the stalled-job scanner.
    #[must_use]
    pub fn run_stalled_scanner(mut self, yes: bool) -> Self {
        self.run_stalled_scanner = yes;
        self
    }

    /// Apply a rate limit to this worker's fetches.
    #[must_use]
    pub fn rate_limit(mut self, max: u32, per: Duration) -> Self {
        self.rate_limit = Some(RateLimit { max, per });
        self
    }

    /// Set the graceful-shutdown drain timeout.
    #[must_use]
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = d;
        self
    }
}

/// Rate-limit configuration.
///
/// Applied per worker via [`WorkerOptions::rate_limit`]. The worker blocks
/// for up to `per` after issuing `max` jobs in a window.
#[derive(Debug, Clone, Copy)]
pub struct RateLimit {
    /// Max tokens issued per `per` window.
    pub max: u32,
    /// Window length.
    pub per: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_options_defaults_are_safe() {
        let o = JobOptions::default();
        assert!(o.id.is_none());
        assert!(o.name.is_none());
        assert!(o.delay.is_none());
        assert_eq!(o.priority, 0);
        assert_eq!(o.attempts, 1);
        assert!(o.backoff.is_none());
        assert!(matches!(o.remove_on_complete, Removal::Keep));
        assert!(matches!(o.remove_on_fail, Removal::Keep));
        assert!(!o.lifo);
    }

    #[test]
    fn job_options_builder_chains() {
        let o = JobOptions::new()
            .id("abc")
            .name("task")
            .delay(Duration::from_secs(5))
            .priority(3)
            .attempts(7)
            .backoff(crate::job::Backoff::Fixed {
                delay: Duration::from_millis(500),
            })
            .deduplicate("dedup-1", Duration::from_secs(60))
            .remove_on_complete(Removal::KeepLast(10))
            .remove_on_fail(Removal::Remove)
            .lifo(true);
        assert_eq!(o.id.as_deref(), Some("abc"));
        assert_eq!(o.name.as_deref(), Some("task"));
        assert_eq!(o.delay, Some(Duration::from_secs(5)));
        assert_eq!(o.priority, 3);
        assert_eq!(o.attempts, 7);
        assert!(o.backoff.is_some());
        assert!(o.deduplication.is_some());
        assert!(matches!(o.remove_on_complete, Removal::KeepLast(10)));
        assert!(matches!(o.remove_on_fail, Removal::Remove));
        assert!(o.lifo);
    }

    #[test]
    fn attempts_enforces_minimum_of_one() {
        assert_eq!(JobOptions::new().attempts(0).attempts, 1);
    }

    #[test]
    fn queue_options_builder() {
        let q = QueueOptions::default()
            .prefix("foo")
            .max_stream_length(1234);
        assert_eq!(q.prefix, "foo");
        assert_eq!(q.max_stream_length, 1234);
    }

    #[test]
    fn worker_options_builder() {
        let w = WorkerOptions::new()
            .concurrency(8)
            .lock_duration(Duration::from_secs(60))
            .lock_renew_interval(Duration::from_secs(30))
            .stalled_interval(Duration::from_secs(45))
            .max_stalled(5)
            .drain_delay(Duration::from_secs(2))
            .run_stalled_scanner(false)
            .rate_limit(100, Duration::from_secs(1))
            .shutdown_timeout(Duration::from_secs(5));
        assert_eq!(w.concurrency, 8);
        assert_eq!(w.lock_duration, Duration::from_secs(60));
        assert_eq!(w.max_stalled, 5);
        assert!(!w.run_stalled_scanner);
        assert!(w.rate_limit.is_some());
    }

    #[test]
    fn worker_concurrency_floor_is_one() {
        assert_eq!(WorkerOptions::new().concurrency(0).concurrency, 1);
    }

    #[test]
    fn removal_serde() {
        for r in [
            Removal::Keep,
            Removal::Remove,
            Removal::KeepLast(5),
            Removal::KeepFor(Duration::from_secs(120)),
        ] {
            let j = serde_json::to_string(&r).unwrap();
            let back: Removal = serde_json::from_str(&j).unwrap();
            match (r, back) {
                (Removal::Keep, Removal::Keep) => {}
                (Removal::Remove, Removal::Remove) => {}
                (Removal::KeepLast(a), Removal::KeepLast(b)) => assert_eq!(a, b),
                (Removal::KeepFor(a), Removal::KeepFor(b)) => assert_eq!(a, b),
                _ => panic!("mismatch"),
            }
        }
    }

    #[test]
    fn job_options_serde_roundtrip() {
        let o = JobOptions::new()
            .name("send-email")
            .attempts(5)
            .delay(Duration::from_millis(250))
            .deduplicate("abc", Duration::from_secs(30));
        let j = serde_json::to_string(&o).unwrap();
        let back: JobOptions = serde_json::from_str(&j).unwrap();
        assert_eq!(back.name.as_deref(), Some("send-email"));
        assert_eq!(back.attempts, 5);
        assert_eq!(back.delay, Some(Duration::from_millis(250)));
        assert_eq!(back.deduplication.unwrap().id, "abc");
    }
}
