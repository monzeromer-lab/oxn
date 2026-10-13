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
    /// Queue-wide defaults: every field a job leaves unset is taken from
    /// here (see [`JobOptions::resolve_with`]).
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

    /// Configure the [`Removal`] policy applied to every job in this queue
    /// when it completes successfully. Equivalent to setting
    /// [`JobOptions::remove_on_complete`] on the queue's default job
    /// options.
    #[must_use]
    pub fn remove_on_complete(mut self, r: Removal) -> Self {
        self.default_job_options.remove_on_complete = Some(r);
        self
    }

    /// Configure the [`Removal`] policy applied to every job in this queue
    /// when it fails permanently. Equivalent to setting
    /// [`JobOptions::remove_on_fail`] on the queue's default job options.
    #[must_use]
    pub fn remove_on_fail(mut self, r: Removal) -> Self {
        self.default_job_options.remove_on_fail = Some(r);
        self
    }
}

/// Per-job submission options.
///
/// Every field is optional: `None` means "not set here", so the queue's
/// [`QueueOptions::default_job_options`] — and after that the built-in
/// default — applies. An explicit value always wins, even one that equals
/// the built-in default (`.priority(0)` overrides a queue default of `4`).
/// [`Self::resolve_with`] performs that merge and yields the
/// [`ResolvedJobOptions`] stored with the job.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct JobOptions {
    /// Optional explicit job id. When omitted, the backend assigns one.
    /// Never inherited from queue defaults.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Logical job name. Defaults to the queue name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Delay before the job becomes eligible for execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay: Option<Duration>,
    /// Priority — lower number = higher priority. `0` means unprioritized
    /// (FIFO via `wait`). Built-in default: `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    /// Total number of attempts before the job is marked failed.
    /// Built-in default: `1`; values below 1 are raised to 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    /// Backoff strategy applied between attempts. Built-in default: none
    /// (retry immediately).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<Backoff>,
    /// If set, the job is deduplicated against this id for the given TTL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduplication: Option<Dedup>,
    /// Retention once the job completes. Built-in default: [`Removal::Keep`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_on_complete: Option<Removal>,
    /// Retention once the job fails permanently. Built-in default:
    /// [`Removal::Keep`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_on_fail: Option<Removal>,
    /// Push to the front of `wait` instead of the back. Built-in default:
    /// `false` (FIFO).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifo: Option<bool>,
}

impl JobOptions {
    /// Equivalent to [`Self::default`]: nothing set. Useful for chainable
    /// construction:
    ///
    /// ```
    /// use oxn::JobOptions;
    /// let opts = JobOptions::new().attempts(5);
    /// assert_eq!(opts.attempts, Some(5));
    /// assert_eq!(opts.priority, None); // left to the queue default
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign an explicit id. If omitted, the backend auto-increments one.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Logical job name.
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

    /// Set priority (lower = higher priority; `0` = unprioritized).
    #[must_use]
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Number of attempts before giving up (floored at 1).
    #[must_use]
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = Some(attempts.max(1));
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

    /// Retention once the job completes.
    #[must_use]
    pub fn remove_on_complete(mut self, r: Removal) -> Self {
        self.remove_on_complete = Some(r);
        self
    }

    /// Retention once the job fails permanently.
    #[must_use]
    pub fn remove_on_fail(mut self, r: Removal) -> Self {
        self.remove_on_fail = Some(r);
        self
    }

    /// Use LIFO ordering.
    #[must_use]
    pub fn lifo(mut self, yes: bool) -> Self {
        self.lifo = Some(yes);
        self
    }

    /// Merge with `defaults` field by field: a value set here wins,
    /// otherwise the default's value, otherwise the built-in default.
    /// `id` is never taken from `defaults` (it would collapse every job
    /// into one).
    ///
    /// ```
    /// use oxn::JobOptions;
    /// use oxn::options::Removal;
    ///
    /// let queue_defaults = JobOptions::new().attempts(5).remove_on_complete(Removal::Remove);
    /// let r = JobOptions::new().attempts(2).resolve_with(&queue_defaults);
    /// assert_eq!(r.attempts, 2);                         // explicit wins
    /// assert_eq!(r.remove_on_complete, Removal::Remove); // inherited
    /// assert_eq!(r.priority, 0);                         // built-in
    /// ```
    pub fn resolve_with(&self, defaults: &JobOptions) -> ResolvedJobOptions {
        let builtin = ResolvedJobOptions::default();
        ResolvedJobOptions {
            id: self.id.clone(),
            name: self.name.clone().or_else(|| defaults.name.clone()),
            delay: self.delay.or(defaults.delay),
            priority: self
                .priority
                .or(defaults.priority)
                .unwrap_or(builtin.priority),
            attempts: self
                .attempts
                .or(defaults.attempts)
                .unwrap_or(builtin.attempts)
                .max(1),
            backoff: self.backoff.or(defaults.backoff),
            deduplication: self
                .deduplication
                .clone()
                .or_else(|| defaults.deduplication.clone()),
            remove_on_complete: self
                .remove_on_complete
                .or(defaults.remove_on_complete)
                .unwrap_or(builtin.remove_on_complete),
            remove_on_fail: self
                .remove_on_fail
                .or(defaults.remove_on_fail)
                .unwrap_or(builtin.remove_on_fail),
            lifo: self.lifo.or(defaults.lifo).unwrap_or(builtin.lifo),
        }
    }

    /// Resolve against built-in defaults only.
    pub fn resolve(&self) -> ResolvedJobOptions {
        self.resolve_with(&JobOptions::default())
    }
}

/// The options a job actually runs with, after merging per-job options,
/// queue defaults and built-in defaults (see [`JobOptions::resolve_with`]).
///
/// This is what is stored with the job and what [`Job::opts`](crate::Job)
/// exposes. Its serialized shape matches 0.2's `JobOptions`, so job
/// records written by 0.2 still deserialize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ResolvedJobOptions {
    /// Explicit job id, if one was given.
    pub id: Option<String>,
    /// Logical job name, if one was given (the job's `name` falls back to
    /// the queue name).
    pub name: Option<String>,
    /// Delay requested at submission.
    pub delay: Option<Duration>,
    /// Priority — lower = higher; `0` = unprioritized.
    pub priority: u32,
    /// Total attempts allowed (≥ 1).
    pub attempts: u32,
    /// Backoff between attempts.
    pub backoff: Option<Backoff>,
    /// Deduplication, if requested.
    pub deduplication: Option<Dedup>,
    /// Retention on completion.
    pub remove_on_complete: Removal,
    /// Retention on permanent failure.
    pub remove_on_fail: Removal,
    /// LIFO push.
    pub lifo: bool,
}

impl Default for ResolvedJobOptions {
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

impl From<ResolvedJobOptions> for JobOptions {
    /// Every field set explicitly — re-submitting with these options
    /// reproduces the job exactly, regardless of queue defaults.
    fn from(r: ResolvedJobOptions) -> Self {
        Self {
            id: r.id,
            name: r.name,
            delay: r.delay,
            priority: Some(r.priority),
            attempts: Some(r.attempts),
            backoff: r.backoff,
            deduplication: r.deduplication,
            remove_on_complete: Some(r.remove_on_complete),
            remove_on_fail: Some(r.remove_on_fail),
            lifo: Some(r.lifo),
        }
    }
}

/// Deduplication configuration.
///
/// Attached to a [`JobOptions`] via [`JobOptions::deduplicate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    fn job_options_start_empty_and_resolve_to_safe_builtins() {
        let o = JobOptions::default();
        assert_eq!(
            o,
            JobOptions {
                id: None,
                name: None,
                delay: None,
                priority: None,
                attempts: None,
                backoff: None,
                deduplication: None,
                remove_on_complete: None,
                remove_on_fail: None,
                lifo: None,
            }
        );
        let r = o.resolve();
        assert_eq!(r, ResolvedJobOptions::default());
        assert_eq!(r.attempts, 1);
        assert_eq!(r.priority, 0);
        assert_eq!(r.remove_on_complete, Removal::Keep);
        assert!(!r.lifo);
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
        assert_eq!(o.priority, Some(3));
        assert_eq!(o.attempts, Some(7));
        assert!(o.backoff.is_some());
        assert!(o.deduplication.is_some());
        assert_eq!(o.remove_on_complete, Some(Removal::KeepLast(10)));
        assert_eq!(o.remove_on_fail, Some(Removal::Remove));
        assert_eq!(o.lifo, Some(true));
    }

    #[test]
    fn attempts_enforces_minimum_of_one() {
        assert_eq!(JobOptions::new().attempts(0).attempts, Some(1));
        // Even a hand-built zero is floored when resolved.
        let o = JobOptions {
            attempts: Some(0),
            ..Default::default()
        };
        assert_eq!(o.resolve().attempts, 1);
    }

    #[test]
    fn resolve_prefers_job_then_defaults_then_builtins() {
        let defaults = JobOptions::new()
            .id("never-inherited")
            .name("default-name")
            .delay(Duration::from_millis(100))
            .priority(4)
            .attempts(5)
            .backoff(Backoff::ONE_SECOND)
            .deduplicate("d", Duration::from_secs(1))
            .remove_on_complete(Removal::Remove)
            .remove_on_fail(Removal::KeepLast(3))
            .lifo(true);
        // Nothing set on the job: everything but `id` comes from defaults.
        let r = JobOptions::new().resolve_with(&defaults);
        assert_eq!(r.id, None);
        assert_eq!(r.name.as_deref(), Some("default-name"));
        assert_eq!(r.delay, Some(Duration::from_millis(100)));
        assert_eq!(r.priority, 4);
        assert_eq!(r.attempts, 5);
        assert_eq!(r.backoff, Some(Backoff::ONE_SECOND));
        assert_eq!(r.deduplication.as_ref().map(|d| d.id.as_str()), Some("d"));
        assert_eq!(r.remove_on_complete, Removal::Remove);
        assert_eq!(r.remove_on_fail, Removal::KeepLast(3));
        assert!(r.lifo);

        // Explicit values win, including ones equal to the built-ins.
        let r = JobOptions::new()
            .priority(0)
            .attempts(1)
            .lifo(false)
            .remove_on_complete(Removal::Keep)
            .resolve_with(&defaults);
        assert_eq!(r.priority, 0);
        assert_eq!(r.attempts, 1);
        assert!(!r.lifo);
        assert_eq!(r.remove_on_complete, Removal::Keep);
        // Untouched fields still inherit.
        assert_eq!(r.remove_on_fail, Removal::KeepLast(3));
    }

    #[test]
    fn resolved_round_trips_through_job_options() {
        let r = JobOptions::new().priority(2).lifo(true).resolve();
        let back = JobOptions::from(r.clone()).resolve_with(&JobOptions::new().priority(9));
        assert_eq!(back, r, "fully-specified options ignore queue defaults");
    }

    #[test]
    fn resolved_reads_0_2_job_records() {
        // `opts` JSON exactly as oxn 0.2.2 stored it in the job hash.
        let v02 = r#"{"id":null,"name":null,"delay":null,"priority":0,"attempts":3,
            "backoff":{"kind":"fixed","delay":{"secs":1,"nanos":0}},"deduplication":null,
            "remove_on_complete":"Keep","remove_on_fail":{"KeepLast":5},"lifo":false}"#;
        let r: ResolvedJobOptions = serde_json::from_str(v02).unwrap();
        assert_eq!(r.attempts, 3);
        assert_eq!(r.remove_on_fail, Removal::KeepLast(5));
        assert_eq!(r.backoff, Some(Backoff::ONE_SECOND));
        // And a sparse record (missing fields) falls back to built-ins.
        let r: ResolvedJobOptions = serde_json::from_str(r#"{"attempts":2}"#).unwrap();
        assert_eq!(r.attempts, 2);
        assert_eq!(r.remove_on_complete, Removal::Keep);
    }

    #[test]
    fn job_options_serialize_only_what_was_set() {
        let j = serde_json::to_string(&JobOptions::new().attempts(2)).unwrap();
        assert_eq!(j, r#"{"attempts":2}"#);
        let back: JobOptions = serde_json::from_str(&j).unwrap();
        assert_eq!(back.attempts, Some(2));
        assert_eq!(back.priority, None);
    }

    #[test]
    fn queue_options_removal_shortcuts_set_defaults() {
        let q = QueueOptions::default()
            .remove_on_complete(Removal::Remove)
            .remove_on_fail(Removal::KeepLast(7));
        assert_eq!(
            q.default_job_options.remove_on_complete,
            Some(Removal::Remove)
        );
        assert_eq!(
            q.default_job_options.remove_on_fail,
            Some(Removal::KeepLast(7))
        );
        assert_eq!(q.default_job_options.attempts, None);
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
        assert_eq!(back.attempts, Some(5));
        assert_eq!(back.delay, Some(Duration::from_millis(250)));
        assert_eq!(back.deduplication.unwrap().id, "abc");
    }
}
