//! Metrics emitted with the `metrics` feature.
//!
//! oxn records through the [`metrics`](https://docs.rs/metrics) facade, so
//! install any recorder (e.g. `metrics-exporter-prometheus`) and the series
//! below appear. Every series carries a `queue` label. Transitions are
//! counted only once Redis has accepted them: a completion refused because
//! another owner holds the job counts as [`LOCK_LOST`], not [`COMPLETED`].
//!
//! | Name | Kind | Extra labels | Meaning |
//! |---|---|---|---|
//! | `oxn_jobs_added_total` | counter | | jobs submitted with `add` / `add_bulk` (including ones collapsed by an existing id or dedup) |
//! | `oxn_jobs_started_total` | counter | | handler runs started (every attempt) |
//! | `oxn_jobs_completed_total` | counter | | jobs completed |
//! | `oxn_jobs_failed_total` | counter | | jobs failed for good (attempts exhausted, unrecoverable, undecodable) |
//! | `oxn_jobs_retried_total` | counter | | failed attempts scheduled for a retry |
//! | `oxn_jobs_postponed_total` | counter | | `Error::Delayed` / `RateLimited` postponements |
//! | `oxn_jobs_released_total` | counter | | jobs handed back by a shutting-down worker |
//! | `oxn_jobs_stalled_total` | counter | | stalled jobs found by the scanner (requeued or failed) |
//! | `oxn_lock_lost_total` | counter | | outcomes discarded because another owner held the job |
//! | `oxn_transition_errors_total` | counter | `to` | outcomes Redis did not record for another reason |
//! | `oxn_fetch_errors_total` | counter | | failed fetches |
//! | `oxn_rate_limited_total` | counter | | fetches refused by the queue's rate limit |
//! | `oxn_job_duration_seconds` | histogram | `outcome` | handler run time; `outcome` is `ok`, `error`, `panicked`, `unrecoverable`, `delayed`, `rate-limited`, `cancelled` (`Error::Closed`) or `aborted` (by `shutdown_timeout`) |
//! | `oxn_job_wait_seconds` | histogram | | first attempt: from enqueue (or the end of its delay) to start; a postponed job's wait includes the postponement |
//! | `oxn_jobs_active` | gauge | | jobs this process is running now |
//!
//! Call [`describe`] once after installing the recorder to register units
//! and help text (workers also call it when they start).

/// `oxn_jobs_added_total`
pub const ADDED: &str = "oxn_jobs_added_total";
/// `oxn_jobs_started_total`
pub const STARTED: &str = "oxn_jobs_started_total";
/// `oxn_jobs_completed_total`
pub const COMPLETED: &str = "oxn_jobs_completed_total";
/// `oxn_jobs_failed_total`
pub const FAILED: &str = "oxn_jobs_failed_total";
/// `oxn_jobs_retried_total`
pub const RETRIED: &str = "oxn_jobs_retried_total";
/// `oxn_jobs_postponed_total`
pub const POSTPONED: &str = "oxn_jobs_postponed_total";
/// `oxn_jobs_released_total`
pub const RELEASED: &str = "oxn_jobs_released_total";
/// `oxn_jobs_stalled_total`
pub const STALLED: &str = "oxn_jobs_stalled_total";
/// `oxn_lock_lost_total`
pub const LOCK_LOST: &str = "oxn_lock_lost_total";
/// `oxn_transition_errors_total`
pub const TRANSITION_ERRORS: &str = "oxn_transition_errors_total";
/// `oxn_fetch_errors_total`
pub const FETCH_ERRORS: &str = "oxn_fetch_errors_total";
/// `oxn_rate_limited_total`
pub const RATE_LIMITED: &str = "oxn_rate_limited_total";
/// `oxn_job_duration_seconds`
pub const JOB_DURATION: &str = "oxn_job_duration_seconds";
/// `oxn_job_wait_seconds`
pub const JOB_WAIT: &str = "oxn_job_wait_seconds";
/// `oxn_jobs_active`
pub const ACTIVE: &str = "oxn_jobs_active";

/// Register units and descriptions with the installed recorder.
pub fn describe() {
    use ::metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
    describe_counter!(ADDED, Unit::Count, "Jobs submitted with add / add_bulk.");
    describe_counter!(
        STARTED,
        Unit::Count,
        "Handler runs started (every attempt)."
    );
    describe_counter!(COMPLETED, Unit::Count, "Jobs completed.");
    describe_counter!(FAILED, Unit::Count, "Jobs failed for good.");
    describe_counter!(
        RETRIED,
        Unit::Count,
        "Failed attempts scheduled for a retry."
    );
    describe_counter!(
        POSTPONED,
        Unit::Count,
        "Jobs postponed by Error::Delayed / RateLimited."
    );
    describe_counter!(
        RELEASED,
        Unit::Count,
        "Jobs handed back by a shutting-down worker."
    );
    describe_counter!(STALLED, Unit::Count, "Stalled jobs found by the scanner.");
    describe_counter!(
        LOCK_LOST,
        Unit::Count,
        "Outcomes discarded because another owner held the job."
    );
    describe_counter!(
        TRANSITION_ERRORS,
        Unit::Count,
        "Outcomes Redis did not record (other than a lost lock)."
    );
    describe_counter!(FETCH_ERRORS, Unit::Count, "Failed fetches.");
    describe_counter!(
        RATE_LIMITED,
        Unit::Count,
        "Fetches refused by the queue's rate limit."
    );
    describe_histogram!(JOB_DURATION, Unit::Seconds, "Handler run time, by outcome.");
    describe_histogram!(
        JOB_WAIT,
        Unit::Seconds,
        "First attempt: enqueue (or delay end) to start."
    );
    describe_gauge!(ACTIVE, Unit::Count, "Jobs this process is running now.");
}
