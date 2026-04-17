//! Scheduler: enqueue_next / fixed interval.

#![cfg(feature = "scheduler")]

mod common;

use std::time::Duration;

use oxn::prelude::*;
use oxn::scheduler::{JobScheduler, RepeatOptions};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Tick(u64);

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_next_inserts_a_delayed_job() {
    let (q, _be) = common::fresh_queue::<Tick>("sched-enqueue").await;

    let sched = JobScheduler::new(
        q.clone(),
        "heartbeat",
        RepeatOptions::every(Duration::from_secs(2)),
        Tick(0),
    );
    let id = sched.enqueue_next().await.unwrap();
    assert!(id.as_str().starts_with("sched:heartbeat:"));

    let c = q.counts().await.unwrap();
    assert_eq!(c.delayed, 1);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn interval_scheduler_fires_multiple_times() {
    let (q, _be) = common::fresh_queue::<Tick>("sched-interval").await;

    let sched = JobScheduler::new(
        q.clone(),
        "fast",
        RepeatOptions::every(Duration::from_millis(150)),
        Tick(0),
    );

    let cancel = CancellationToken::new();
    let cancel_c = cancel.clone();
    let handle = tokio::spawn(async move { sched.run(cancel_c).await });

    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    let _ = handle.await;

    // We expect >= 2 ticks in 500ms with 150ms interval.
    // (They land in `waiting` or `delayed` depending on timing.)
    let c = q.counts().await.unwrap();
    let total = c.waiting + c.delayed;
    assert!(total >= 2, "expected ≥2 jobs, got {total} (counts: {c:?})");

    common::teardown(&q).await;
}

#[test]
fn cron_expression_parses() {
    let opts = RepeatOptions::cron("0 0 * * * *").unwrap();
    let delay = opts.next_delay();
    // Next hour is always within 1 hour.
    assert!(delay <= Duration::from_secs(3600));
}

#[test]
fn cron_parse_error_surfaces_as_config() {
    let err = RepeatOptions::cron("not a cron expression").unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}
