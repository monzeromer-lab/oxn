//! B7 — the stalled-job lifecycle.
//!
//! In 0.2 a job that stalled more than `max_stalled` times was removed from
//! `active` but never added to `failed`: it lived in no container, could
//! not be listed or retried, and its `remove_on_fail` policy never ran. A
//! requeued stalled job ignored its priority and the pause flag, and the
//! stall consumed one of the job's attempts, so whether a job got its full
//! retry budget depended on the order of stalls and failures.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common;
use oxn::backend::Backend;
use oxn::backend::Failure;
use oxn::backend::redis::RedisBackend;
use oxn::options::Removal;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

/// Fetch with a 30ms lock and let it lapse: the deterministic way to stall.
async fn stall(b: &Arc<RedisBackend>, q: &str) -> JobId {
    let f = b
        .fetch_next(
            q,
            "dead-worker",
            ms(30),
            ms(10),
            &CancellationToken::new(),
            None,
        )
        .await
        .unwrap()
        .expect("a job to stall");
    tokio::time::sleep(ms(60)).await;
    f.job.id
}

/// `scan_stalled` runs at most once per second per queue; wait out the
/// rate-limit flag between scans.
async fn scan(b: &Arc<RedisBackend>, q: &str, max_stalled: u32) -> Vec<JobId> {
    tokio::time::sleep(ms(1050)).await;
    b.scan_stalled(q, max_stalled).await.unwrap().requeued
}

#[tokio::test(flavor = "multi_thread")]
async fn stalling_past_the_limit_fails_the_job_into_failed() {
    let (q, b) = common::fresh_queue::<T>("b7-fail").await;
    let mut rec = Recorder::new("b7_stall_limit_fails", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new().attempts(5)))
        .await
        .unwrap();
    rec.step("stall #1", stall(&b, q.name())).await;
    let moved = rec
        .step("scan (max_stalled=1)", scan(&b, q.name(), 1))
        .await;
    assert_eq!(moved, vec![id.clone()], "first stall requeues");
    rec.step("stall #2", stall(&b, q.name())).await;
    let moved = rec.step("scan again", scan(&b, q.name(), 1)).await;
    assert!(moved.is_empty(), "second stall exceeds the limit");

    let s = rec.snapshot();
    assert_eq!(s.location(id.as_str()), "failed", "{}", s.summary());
    let job = &s.jobs[id.as_str()];
    assert!(job.fields.contains_key("finishedOn"));
    assert_eq!(
        job.fields.get("failedReason").map(String::as_str),
        Some("job stalled more than allowable limit")
    );
    assert!(!job.fields.contains_key("lockToken"));
    let failed = rec.events_named("failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].fields.get("failedReason").map(String::as_str),
        Some("job stalled more than allowable limit")
    );
    assert!(failed[0].fields.contains_key("attemptsMade"));

    // It is a real failed job: listable and retryable.
    assert_eq!(
        q.list(JobState::Failed, oxn::backend::JobRange::first(10))
            .await
            .unwrap()
            .len(),
        1
    );
    rec.step("retry it", q.retry(&id)).await.unwrap();
    assert_eq!(rec.snapshot().location(id.as_str()), "wait");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stall_failure_applies_remove_on_fail() {
    let (q, b) = common::fresh_queue::<T>("b7-retention").await;
    let mut rec = Recorder::new("b7_stall_failure_retention", q.name())
        .await
        .strict();
    let removed = rec
        .step(
            "add (remove_on_fail=Remove)",
            q.add(T(1), JobOptions::new().remove_on_fail(Removal::Remove)),
        )
        .await
        .unwrap();
    rec.step("stall", stall(&b, q.name())).await;
    rec.step("scan (max_stalled=0)", scan(&b, q.name(), 0))
        .await;
    assert_eq!(rec.snapshot().location(removed.as_str()), "(gone)");
    assert!(
        rec.snapshot().jobs.is_empty(),
        "no orphan left: {}",
        rec.snapshot().summary()
    );

    for i in 0..3 {
        q.add(T(i), JobOptions::new().remove_on_fail(Removal::KeepLast(1)))
            .await
            .unwrap();
    }
    for _ in 0..3 {
        rec.step("stall", stall(&b, q.name())).await;
        rec.step("scan (max_stalled=0)", scan(&b, q.name(), 0))
            .await;
    }
    let s = rec.snapshot();
    assert_eq!(s.len("failed"), 1, "KeepLast(1) honored: {}", s.summary());
    assert_eq!(s.jobs.len(), 1, "trimmed jobs deleted: {}", s.summary());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn requeued_jobs_keep_priority_respect_pause_and_go_first() {
    let (q, b) = common::fresh_queue::<T>("b7-requeue").await;
    let mut rec = Recorder::new("b7_requeue_priority_pause_front", q.name()).await;
    let prio = rec
        .step(
            "add prioritized",
            q.add(T(1), JobOptions::new().priority(3)),
        )
        .await
        .unwrap();
    assert_eq!(rec.step("stall it", stall(&b, q.name())).await, prio);
    rec.step("scan", scan(&b, q.name(), 5)).await;
    assert_eq!(
        rec.snapshot().location(prio.as_str()),
        "prioritized",
        "{}",
        rec.snapshot().summary()
    );

    // Head of the line: the stalled job runs before jobs that were waiting.
    let first = rec
        .step("add plain A", q.add(T(2), JobOptions::new()))
        .await
        .unwrap();
    let _second = rec
        .step("add plain B", q.add(T(3), JobOptions::new()))
        .await
        .unwrap();
    rec.step("drop the prioritized job", q.remove(&prio))
        .await
        .unwrap();
    assert_eq!(rec.step("stall A", stall(&b, q.name())).await, first);
    rec.step("scan", scan(&b, q.name(), 5)).await;
    assert_eq!(
        rec.snapshot().lists["wait"].last().map(String::as_str),
        Some(first.as_str()),
        "A is next to pop"
    );

    rec.step("pause", q.pause()).await.unwrap();
    let paused_stall = rec
        .step("stall while paused", async {
            // fetch is refused while paused, so take the lock by resuming briefly
            q.resume().await.unwrap();
            let id = stall(&b, q.name()).await;
            q.pause().await.unwrap();
            id
        })
        .await;
    rec.step("scan while paused", scan(&b, q.name(), 5)).await;
    assert_eq!(
        rec.snapshot().location(paused_stall.as_str()),
        "paused",
        "{}",
        rec.snapshot().summary()
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stall_does_not_consume_an_attempt() {
    let (q, b) = common::fresh_queue::<T>("b7-attempts").await;
    let mut rec = Recorder::new("b7_stall_refunds_attempt", q.name()).await;
    let id = rec
        .step("add attempts=2", q.add(T(1), JobOptions::new().attempts(2)))
        .await
        .unwrap();
    rec.step("stall (attempt 1 interrupted)", stall(&b, q.name()))
        .await;
    rec.step("scan", scan(&b, q.name(), 5)).await;
    assert_eq!(
        rec.snapshot().jobs[id.as_str()].attempts_made(),
        0,
        "stall refunded"
    );

    // Two real failures are still allowed: the stall didn't eat one.
    let none = CancellationToken::new();
    let f = b
        .fetch_next(q.name(), "w", secs(30), ms(10), &none, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.job.attempts_made, 1);
    rec.step(
        "fail attempt 1 (retry)",
        b.fail(q.name(), &id, "w", Failure::new("boom").retry_at(0)),
    )
    .await
    .unwrap();
    let f = b
        .fetch_next(q.name(), "w", secs(30), ms(10), &none, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.job.attempts_made, 2, "second real attempt");
    rec.step(
        "complete attempt 2",
        b.complete(q.name(), &id, "w", None, Removal::Keep),
    )
    .await
    .unwrap();
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_with_lapsing_lock_is_recovered_end_to_end() {
    let (q, _b) = common::fresh_queue::<T>("b7-e2e").await;
    let mut rec = Recorder::new("b7_e2e_lapsed_lock", q.name()).await;
    let id = rec
        .step("add attempts=1", q.add(T(1), JobOptions::new().attempts(1)))
        .await
        .unwrap();
    let runs = Arc::new(AtomicU32::new(0));
    let r = runs.clone();
    // Misconfigured on purpose: the lock (300ms) lapses long before the
    // first renewal (10s), while the first run takes 1.5s.
    let w = Worker::builder(q.clone(), move |_j: Job<T>| {
        let r = r.clone();
        async move {
            if r.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(ms(1500)).await;
            }
            Ok::<_, Error>(())
        }
    })
    .concurrency(2)
    .lock_duration(ms(300))
    .lock_renew_interval(secs(10))
    .stalled_interval(ms(100))
    .drain_delay(ms(50))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let done = rec
        .wait_until("re-run completes", secs(10), |s| s.len("completed") == 1)
        .await;
    tokio::time::sleep(ms(1600)).await; // let the original run finish too
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("after shutdown").await;
    assert!(done);
    assert_eq!(rec.snapshot().location(id.as_str()), "completed");
    assert_eq!(rec.events_named("stalled").len(), 1);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "at-least-once: the original run kept going"
    );
    // attempts=1 still completed: the stall didn't use up the only attempt.
    assert!(rec.events_named("failed").is_empty());
    // The original run's late completion was rejected (lock lost), not applied.
    let rejected = rec
        .steps()
        .iter()
        .flat_map(|s| s.traces.iter())
        .filter(|t| t.name.starts_with("transition rejected"))
        .count();
    assert_eq!(rejected, 1);
    rec.assert_clean();
    common::teardown(&q).await;
}
