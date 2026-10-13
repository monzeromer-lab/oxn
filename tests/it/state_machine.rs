//! B9 — state-machine correctness.
//!
//! In 0.2 several transitions pushed jobs straight onto `wait`, losing
//! their priority and ignoring the pause flag: delayed jobs coming due (and
//! retry backoffs), `promote`, `promote_all` and `retry`. `Error::Delayed`
//! and `Error::RateLimited` consumed one of the job's attempts and stamped a
//! fake `failedReason`. `list()` returned list-backed states newest-first
//! while documenting oldest-first, and looked each job's state up again
//! with up to eight commands per job.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common;
use oxn::backend::Failure;
use oxn::backend::redis::RedisBackend;
use oxn::backend::{Backend, JobRange};
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

async fn fetch(b: &Arc<RedisBackend>, q: &str) -> JobId {
    b.fetch_next(q, "w", secs(30), ms(10), &CancellationToken::new())
        .await
        .unwrap()
        .expect("a job")
        .job
        .id
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delayed_job_keeps_its_priority_when_it_comes_due() {
    let (q, b) = common::fresh_queue::<T>("b9-delay-prio").await;
    let mut rec = Recorder::new("b9_delay_keeps_priority", q.name()).await;
    let plain = rec
        .step("add plain job", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let prio = rec
        .step(
            "add priority-1 job delayed 200ms",
            q.add(T(2), JobOptions::new().priority(1).delay(ms(200))),
        )
        .await
        .unwrap();
    tokio::time::sleep(ms(300)).await;
    let first = rec.step("fetch", fetch(&b, q.name())).await;
    assert_eq!(
        first, prio,
        "the prioritized job must beat {plain} once due"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_backoff_keeps_priority() {
    let (q, b) = common::fresh_queue::<T>("b9-backoff-prio").await;
    let mut rec = Recorder::new("b9_backoff_keeps_priority", q.name()).await;
    let prio = rec
        .step(
            "add priority-1 job",
            q.add(T(1), JobOptions::new().priority(1).attempts(3)),
        )
        .await
        .unwrap();
    assert_eq!(fetch(&b, q.name()).await, prio);
    rec.step("fail with a 100ms backoff", async {
        let at = now_ms() + 100;
        b.fail(q.name(), &prio, "w", Failure::new("transient").retry_at(at))
            .await
    })
    .await
    .unwrap();
    rec.step("add plain job meanwhile", q.add(T(2), JobOptions::new()))
        .await
        .unwrap();
    tokio::time::sleep(ms(200)).await;
    assert_eq!(rec.step("fetch", fetch(&b, q.name())).await, prio);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_and_promote_all_keep_priority_and_respect_pause() {
    let (q, _b) = common::fresh_queue::<T>("b9-promote").await;
    let mut rec = Recorder::new("b9_promote_priority_pause", q.name()).await;
    let p1 = rec
        .step(
            "add prio delayed",
            q.add(T(1), JobOptions::new().priority(2).delay(secs(60))),
        )
        .await
        .unwrap();
    rec.step("promote it", q.promote(&p1)).await.unwrap();
    assert_eq!(rec.snapshot().location(p1.as_str()), "prioritized");

    let p2 = rec
        .step(
            "add prio delayed",
            q.add(T(2), JobOptions::new().priority(2).delay(secs(60))),
        )
        .await
        .unwrap();
    let plain = rec
        .step(
            "add plain delayed",
            q.add(T(3), JobOptions::new().delay(secs(60))),
        )
        .await
        .unwrap();
    rec.step("pause", q.pause()).await.unwrap();
    let n = rec
        .step(
            "promote_all while paused",
            q.backend().promote_all(q.name()),
        )
        .await
        .unwrap();
    assert_eq!(n, 2);
    let s = rec.snapshot();
    assert_eq!(s.location(p2.as_str()), "prioritized", "{}", s.summary());
    assert_eq!(s.location(plain.as_str()), "paused", "{}", s.summary());

    let p3 = rec
        .step(
            "add plain delayed",
            q.add(T(4), JobOptions::new().delay(secs(60))),
        )
        .await
        .unwrap();
    rec.step("promote one while paused", q.promote(&p3))
        .await
        .unwrap();
    assert_eq!(rec.snapshot().location(p3.as_str()), "paused");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_of_a_failed_job_keeps_priority_and_respects_pause() {
    let (q, b) = common::fresh_queue::<T>("b9-retry").await;
    let mut rec = Recorder::new("b9_retry_priority_pause", q.name()).await;
    let prio = rec
        .step("add prio", q.add(T(1), JobOptions::new().priority(1)))
        .await
        .unwrap();
    let plain = rec
        .step("add plain", q.add(T(2), JobOptions::new()))
        .await
        .unwrap();
    for _ in 0..2 {
        let id = fetch(&b, q.name()).await;
        b.fail(q.name(), &id, "w", Failure::new("boom"))
            .await
            .unwrap();
    }
    rec.step("retry prio", q.retry(&prio)).await.unwrap();
    assert_eq!(rec.snapshot().location(prio.as_str()), "prioritized");
    rec.step("pause", q.pause()).await.unwrap();
    rec.step("retry plain while paused", q.retry(&plain))
        .await
        .unwrap();
    assert_eq!(rec.snapshot().location(plain.as_str()), "paused");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn error_delayed_does_not_consume_an_attempt() {
    let (q, _b) = common::fresh_queue::<T>("b9-delayed-err").await;
    let mut rec = Recorder::new("b9_delayed_refunds_attempt", q.name()).await;
    let id = rec
        .step("add attempts=2", q.add(T(1), JobOptions::new().attempts(2)))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    // Call 1: postpone. Calls 2 and 3: real failures. With two attempts,
    // both real failures must run; in 0.2 the postponement used one up.
    let w = Worker::builder(q.clone(), move |_j: Job<T>| {
        let c = c.clone();
        async move {
            match c.fetch_add(1, Ordering::SeqCst) {
                0 => Err::<(), _>(Error::Delayed { delay_ms: 50 }),
                _ => Err(Error::handler(std::io::Error::other("real failure"))),
            }
        }
    })
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let delayed_seen = rec
        .wait_until("postponed", secs(3), |s| s.len("delayed") == 1)
        .await;
    assert!(delayed_seen);
    let job = &rec.snapshot().jobs[id.as_str()];
    assert_eq!(job.attempts_made(), 0, "the postponement is refunded");
    assert!(
        !job.fields.contains_key("failedReason"),
        "a postponement is not a failure"
    );
    assert!(
        rec.wait_until("fails for good", secs(5), |s| s.len("failed") == 1)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "postpone + two real attempts"
    );
    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(job.attempts_made, 2);
    assert_eq!(
        job.failed_reason.as_deref(),
        Some("handler error: real failure")
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rate_limited_is_a_postponement_too() {
    let (q, _b) = common::fresh_queue::<T>("b9-ratelimited").await;
    let mut rec = Recorder::new("b9_rate_limited_refunds", q.name()).await;
    let id = rec
        .step("add attempts=1", q.add(T(1), JobOptions::new().attempts(1)))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let w = Worker::builder(q.clone(), move |j: Job<T>| {
        let c = c.clone();
        async move {
            if c.fetch_add(1, Ordering::SeqCst) < 2 {
                return Err(Error::RateLimited { delay_ms: 30 });
            }
            assert_eq!(
                j.attempts_made, 1,
                "two rate-limit postponements left the one attempt intact"
            );
            Ok(())
        }
    })
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    assert!(
        rec.wait_until("completes", secs(5), |s| s.len("completed") == 1)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(rec.snapshot().location(id.as_str()), "completed");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_returns_processing_order_with_states() {
    let (q, b) = common::fresh_queue::<T>("b9-list").await;
    let mut rec = Recorder::new("b9_list_order", q.name()).await;
    for i in 1..=3 {
        rec.step("add", q.add(T(i), JobOptions::new()))
            .await
            .unwrap();
    }
    rec.step("add LIFO", q.add(T(4), JobOptions::new().lifo(true)))
        .await
        .unwrap();
    let ids = |v: Vec<Job<T>>| v.into_iter().map(|j| j.id.to_string()).collect::<Vec<_>>();

    // Ascending = processing order: the LIFO job runs first, then FIFO.
    let next = ids(q
        .list(JobState::Waiting, JobRange::first(10))
        .await
        .unwrap());
    assert_eq!(next, ["4", "1", "2", "3"]);
    assert_eq!(
        fetch(&b, q.name()).await.as_str(),
        "4",
        "list order == fetch order"
    );
    let newest = ids(q.list(JobState::Waiting, JobRange::last(2)).await.unwrap());
    assert_eq!(newest, ["3", "2"]);
    let page = ids(q
        .list(
            JobState::Waiting,
            JobRange {
                start: 1,
                end: 1,
                descending: false,
            },
        )
        .await
        .unwrap());
    assert_eq!(page, ["2"]);

    rec.step("pause", q.pause()).await.unwrap();
    let paused = ids(q.list(JobState::Paused, JobRange::first(10)).await.unwrap());
    assert_eq!(paused, ["1", "2", "3"], "paused keeps processing order");

    let raw = b
        .list(q.name(), JobState::Paused, JobRange::first(10))
        .await
        .unwrap();
    assert!(raw.iter().all(|r| r.state == Some(JobState::Paused)));
    let active = b
        .list(q.name(), JobState::Active, JobRange::first(10))
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].state, Some(JobState::Active));
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn listing_hundreds_of_jobs_is_a_couple_of_round_trips() {
    let (q, b) = common::fresh_queue::<T>("b9-list-fast").await;
    q.add_bulk((0..500).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    let mut admin = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let stat = |raw: String, cmd: &str| -> u64 {
        raw.lines()
            .find(|l| l.starts_with(&format!("cmdstat_{cmd}:")))
            .and_then(|l| l.split("calls=").nth(1))
            .and_then(|v| v.split(',').next())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0)
    };
    let before: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut admin)
        .await
        .unwrap();
    let jobs = b
        .list(q.name(), JobState::Waiting, JobRange::first(500))
        .await
        .unwrap();
    let after: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 500);
    // Other tests run concurrently, so allow slack; 0.2 issued an LPOS per
    // job (≥ 500) on top of an HGETALL each.
    let lpos = stat(after.clone(), "lpos") - stat(before.clone(), "lpos");
    assert!(
        lpos < 100,
        "list() still looks up each job's state: {lpos} LPOS calls"
    );
    let g = b.get(q.name(), &jobs[0].id).await.unwrap().unwrap();
    assert_eq!(g.state, Some(JobState::Waiting));
    common::teardown(&q).await;
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
