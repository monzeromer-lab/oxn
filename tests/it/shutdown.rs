//! B4 — graceful shutdown.
//!
//! In 0.2 cancelling a worker aborted every running handler immediately
//! with `Error::Closed`, which then went down the normal failure path: the
//! attempt was consumed and a job on its last attempt was failed for good
//! ("backend closed"). `run()` returned in milliseconds regardless of
//! `shutdown_timeout`. Now cancellation stops fetching, lets handlers
//! finish until `shutdown_timeout`, and only then aborts what is left and
//! **releases** it back to the queue without consuming an attempt.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Work {
    ms: u64,
}

fn worker(
    q: &Queue<Work>,
    concurrency: usize,
    shutdown_timeout: Duration,
    started: Arc<AtomicUsize>,
) -> Worker<Work> {
    Worker::builder(q.clone(), move |j: Job<Work>| {
        let started = started.clone();
        async move {
            started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(j.data.ms)).await;
            Ok::<_, Error>(())
        }
    })
    .concurrency(concurrency)
    .shutdown_timeout(shutdown_timeout)
    .drain_delay(ms(50))
    .build()
}

async fn wait_started(started: &AtomicUsize, n: usize) {
    let deadline = Instant::now() + secs(5);
    while started.load(Ordering::SeqCst) < n {
        assert!(Instant::now() < deadline, "handlers never started");
        tokio::time::sleep(ms(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_in_flight_jobs_and_stops_fetching() {
    let (q, _b) = common::fresh_queue::<Work>("b4-drain").await;
    let mut rec = Recorder::new("b4_drain_in_flight", q.name()).await;
    rec.step(
        "add 3 × 500ms jobs + 1 extra",
        q.add_bulk(
            (0..4)
                .map(|_| (Work { ms: 500 }, JobOptions::new()))
                .collect(),
        ),
    )
    .await
    .unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let w = worker(&q, 3, secs(5), started.clone());
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_started(&started, 3).await;

    let t = Instant::now();
    rec.step("cancel while 3 handlers run", async { cancel.cancel() })
        .await;
    h.await.unwrap().unwrap();
    let took = t.elapsed();
    rec.observe("after run() returned").await;

    assert!(
        took >= ms(350),
        "run() returned after {took:?} — it must wait for handlers"
    );
    assert!(took < secs(3), "run() took {took:?}");
    let s = rec.snapshot();
    assert_eq!(
        s.len("completed"),
        3,
        "in-flight jobs finished: {}",
        s.summary()
    );
    assert_eq!(s.len("failed"), 0, "nothing failed because of shutdown");
    assert_eq!(s.len("wait"), 1, "the 4th job was never fetched");
    assert_eq!(started.load(Ordering::SeqCst), 3);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_releases_jobs_without_consuming_an_attempt() {
    let (q, _b) = common::fresh_queue::<Work>("b4-release").await;
    let mut rec = Recorder::new("b4_timeout_releases", q.name()).await;
    let id = rec
        .step(
            "add 10s job, attempts=1",
            q.add(Work { ms: 10_000 }, JobOptions::new().attempts(1)),
        )
        .await
        .unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let w = worker(&q, 1, ms(300), started.clone());
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_started(&started, 1).await;

    let t = Instant::now();
    rec.step("cancel; handler outlives shutdown_timeout", async {
        cancel.cancel()
    })
    .await;
    h.await.unwrap().unwrap();
    let took = t.elapsed();
    rec.observe("after run() returned").await;

    assert!(
        took >= ms(280),
        "returned before shutdown_timeout: {took:?}"
    );
    assert!(took < secs(3), "returned after {took:?}");
    let s = rec.snapshot();
    assert_eq!(
        s.location(id.as_str()),
        "wait",
        "released back to wait: {}",
        s.summary()
    );
    let job = &s.jobs[id.as_str()];
    assert_eq!(job.attempts_made(), 0, "the aborted attempt is refunded");
    assert!(job.lock.is_none() && !job.fields.contains_key("lockToken"));
    assert!(rec.events_named("failed").is_empty(), "never marked failed");

    // A fresh worker runs it to completion on its one and only attempt.
    let started2 = Arc::new(AtomicUsize::new(0));
    let q2 = q.clone();
    let w2 = Worker::builder(q2, move |j: Job<Work>| {
        let s = started2.clone();
        async move {
            s.fetch_add(1, Ordering::SeqCst);
            assert_eq!(j.attempts_made, 1);
            Ok::<_, Error>(())
        }
    })
    .drain_delay(ms(50))
    .build();
    let c2 = w2.cancellation_token();
    let h2 = tokio::spawn(w2.run());
    assert!(
        rec.wait_until("re-run completes", secs(5), |s| s.len("completed") == 1)
            .await
    );
    c2.cancel();
    h2.await.unwrap().unwrap();
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn released_jobs_keep_priority_and_respect_pause() {
    let (q, _b) = common::fresh_queue::<Work>("b4-prio").await;
    let mut rec = Recorder::new("b4_release_priority_pause", q.name()).await;
    let prio = rec
        .step(
            "add prioritized 10s job",
            q.add(Work { ms: 10_000 }, JobOptions::new().priority(2)),
        )
        .await
        .unwrap();
    let plain = rec
        .step(
            "add plain 10s job",
            q.add(Work { ms: 10_000 }, JobOptions::new()),
        )
        .await
        .unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let w = worker(&q, 2, ms(200), started.clone());
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_started(&started, 2).await;
    rec.step("pause the queue", q.pause()).await.unwrap();
    rec.step("cancel", async { cancel.cancel() }).await;
    h.await.unwrap().unwrap();
    rec.observe("after run() returned").await;
    let s = rec.snapshot();
    assert_eq!(s.location(prio.as_str()), "prioritized", "{}", s.summary());
    assert_eq!(s.location(plain.as_str()), "paused", "{}", s.summary());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_worker_shuts_down_promptly_without_leaving_a_blocked_connection() {
    // A pool of exactly one connection: if shutdown hands the slot that is
    // blocked in BZPOPMIN back to the pool, the next command queues behind
    // the pending 30s block on that same connection.
    let backend = Arc::new(
        oxn::backend::redis::RedisBackend::connect(common::test_config().pool_size(1))
            .await
            .unwrap(),
    );
    let name = common::unique_queue_name("b4-idle");
    let q: Queue<Work> = common::queue(backend.clone(), &name).await;
    let mut rec = Recorder::new("b4_idle_shutdown", q.name()).await;
    let w = Worker::builder(q.clone(), |_j: Job<Work>| async { Ok::<_, Error>(()) })
        .drain_delay(secs(30))
        .stalled_interval(ms(50))
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    tokio::time::sleep(ms(300)).await; // now blocked in BZPOPMIN for up to 30s

    let t = Instant::now();
    rec.step("cancel idle worker", async { cancel.cancel() })
        .await;
    h.await.unwrap().unwrap();
    let took = t.elapsed();
    assert!(
        took < ms(500),
        "idle shutdown took {took:?} (drain_delay 30s)"
    );

    let t = Instant::now();
    tokio::time::timeout(secs(3), q.counts())
        .await
        .expect("the only pool slot is still stuck behind BZPOPMIN")
        .unwrap();
    assert!(
        t.elapsed() < ms(500),
        "pool slot was left blocked: {:?}",
        t.elapsed()
    );

    // The stalled scanner stopped with the worker.
    rec.observe("after shutdown").await;
    let scans = |rec: &Recorder| {
        rec.traces_named("oxn.script")
            .iter()
            .filter(|t| t.fields.get("script").map(String::as_str) == Some("scan_stalled"))
            .count()
    };
    let before = scans(&rec);
    tokio::time::sleep(ms(300)).await;
    rec.observe("300ms later").await;
    assert_eq!(
        before,
        scans(&rec),
        "stalled scanner kept running after run() returned"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn release_api_contract() {
    use oxn::backend::Backend;
    use oxn::error::ScriptError;
    let (q, b) = common::fresh_queue::<Work>("b4-api").await;
    let mut rec = Recorder::new("b4_release_api", q.name()).await;
    let first = rec
        .step("add job 1", q.add(Work { ms: 1 }, JobOptions::new()))
        .await
        .unwrap();
    let second = rec
        .step("add job 2", q.add(Work { ms: 1 }, JobOptions::new()))
        .await
        .unwrap();
    let none = tokio_util::sync::CancellationToken::new();
    let f = rec
        .step(
            "fetch with token A",
            b.fetch_next(q.name(), "A", secs(30), ms(10), &none),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.job.id, first);
    assert_eq!(f.job.attempts_made, 1);

    let r = rec
        .step("release with token B", b.release(q.name(), &first, "B"))
        .await;
    assert!(
        matches!(r, Err(Error::Script(ScriptError::LockMismatch))),
        "{r:?}"
    );
    let r = rec
        .step(
            "release unknown job",
            b.release(q.name(), &JobId::new("nope"), "A"),
        )
        .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
    rec.step("release with token A", b.release(q.name(), &first, "A"))
        .await
        .unwrap();

    let s = rec.snapshot();
    assert_eq!(s.location(first.as_str()), "wait");
    assert_eq!(
        s.jobs[first.as_str()].attempts_made(),
        0,
        "attempt refunded"
    );
    let released = rec
        .events_named("waiting")
        .into_iter()
        .find(|e| {
            e.job_id.as_deref() == Some(first.as_str())
                && e.fields.get("prev").map(String::as_str) == Some("active")
        })
        .expect("waiting event with prev=active");
    assert_eq!(
        released.fields.get("released").map(String::as_str),
        Some("wait")
    );

    // Released jobs go to the head of the line: job 1 is fetched before 2.
    let f = rec
        .step(
            "fetch again",
            b.fetch_next(q.name(), "C", secs(30), ms(10), &none),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        f.job.id, first,
        "released job must be next, ahead of {second}"
    );
    assert_eq!(f.job.attempts_made, 1, "fresh attempt 1 after the refund");
    rec.step(
        "complete",
        b.complete(q.name(), &first, "C", None, oxn::options::Removal::Keep),
    )
    .await
    .unwrap();
    rec.assert_clean();
    common::teardown(&q).await;
}
