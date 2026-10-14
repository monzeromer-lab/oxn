//! B3 — workers pick up new work immediately.
//!
//! In 0.2 a worker that found the queue empty blocked on `BZPOPMIN` for the
//! whole `drain_delay` (5s by default), and nothing woke it: plain and
//! prioritized adds never touched the marker, a burst woke at most one of
//! several blocked workers, and a delayed job woke workers immediately only
//! for them to go back to sleep for another full `drain_delay`.
//!
//! Every test runs workers at the **default** `drain_delay`, which is
//! exactly what the old test-suite avoided by setting it to 20–100ms.

use crate::common;
use oxn::backend::Failure;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use oxn::backend::Backend;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

/// Start times of every handler invocation, relative to `t0`.
#[derive(Clone)]
struct Starts(Arc<Mutex<Vec<Duration>>>, Instant);

impl Starts {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())), Instant::now())
    }
    fn record(&self) {
        self.0.lock().push(self.1.elapsed());
    }
    fn len(&self) -> usize {
        self.0.lock().len()
    }
    async fn wait_for(&self, n: usize, max: Duration) -> bool {
        let deadline = Instant::now() + max;
        while Instant::now() < deadline {
            if self.len() >= n {
                return true;
            }
            tokio::time::sleep(ms(5)).await;
        }
        false
    }
}

/// Default-configured worker (no drain_delay override) that records starts.
fn spawn_worker(
    q: &Queue<T>,
    starts: &Starts,
    work: Duration,
    concurrency: usize,
    cancel: &CancellationToken,
) -> tokio::task::JoinHandle<oxn::Result<()>> {
    let s = starts.clone();
    let w = Worker::builder(q.clone(), move |_j: Job<T>| {
        let s = s.clone();
        async move {
            s.record();
            tokio::time::sleep(work).await;
            Ok::<_, Error>(())
        }
    })
    .concurrency(concurrency)
    .cancellation_token(cancel.clone())
    .build();
    tokio::spawn(w.run())
}

/// Give a freshly spawned worker time to find the queue empty and block.
async fn let_workers_block() {
    tokio::time::sleep(ms(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn add_wakes_a_blocked_worker() {
    let (q, _b) = common::fresh_queue::<T>("b3-add").await;
    let mut rec = Recorder::new("b3_add_wakes_blocked_worker", q.name()).await;
    let starts = Starts::new();
    let cancel = CancellationToken::new();
    let h = spawn_worker(&q, &starts, ms(1), 1, &cancel);
    let_workers_block().await;

    let t = Instant::now();
    rec.step("add plain job", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    assert!(starts.wait_for(1, secs(10)).await);
    let plain = t.elapsed();

    let t = Instant::now();
    rec.step(
        "add prioritized job",
        q.add(T(2), JobOptions::new().priority(3)),
    )
    .await
    .unwrap();
    assert!(starts.wait_for(2, secs(10)).await);
    let prio = t.elapsed();
    rec.observe("both picked up").await;

    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(
        plain < ms(250),
        "plain add picked up after {plain:?} (drain_delay is 5s)"
    );
    assert!(prio < ms(250), "prioritized add picked up after {prio:?}");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn burst_wakes_every_blocked_worker() {
    let (q, b) = common::fresh_queue::<T>("b3-burst").await;
    let mut rec = Recorder::new("b3_burst_wakes_all_workers", q.name()).await;
    let starts = Starts::new();
    let cancel = CancellationToken::new();
    // Four separate workers (each its own backend + pool, as in four
    // processes), concurrency 1 each, 400ms per job.
    let mut hs = Vec::new();
    for _ in 0..4 {
        let backend = common::build_backend().await as Arc<dyn Backend>;
        let wq: Queue<T> = Queue::from_backend(
            q.name(),
            backend,
            QueueOptions::default().prefix(common::TEST_PREFIX),
        )
        .unwrap();
        hs.push(spawn_worker(&wq, &starts, ms(400), 1, &cancel));
    }
    let_workers_block().await;
    let t = Instant::now();
    rec.step(
        "add 8 jobs at once",
        q.add_bulk((0..8).map(|i| (T(i), JobOptions::new())).collect()),
    )
    .await
    .unwrap();
    assert!(starts.wait_for(4, secs(10)).await);
    let four = t.elapsed();
    assert!(
        rec.wait_until("all 8 done", secs(15), |s| s.len("completed") == 8)
            .await
    );
    let all = t.elapsed();
    cancel.cancel();
    for h in hs {
        h.await.unwrap().unwrap();
    }
    // All four workers must engage within the first job's runtime; with
    // one wake-up per burst the other three would sleep for 5s.
    assert!(four < ms(300), "4 workers busy after {four:?}");
    assert!(
        all < ms(1500),
        "8×400ms on 4 workers took {all:?} (≈800ms expected)"
    );
    let _ = b;
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_job_runs_at_its_due_time_not_a_drain_delay_later() {
    let (q, _b) = common::fresh_queue::<T>("b3-delay").await;
    let mut rec = Recorder::new("b3_delayed_on_time", q.name()).await;
    let starts = Starts::new();
    let cancel = CancellationToken::new();
    let h = spawn_worker(&q, &starts, ms(1), 1, &cancel);
    let_workers_block().await;
    let t = Instant::now();
    rec.step(
        "add job delayed 400ms",
        q.add(T(1), JobOptions::new().delay(ms(400))),
    )
    .await
    .unwrap();
    assert!(starts.wait_for(1, secs(10)).await);
    let took = t.elapsed();
    rec.observe("picked up").await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(took >= ms(390), "ran early: {took:?}");
    assert!(took < ms(650), "ran {took:?} after add; due at 400ms");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_promote_and_retry_wake_blocked_workers() {
    let (q, b) = common::fresh_queue::<T>("b3-ops").await;
    let mut rec = Recorder::new("b3_resume_promote_retry_wake", q.name()).await;

    // A failed job to retry later (fail it directly through the backend).
    let failed = rec
        .step("add job to fail", q.add(T(9), JobOptions::new()))
        .await
        .unwrap();
    let f = b
        .fetch_next(
            q.name(),
            "setup",
            secs(30),
            ms(10),
            &tokio_util::sync::CancellationToken::new(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    rec.step(
        "fail it permanently",
        b.fail(q.name(), &f.job.id, "setup", Failure::new("boom")),
    )
    .await
    .unwrap();
    let delayed = rec
        .step(
            "add job delayed 30s",
            q.add(T(2), JobOptions::new().delay(secs(30))),
        )
        .await
        .unwrap();
    rec.step("pause", q.pause()).await.unwrap();
    rec.step("add while paused", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();

    let starts = Starts::new();
    let cancel = CancellationToken::new();
    let h = spawn_worker(&q, &starts, ms(1), 1, &cancel);
    let_workers_block().await;
    assert_eq!(starts.len(), 0, "paused queue must not run anything");

    for (label, n) in [("resume", 1usize), ("promote", 2), ("retry", 3)] {
        let t = Instant::now();
        match label {
            "resume" => rec.step(label, q.resume()).await.unwrap(),
            "promote" => rec.step(label, q.promote(&delayed)).await.unwrap(),
            _ => rec.step(label, q.retry(&failed)).await.unwrap(),
        }
        assert!(starts.wait_for(n, secs(10)).await, "{label}: nothing ran");
        let took = t.elapsed();
        assert!(took < ms(250), "{label} → pickup took {took:?}");
        let_workers_block().await;
    }
    rec.observe("all three ran").await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_drain_delay_does_not_block_forever() {
    let (q, _b) = common::fresh_queue::<T>("b3-zero").await;
    let mut rec = Recorder::new("b3_zero_drain_delay", q.name()).await;
    let seen = Arc::new(AtomicUsize::new(0));
    let s2 = seen.clone();
    let w = Worker::builder(q.clone(), move |_j: Job<T>| {
        let s = s2.clone();
        async move {
            s.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .drain_delay(Duration::ZERO)
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let_workers_block().await;
    rec.step("add after worker idles", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    assert!(
        rec.wait_until("job ran", secs(3), |s| s.len("completed") == 1)
            .await
    );
    cancel.cancel();
    // BZPOPMIN 0 blocks forever; shutdown must still return promptly.
    tokio::time::timeout(secs(5), h)
        .await
        .expect("worker hung")
        .unwrap()
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    rec.assert_clean();
    common::teardown(&q).await;
}
