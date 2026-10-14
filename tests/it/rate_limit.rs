//! B12d — `rate_limit(max, per)` actually limits.
//!
//! In 0.2 `WorkerOptions::rate_limit` was stored and never read. It is now
//! a queue-wide limit kept in Redis (`{prefix}:{queue}:limiter`): at most
//! `max` jobs move to active per `per` window across every worker that
//! sets it, and a limited worker sleeps until the window reopens instead of
//! polling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common;
use oxn::backend::Backend;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

fn limited_worker(
    q: &Queue<T>,
    starts: Arc<Mutex<Vec<Instant>>>,
    max: u32,
    per: Duration,
) -> Worker<T> {
    Worker::builder(q.clone(), move |_j: Job<T>| {
        let starts = starts.clone();
        async move {
            starts.lock().push(Instant::now());
            Ok::<_, Error>(())
        }
    })
    .concurrency(4)
    .rate_limit(max, per)
    .drain_delay(ms(50))
    .build()
}

/// Most starts inside any sliding window of `per`.
fn peak(starts: &[Instant], per: Duration) -> usize {
    let mut s = starts.to_vec();
    s.sort();
    (0..s.len())
        .map(|i| s[i..].iter().take_while(|t| **t - s[i] < per).count())
        .max()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_limit_is_shared_by_every_worker_on_the_queue() {
    let (q, _b) = common::fresh_queue::<T>("b12d-shared").await;
    let mut rec = Recorder::new("b12d_rate_limit_shared", q.name()).await;
    rec.step(
        "add 30 jobs",
        q.add_bulk((0..30).map(|i| (T(i), JobOptions::new())).collect()),
    )
    .await
    .unwrap();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let t0 = Instant::now();
    // Two workers (separate backends, like two processes), 10 jobs per 500ms.
    let mut handles = Vec::new();
    let mut cancels = Vec::new();
    for _ in 0..2 {
        let b2 = common::build_backend().await as Arc<dyn Backend>;
        let wq: Queue<T> = Queue::from_backend(
            q.name(),
            b2,
            QueueOptions::default().prefix(common::TEST_PREFIX),
        )
        .unwrap();
        let w = limited_worker(&wq, starts.clone(), 10, ms(500));
        cancels.push(w.cancellation_token());
        handles.push(tokio::spawn(w.run()));
    }
    assert!(
        rec.wait_until("all 30 done", secs(10), |s| s.len("completed") == 30)
            .await
    );
    let took = t0.elapsed();
    for c in &cancels {
        c.cancel();
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let s = starts.lock().clone();
    assert_eq!(s.len(), 30);
    // 30 jobs at 10 per 500ms is at least two full windows after the first.
    assert!(
        took >= ms(950),
        "30 jobs finished in {took:?}: the limit didn't hold"
    );
    // Fixed windows allow up to 2×max across a window boundary, never more.
    let peak = peak(&s, ms(500));
    assert!(
        peak <= 20,
        "{peak} starts within one 500ms span (limit 10, shared)"
    );
    let first_window = s.iter().filter(|t| **t - t0 < ms(450)).count();
    assert!(
        first_window <= 10,
        "{first_window} starts in the first window"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_limited_worker_waits_instead_of_polling() {
    let (q, _b) = common::fresh_queue::<T>("b12d-idle").await;
    let mut rec = Recorder::new("b12d_rate_limit_waits", q.name()).await;
    rec.step(
        "add 6 jobs",
        q.add_bulk((0..6).map(|i| (T(i), JobOptions::new())).collect()),
    )
    .await
    .unwrap();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let w = limited_worker(&q, starts.clone(), 2, secs(1));
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    assert!(
        rec.wait_until("all 6 done", secs(6), |s| s.len("completed") == 6)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    // ~3 windows. Polling would issue hundreds of move_to_active calls.
    let calls = rec
        .traces_named("oxn.script")
        .iter()
        .filter(|t| t.fields.get("script").map(String::as_str) == Some("move_to_active"))
        .count();
    assert!(
        calls < 40,
        "{calls} move_to_active calls while rate limited"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_interrupts_a_rate_limit_wait() {
    let (q, _b) = common::fresh_queue::<T>("b12d-cancel").await;
    q.add_bulk((0..5).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let w = limited_worker(&q, starts.clone(), 1, secs(30));
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    tokio::time::sleep(ms(300)).await; // one job ran; now waiting ~30s
    let t = Instant::now();
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(
        t.elapsed() < ms(500),
        "shutdown waited {:?} for the rate-limit window",
        t.elapsed()
    );
    assert_eq!(starts.lock().len(), 1);
    common::teardown(&q).await;
}
