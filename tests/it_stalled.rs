//! Stalled recovery: if a worker dies while holding a lock, the stalled
//! scanner should requeue the job.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oxn::backend::Backend;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn scan_stalled_requeues_jobs_with_expired_locks() {
    let (q, backend) = common::fresh_queue::<T>("stalled").await;

    // Add a job and manually fetch it — simulating a worker that then dies.
    q.add(T, JobOptions::new()).await.unwrap();

    let token = "ghost-worker-1:0".to_string();
    let fetched = backend
        .fetch_next(q.name(), &token, Duration::from_millis(300), Duration::from_millis(100))
        .await
        .unwrap();
    assert!(fetched.is_some(), "fetch_next should succeed");

    // Let the lock expire.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Call scan_stalled — it should move the job back to `wait`.
    let moved = backend.scan_stalled(q.name(), 3).await.unwrap();
    assert_eq!(moved.len(), 1);
    let c = q.counts().await.unwrap();
    assert_eq!(c.waiting, 1);
    assert_eq!(c.active, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stalled_scanner_is_rate_limited() {
    let (q, backend) = common::fresh_queue::<T>("stalled-rate").await;
    q.add(T, JobOptions::new()).await.unwrap();
    let token = "t".to_string();
    backend
        .fetch_next(q.name(), &token, Duration::from_millis(200), Duration::from_millis(100))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // First scan picks up the stalled job…
    let first = backend.scan_stalled(q.name(), 3).await.unwrap();
    assert_eq!(first.len(), 1);

    // …a second scan within 1s should be a no-op (rate-limited).
    let second = backend.scan_stalled(q.name(), 3).await.unwrap();
    assert!(
        second.is_empty(),
        "scanner should be rate limited, got {second:?}"
    );

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_recovers_stalled_job_automatically() {
    let (q, backend) = common::fresh_queue::<T>("stalled-auto").await;
    q.add(T, JobOptions::new()).await.unwrap();

    // Simulate a dead worker by fetching the job with a short lock and not
    // renewing it. We call fetch_next directly via the backend and drop the
    // result.
    let token = "dead-1:0".to_string();
    backend
        .fetch_next(q.name(), &token, Duration::from_millis(200), Duration::from_millis(100))
        .await
        .unwrap();

    // Wait for the lock to expire.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Start a live worker — it should run the stalled scanner and recover.
    let processed = Arc::new(AtomicU32::new(0));
    let pc = processed.clone();
    let worker = Worker::builder(q.clone(), move |_job: Job<T>| {
        let pc = pc.clone();
        async move {
            pc.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .concurrency(1)
    .stalled_interval(Duration::from_millis(200))
    .drain_delay(Duration::from_millis(100))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(5), || {
        let p = processed.clone();
        async move { p.load(Ordering::SeqCst) >= 1 }
    })
    .await;
    assert!(ok, "stalled job never recovered");

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}
