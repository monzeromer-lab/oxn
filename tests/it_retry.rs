//! Retry with backoff; unrecoverable terminal error; attempts exhausted.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxn::job::Backoff;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn transient_failure_triggers_retry_with_backoff() {
    let (q, _be) = common::fresh_queue::<T>("retry-backoff").await;

    q.add(
        T,
        JobOptions::new().attempts(3).backoff(Backoff::Fixed {
            delay: Duration::from_millis(150),
        }),
    )
    .await
    .unwrap();

    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let started = Instant::now();
    let worker = Worker::builder(q.clone(), move |_job: Job<T>| {
        let a = a.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(Error::handler(std::io::Error::other("flaky")))
            } else {
                Ok::<_, Error>(())
            }
        }
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(5), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 1).unwrap_or(false) }
    })
    .await;
    assert!(
        ok,
        "job never succeeded; attempts={} counts={:?}",
        attempts.load(Ordering::SeqCst),
        q.counts().await.unwrap()
    );

    assert_eq!(attempts.load(Ordering::SeqCst), 3, "3 attempts expected");
    // Retries are 150ms each, so at least 300ms total.
    assert!(started.elapsed() >= Duration::from_millis(290));

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exhausted_attempts_put_job_in_failed_set() {
    let (q, _be) = common::fresh_queue::<T>("retry-exhaust").await;

    q.add(
        T,
        JobOptions::new()
            .attempts(2)
            .backoff(Backoff::Fixed {
                delay: Duration::from_millis(50),
            }),
    )
    .await
    .unwrap();

    let worker = Worker::builder(q.clone(), |_job: Job<T>| async move {
        Err::<(), _>(Error::handler(std::io::Error::other("always-fails")))
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(4), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.failed == 1).unwrap_or(false) }
    })
    .await;
    assert!(ok, "job never landed in failed set: {:?}", q.counts().await.unwrap());

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unrecoverable_skips_retries() {
    let (q, _be) = common::fresh_queue::<T>("retry-unrecoverable").await;

    q.add(T, JobOptions::new().attempts(5)).await.unwrap();

    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let worker = Worker::builder(q.clone(), move |_job: Job<T>| {
        let a = a.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(Error::unrecoverable("nope"))
        }
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(3), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.failed == 1).unwrap_or(false) }
    })
    .await;
    assert!(ok, "failed state never reached");

    // We should not have used more than the 1 attempt — no retry on unrecoverable.
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_api_moves_failed_back_to_wait() {
    let (q, _be) = common::fresh_queue::<T>("retry-api").await;

    q.add(T, JobOptions::new().attempts(1)).await.unwrap();

    let worker = Worker::builder(q.clone(), |_job: Job<T>| async move {
        Err::<(), _>(Error::handler(std::io::Error::other("fail")))
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    // wait for it to fail
    common::wait_for(Duration::from_secs(3), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.failed == 1).unwrap_or(false) }
    })
    .await;

    // Grab the id from the failed zset
    let failed = q
        .list(JobState::Failed, oxn::backend::JobRange::first(5))
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    let id = failed[0].id.clone();

    cancel.cancel();
    handle.await.unwrap().unwrap();

    // Now retry via the queue API.
    q.retry(&id).await.unwrap();
    let c = q.counts().await.unwrap();
    assert_eq!(c.failed, 0);
    assert_eq!(c.waiting, 1);

    common::teardown(&q).await;
}
