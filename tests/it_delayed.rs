//! Delayed jobs: they should sit in the `delayed` zset until their time,
//! then be picked up by the worker.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Task;

#[tokio::test(flavor = "multi_thread")]
async fn delayed_job_stays_in_delayed_zset_until_due() {
    let (q, _be) = common::fresh_queue::<Task>("delayed-zset").await;

    q.add(Task, JobOptions::new().delay(Duration::from_secs(5)))
        .await
        .unwrap();

    let counts = q.counts().await.unwrap();
    assert_eq!(counts.delayed, 1, "should be in delayed");
    assert_eq!(counts.waiting, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_job_runs_only_after_delay_elapses() {
    let (q, _be) = common::fresh_queue::<Task>("delayed-runs").await;

    q.add(Task, JobOptions::new().delay(Duration::from_millis(400)))
        .await
        .unwrap();

    let started = Instant::now();
    let ran_at = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let ran_at_c = ran_at.clone();

    let worker = Worker::builder(q.clone(), move |_job: Job<Task>| {
        let ran_at_c = ran_at_c.clone();
        async move {
            *ran_at_c.lock().unwrap() = Some(Instant::now());
            Ok::<_, Error>(())
        }
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(100))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(3), || {
        let r = ran_at.clone();
        async move { r.lock().unwrap().is_some() }
    })
    .await;
    assert!(ok, "handler never fired");

    let ran = ran_at.lock().unwrap().unwrap();
    let elapsed = ran.duration_since(started);
    assert!(
        elapsed >= Duration::from_millis(380),
        "ran too early after only {elapsed:?}"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_moves_delayed_into_waiting() {
    let (q, _be) = common::fresh_queue::<Task>("promote-delayed").await;

    let id = q
        .add(Task, JobOptions::new().delay(Duration::from_secs(60)))
        .await
        .unwrap();

    assert_eq!(q.counts().await.unwrap().delayed, 1);
    q.promote(&id).await.unwrap();
    let c = q.counts().await.unwrap();
    assert_eq!(c.delayed, 0);
    assert_eq!(c.waiting, 1);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_returning_delayed_reschedules_job() {
    let (q, _be) = common::fresh_queue::<Task>("handler-delayed").await;

    q.add(Task, JobOptions::new().attempts(5))
        .await
        .unwrap();

    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let worker = Worker::builder(q.clone(), move |_job: Job<Task>| {
        let a = a.clone();
        async move {
            if a.fetch_add(1, Ordering::SeqCst) == 0 {
                // First run: postpone.
                Err(Error::Delayed { delay_ms: 300 })
            } else {
                Ok::<_, Error>(())
            }
        }
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(100))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(4), || {
        let a = attempts.clone();
        async move { a.load(Ordering::SeqCst) >= 2 }
    })
    .await;
    assert!(ok, "job never resumed after delay");

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}
