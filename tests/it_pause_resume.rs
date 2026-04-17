//! Pause/resume semantics.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn pause_moves_wait_to_paused_and_flag_is_set() {
    let (q, _be) = common::fresh_queue::<T>("pause-basic").await;

    q.add(T, JobOptions::new()).await.unwrap();
    q.add(T, JobOptions::new()).await.unwrap();

    assert_eq!(q.counts().await.unwrap().waiting, 2);
    q.pause().await.unwrap();
    let c = q.counts().await.unwrap();
    assert_eq!(c.waiting, 0);
    assert_eq!(c.paused, 2);
    assert!(q.is_paused().await.unwrap());

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_moves_paused_back_to_wait() {
    let (q, _be) = common::fresh_queue::<T>("pause-resume").await;

    q.add(T, JobOptions::new()).await.unwrap();
    q.pause().await.unwrap();
    q.resume().await.unwrap();

    let c = q.counts().await.unwrap();
    assert_eq!(c.paused, 0);
    assert_eq!(c.waiting, 1);
    assert!(!q.is_paused().await.unwrap());

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn new_adds_while_paused_go_to_paused_list() {
    let (q, _be) = common::fresh_queue::<T>("pause-add-while").await;

    q.pause().await.unwrap();
    q.add(T, JobOptions::new()).await.unwrap();
    q.add(T, JobOptions::new()).await.unwrap();

    let c = q.counts().await.unwrap();
    assert_eq!(c.paused, 2);
    assert_eq!(c.waiting, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_does_not_process_while_paused() {
    let (q, _be) = common::fresh_queue::<T>("pause-worker").await;

    q.add(T, JobOptions::new()).await.unwrap();
    q.pause().await.unwrap();

    let processed = Arc::new(AtomicU32::new(0));
    let p = processed.clone();
    let worker = Worker::builder(q.clone(), move |_job: Job<T>| {
        let p = p.clone();
        async move {
            p.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(100))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(processed.load(Ordering::SeqCst), 0, "handler ran despite pause");

    q.resume().await.unwrap();
    let ok = common::wait_for(Duration::from_secs(3), || {
        let p = processed.clone();
        async move { p.load(Ordering::SeqCst) == 1 }
    })
    .await;
    assert!(ok, "did not process after resume");

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}
