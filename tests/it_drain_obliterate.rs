//! drain(), obliterate(), remove() cleanups.

mod common;

use oxn::backend::Backend;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn drain_empties_pending_but_preserves_delayed_by_default() {
    let (q, _be) = common::fresh_queue::<T>("drain-pending").await;

    q.add(T, JobOptions::new()).await.unwrap();
    q.add(T, JobOptions::new().priority(3)).await.unwrap();
    q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
        .await
        .unwrap();

    q.drain(false).await.unwrap();
    let c = q.counts().await.unwrap();
    assert_eq!(c.waiting, 0);
    assert_eq!(c.prioritized, 0);
    assert_eq!(c.delayed, 1, "delayed survives default drain");

    q.drain(true).await.unwrap();
    assert_eq!(q.counts().await.unwrap().delayed, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn obliterate_refuses_while_active_unless_forced() {
    let (q, backend) = common::fresh_queue::<T>("obliterate-active").await;
    q.add(T, JobOptions::new()).await.unwrap();

    // Take the job into `active` without completing it.
    let tok = "w:0".to_string();
    backend
        .fetch_next(q.name(), &tok, Duration::from_secs(30), Duration::from_millis(100))
        .await
        .unwrap();

    let err = q.obliterate(false).await.unwrap_err();
    assert!(matches!(err, Error::Config(_)));

    // Forced obliterate should succeed even with active jobs.
    q.obliterate(true).await.unwrap();
    // Reset backend's queue registry entry by calling list_queues.
    let _ = backend.list_queues().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_deletes_job_hash_and_lists() {
    let (q, _be) = common::fresh_queue::<T>("remove").await;

    let id = q.add(T, JobOptions::new()).await.unwrap();
    assert!(q.get(&id).await.unwrap().is_some());

    q.remove(&id).await.unwrap();
    assert!(q.get(&id).await.unwrap().is_none());
    assert_eq!(q.counts().await.unwrap().waiting, 0);

    common::teardown(&q).await;
}
