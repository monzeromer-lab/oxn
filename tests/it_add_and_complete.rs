//! Basic producer/consumer happy path.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oxn::prelude::*;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Payload {
    step: u32,
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_then_worker_completes_and_counts_reflect_state() {
    let (q, _be) = common::fresh_queue::<Payload>("add-complete").await;

    // Submit a few jobs.
    for i in 0..3u32 {
        let id = q
            .add(Payload { step: i }, JobOptions::new())
            .await
            .expect("add");
        assert!(!id.as_str().is_empty(), "id is empty");
    }

    // Pre-run, everything is waiting.
    let counts = q.counts().await.unwrap();
    assert_eq!(counts.waiting, 3, "expected 3 waiting, got {counts:?}");

    let processed = Arc::new(AtomicU32::new(0));
    let counter = processed.clone();

    let worker = Worker::builder(q.clone(), move |_job: Job<Payload>| {
        let c = counter.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .concurrency(2)
    .cancellation_token(CancellationToken::new())
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    // Wait for all jobs to process.
    let ok = common::wait_for(Duration::from_secs(5), || {
        let c = processed.clone();
        async move { c.load(Ordering::SeqCst) == 3 }
    })
    .await;
    assert!(ok, "handler never processed all 3 jobs");

    // Post-run, counts reflect completion.
    let counts = q.counts().await.unwrap();
    assert_eq!(counts.waiting, 0);
    assert_eq!(counts.active, 0);
    assert_eq!(counts.completed, 3, "completed zset should hold 3");

    cancel.cancel();
    handle.await.unwrap().unwrap();

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_preserves_return_value() {
    let (q, _be) = common::fresh_queue::<Payload>("return-value").await;

    q.add(Payload { step: 5 }, JobOptions::new()).await.unwrap();

    // Handler returns a struct; we verify the job's hash on Redis stores it.
    let worker = Worker::builder(q.clone(), |_job: Job<Payload>| async move {
        Ok::<_, Error>("done-result".to_string())
    })
    .concurrency(1)
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(5), || async {
        q.counts().await.map(|c| c.completed == 1).unwrap_or(false)
    })
    .await;
    assert!(ok);

    // List the completed jobs and confirm return_value decoded.
    let jobs: Vec<Job<Payload>> = q
        .list(
            JobState::Completed,
            oxn::backend::JobRange::first(5),
        )
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    // The return type parameter on list() is unit by default, so we read the
    // raw hash for the string value.
    let raw = q.backend().get(q.name(), &jobs[0].id).await.unwrap().unwrap();
    assert_eq!(
        raw.return_value_json.as_deref(),
        Some("\"done-result\""),
        "return_value should be the JSON-encoded string"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn add_returns_distinct_ids_for_distinct_jobs() {
    let (q, _be) = common::fresh_queue::<Payload>("distinct-ids").await;

    let a = q.add(Payload { step: 1 }, JobOptions::new()).await.unwrap();
    let b = q.add(Payload { step: 2 }, JobOptions::new()).await.unwrap();
    assert_ne!(a.as_str(), b.as_str(), "ids should be monotonically distinct");

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_job_id_is_honored() {
    let (q, _be) = common::fresh_queue::<Payload>("explicit-id").await;

    let id = q
        .add(
            Payload { step: 0 },
            JobOptions::new().id("my-custom-id"),
        )
        .await
        .unwrap();
    assert_eq!(id.as_str(), "my-custom-id");

    let fetched = q.get(&id).await.unwrap().unwrap();
    assert_eq!(fetched.data.step, 0);

    common::teardown(&q).await;
}
