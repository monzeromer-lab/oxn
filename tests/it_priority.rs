//! Priority ordering: lower `priority` number ⇒ processed first.

mod common;

use std::sync::Arc;
use std::time::Duration;

use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Task {
    label: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn prioritized_jobs_land_in_prioritized_zset() {
    let (q, _be) = common::fresh_queue::<Task>("prio-zset").await;

    q.add(
        Task {
            label: "low".into(),
        },
        JobOptions::new().priority(10),
    )
    .await
    .unwrap();

    let c = q.counts().await.unwrap();
    assert_eq!(c.prioritized, 1);
    assert_eq!(c.waiting, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_consumes_high_priority_first() {
    let (q, _be) = common::fresh_queue::<Task>("prio-order").await;

    // Submit in reverse order on purpose.
    q.add(
        Task {
            label: "c".into(),
        },
        JobOptions::new().priority(30),
    )
    .await
    .unwrap();
    q.add(
        Task {
            label: "a".into(),
        },
        JobOptions::new().priority(10),
    )
    .await
    .unwrap();
    q.add(
        Task {
            label: "b".into(),
        },
        JobOptions::new().priority(20),
    )
    .await
    .unwrap();

    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen_c = seen.clone();

    let worker = Worker::builder(q.clone(), move |job: Job<Task>| {
        let seen_c = seen_c.clone();
        async move {
            seen_c.lock().unwrap().push(job.data.label);
            Ok::<_, Error>(())
        }
    })
    .concurrency(1) // force ordered consumption
    .drain_delay(Duration::from_millis(100))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(5), || {
        let s = seen.clone();
        async move { s.lock().unwrap().len() == 3 }
    })
    .await;
    assert!(ok, "3 jobs never processed: {:?}", seen.lock().unwrap());

    let order = seen.lock().unwrap().clone();
    assert_eq!(order, vec!["a", "b", "c"], "priority order broken");

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}
