//! Concurrency invariants under load.

mod common;

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T(u32);

#[tokio::test(flavor = "multi_thread")]
async fn every_job_runs_exactly_once_under_concurrency() {
    let (q, _be) = common::fresh_queue::<T>("conc-once").await;

    let n = 50u32;
    for i in 0..n {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }

    let seen = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::<u32>::new()));
    let done = Arc::new(AtomicU32::new(0));

    let seen_c = seen.clone();
    let done_c = done.clone();
    let worker = Worker::builder(q.clone(), move |job: Job<T>| {
        let seen_c = seen_c.clone();
        let done_c = done_c.clone();
        async move {
            let inserted = seen_c.lock().unwrap().insert(job.data.0);
            assert!(inserted, "job {} processed twice", job.data.0);
            done_c.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .concurrency(8)
    .drain_delay(Duration::from_millis(50))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    let ok = common::wait_for(Duration::from_secs(10), || {
        let d = done.clone();
        async move { d.load(Ordering::SeqCst) == n }
    })
    .await;
    assert!(ok, "only {} of {n} processed", done.load(Ordering::SeqCst));

    assert_eq!(seen.lock().unwrap().len(), n as usize);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrency_limit_is_enforced() {
    let (q, _be) = common::fresh_queue::<T>("conc-limit").await;

    for i in 0..10u32 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }

    let in_flight = Arc::new(AtomicI64::new(0));
    let peak = Arc::new(AtomicI64::new(0));
    let inf = in_flight.clone();
    let pk = peak.clone();

    let worker = Worker::builder(q.clone(), move |_job: Job<T>| {
        let inf = inf.clone();
        let pk = pk.clone();
        async move {
            let now = inf.fetch_add(1, Ordering::SeqCst) + 1;
            // Track peak in-flight count.
            let mut prev = pk.load(Ordering::SeqCst);
            while now > prev {
                match pk.compare_exchange(prev, now, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => break,
                    Err(cur) => prev = cur,
                }
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
            inf.fetch_sub(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .concurrency(3)
    .drain_delay(Duration::from_millis(20))
    .build();

    let cancel = worker.cancellation_token();
    let handle = tokio::spawn(worker.run());

    common::wait_for(Duration::from_secs(8), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 10).unwrap_or(false) }
    })
    .await;

    let p = peak.load(Ordering::SeqCst);
    assert!(p >= 2, "expected some parallelism, peak was {p}");
    assert!(p <= 3, "concurrency=3 was exceeded: peak={p}");

    cancel.cancel();
    handle.await.unwrap().unwrap();
    common::teardown(&q).await;
}
