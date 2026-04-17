//! Deduplication: adding a second job with the same dedup id within the TTL
//! should return the existing id and not produce a second job.

mod common;

use std::time::Duration;

use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_submissions_collapse_within_ttl() {
    let (q, _be) = common::fresh_queue::<T>("dedup").await;

    let first = q
        .add(
            T,
            JobOptions::new().deduplicate("abc", Duration::from_secs(60)),
        )
        .await
        .unwrap();
    let second = q
        .add(
            T,
            JobOptions::new().deduplicate("abc", Duration::from_secs(60)),
        )
        .await
        .unwrap();

    assert_eq!(first.as_str(), second.as_str(), "ids should collapse");
    assert_eq!(q.counts().await.unwrap().waiting, 1, "only one real job");

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_dedup_keys_produce_distinct_jobs() {
    let (q, _be) = common::fresh_queue::<T>("dedup-distinct").await;

    let a = q
        .add(
            T,
            JobOptions::new().deduplicate("a", Duration::from_secs(60)),
        )
        .await
        .unwrap();
    let b = q
        .add(
            T,
            JobOptions::new().deduplicate("b", Duration::from_secs(60)),
        )
        .await
        .unwrap();

    assert_ne!(a.as_str(), b.as_str());
    assert_eq!(q.counts().await.unwrap().waiting, 2);

    common::teardown(&q).await;
}
