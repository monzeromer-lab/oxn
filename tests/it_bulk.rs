//! Bulk submission and draining.

mod common;

use serde::{Deserialize, Serialize};

use oxn::prelude::*;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Item(u32);

#[tokio::test(flavor = "multi_thread")]
async fn add_bulk_returns_one_id_per_input() {
    let (q, _be) = common::fresh_queue::<Item>("bulk").await;

    let jobs: Vec<_> = (0..25).map(|i| (Item(i), JobOptions::new())).collect();
    let ids = q.add_bulk(jobs).await.unwrap();
    assert_eq!(ids.len(), 25);
    let unique: std::collections::HashSet<_> = ids.iter().map(|i| i.as_str().to_string()).collect();
    assert_eq!(unique.len(), 25, "ids must all be distinct");

    let counts = q.counts().await.unwrap();
    assert_eq!(counts.waiting, 25);

    common::teardown(&q).await;
}
