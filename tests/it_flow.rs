//! FlowProducer integration (requires the `flow` feature).

#![cfg(feature = "flow")]

mod common;

use oxn::backend::Backend;
use oxn::flow::{FlowNode, FlowProducer};
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Job {
    step: u32,
}

#[tokio::test(flavor = "multi_thread")]
async fn flow_tree_creates_one_job_per_node() {
    let (root_q, backend) = common::fresh_queue::<Job>("flow-root").await;
    let child_name = format!("{}-child", root_q.name());
    let child_q: Queue<Job> = common::queue(backend.clone(), &child_name).await;

    let fp = FlowProducer::new(backend.clone());
    let tree = FlowNode::new(root_q.name(), "root", &Job { step: 0 }, JobOptions::default())
        .unwrap()
        .child(
            FlowNode::new(&child_name, "a", &Job { step: 1 }, JobOptions::default())
                .unwrap(),
        )
        .child(
            FlowNode::new(&child_name, "b", &Job { step: 2 }, JobOptions::default())
                .unwrap(),
        );

    let root_id = fp.add(tree).await.unwrap();

    // Parent exists.
    assert!(backend.get(root_q.name(), &root_id).await.unwrap().is_some());
    // Each child queue has one waiting job.
    let c = backend.counts(&child_name).await.unwrap();
    assert_eq!(c.waiting, 2);

    common::teardown(&root_q).await;
    common::teardown(&child_q).await;
}
