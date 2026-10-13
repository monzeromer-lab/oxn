//! B12a — `add_bulk` is one script call per 1,000 jobs, not one per job.
//!
//! In 0.2 the Redis backend had no `add_bulk` of its own: the trait's
//! default looped `add()`, one round trip and one script call per job, with
//! other clients free to interleave. The doc comment claimed a pipelined
//! override that didn't exist.

use crate::common;
use oxn::prelude::*;
use oxn_testkit::{Recorder, secs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct T(u32);

fn script_calls(rec: &Recorder, script: &str) -> usize {
    rec.traces_named("oxn.script")
        .iter()
        .filter(|t| t.fields.get("script").map(String::as_str) == Some(script))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_bulk_is_one_script_call_per_thousand_jobs() {
    let (q, _b) = common::fresh_queue::<T>("b12a-calls").await;
    let mut rec = Recorder::new("b12a_bulk_calls", q.name()).await;
    let ids = rec
        .step(
            "add_bulk 2,500",
            q.add_bulk((0..2500).map(|i| (T(i), JobOptions::new())).collect()),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 2500);
    let ids: Vec<u64> = ids.iter().map(|i| i.as_str().parse().unwrap()).collect();
    assert_eq!(
        ids,
        (1..=2500).collect::<Vec<_>>(),
        "ids in submission order"
    );
    assert_eq!(script_calls(&rec, "add_job"), 0, "no per-job calls");
    assert_eq!(script_calls(&rec, "add_bulk"), 3, "2,500 jobs = 3 chunks");
    let s = rec.snapshot();
    assert_eq!(s.len("wait"), 2500);
    assert_eq!(rec.events_named("added").len(), 2500);
    // processing order == submission order
    let first = q
        .list(JobState::Waiting, oxn::backend::JobRange::first(3))
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|j| j.data.clone()).collect::<Vec<_>>(),
        vec![T(0), T(1), T(2)]
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bulk_honors_every_option_exactly_like_single_adds() {
    let (q, _b) = common::fresh_queue::<T>("b12a-opts").await;
    let mut rec = Recorder::new("b12a_bulk_options", q.name()).await;
    let dedup = || JobOptions::new().deduplicate("d-1", secs(60));
    let ids = rec
        .step(
            "add_bulk with mixed options",
            q.add_bulk(vec![
                (T(0), JobOptions::new()),
                (T(1), JobOptions::new().delay(secs(60))),
                (T(2), JobOptions::new().priority(3)),
                (T(3), JobOptions::new().id("x")),
                (T(4), JobOptions::new().id("x")), // duplicate within the batch
                (T(5), dedup()),
                (T(6), dedup()), // dedup collision within the batch
                (T(7), JobOptions::new().lifo(true)),
            ]),
        )
        .await
        .unwrap();
    let ids: Vec<&str> = ids.iter().map(JobId::as_str).collect();
    assert_eq!(ids[3], "x");
    assert_eq!(
        ids[4], "x",
        "second add of an existing explicit id returns it"
    );
    assert_eq!(ids[5], ids[6], "dedup collapses within the batch");
    let s = rec.snapshot();
    assert_eq!(s.location(ids[1]), "delayed");
    assert_eq!(s.location(ids[2]), "prioritized");
    assert_eq!(s.location("x"), "wait");
    assert_eq!(
        s.lists["wait"].last().map(String::as_str),
        Some(ids[7]),
        "LIFO job is next"
    );
    assert_eq!(
        q.get(&JobId::new("x")).await.unwrap().unwrap().data,
        T(3),
        "first write wins"
    );
    assert_eq!(rec.events_named("duplicated").len(), 1);
    assert_eq!(rec.events_named("deduplicated").len(), 1);
    assert_eq!(rec.events_named("added").len(), 6);

    // A single add of the same options produces an identical job record.
    let single = q.add(T(2), JobOptions::new().priority(3)).await.unwrap();
    rec.observe("single add for comparison").await;
    let s = rec.snapshot();
    let strip = |id: &str| {
        let mut f = s.jobs[id].fields.clone();
        f.remove("timestamp");
        f
    };
    assert_eq!(
        strip(ids[2]),
        strip(single.as_str()),
        "bulk and single adds write the same hash"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_jobs_are_rejected_before_any_chunk_is_written() {
    let (q, _b) = common::fresh_queue::<T>("b12a-invalid").await;
    let mut rec = Recorder::new("b12a_bulk_invalid", q.name()).await;
    let mut batch: Vec<(T, JobOptions)> = (0..1500).map(|i| (T(i), JobOptions::new())).collect();
    batch.push((T(9), JobOptions::new().id("bad:id")));
    let r = rec
        .step("add_bulk with one invalid id at the end", q.add_bulk(batch))
        .await;
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    assert_eq!(
        rec.snapshot().key_count,
        0,
        "nothing written, not even the first chunk"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}
