//! B5 — nothing leaks.
//!
//! 0.2 leaked in five places: `KeepLast`/`KeepFor` trimmed the zset but
//! left the trimmed jobs' hashes (and logs) behind; `Remove` left `:logs`;
//! `drain` emptied the lists but left every job hash; the `stalled` set was
//! written and never read or cleared; dedup pointers outlived the job they
//! pointed at. The recorder's `orphan-hash` / `orphan-aux` /
//! `dedup-dangling` / `stalled-set-leak` rules catch each of these.

use crate::common;

use std::sync::Arc;

use oxn::backend::Backend;
use oxn::options::Removal;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

/// Fetch + finish one job by hand (deterministic, no worker timing).
async fn finish(
    b: &Arc<oxn::backend::redis::RedisBackend>,
    q: &str,
    ok: bool,
    removal: Removal,
) -> JobId {
    let f = b
        .fetch_next(q, "tok", secs(30), ms(10), &CancellationToken::new())
        .await
        .unwrap()
        .expect("a job to finish");
    b.log(q, &f.job.id, "a log line").await.unwrap();
    if ok {
        b.complete(q, &f.job.id, "tok", None, removal)
            .await
            .unwrap();
    } else {
        b.fail(q, &f.job.id, "tok", "boom", None, removal, None)
            .await
            .unwrap();
    }
    f.job.id
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_last_deletes_trimmed_jobs_and_their_keys() {
    let (q, b) = common::fresh_queue::<T>("b5-last").await;
    let mut rec = Recorder::new("b5_keep_last", q.name()).await.strict();
    for i in 0..6 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }
    let mut done = Vec::new();
    for _ in 0..6 {
        done.push(
            rec.step(
                "finish with KeepLast(2)",
                finish(&b, q.name(), true, Removal::KeepLast(2)),
            )
            .await,
        );
    }
    let s = rec.snapshot();
    assert_eq!(s.len("completed"), 2, "{}", s.summary());
    assert_eq!(
        s.jobs.len(),
        2,
        "only the 2 retained hashes remain: {}",
        s.summary()
    );
    for id in &done[..4] {
        assert_eq!(s.location(id.as_str()), "(gone)");
    }
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_for_deletes_expired_jobs_and_their_keys() {
    let (q, b) = common::fresh_queue::<T>("b5-for").await;
    let mut rec = Recorder::new("b5_keep_for", q.name()).await.strict();
    for i in 0..4 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }
    let keep = Removal::KeepFor(ms(200));
    for _ in 0..3 {
        rec.step(
            "fail with KeepFor(200ms)",
            finish(&b, q.name(), false, keep),
        )
        .await;
    }
    tokio::time::sleep(ms(300)).await;
    let last = rec
        .step(
            "fail one more after 300ms",
            finish(&b, q.name(), false, keep),
        )
        .await;
    let s = rec.snapshot();
    assert_eq!(s.len("failed"), 1, "{}", s.summary());
    assert_eq!(s.location(last.as_str()), "failed");
    assert_eq!(s.jobs.len(), 1, "expired hashes deleted: {}", s.summary());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_policy_deletes_logs_too() {
    let (q, b) = common::fresh_queue::<T>("b5-remove").await;
    let mut rec = Recorder::new("b5_remove_policy", q.name()).await.strict();
    q.add(T(1), JobOptions::new()).await.unwrap();
    q.add(T(2), JobOptions::new()).await.unwrap();
    rec.step(
        "complete with Remove",
        finish(&b, q.name(), true, Removal::Remove),
    )
    .await;
    rec.step(
        "fail with Remove",
        finish(&b, q.name(), false, Removal::Remove),
    )
    .await;
    let s = rec.snapshot();
    assert!(
        s.jobs.is_empty() && s.orphan_aux.is_empty(),
        "{:?}",
        s.orphan_aux
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_deletes_job_hashes_and_emits_drained() {
    let (q, b) = common::fresh_queue::<T>("b5-drain").await;
    let mut rec = Recorder::new("b5_drain", q.name()).await.strict();
    for i in 0..5 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }
    for i in 0..2 {
        q.add(T(i), JobOptions::new().priority(1)).await.unwrap();
    }
    let delayed = q
        .add(T(9), JobOptions::new().delay(secs(60)))
        .await
        .unwrap();
    for id in ["1", "6"] {
        b.log(q.name(), &JobId::new(id), "line").await.unwrap();
    }
    rec.step("drain (keep delayed)", q.drain(false))
        .await
        .unwrap();
    let s = rec.snapshot();
    assert_eq!(
        s.jobs.len(),
        1,
        "only the delayed job's hash remains: {}",
        s.summary()
    );
    assert_eq!(s.location(delayed.as_str()), "delayed");
    assert!(
        s.orphan_aux.is_empty(),
        "drained jobs' logs deleted: {:?}",
        s.orphan_aux
    );
    assert_eq!(rec.events_named("drained").len(), 1);

    rec.step("drain (include delayed)", q.drain(true))
        .await
        .unwrap();
    assert!(
        rec.snapshot().jobs.is_empty(),
        "{}",
        rec.snapshot().summary()
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_a_job_clears_its_dedup_pointer() {
    let (q, b) = common::fresh_queue::<T>("b5-dedup").await;
    let mut rec = Recorder::new("b5_dedup_pointer", q.name()).await.strict();
    let dd = || JobOptions::new().deduplicate("upload-42", secs(60));

    let first = rec
        .step("add with dedup id", q.add(T(1), dd()))
        .await
        .unwrap();
    assert_eq!(
        rec.step("add same dedup id", q.add(T(2), dd()))
            .await
            .unwrap(),
        first,
        "deduplicated"
    );
    rec.step(
        "complete with Remove",
        finish(&b, q.name(), true, Removal::Remove),
    )
    .await;
    let second = rec
        .step("add same dedup id again", q.add(T(3), dd()))
        .await
        .unwrap();
    assert_ne!(second, first, "a removed job must not swallow new adds");
    assert_eq!(rec.snapshot().location(second.as_str()), "wait");

    rec.step("explicit remove", q.remove(&second))
        .await
        .unwrap();
    let third = rec
        .step("add after explicit remove", q.add(T(4), dd()))
        .await
        .unwrap();
    assert_ne!(third, second);
    rec.step("drain", q.drain(false)).await.unwrap();
    let fourth = rec
        .step("add after drain", q.add(T(5), dd()))
        .await
        .unwrap();
    assert_ne!(fourth, third);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_everything_is_batched_and_complete() {
    let (q, b) = common::fresh_queue::<T>("b5-clean").await;
    let mut rec = Recorder::new("b5_clean_batched", q.name()).await;
    let n = 2_500u32;
    q.add_bulk((0..n).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    for _ in 0..n {
        finish(&b, q.name(), true, Removal::Keep).await;
    }
    rec.observe("2500 completed").await;
    let removed = rec
        .step(
            "clean(completed, 0)",
            q.backend().clean(q.name(), JobState::Completed, 0),
        )
        .await
        .unwrap();
    assert_eq!(removed, n as u64);
    let s = rec.snapshot();
    assert!(
        s.jobs.is_empty() && s.orphan_aux.is_empty(),
        "{}",
        s.summary()
    );
    assert!(!rec.events_named("cleaned").is_empty());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_change_trims_in_bounded_steps_without_orphans() {
    let (q, b) = common::fresh_queue::<T>("b5-bounded").await;
    let mut rec = Recorder::new("b5_bounded_trim", q.name()).await;
    let n = 2_500u32;
    q.add_bulk((0..n + 4).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    for _ in 0..n {
        finish(&b, q.name(), true, Removal::Keep).await;
    }
    rec.observe("2500 kept").await;
    let mut sizes = Vec::new();
    for _ in 0..4 {
        rec.step(
            "finish with KeepLast(10)",
            finish(&b, q.name(), true, Removal::KeepLast(10)),
        )
        .await;
        sizes.push(rec.snapshot().len("completed"));
    }
    // ≤ 1000 deletions per finish keeps every call short; the set still
    // converges to the policy.
    assert_eq!(sizes, vec![1501, 502, 10, 10], "trim steps");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stall_recovery_leaves_no_stalled_set_behind() {
    let (q, b) = common::fresh_queue::<T>("b5-stalled").await;
    let mut rec = Recorder::new("b5_stalled_set", q.name()).await.strict();
    q.add(T(1), JobOptions::new()).await.unwrap();
    let none = CancellationToken::new();
    rec.step(
        "fetch with a 50ms lock",
        b.fetch_next(q.name(), "x", ms(50), ms(10), &none),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(ms(120)).await;
    let moved = rec
        .step("scan stalled", b.scan_stalled(q.name(), 5))
        .await
        .unwrap();
    assert_eq!(moved.len(), 1);
    let s = rec.snapshot();
    assert!(
        s.stalled.is_empty(),
        "stalled set must not accumulate: {:?}",
        s.stalled
    );
    rec.assert_clean();
    common::teardown(&q).await;
}
