//! B6 — explicit job ids are stable and validated.
//!
//! In 0.2 re-adding an explicit id overwrote the existing job's hash in
//! place (keeping stale fields such as `returnvalue`) and pushed the id into
//! `wait` again — a completed job reappeared in two containers at once. Ids
//! and queue names were not validated, so an id like `wait` or `a:lock`
//! collided with queue-level or per-job keys.

use crate::common;

use std::sync::Arc;

use oxn::backend::Backend;
use oxn::options::Removal;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct T(u32);

async fn complete_one(b: &Arc<oxn::backend::redis::RedisBackend>, q: &str, removal: Removal) {
    let f = b
        .fetch_next(q, "tok", secs(30), ms(10), &CancellationToken::new(), None)
        .await
        .unwrap()
        .unwrap();
    b.complete(q, &f.job.id, "tok", Some("\"done\"".into()), removal)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn re_adding_a_waiting_id_returns_it_unchanged() {
    let (q, _b) = common::fresh_queue::<T>("b6-wait").await;
    let mut rec = Recorder::new("b6_readd_waiting", q.name()).await;
    let id = rec
        .step(
            "add id=order-7",
            q.add(T(1), JobOptions::new().id("order-7")),
        )
        .await
        .unwrap();
    let again = rec
        .step(
            "add id=order-7 again",
            q.add(T(2), JobOptions::new().id("order-7")),
        )
        .await
        .unwrap();
    assert_eq!(again, id);
    let s = rec.snapshot();
    assert_eq!(s.lists["wait"], vec!["order-7".to_string()], "queued once");
    assert_eq!(
        q.get(&id).await.unwrap().unwrap().data,
        T(1),
        "original payload kept"
    );
    let dup = rec.events_named("duplicated");
    assert_eq!(dup.len(), 1);
    assert_eq!(dup[0].job_id.as_deref(), Some("order-7"));
    assert_eq!(rec.events_named("added").len(), 1, "no second 'added'");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn re_adding_a_completed_id_does_not_resurrect_it() {
    let (q, b) = common::fresh_queue::<T>("b6-done").await;
    let mut rec = Recorder::new("b6_readd_completed", q.name()).await;
    rec.step("add id=x", q.add(T(1), JobOptions::new().id("x")))
        .await
        .unwrap();
    rec.step("complete it", complete_one(&b, q.name(), Removal::Keep))
        .await;
    let again = rec
        .step("add id=x again", q.add(T(2), JobOptions::new().id("x")))
        .await
        .unwrap();
    assert_eq!(again.as_str(), "x");
    let s = rec.snapshot();
    assert_eq!(
        s.location("x"),
        "completed",
        "not queued a second time: {}",
        s.summary()
    );
    let job = q.get(&again).await.unwrap().unwrap();
    assert_eq!(job.data, T(1));
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_id_is_reusable_once_its_job_is_gone() {
    let (q, b) = common::fresh_queue::<T>("b6-reuse").await;
    let mut rec = Recorder::new("b6_reuse_after_remove", q.name()).await;
    rec.step("add id=x", q.add(T(1), JobOptions::new().id("x")))
        .await
        .unwrap();
    rec.step(
        "complete with Remove",
        complete_one(&b, q.name(), Removal::Remove),
    )
    .await;
    rec.step("add id=x again", q.add(T(2), JobOptions::new().id("x")))
        .await
        .unwrap();
    assert_eq!(rec.snapshot().location("x"), "wait");
    assert_eq!(
        q.get(&JobId::new("x")).await.unwrap().unwrap().data,
        T(2),
        "a brand-new job"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_adds_of_one_id_create_exactly_one_job() {
    let (q, _b) = common::fresh_queue::<T>("b6-race").await;
    let mut rec = Recorder::new("b6_concurrent_adds", q.name()).await;
    let adds = (0..20).map(|i| {
        let q = q.clone();
        tokio::spawn(async move { q.add(T(i), JobOptions::new().id("same")).await })
    });
    let ids = rec
        .step(
            "20 concurrent adds of id=same",
            futures::future::join_all(adds),
        )
        .await;
    for r in ids {
        assert_eq!(r.unwrap().unwrap().as_str(), "same");
    }
    let s = rec.snapshot();
    assert_eq!(s.lists["wait"].len(), 1, "{}", s.summary());
    assert_eq!(rec.events_named("added").len(), 1);
    assert_eq!(rec.events_named("duplicated").len(), 19);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_ids_are_rejected_before_anything_is_written() {
    let (q, _b) = common::fresh_queue::<T>("b6-invalid").await;
    let mut rec = Recorder::new("b6_invalid_ids", q.name()).await;
    for bad in [
        "",
        "a:b",
        "x:lock",
        "123",
        "007",
        "wait",
        "meta",
        "events",
        "marker",
        "id",
        "waiting-children",
    ] {
        let r = rec
            .step(
                &format!("add id={bad:?}"),
                q.add(T(1), JobOptions::new().id(bad)),
            )
            .await;
        assert!(
            matches!(r, Err(Error::Config(_))),
            "id {bad:?} accepted: {r:?}"
        );
    }
    assert_eq!(
        rec.snapshot().key_count,
        0,
        "nothing written: {}",
        rec.snapshot().summary()
    );
    // Ids that look similar but are fine.
    for ok in ["order-123", "a.b", "user_42", "12a", "waiting"] {
        q.add(T(1), JobOptions::new().id(ok))
            .await
            .unwrap_or_else(|e| panic!("{ok}: {e}"));
    }
    rec.observe("valid ids added").await;
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_queue_names_are_rejected() {
    let backend = common::build_backend().await as Arc<dyn Backend>;
    for bad in ["", "a:b", "has space", "tab\tname", "new\nline"] {
        let r = Queue::<T>::from_backend(bad, backend.clone(), QueueOptions::default());
        assert!(
            matches!(r, Err(Error::Config(_))),
            "queue name {bad:?} accepted"
        );
        let r = Queue::<T>::builder(bad)
            .backend(backend.clone())
            .build()
            .await;
        assert!(
            matches!(r, Err(Error::Config(_))),
            "builder accepted {bad:?}"
        );
    }
    for ok in ["emails", "emails.v2", "tenant-7_reports"] {
        Queue::<T>::from_backend(ok, backend.clone(), QueueOptions::default()).unwrap();
    }
}
