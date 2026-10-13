//! B1 — every Lua return code surfaces as a typed error.
//!
//! Before 0.3, only `extend_lock` checked its script's return value;
//! `retry`, `promote`, `remove`, `complete` and `fail` reported success no
//! matter what the script decided. Each test here performs an operation the
//! script must reject, asserts the error, and uses the recorder to prove
//! the rejected call left Redis exactly as it was.

mod common;

use std::time::Duration;

use oxn::backend::Backend;
use oxn::error::ScriptError;
use oxn::options::Removal;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

fn is_not_found<T: std::fmt::Debug>(r: &oxn::Result<T>) -> bool {
    matches!(r, Err(Error::NotFound(_)))
}

fn is_script<T: std::fmt::Debug>(r: &oxn::Result<T>, want: ScriptError) -> bool {
    matches!(r, Err(Error::Script(e)) if *e == want)
}

async fn raw() -> redis::aio::MultiplexedConnection {
    redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_and_promote_reject_wrong_state_and_missing_jobs() {
    let (q, _b) = common::fresh_queue::<T>("b1-retry-promote").await;
    let mut rec = Recorder::new("b1_retry_promote", q.name()).await;
    let id = rec
        .step("add waiting job", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let before = rec.snapshot().summary();

    let r = rec.step("retry a waiting job", q.retry(&id)).await;
    assert!(is_script(&r, ScriptError::NotInExpectedState), "{r:?}");
    let r = rec.step("promote a waiting job", q.promote(&id)).await;
    assert!(is_script(&r, ScriptError::NotInExpectedState), "{r:?}");
    let r = rec
        .step("retry missing job", q.retry(&JobId::new("nope")))
        .await;
    assert!(is_not_found(&r), "{r:?}");
    let r = rec
        .step("promote missing job", q.promote(&JobId::new("nope")))
        .await;
    assert!(is_not_found(&r), "{r:?}");

    assert_eq!(
        rec.snapshot().summary(),
        before,
        "rejected calls must not change state"
    );
    assert_eq!(rec.snapshot().location(id.as_str()), "wait");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finishing_with_a_foreign_or_expired_lock_is_rejected() {
    let (q, b) = common::fresh_queue::<T>("b1-lock").await;
    let mut rec = Recorder::new("b1_lock_checks", q.name()).await;
    rec.step("add", q.add(T(1), JobOptions::new().attempts(3)))
        .await
        .unwrap();
    let f = rec
        .step(
            "fetch with token A",
            b.fetch_next(
                q.name(),
                "A",
                secs(30),
                ms(10),
                &tokio_util::sync::CancellationToken::new(),
            ),
        )
        .await
        .unwrap()
        .expect("job available");
    let id = f.job.id.clone();

    let r = rec
        .step(
            "complete with token B",
            b.complete(q.name(), &id, "B", None, Removal::Keep),
        )
        .await;
    assert!(is_script(&r, ScriptError::LockMismatch), "{r:?}");
    let r = rec
        .step(
            "retry-fail with token B",
            b.fail(q.name(), &id, "B", "x", Some(0), Removal::Keep),
        )
        .await;
    assert!(is_script(&r, ScriptError::LockMismatch), "{r:?}");
    let r = rec
        .step(
            "final-fail with token B",
            b.fail(q.name(), &id, "B", "x", None, Removal::Keep),
        )
        .await;
    assert!(is_script(&r, ScriptError::LockMismatch), "{r:?}");
    let r = rec
        .step(
            "extend with token B",
            b.extend_lock(q.name(), &id, "B", secs(30)),
        )
        .await;
    assert!(is_script(&r, ScriptError::LockMismatch), "{r:?}");
    assert_eq!(
        rec.snapshot().location(id.as_str()),
        "active",
        "still owned by A"
    );

    // Let A's lock expire, then A itself is too late.
    let mut c = raw().await;
    let lock = format!("{}:{}:{}:lock", common::TEST_PREFIX, q.name(), id);
    rec.step("expire A's lock", async {
        let _: () = c.del(&lock).await.unwrap();
    })
    .await;
    let r = rec
        .step(
            "complete with expired token A",
            b.complete(q.name(), &id, "A", None, Removal::Keep),
        )
        .await;
    assert!(is_script(&r, ScriptError::LockNotFound), "{r:?}");
    assert_eq!(
        rec.snapshot().location(id.as_str()),
        "active",
        "left for the stalled scanner"
    );

    // Only `lock-expired` (transient) may remain.
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_reports_job_deleted_while_active() {
    let (q, b) = common::fresh_queue::<T>("b1-gone").await;
    let mut rec = Recorder::new("b1_job_deleted_while_active", q.name()).await;
    rec.step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let f = b
        .fetch_next(
            q.name(),
            "A",
            secs(30),
            ms(10),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
    let id = f.job.id.clone();
    let mut c = raw().await;
    let hash = format!("{}:{}:{}", common::TEST_PREFIX, q.name(), id);
    rec.step("delete job hash behind oxn's back", async {
        let _: () = c.del(&hash).await.unwrap();
    })
    .await;
    let r = rec
        .step(
            "complete",
            b.complete(q.name(), &id, "A", None, Removal::Keep),
        )
        .await;
    assert!(is_not_found(&r), "{r:?}");
    // Step 2 is our own corruption and must have been detected...
    assert!(!rec.violations_of("dangling-entry").is_empty());
    // ...and the script cleans up the dangling active entry and lock.
    assert_eq!(rec.snapshot().location(id.as_str()), "(gone)");
    rec.assert_clean_from(3);
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_refuses_locked_jobs_and_reports_missing_ones() {
    let (q, b) = common::fresh_queue::<T>("b1-remove").await;
    let mut rec = Recorder::new("b1_remove", q.name()).await;
    let a = rec
        .step("add job a", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let bb = rec
        .step("add job b", q.add(T(2), JobOptions::new()))
        .await
        .unwrap();
    let f = rec
        .step(
            "fetch one",
            b.fetch_next(
                q.name(),
                "A",
                secs(30),
                ms(10),
                &tokio_util::sync::CancellationToken::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
    let active = f.job.id.clone();
    let waiting = if active == a { bb } else { a };

    let r = rec
        .step("remove the active, locked job", q.remove(&active))
        .await;
    assert!(is_script(&r, ScriptError::JobLocked), "{r:?}");
    assert_eq!(rec.snapshot().location(active.as_str()), "active");

    let r = rec.step("remove the waiting job", q.remove(&waiting)).await;
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(rec.snapshot().location(waiting.as_str()), "(gone)");

    let r = rec.step("remove it again", q.remove(&waiting)).await;
    assert!(is_not_found(&r), "{r:?}");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_reports_a_stolen_lock_instead_of_completing() {
    let (q, _b) = common::fresh_queue::<T>("b1-stolen").await;
    let mut rec = Recorder::new("b1_worker_stolen_lock", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();

    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let started_tx = std::sync::Mutex::new(Some(started_tx));
    let worker = Worker::builder(q.clone(), move |_j: Job<T>| {
        let tx = started_tx.lock().unwrap().take();
        async move {
            if let Some(tx) = tx {
                let _ = tx.send(());
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, Error>(())
        }
    })
    .drain_delay(ms(50))
    .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());
    started_rx.await.unwrap();

    let mut c = raw().await;
    let lock = format!("{}:{}:{}:lock", common::TEST_PREFIX, q.name(), id);
    rec.step("another worker steals the lock", async {
        let _: () = c.set(&lock, "thief").await.unwrap();
        let _: () = c
            .hset(
                format!("{}:{}:{}", common::TEST_PREFIX, q.name(), id),
                "lockToken",
                "thief",
            )
            .await
            .unwrap();
    })
    .await;
    rec.wait_until("handler finishes", secs(3), |_| false).await; // just let time pass
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("after shutdown").await;

    assert_eq!(
        rec.snapshot().location(id.as_str()),
        "active",
        "the thief still owns it"
    );
    assert!(
        rec.events_named("completed").is_empty(),
        "must not complete under a stolen lock"
    );
    let rejected: Vec<_> = rec
        .steps()
        .iter()
        .flat_map(|s| s.traces.iter())
        .filter(|t| t.name.starts_with("transition rejected"))
        .collect();
    assert_eq!(
        rejected.len(),
        1,
        "worker must report the rejected completion"
    );
    assert_eq!(
        rejected[0].level, "WARN",
        "a lost lock is a warning, not an error"
    );
    assert!(
        rejected[0]
            .fields
            .get("error")
            .is_some_and(|e| e.contains("lock token does not match"))
    );
    rec.assert_clean();
    common::teardown(&q).await;
}
