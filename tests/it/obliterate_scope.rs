//! B11 — `obliterate` deletes exactly one queue, safely.
//!
//! In 0.2 `obliterate` deleted every key matching the unescaped SCAN glob
//! `{prefix}:{queue}:*`. Queue names may contain `*`, `?` and `[` (they
//! pass validation), so obliterating a queue named `*` matched every queue
//! under the prefix. The non-force "no active jobs" check was a separate
//! command from the deletion, so a worker could take a job in between and
//! have it deleted out from under it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common;
use oxn::backend::Backend;
use oxn::prelude::*;
use oxn_testkit::{ms, secs};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

async fn queue_named(b: &Arc<oxn::backend::redis::RedisBackend>, name: &str) -> Queue<T> {
    common::queue(b.clone(), name).await
}

async fn keys_of(q: &str) -> usize {
    let mut c = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let pattern = format!(
        "{}:{}:*",
        common::TEST_PREFIX,
        q.replace('*', "\\*")
            .replace('?', "\\?")
            .replace('[', "\\[")
    );
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(pattern)
        .query_async(&mut c)
        .await
        .unwrap();
    keys.len()
}

#[tokio::test(flavor = "multi_thread")]
async fn glob_characters_in_a_queue_name_do_not_widen_obliterate() {
    let b = common::build_backend().await;
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let victims = [
        format!("v{tag}-a"),
        format!("v{tag}-bb"),
        format!("v{tag}[ab]"),
    ];
    let mut queues = Vec::new();
    for name in &victims {
        let q = queue_named(&b, name).await;
        q.add(T(1), JobOptions::new()).await.unwrap();
        queues.push(q);
    }
    // Each of these, unescaped, matches some or all of the victims.
    for pattern_name in [
        format!("v{tag}*"),
        format!("v{tag}-?"),
        format!("v{tag}-?b"),
    ] {
        let q = queue_named(&b, &pattern_name).await;
        q.obliterate(true).await.unwrap();
    }
    for name in &victims {
        assert!(
            keys_of(name).await > 0,
            "queue {name} was deleted by a glob-shaped name"
        );
    }
    // And the literal "[ab]" queue obliterates only itself.
    queues[2].obliterate(true).await.unwrap();
    assert_eq!(keys_of(&victims[2]).await, 0);
    assert!(keys_of(&victims[0]).await > 0 && keys_of(&victims[1]).await > 0);
    for q in &queues {
        common::teardown(q).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn non_force_obliterate_refuses_with_active_jobs_and_changes_nothing() {
    let (q, b) = common::fresh_queue::<T>("b11-active").await;
    q.add(T(1), JobOptions::new()).await.unwrap();
    q.add(T(2), JobOptions::new()).await.unwrap();
    b.fetch_next(
        q.name(),
        "w",
        secs(30),
        ms(10),
        &CancellationToken::new(),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    let before = keys_of(q.name()).await;
    let r = q.obliterate(false).await;
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    assert_eq!(keys_of(q.name()).await, before, "nothing deleted");
    assert!(
        !q.is_paused().await.unwrap(),
        "a refused obliterate must not leave the queue paused"
    );
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn obliterate_removes_everything_including_orphans_and_the_registry_entry() {
    let (q, b) = common::fresh_queue::<T>("b11-all").await;
    q.add_bulk((0..3000).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    // A 0.2-style orphan hash and a stray aux key belong to the queue too.
    let mut c = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let base = format!("{}:{}:", common::TEST_PREFIX, q.name());
    let _: () = redis::cmd("HSET")
        .arg(format!("{base}orphan-1"))
        .arg("data")
        .arg("1")
        .query_async(&mut c)
        .await
        .unwrap();
    let _: () = redis::cmd("RPUSH")
        .arg(format!("{base}orphan-2:logs"))
        .arg("x")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(
        b.list_queues()
            .await
            .unwrap()
            .contains(&q.name().to_string())
    );
    q.obliterate(false).await.unwrap();
    assert_eq!(keys_of(q.name()).await, 0);
    assert!(
        !b.list_queues()
            .await
            .unwrap()
            .contains(&q.name().to_string())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_job_is_deleted_out_from_under_a_running_worker() {
    // Workers race obliterate(force=false): whenever it succeeds, no worker
    // may be left holding a job whose hash it just deleted.
    let (q, _b) = common::fresh_queue::<T>("b11-race").await;
    let rejected = Arc::new(AtomicU32::new(0));
    for round in 0..15 {
        q.add_bulk((0..60).map(|i| (T(i), JobOptions::new())).collect())
            .await
            .unwrap();
        let w = Worker::builder(q.clone(), |_j: Job<T>| async {
            tokio::time::sleep(ms(1)).await;
            Ok::<_, Error>(())
        })
        .concurrency(8)
        .drain_delay(ms(5))
        .build();
        let cancel = w.cancellation_token();
        let h = tokio::spawn(w.run());
        tokio::time::sleep(ms(5 + round)).await;
        // Retry until a moment with no active job; that moment must stick.
        loop {
            match q.obliterate(false).await {
                Ok(()) => break,
                Err(Error::Config(_)) => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
        cancel.cancel();
        h.await.unwrap().unwrap();
        // A job a worker took mid-delete would re-create a partial hash
        // (move_to_active writes onto it) that outlives the obliterate.
        let left = keys_of(q.name()).await;
        assert_eq!(left, 0, "round {round}: {left} key(s) survived obliterate");
        // Any job still processed after the obliterate finds its hash gone.
        let traces = oxn_testkit::capture::drain_for(q.name());
        rejected.fetch_add(
            traces
                .iter()
                .filter(|t| {
                    t.name.starts_with("transition rejected")
                        && t.fields
                            .get("error")
                            .is_some_and(|e| e.contains("not found"))
                })
                .count() as u32,
            Ordering::Relaxed,
        );
    }
    assert_eq!(
        rejected.load(Ordering::Relaxed),
        0,
        "a worker completed a job obliterate had deleted"
    );
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn obliterate_under_constant_fetching_leaves_nothing_behind() {
    // One worker flips between 0 and 1 active job thousands of times a
    // second; obliterate(false) wins in one of the idle gaps while
    // thousands of jobs still wait. With the check and the deletion as
    // separate commands, the worker took jobs mid-delete.
    for round in 0..5 {
        let (q, _b) = common::fresh_queue::<T>("b11-hammer").await;
        q.add_bulk((0..3000).map(|i| (T(i), JobOptions::new())).collect())
            .await
            .unwrap();
        let w = Worker::builder(q.clone(), |_j: Job<T>| async { Ok::<_, Error>(()) })
            .concurrency(1)
            .drain_delay(ms(5))
            .build();
        let cancel = w.cancellation_token();
        let h = tokio::spawn(w.run());
        tokio::time::sleep(ms(20)).await;
        let mut tries = 0u32;
        loop {
            tries += 1;
            match q.obliterate(false).await {
                Ok(()) => break,
                Err(Error::Config(_)) => continue,
                Err(e) => panic!("{e}"),
            }
        }
        tokio::time::sleep(ms(50)).await;
        cancel.cancel();
        h.await.unwrap().unwrap();
        let left = keys_of(q.name()).await;
        assert_eq!(
            left, 0,
            "round {round} (after {tries} tries): {left} key(s) survived obliterate"
        );
        common::teardown(&q).await;
    }
}
