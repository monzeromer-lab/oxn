//! B2 — queue-level default job options are honored, and an explicit
//! per-job value always wins, even when it equals the built-in default.
//!
//! In 0.2 `merge_opts` took `attempts`, `remove_on_complete`,
//! `remove_on_fail` and `lifo` from the per-job options unconditionally, so
//! `Queue::builder(..).remove_on_complete(Removal::Remove)` did nothing for
//! a job added with `JobOptions::new()`.

mod common;

use std::sync::Arc;

use oxn::backend::Backend;
use oxn::options::Removal;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

fn queue_with(backend: Arc<dyn Backend>, name: &str, defaults: JobOptions) -> Queue<T> {
    Queue::from_backend(
        name,
        backend,
        QueueOptions::default()
            .prefix(common::TEST_PREFIX)
            .default_job_options(defaults),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn defaults_apply_when_the_job_leaves_a_field_unset() {
    let (base, b) = common::fresh_queue::<T>("b2-inherit").await;
    let q = queue_with(
        b.clone(),
        base.name(),
        JobOptions::new()
            .attempts(5)
            .priority(4)
            .lifo(true)
            .backoff(Backoff::ONE_SECOND)
            .remove_on_complete(Removal::KeepLast(10))
            .remove_on_fail(Removal::Remove),
    );
    let mut rec = Recorder::new("b2_defaults_inherit", q.name()).await;
    let id = rec
        .step("add with JobOptions::new()", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(job.opts.attempts, 5, "attempts inherited");
    assert_eq!(job.opts.priority, 4, "priority inherited");
    assert!(job.opts.lifo, "lifo inherited");
    assert_eq!(job.opts.backoff, Some(Backoff::ONE_SECOND));
    assert_eq!(job.opts.remove_on_complete, Removal::KeepLast(10));
    assert_eq!(job.opts.remove_on_fail, Removal::Remove);
    assert_eq!(rec.snapshot().location(id.as_str()), "prioritized");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_values_override_defaults_even_when_equal_to_builtins() {
    let (base, b) = common::fresh_queue::<T>("b2-override").await;
    let q = queue_with(
        b.clone(),
        base.name(),
        JobOptions::new()
            .attempts(5)
            .priority(4)
            .lifo(true)
            .remove_on_complete(Removal::Remove),
    );
    let mut rec = Recorder::new("b2_explicit_override", q.name()).await;
    // Every explicit value below equals the *built-in* default, which is
    // exactly what 0.2 could not distinguish from "unset".
    let id = rec
        .step(
            "add with explicit built-in values",
            q.add(
                T(1),
                JobOptions::new()
                    .attempts(1)
                    .priority(0)
                    .lifo(false)
                    .remove_on_complete(Removal::Keep),
            ),
        )
        .await
        .unwrap();
    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(job.opts.attempts, 1);
    assert_eq!(job.opts.priority, 0, "explicit priority(0) = unprioritized");
    assert!(!job.opts.lifo);
    assert_eq!(job.opts.remove_on_complete, Removal::Keep);
    assert_eq!(
        rec.snapshot().location(id.as_str()),
        "wait",
        "not prioritized"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_level_remove_on_complete_deletes_finished_jobs() {
    let (base, b) = common::fresh_queue::<T>("b2-remove").await;
    let q: Queue<T> = Queue::from_backend(
        base.name(),
        b.clone(),
        QueueOptions::default()
            .prefix(common::TEST_PREFIX)
            .remove_on_complete(Removal::Remove),
    );
    let mut rec = Recorder::new("b2_queue_remove_on_complete", q.name()).await;
    let dropped = rec
        .step("add (inherits Remove)", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let kept = rec
        .step(
            "add (explicit Keep)",
            q.add(T(2), JobOptions::new().remove_on_complete(Removal::Keep)),
        )
        .await
        .unwrap();
    let w = Worker::builder(q.clone(), |_j: Job<T>| async { Ok::<_, Error>(()) })
        .drain_delay(ms(50))
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    assert!(
        rec.wait_until("both processed", secs(5), |s| s.len("wait") == 0
            && s.len("active") == 0)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("after shutdown").await;

    assert_eq!(
        rec.snapshot().location(dropped.as_str()),
        "(gone)",
        "queue default Remove applied"
    );
    assert_eq!(
        rec.snapshot().location(kept.as_str()),
        "completed",
        "explicit Keep wins"
    );
    assert_eq!(rec.events_named("completed").len(), 2);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_default_attempts_drive_retries() {
    let (base, b) = common::fresh_queue::<T>("b2-attempts").await;
    let q: Queue<T> = Queue::from_backend(
        base.name(),
        b.clone(),
        QueueOptions::default()
            .prefix(common::TEST_PREFIX)
            .default_job_options(JobOptions::new().attempts(3)),
    );
    let mut rec = Recorder::new("b2_default_attempts_retry", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let w = Worker::builder(q.clone(), |j: Job<T>| async move {
        if j.attempts_made < 3 {
            Err(Error::handler(std::io::Error::other("transient")))
        } else {
            Ok(())
        }
    })
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let done = rec
        .wait_until("completes on attempt 3", secs(5), |s| {
            s.len("completed") + s.len("failed") == 1
        })
        .await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(done);
    assert_eq!(
        rec.snapshot().location(id.as_str()),
        "completed",
        "3 attempts from the queue default"
    );
    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(job.attempts_made, 3);
    rec.assert_clean();
    common::teardown(&q).await;
}
