//! B16 — a panicking handler is a failure, not a stuck job.
//!
//! In 0.2 a panic inside a handler unwound the task that was processing the
//! job: no outcome was recorded and the job stayed locked in `active` until
//! its lock expired (30s by default), when the stalled scanner finally
//! picked it up. Now the panic is caught and handled like an error: the
//! job is retried or failed right away according to its policy.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use crate::common;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

fn spawn<F, Fut>(
    q: &Queue<T>,
    f: F,
) -> (
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<oxn::Result<()>>,
)
where
    F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = oxn::Result<()>> + Send + 'static,
{
    // Default 30s lock: if recovery depended on lock expiry, these tests
    // would time out.
    let w = Worker::builder(q.clone(), f).drain_delay(ms(20)).build();
    (w.cancellation_token(), tokio::spawn(w.run()))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_is_retried_immediately() {
    let (q, _b) = common::fresh_queue::<T>("b16-retry").await;
    let mut rec = Recorder::new("b16_panic_retried", q.name()).await;
    let id = rec
        .step("add attempts=2", q.add(T(1), JobOptions::new().attempts(2)))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let t = Instant::now();
    let (cancel, h) = spawn(&q, move |_j| {
        let c = c.clone();
        async move {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("boom");
            }
            Ok(())
        }
    });
    assert!(
        rec.wait_until("completes on attempt 2", secs(5), |s| s.len("completed")
            == 1)
            .await
    );
    assert!(
        t.elapsed() < secs(3),
        "recovery waited for lock expiry: {:?}",
        t.elapsed()
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(job.attempts_made, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        rec.events_named("stalled").is_empty(),
        "never needed the stalled scanner"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_on_the_last_attempt_fails_with_the_panic_message() {
    let (q, _b) = common::fresh_queue::<T>("b16-fail").await;
    let mut rec = Recorder::new("b16_panic_fails", q.name()).await;
    let a = rec
        .step("add (str panic)", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let b = rec
        .step("add (non-string panic)", q.add(T(2), JobOptions::new()))
        .await
        .unwrap();
    let after = rec
        .step("add (succeeds)", q.add(T(3), JobOptions::new()))
        .await
        .unwrap();
    let (cancel, h) = spawn(&q, |j| async move {
        match j.data.0 {
            1 => panic!("disk on fire: {}", 42),
            2 => std::panic::panic_any(7u8),
            _ => Ok(()),
        }
    });
    assert!(
        rec.wait_until("all three finished", secs(5), |s| s.len("failed") == 2
            && s.len("completed") == 1)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    let reason = |id: &JobId| {
        rec.snapshot().jobs[id.as_str()]
            .fields
            .get("failedReason")
            .cloned()
    };
    assert_eq!(
        reason(&a).as_deref(),
        Some("handler panicked: disk on fire: 42")
    );
    assert_eq!(
        reason(&b).as_deref(),
        Some("handler panicked: (non-string panic payload)")
    );
    assert_eq!(
        rec.snapshot().location(after.as_str()),
        "completed",
        "the worker kept going"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}
