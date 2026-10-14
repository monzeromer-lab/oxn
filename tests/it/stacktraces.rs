//! B12b — failed attempts leave a stack trace.
//!
//! `Job::stacktrace` (and the dashboard's "Stack trace" section) existed in
//! 0.2 but nothing ever wrote it: it was always empty. Each real failure now
//! appends `attempt N: <error>` plus its `caused by:` chain, keeping the 10
//! most recent.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

#[derive(Debug)]
struct Outer(Inner);
#[derive(Debug)]
struct Inner(&'static str);
impl std::fmt::Display for Outer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upload failed")
    }
}
impl std::fmt::Display for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection reset: {}", self.0)
    }
}
impl std::error::Error for Outer {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl std::error::Error for Inner {}

async fn run<F, Fut>(
    q: &Queue<T>,
    rec: &mut Recorder,
    until: &str,
    done: impl FnMut(&oxn_testkit::Snapshot) -> bool,
    f: F,
) where
    F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = oxn::Result<()>> + Send + 'static,
{
    let w = Worker::builder(q.clone(), f).drain_delay(ms(10)).build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    assert!(rec.wait_until(until, secs(10), done).await, "{until}");
    cancel.cancel();
    h.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn each_failed_attempt_records_its_error_chain() {
    let (q, _b) = common::fresh_queue::<T>("b12b-chain").await;
    let mut rec = Recorder::new("b12b_error_chain", q.name()).await;
    let id = rec
        .step("add attempts=3", q.add(T(1), JobOptions::new().attempts(3)))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    run(
        &q,
        &mut rec,
        "fails for good",
        |s| s.len("failed") == 1,
        move |_j| {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                match n {
                    0 => Err(Error::Delayed { delay_ms: 10 }), // postponement: no entry
                    1 => panic!("boom"),
                    2 => Err(Error::handler(Outer(Inner("peer 10.0.0.7")))),
                    _ => Err(Error::unrecoverable("bad input")),
                }
            }
        },
    )
    .await;
    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(
        job.stacktrace,
        vec![
            "attempt 1: handler panicked: boom".to_string(),
            "attempt 2: handler error: upload failed\n  caused by: connection reset: peer 10.0.0.7"
                .to_string(),
            "attempt 3: unrecoverable: bad input".to_string(),
        ]
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_ten_most_recent_attempts_are_kept() {
    let (q, _b) = common::fresh_queue::<T>("b12b-cap").await;
    let mut rec = Recorder::new("b12b_stack_cap", q.name()).await;
    let id = rec
        .step(
            "add attempts=14",
            q.add(T(1), JobOptions::new().attempts(14)),
        )
        .await
        .unwrap();
    run(
        &q,
        &mut rec,
        "fails for good",
        |s| s.len("failed") == 1,
        |j| async move {
            Err(Error::handler(std::io::Error::other(format!(
                "try {}",
                j.attempts_made
            ))))
        },
    )
    .await;
    let st = q.get(&id).await.unwrap().unwrap().stacktrace;
    assert_eq!(st.len(), 10);
    assert!(st[0].starts_with("attempt 5: "), "{}", st[0]);
    assert!(st[9].starts_with("attempt 14: "), "{}", st[9]);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn success_keeps_the_history_and_manual_retry_clears_it() {
    let (q, b) = common::fresh_queue::<T>("b12b-history").await;
    let mut rec = Recorder::new("b12b_stack_history", q.name()).await;
    let ok = rec
        .step("add attempts=2", q.add(T(1), JobOptions::new().attempts(2)))
        .await
        .unwrap();
    let bad = rec
        .step("add attempts=1", q.add(T(2), JobOptions::new().attempts(1)))
        .await
        .unwrap();
    run(
        &q,
        &mut rec,
        "both finished",
        |s| s.len("completed") == 1 && s.len("failed") == 1,
        |j| async move {
            if j.data.0 == 1 && j.attempts_made == 2 {
                return Ok(());
            }
            Err(Error::handler(std::io::Error::other("nope")))
        },
    )
    .await;
    assert_eq!(
        q.get(&ok).await.unwrap().unwrap().stacktrace.len(),
        1,
        "a later success keeps the failure history"
    );
    assert_eq!(q.get(&bad).await.unwrap().unwrap().stacktrace.len(), 1);
    rec.step("manual retry", q.retry(&bad)).await.unwrap();
    assert!(
        q.get(&bad).await.unwrap().unwrap().stacktrace.is_empty(),
        "retry starts a fresh history"
    );
    let _ = b;
    rec.assert_clean();
    common::teardown(&q).await;
}
