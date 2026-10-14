//! B15 — a dropped connection doesn't lose a job's outcome.
//!
//! In 0.2 `complete` / `fail` / `delay` / `release` failing on a dropped
//! connection were logged and never retried. If the call never reached
//! Redis the job stayed in `active` until its lock expired, then the
//! stalled scanner ran it **again** (its first result lost). If Redis ran
//! it but the reply was lost, the worker reported a failed transition for
//! an outcome that was in fact recorded — and a naive retry would find its
//! lock gone and report "lock lost".
//!
//! A [`FaultProxy`] between the backend and Redis cuts the connection at
//! the exact script call, either before Redis sees it or after Redis ran it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common;
use oxn::backend::redis::{RedisBackend, script_hashes};
use oxn::backend::{Backend, Failure};
use oxn::error::ScriptError;
use oxn::prelude::*;
use oxn_testkit::fault::{Drop, FaultProxy};
use oxn_testkit::{Recorder, ms, secs};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

fn sha(script: &str) -> String {
    script_hashes()
        .into_iter()
        .find(|(n, _)| *n == script)
        .map(|(_, s)| s)
        .unwrap_or_else(|| panic!("no script {script}"))
}

/// A backend whose every connection goes through a fault proxy.
async fn proxied() -> (Arc<RedisBackend>, FaultProxy) {
    let upstream = common::redis_url()
        .trim_start_matches("redis://")
        .trim_end_matches('/')
        .to_string();
    let proxy = FaultProxy::start(&upstream).await.unwrap();
    let mut cfg = common::test_config();
    cfg.url = proxy.url();
    (Arc::new(RedisBackend::connect(cfg).await.unwrap()), proxy)
}

async fn proxied_queue(slug: &str) -> (Queue<T>, Arc<RedisBackend>, FaultProxy) {
    let (b, proxy) = proxied().await;
    let q = common::queue(b.clone(), &common::unique_queue_name(slug)).await;
    (q, b, proxy)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_completion_cut_off_before_redis_is_retried_not_rerun() {
    for mode in [Drop::BeforeForward, Drop::AfterForward] {
        let (q, _b, proxy) = proxied_queue("b15-worker").await;
        let mut rec = Recorder::new(&format!("b15_worker_{mode:?}"), q.name()).await;
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        let w = Worker::builder(q.clone(), move |_j: Job<T>| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            }
        })
        .lock_duration(secs(1))
        .lock_renew_interval(ms(500))
        .stalled_interval(ms(200))
        .drain_delay(ms(20))
        .build();
        let cancel = w.cancellation_token();
        let h = tokio::spawn(w.run());
        proxy.arm(sha("move_to_finished"), mode);
        rec.step("add", q.add(T(1), JobOptions::new()))
            .await
            .unwrap();
        assert!(
            rec.wait_until("completed", secs(5), |s| s.len("completed") == 1)
                .await,
            "{mode:?}"
        );
        // Past the lock's expiry plus a scan: a lost completion would have
        // stalled and run again by now.
        tokio::time::sleep(ms(1800)).await;
        rec.observe("after the lock would have expired").await;
        cancel.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(proxy.fired(), 1, "{mode:?}: the fault fired");
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "{mode:?}: the job ran again"
        );
        assert!(rec.events_named("stalled").is_empty(), "{mode:?}");
        assert_eq!(rec.events_named("completed").len(), 1, "{mode:?}");
        let rejected = rec
            .traces_named("transition rejected")
            .len()
            + rec
                .traces_named(
                    "transition rejected: lock lost, outcome discarded (the job now belongs to another worker or the stalled scanner)",
                )
                .len();
        assert_eq!(
            rejected, 0,
            "{mode:?}: the worker reported the outcome lost"
        );
        rec.assert_clean();
        common::teardown(&q).await;
    }
}

/// Fetch one job through `b`; returns (id, token).
async fn take(b: &Arc<RedisBackend>, q: &str) -> (JobId, String) {
    let f = b
        .fetch_next(q, "w", secs(30), ms(10), &CancellationToken::new(), None)
        .await
        .unwrap()
        .expect("a job");
    (f.job.id, f.token)
}

#[derive(Debug, Clone, Copy)]
enum Transition {
    Complete,
    FailFinal,
    FailRetry,
    Postpone,
    Release,
    WaitForChildren,
}

impl Transition {
    fn script(self) -> &'static str {
        match self {
            Self::Complete | Self::FailFinal => "move_to_finished",
            Self::FailRetry | Self::Postpone => "move_to_delayed",
            Self::Release => "release",
            Self::WaitForChildren => "move_to_waiting_children",
        }
    }
    async fn apply(self, b: &RedisBackend, q: &str, id: &JobId, token: &str) -> oxn::Result<()> {
        let far = 4_102_444_800_000; // 2100: stays delayed
        match self {
            Self::Complete => {
                b.complete(q, id, token, Some("42".into()), oxn::options::Removal::Keep)
                    .await
            }
            Self::FailFinal => {
                b.fail(q, id, token, Failure::new("boom").trace("attempt 1: boom"))
                    .await
            }
            Self::FailRetry => {
                b.fail(
                    q,
                    id,
                    token,
                    Failure::new("again")
                        .retry_at(far)
                        .trace("attempt 1: again"),
                )
                .await
            }
            Self::Postpone => b.delay(q, id, token, far).await,
            Self::Release => b.release(q, id, token).await,
            Self::WaitForChildren => b.wait_for_children(q, id, token).await.map(|_| ()),
        }
    }
    fn lands_in(self) -> &'static str {
        match self {
            Self::Complete => "completed",
            Self::FailFinal => "failed",
            Self::FailRetry | Self::Postpone => "delayed",
            Self::Release | Self::WaitForChildren => "wait",
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_lock_consuming_transition_survives_a_cut_connection_exactly_once() {
    use Transition::*;
    for t in [
        Complete,
        FailFinal,
        FailRetry,
        Postpone,
        Release,
        WaitForChildren,
    ] {
        for mode in [Drop::BeforeForward, Drop::AfterForward] {
            let (q, b, proxy) = proxied_queue("b15-matrix").await;
            let label = format!("b15_{t:?}_{mode:?}");
            let mut rec = Recorder::new(&label, q.name()).await;
            q.add(T(1), JobOptions::new().attempts(3)).await.unwrap();
            let (id, token) = take(&b, q.name()).await;
            proxy.arm(sha(t.script()), mode);
            let r = rec
                .step(
                    &format!("{t:?} with the connection cut {mode:?}"),
                    t.apply(&b, q.name(), &id, &token),
                )
                .await;
            assert!(r.is_ok(), "{label}: {r:?}");
            assert_eq!(proxy.fired(), 1, "{label}");
            let s = rec.snapshot();
            assert_eq!(s.location(id.as_str()), t.lands_in(), "{label}");
            let job = &s.jobs[id.as_str()];
            // Applied once: attempt refunds and stack entries not doubled.
            let want_attempts = match t {
                Postpone | Release | WaitForChildren => 0,
                _ => 1,
            };
            assert_eq!(job.attempts_made(), want_attempts, "{label}: attemptsMade");
            let stack: Vec<String> = job
                .fields
                .get("stacktrace")
                .map(|s| serde_json::from_str(s).unwrap())
                .unwrap_or_default();
            let want_stack = usize::from(matches!(t, FailFinal | FailRetry));
            assert_eq!(stack.len(), want_stack, "{label}: stacktrace {stack:?}");
            // A second, duplicate retry is still a no-op success.
            let again = rec
                .step("the same call again", t.apply(&b, q.name(), &id, &token))
                .await;
            assert!(again.is_ok(), "{label} (repeat): {again:?}");
            assert_eq!(
                rec.snapshot().jobs[id.as_str()].attempts_made(),
                want_attempts,
                "{label}: repeat"
            );
            rec.assert_clean();
            common::teardown(&q).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_genuinely_lost_lock_is_still_reported_as_lost() {
    let (q, b, _proxy) = proxied_queue("b15-lost").await;
    let mut rec = Recorder::new("b15_lock_really_lost", q.name()).await;
    q.add(T(1), JobOptions::new()).await.unwrap();
    let (id, token) = take(&b, q.name()).await;
    let mut c = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let base = format!("{}:{}:{}", common::TEST_PREFIX, q.name(), id);
    rec.step("another owner takes the job", async {
        let _: () = c.set(format!("{base}:lock"), "thief").await.unwrap();
        let _: () = c.hset(&base, "lockToken", "thief").await.unwrap();
    })
    .await;
    let r = rec
        .step(
            "complete with the old token",
            b.complete(q.name(), &id, &token, None, oxn::options::Removal::Keep),
        )
        .await;
    assert!(
        matches!(r, Err(Error::Script(ScriptError::LockMismatch))),
        "{r:?}"
    );
    assert_eq!(rec.snapshot().location(id.as_str()), "active");
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_whose_reply_was_lost_hands_back_the_job_it_took() {
    for mode in [Drop::BeforeForward, Drop::AfterForward] {
        let (q, b, proxy) = proxied_queue("b15-fetch").await;
        let mut rec = Recorder::new(&format!("b15_fetch_{mode:?}"), q.name()).await;
        let first = q.add(T(1), JobOptions::new()).await.unwrap();
        q.add(T(2), JobOptions::new()).await.unwrap();
        proxy.arm(sha("move_to_active"), mode);
        let f = rec
            .step(
                "fetch with the reply cut",
                b.fetch_next(
                    q.name(),
                    "w:7",
                    secs(30),
                    ms(10),
                    &CancellationToken::new(),
                    None,
                ),
            )
            .await
            .unwrap()
            .expect("a job");
        assert_eq!(proxy.fired(), 1);
        assert_eq!(f.job.id, first, "{mode:?}: the first job, not a second one");
        assert_eq!(f.token, "w:7");
        let s = rec.snapshot();
        assert_eq!(
            s.len("active"),
            1,
            "{mode:?}: only one job taken: {}",
            s.summary()
        );
        assert_eq!(s.len("wait"), 1);
        assert_eq!(
            s.jobs[first.as_str()].attempts_made(),
            1,
            "{mode:?}: one attempt"
        );
        assert!(s.jobs[first.as_str()].lock.is_some());
        b.complete(
            q.name(),
            &f.job.id,
            &f.token,
            None,
            oxn::options::Removal::Keep,
        )
        .await
        .unwrap();
        rec.observe("completed").await;
        rec.assert_clean();
        common::teardown(&q).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_whose_fetch_reply_was_lost_runs_now_not_after_its_lock_expires() {
    let (q, _b, proxy) = proxied_queue("b15-fetch-worker").await;
    let mut rec = Recorder::new("b15_fetch_worker", q.name()).await;
    let w = Worker::builder(q.clone(), |_j: Job<T>| async { Ok::<_, Error>(()) })
        .lock_duration(secs(10))
        .drain_delay(ms(20))
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    tokio::time::sleep(ms(100)).await;
    proxy.arm(sha("move_to_active"), Drop::AfterForward);
    let t = std::time::Instant::now();
    rec.step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    assert!(
        rec.wait_until("completed", secs(5), |s| s.len("completed") == 1)
            .await
    );
    assert!(
        t.elapsed() < secs(2),
        "ran after {:?} (lock is 10s)",
        t.elapsed()
    );
    assert!(proxy.fired() >= 1);
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(rec.events_named("stalled").is_empty());
    rec.assert_clean();
    common::teardown(&q).await;
}
