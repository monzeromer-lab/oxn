//! B12e — the `metrics` feature emits metrics.
//!
//! In 0.2 the `metrics` feature only pulled in the `metrics` crate: nothing
//! in oxn ever recorded a counter, gauge or histogram. Every series below is
//! labelled with `queue`, and a transition is only counted once Redis has
//! accepted it (a lost lock is counted as such, not as a completion).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common;
use oxn::backend::Backend;
use oxn::prelude::*;
use oxn_testkit::{Recorder, metrics, ms, secs};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Plan {
    Ok,
    FailTwiceThenOk,
    Fatal,
    PostponeOnce,
    AlwaysFail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

#[tokio::test(flavor = "multi_thread")]
async fn every_outcome_is_counted_once_redis_accepts_it() {
    let m = metrics::install();
    let (q, _b) = common::fresh_queue::<Plan>("b12e-outcomes").await;
    let mut rec = Recorder::new("b12e_outcomes", q.name()).await;
    let qn = q.name().to_string();
    let l = [("queue", qn.as_str())];
    rec.step("add 3 ok", async {
        for _ in 0..3 {
            q.add(Plan::Ok, JobOptions::new()).await.unwrap();
        }
    })
    .await;
    rec.step(
        "add_bulk the rest",
        q.add_bulk(vec![
            (Plan::FailTwiceThenOk, JobOptions::new().attempts(3)),
            (Plan::Fatal, JobOptions::new().attempts(3)),
            (Plan::PostponeOnce, JobOptions::new()),
            (Plan::AlwaysFail, JobOptions::new().attempts(2)),
        ]),
    )
    .await
    .unwrap();
    assert_eq!(m.counter("oxn_jobs_added_total", &l), 7, "{}", m.dump(&qn));

    let postponed = Arc::new(AtomicU32::new(0));
    let p = postponed.clone();
    let w = Worker::builder(q.clone(), move |j: Job<Plan>| {
        let p = p.clone();
        async move {
            match j.data {
                Plan::Ok => Ok(()),
                Plan::FailTwiceThenOk if j.attempts_made <= 2 => {
                    Err(Error::handler(std::io::Error::other("transient")))
                }
                Plan::FailTwiceThenOk => Ok(()),
                Plan::Fatal => Err(Error::unrecoverable("bad input")),
                Plan::PostponeOnce if p.fetch_add(1, Ordering::SeqCst) == 0 => {
                    Err(Error::Delayed { delay_ms: 20 })
                }
                Plan::PostponeOnce => Ok(()),
                Plan::AlwaysFail => Err(Error::handler(std::io::Error::other("nope"))),
            }
        }
    })
    .concurrency(4)
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    assert!(
        rec.wait_until("all settled", secs(10), |s| s.len("completed") == 5
            && s.len("failed") == 2)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();

    let c = |name: &str| m.counter(name, &l);
    let dump = m.dump(&qn);
    // 3 ok + 3 (fail, fail, ok) + 1 fatal + 2 (postpone, ok) + 2 (fail, fail)
    assert_eq!(c("oxn_jobs_started_total"), 11, "{dump}");
    assert_eq!(c("oxn_jobs_completed_total"), 5, "{dump}");
    assert_eq!(c("oxn_jobs_failed_total"), 2, "final failures only\n{dump}");
    assert_eq!(c("oxn_jobs_retried_total"), 3, "{dump}");
    assert_eq!(c("oxn_jobs_postponed_total"), 1, "{dump}");
    assert_eq!(c("oxn_lock_lost_total"), 0, "{dump}");
    assert_eq!(c("oxn_transition_errors_total"), 0, "{dump}");
    let by_outcome = |o: &str| {
        m.histogram(
            "oxn_job_duration_seconds",
            &[("queue", &qn), ("outcome", o)],
        )
        .len()
    };
    assert_eq!(by_outcome("ok"), 5, "{dump}");
    assert_eq!(by_outcome("error"), 4, "{dump}");
    assert_eq!(by_outcome("unrecoverable"), 1, "{dump}");
    assert_eq!(by_outcome("delayed"), 1, "{dump}");
    let durations = m.histogram("oxn_job_duration_seconds", &l);
    assert_eq!(durations.len(), 11);
    assert!(
        durations.iter().all(|d| (0.0..5.0).contains(d)),
        "{durations:?}"
    );
    // First-attempt pickups (the postponed job's second pickup is still
    // attempt 1): enqueue → start.
    let waits = m.histogram("oxn_job_wait_seconds", &l);
    assert_eq!(waits.len(), 8, "{dump}");
    assert!(waits.iter().all(|w| (0.0..5.0).contains(w)), "{waits:?}");
    assert_eq!(m.gauge("oxn_jobs_active", &l), 0.0, "{dump}");
    let (unit, help) = m
        .description("oxn_job_duration_seconds")
        .expect("described");
    assert_eq!(unit, Some(::metrics::Unit::Seconds));
    assert!(!help.is_empty());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_active_gauge_tracks_in_flight_jobs() {
    let m = metrics::install();
    let (q, _b) = common::fresh_queue::<T>("b12e-active").await;
    let qn = q.name().to_string();
    q.add_bulk((0..3).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    // A watch channel, not a Notify: a handler that is counted as started
    // but hasn't reached its wait yet must still see the gate open.
    let (gate, g) = tokio::sync::watch::channel(false);
    let w = Worker::builder(q.clone(), move |_j: Job<T>| {
        let mut g = g.clone();
        async move {
            let _ = g.wait_for(|open| *open).await;
            Ok::<_, Error>(())
        }
    })
    .concurrency(3)
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let t = std::time::Instant::now();
    while m.gauge("oxn_jobs_active", &[("queue", &qn)]) < 3.0 {
        assert!(t.elapsed() < secs(3), "{}", m.dump(&qn));
        tokio::time::sleep(ms(10)).await;
    }
    gate.send(true).unwrap();
    let t = std::time::Instant::now();
    while m.counter("oxn_jobs_completed_total", &[("queue", &qn)]) < 3 {
        assert!(t.elapsed() < secs(3), "{}", m.dump(&qn));
        tokio::time::sleep(ms(10)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(m.gauge("oxn_jobs_active", &[("queue", &qn)]), 0.0);
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stalls_releases_lost_locks_and_rate_limits_are_counted() {
    let m = metrics::install();

    // A stall: a dead worker's lock lapses and the scanner requeues the job.
    let (q, b) = common::fresh_queue::<T>("b12e-stall").await;
    let qn = q.name().to_string();
    q.add(T(1), JobOptions::new()).await.unwrap();
    b.fetch_next(
        q.name(),
        "dead",
        ms(30),
        ms(10),
        &CancellationToken::new(),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(ms(60)).await;
    let w = Worker::builder(q.clone(), |_j: Job<T>| async { Ok::<_, Error>(()) })
        .stalled_interval(ms(50))
        .drain_delay(ms(20))
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let t = std::time::Instant::now();
    while m.counter("oxn_jobs_completed_total", &[("queue", &qn)]) < 1 {
        assert!(t.elapsed() < secs(5), "{}", m.dump(&qn));
        tokio::time::sleep(ms(10)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(
        m.counter("oxn_jobs_stalled_total", &[("queue", &qn)]),
        1,
        "{}",
        m.dump(&qn)
    );
    common::teardown(&q).await;

    // A release: shutdown_timeout elapses with the handler still running.
    let (q, _b) = common::fresh_queue::<T>("b12e-release").await;
    let qn = q.name().to_string();
    q.add(T(1), JobOptions::new()).await.unwrap();
    let w = Worker::builder(q.clone(), |_j: Job<T>| async {
        tokio::time::sleep(secs(30)).await;
        Ok::<_, Error>(())
    })
    .shutdown_timeout(ms(100))
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let t = std::time::Instant::now();
    while m.counter("oxn_jobs_started_total", &[("queue", &qn)]) < 1 {
        assert!(t.elapsed() < secs(3));
        tokio::time::sleep(ms(10)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(
        m.counter("oxn_jobs_released_total", &[("queue", &qn)]),
        1,
        "{}",
        m.dump(&qn)
    );
    assert_eq!(m.counter("oxn_jobs_failed_total", &[("queue", &qn)]), 0);
    assert_eq!(m.gauge("oxn_jobs_active", &[("queue", &qn)]), 0.0);
    common::teardown(&q).await;

    // A lost lock: another owner takes the job; the completion is refused.
    let (q, _b) = common::fresh_queue::<T>("b12e-lock").await;
    let qn = q.name().to_string();
    let id = q.add(T(1), JobOptions::new()).await.unwrap();
    let base = format!("{}:{}:{}", common::TEST_PREFIX, q.name(), id);
    let w = Worker::builder(q.clone(), move |_j: Job<T>| {
        let base = base.clone();
        async move {
            let mut c = redis::Client::open(common::redis_url())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            let _: () = c.set(format!("{base}:lock"), "thief").await.unwrap();
            let _: () = c.hset(&base, "lockToken", "thief").await.unwrap();
            Ok::<_, Error>(())
        }
    })
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let t = std::time::Instant::now();
    while m.counter("oxn_lock_lost_total", &[("queue", &qn)]) < 1 {
        assert!(t.elapsed() < secs(3), "{}", m.dump(&qn));
        tokio::time::sleep(ms(10)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(
        m.counter("oxn_jobs_completed_total", &[("queue", &qn)]),
        0,
        "{}",
        m.dump(&qn)
    );
    common::teardown(&q).await;

    // Rate limiting: 1 per 30s, so the second fetch is limited.
    let (q, _b) = common::fresh_queue::<T>("b12e-rl").await;
    let qn = q.name().to_string();
    q.add_bulk((0..3).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    let w = Worker::builder(q.clone(), |_j: Job<T>| async { Ok::<_, Error>(()) })
        .rate_limit(1, secs(30))
        .drain_delay(ms(20))
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let t = std::time::Instant::now();
    while m.counter("oxn_rate_limited_total", &[("queue", &qn)]) < 1 {
        assert!(t.elapsed() < secs(3), "{}", m.dump(&qn));
        tokio::time::sleep(ms(10)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(m.counter("oxn_jobs_started_total", &[("queue", &qn)]), 1);
    common::teardown(&q).await;
}
