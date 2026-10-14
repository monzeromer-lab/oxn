//! B14 — schedulers are safe to run in every process.
//!
//! In 0.2 each tick's job id was the wall-clock time of the enqueue
//! (`sched-{id}-{now_ms}`), so N processes running the same scheduler
//! added N jobs per tick (a probe with 5 instances: 25 jobs for 5 ticks).
//! `run` added each tick when it fired while `enqueue_next` added the next
//! one ahead, and fixed intervals drifted per process. Now every instance
//! computes the same fire times, tick T is the job `sched-{id}-{T}`, and
//! Redis keeps each scheduler's latest tick so none is added twice.

#![cfg(feature = "scheduler")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common;
use oxn::backend::{Backend, Repeat, SchedulerTick, TickOutcome};
use oxn::options::Removal;
use oxn::prelude::*;
use oxn::scheduler::{JobScheduler, RepeatOptions};
use oxn_testkit::{Recorder, ms, secs};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Tick(u64);

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A queue handle on its own backend: another "process".
async fn instance(q: &Queue<Tick>) -> Queue<Tick> {
    let b = common::build_backend().await as Arc<dyn Backend>;
    Queue::from_backend(
        q.name(),
        b,
        QueueOptions::default().prefix(common::TEST_PREFIX),
    )
    .unwrap()
}

fn fire_time(job_id: &str) -> i64 {
    job_id.rsplit('-').next().unwrap().parse().unwrap()
}

/// Run a worker recording `(job id, start ms)`.
fn worker(
    q: &Queue<Tick>,
    ran: Arc<Mutex<Vec<(String, i64)>>>,
) -> (CancellationToken, tokio::task::JoinHandle<oxn::Result<()>>) {
    let w = Worker::builder(q.clone(), move |j: Job<Tick>| {
        let ran = ran.clone();
        async move {
            ran.lock().push((j.id.to_string(), now_ms()));
            Ok::<_, Error>(())
        }
    })
    .concurrency(4)
    .drain_delay(ms(20))
    .build();
    (w.cancellation_token(), tokio::spawn(w.run()))
}

#[tokio::test(flavor = "multi_thread")]
async fn five_instances_add_one_job_per_tick() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-instances").await;
    let mut rec = Recorder::new("b14_one_job_per_tick", q.name()).await;
    let ran = Arc::new(Mutex::new(Vec::new()));
    let (wc, wh) = worker(&q, ran.clone());
    let cancel = CancellationToken::new();
    let mut hs = Vec::new();
    for _ in 0..5 {
        let s = JobScheduler::new(
            instance(&q).await,
            "hb",
            RepeatOptions::every(ms(200)),
            Tick(0),
        )
        .with_options(JobOptions::new().remove_on_complete(Removal::Remove));
        hs.push(tokio::spawn(s.run(cancel.clone())));
    }
    tokio::time::sleep(ms(1500)).await;
    cancel.cancel();
    for h in hs {
        h.await.unwrap().unwrap();
    }
    // Let the last queued tick fire too.
    tokio::time::sleep(ms(300)).await;
    wc.cancel();
    wh.await.unwrap().unwrap();
    rec.observe("after ~7 ticks").await;

    let ran = ran.lock().clone();
    let mut per_tick: BTreeMap<i64, usize> = BTreeMap::new();
    for (id, _) in &ran {
        *per_tick.entry(fire_time(id)).or_default() += 1;
    }
    assert!(per_tick.len() >= 6, "{per_tick:?}");
    assert!(
        per_tick.values().all(|&n| n == 1),
        "a tick ran more than once: {per_tick:?}"
    );
    let ticks: Vec<i64> = per_tick.keys().copied().collect();
    assert!(
        ticks.iter().all(|t| t % 200 == 0),
        "aligned to 200ms: {ticks:?}"
    );
    assert!(
        ticks.windows(2).all(|w| w[1] - w[0] == 200),
        "no gaps: {ticks:?}"
    );
    for (id, started) in &ran {
        let t = fire_time(id);
        assert!(*started >= t - 5, "{id} ran {}ms early", t - started);
    }
    let added = rec.events_named("added").len();
    assert_eq!(
        added,
        per_tick.len(),
        "one `added` per tick, none duplicated"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_instance_cannot_resurrect_a_fired_tick() {
    let (q, b) = common::fresh_queue::<Tick>("b14-late").await;
    let mut rec = Recorder::new("b14_no_resurrection", q.name()).await;
    let tick = |t: i64, register: bool| SchedulerTick {
        scheduler_id: "hb".into(),
        fire_at_ms: t,
        repeat: Repeat::EveryMs(1000),
        register,
        insert: oxn::backend::JobInsert {
            name: "hb".into(),
            data: "0".into(),
            opts: JobOptions::new()
                .remove_on_complete(Removal::Remove)
                .resolve(),
            timestamp_ms: now_ms(),
        },
    };
    let t = now_ms() / 1000 * 1000; // a tick that is due already
    let r = rec
        .step(
            "instance A enqueues tick T",
            b.schedule_tick(q.name(), tick(t, true)),
        )
        .await
        .unwrap();
    assert_eq!(r, TickOutcome::Added(JobId::new(format!("sched-hb-{t}"))));
    let ran = Arc::new(Mutex::new(Vec::new()));
    let (wc, wh) = worker(&q, ran.clone());
    assert!(
        rec.wait_until("T ran and was removed", secs(5), |s| s.jobs.is_empty())
            .await
    );
    wc.cancel();
    wh.await.unwrap().unwrap();
    // Instance B's clock lags: it still thinks T is next.
    let r = rec
        .step(
            "lagging instance B enqueues T",
            b.schedule_tick(q.name(), tick(t, false)),
        )
        .await
        .unwrap();
    assert_eq!(r, TickOutcome::Exists(JobId::new(format!("sched-hb-{t}"))));
    let r = rec
        .step(
            "and an even older tick",
            b.schedule_tick(q.name(), tick(t - 1000, false)),
        )
        .await
        .unwrap();
    assert!(matches!(r, TickOutcome::Exists(_)));
    assert!(rec.snapshot().jobs.is_empty(), "nothing re-created");
    assert_eq!(ran.lock().len(), 1);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_next_and_run_agree_on_the_next_tick() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-agree").await;
    let mut rec = Recorder::new("b14_enqueue_next_and_run", q.name()).await;
    let s = JobScheduler::new(
        q.clone(),
        "nightly",
        RepeatOptions::every(secs(3600)),
        Tick(1),
    );
    let id = rec.step("enqueue_next", s.enqueue_next()).await.unwrap();
    let t = fire_time(id.as_str());
    assert_eq!(t % 3_600_000, 0, "on the hour: {id}");
    assert!(t > now_ms());
    let snap = rec.snapshot();
    assert_eq!(snap.location(id.as_str()), "delayed");
    assert_eq!(
        snap.zsets["delayed"][0].1 as i64, t,
        "due exactly at the tick"
    );
    // A second instance's enqueue_next and run() reuse it.
    let other = JobScheduler::new(
        instance(&q).await,
        "nightly",
        RepeatOptions::every(secs(3600)),
        Tick(1),
    );
    assert_eq!(other.enqueue_next().await.unwrap(), id);
    let cancel = CancellationToken::new();
    let h = tokio::spawn(other.run(cancel.clone()));
    tokio::time::sleep(ms(200)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("after run() started").await;
    assert_eq!(rec.snapshot().jobs.len(), 1);
    let info = q.schedulers().await.unwrap();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].id, "nightly");
    assert_eq!(info[0].repeat, Repeat::EveryMs(3_600_000));
    assert_eq!(info[0].next_fire_ms, t);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_next_tick_fires_even_if_every_scheduler_stops() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-ahead").await;
    let mut rec = Recorder::new("b14_tick_queued_ahead", q.name()).await;
    let cancel = CancellationToken::new();
    let s = JobScheduler::new(q.clone(), "once", RepeatOptions::every(ms(500)), Tick(2));
    let h = tokio::spawn(s.run(cancel.clone()));
    // Usually one tick is queued; two when the start fell within a few ms
    // of a tick boundary (the first was due at once). Either is correct.
    assert!(
        rec.wait_until("next tick queued", secs(2), |s| s.len("delayed") >= 1)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("every scheduler stopped").await;
    let pending: Vec<String> = rec.snapshot().zsets["delayed"]
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    assert!((1..=2).contains(&pending.len()), "{pending:?}");
    let ran = Arc::new(Mutex::new(Vec::new()));
    let (wc, wh) = worker(&q, ran.clone());
    assert!(
        rec.wait_until("they ran", secs(3), |s| s.len("completed") == pending.len())
            .await
    );
    wc.cancel();
    wh.await.unwrap().unwrap();
    let mut ran: Vec<String> = ran.lock().iter().map(|(id, _)| id.clone()).collect();
    ran.sort();
    assert_eq!(ran, pending, "the queued ticks ran with no scheduler alive");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cron_ticks_land_on_the_schedule() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-cron").await;
    let mut rec = Recorder::new("b14_cron", q.name()).await;
    let ran = Arc::new(Mutex::new(Vec::new()));
    let (wc, wh) = worker(&q, ran.clone());
    let cancel = CancellationToken::new();
    let mut hs = Vec::new();
    for _ in 0..3 {
        let s = JobScheduler::new(
            instance(&q).await,
            "every-sec",
            RepeatOptions::cron("* * * * * *").unwrap(),
            Tick(3),
        );
        hs.push(tokio::spawn(s.run(cancel.clone())));
    }
    tokio::time::sleep(ms(2600)).await;
    cancel.cancel();
    for h in hs {
        h.await.unwrap().unwrap();
    }
    tokio::time::sleep(ms(500)).await;
    wc.cancel();
    wh.await.unwrap().unwrap();
    let ran = ran.lock().clone();
    let ticks: Vec<i64> = ran.iter().map(|(id, _)| fire_time(id)).collect();
    assert!(ticks.len() >= 2, "{ticks:?}");
    assert!(
        ticks.iter().all(|t| t % 1000 == 0),
        "whole seconds: {ticks:?}"
    );
    let mut dedup = ticks.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        ticks.len(),
        "3 instances, one job per second: {ticks:?}"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_a_scheduler_stops_every_instance() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-remove").await;
    let mut rec = Recorder::new("b14_remove_scheduler", q.name()).await;
    let cancel = CancellationToken::new();
    let mut hs = Vec::new();
    for _ in 0..3 {
        let s = JobScheduler::new(
            instance(&q).await,
            "gone",
            RepeatOptions::every(ms(150)),
            Tick(4),
        );
        hs.push(tokio::spawn(s.run(cancel.clone())));
    }
    assert!(
        rec.wait_until("registered", secs(2), |s| s.len("delayed") + s.len("wait")
            >= 1)
            .await
    );
    assert_eq!(q.schedulers().await.unwrap().len(), 1);
    assert!(
        rec.step("remove", q.remove_scheduler("gone"))
            .await
            .unwrap()
    );
    // Every instance stops by itself within a tick or two.
    for h in hs {
        tokio::time::timeout(secs(2), h)
            .await
            .expect("instance stopped")
            .unwrap()
            .unwrap();
    }
    rec.observe("instances stopped").await;
    let s = rec.snapshot();
    assert_eq!(
        s.len("delayed") + s.len("wait"),
        0,
        "no pending tick left: {}",
        s.summary()
    );
    assert!(q.schedulers().await.unwrap().is_empty());
    assert!(
        !q.remove_scheduler("gone").await.unwrap(),
        "already removed"
    );
    // Starting it again registers it again.
    let s = JobScheduler::new(q.clone(), "gone", RepeatOptions::every(ms(150)), Tick(4));
    let id = s.enqueue_next().await.unwrap();
    assert_eq!(q.schedulers().await.unwrap().len(), 1);
    assert!(fire_time(id.as_str()) > now_ms() - 150);
    cancel.cancel();
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ticks_that_piled_up_without_workers_are_all_removed() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-pileup").await;
    let mut rec = Recorder::new("b14_remove_piled_up_ticks", q.name()).await;
    let cancel = CancellationToken::new();
    let s = JobScheduler::new(q.clone(), "pile", RepeatOptions::every(ms(100)), Tick(5));
    let h = tokio::spawn(s.run(cancel.clone()));
    // No worker: every tick stays pending.
    assert!(
        rec.wait_until("5 ticks pending", secs(3), |s| s.len("delayed") >= 5)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("scheduler stopped").await;
    let pending = rec.snapshot().len("delayed");
    assert_eq!(rec.snapshot().scheduler_ticks["pile"].len(), pending);
    rec.step("remove", q.remove_scheduler("pile"))
        .await
        .unwrap();
    let s = rec.snapshot();
    assert_eq!(
        s.jobs.len(),
        0,
        "{} ticks were pending, all removed: {}",
        pending,
        s.summary()
    );
    assert!(s.scheduler_ticks.is_empty());
    assert_eq!(rec.events_named("removed").len(), pending);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_tick_index_stays_small_while_ticks_run() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-index").await;
    let mut rec = Recorder::new("b14_tick_index_pruned", q.name()).await;
    let ran = Arc::new(Mutex::new(Vec::new()));
    let (wc, wh) = worker(&q, ran.clone());
    let cancel = CancellationToken::new();
    let s = JobScheduler::new(q.clone(), "busy", RepeatOptions::every(ms(50)), Tick(6))
        .with_options(JobOptions::new().remove_on_complete(Removal::KeepLast(3)));
    let h = tokio::spawn(s.run(cancel.clone()));
    assert!(
        rec.wait_until("30 ticks ran", secs(5), |_| ran.lock().len() >= 30)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    wc.cancel();
    wh.await.unwrap().unwrap();
    rec.observe("after").await;
    let idx = rec
        .snapshot()
        .scheduler_ticks
        .get("busy")
        .map_or(0, Vec::len);
    assert!(idx <= 3, "{idx} ticks indexed after 30 ran");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_schedulers_are_refused() {
    let (q, _b) = common::fresh_queue::<Tick>("b14-invalid").await;
    for (id, repeat) in [
        ("has:colon", RepeatOptions::every(secs(1))),
        ("", RepeatOptions::every(secs(1))),
        ("zero", RepeatOptions::every(Duration::ZERO)),
    ] {
        let s = JobScheduler::new(q.clone(), id, repeat, Tick(0));
        assert!(
            matches!(s.enqueue_next().await, Err(Error::Config(_))),
            "{id:?}"
        );
        let r = s.run(CancellationToken::new()).await;
        assert!(matches!(r, Err(Error::Config(_))), "{id:?}");
    }
    assert_eq!(q.counts().await.unwrap().delayed, 0);
    common::teardown(&q).await;
}

#[test]
fn cron_parse_error_surfaces_as_config() {
    let err = RepeatOptions::cron("not a cron expression").unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}
