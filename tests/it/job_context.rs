//! B12c — handlers get a `JobContext`.
//!
//! In 0.2 a handler only received `Job<D>`: it could not report progress,
//! write log lines, find out that its lock had been lost, or notice that the
//! worker was shutting down. `Backend::update_progress` / `log` also wrote
//! to a job's keys without checking that the job existed, so calling them
//! for a removed job re-created an orphan hash / `:logs` key.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::common;
use oxn::backend::{Backend, JobRange};
use oxn::prelude::*;
use oxn::worker::JobContext;
use oxn_testkit::{Recorder, ms, secs};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

#[derive(Serialize)]
struct Stage {
    step: &'static str,
    done: u32,
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_report_progress_and_logs() {
    let (q, b) = common::fresh_queue::<T>("b12c-progress").await;
    let mut rec = Recorder::new("b12c_progress_logs", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let w = Worker::builder_with_ctx(q.clone(), |job: Job<T>, ctx: JobContext| async move {
        assert_eq!(ctx.job_id(), &job.id);
        assert_eq!(ctx.attempt(), 1);
        ctx.log("starting").await?;
        for p in [10u8, 50, 100] {
            ctx.progress(p).await?;
        }
        ctx.progress(Progress::from_serializable(&Stage {
            step: "upload",
            done: 3,
        })?)
        .await?;
        ctx.log(format!("finished job {}", ctx.job_id())).await?;
        Ok::<_, Error>(())
    })
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    assert!(
        rec.wait_until("completes", secs(5), |s| s.len("completed") == 1)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();

    let job = q.get(&id).await.unwrap().unwrap();
    assert_eq!(
        job.progress,
        Some(Progress::Arbitrary(
            serde_json::json!({"step": "upload", "done": 3})
        ))
    );
    let logs = b
        .get_logs(q.name(), &id, JobRange::first(10))
        .await
        .unwrap();
    assert_eq!(
        logs,
        vec!["starting".to_string(), format!("finished job {id}")]
    );
    let progress: Vec<String> = rec
        .events_named("progress")
        .iter()
        .map(|e| e.fields["data"].clone())
        .collect();
    assert_eq!(
        progress,
        ["10.0", "50.0", "100.0", r#"{"done":3,"step":"upload"}"#]
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn progress_and_logs_for_a_missing_job_are_refused_not_recreated() {
    let (q, b) = common::fresh_queue::<T>("b12c-missing").await;
    let mut rec = Recorder::new("b12c_no_orphans", q.name()).await.strict();
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    rec.step("remove it", q.remove(&id)).await.unwrap();
    let r = rec
        .step(
            "progress on removed job",
            b.update_progress(q.name(), &id, "42"),
        )
        .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
    let r = rec
        .step("log on removed job", b.log(q.name(), &id, "late line"))
        .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
    let s = rec.snapshot();
    assert!(
        s.jobs.is_empty() && s.orphan_aux.is_empty(),
        "{}",
        s.summary()
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_lock_is_signalled_to_the_handler() {
    let (q, _b) = common::fresh_queue::<T>("b12c-lock").await;
    let mut rec = Recorder::new("b12c_lock_lost", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let noticed = Arc::new(AtomicBool::new(false));
    let n = noticed.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = std::sync::Mutex::new(Some(started_tx));
    let w = Worker::builder_with_ctx(q.clone(), move |_job: Job<T>, ctx: JobContext| {
        let n = n.clone();
        let tx = started_tx.lock().unwrap().take();
        async move {
            if let Some(tx) = tx {
                let _ = tx.send(());
            }
            tokio::select! {
                _ = ctx.lock_lost_signal() => {
                    assert!(ctx.lock_lost());
                    n.store(true, Ordering::SeqCst);
                    Err(Error::unrecoverable("stopping: lock lost"))
                }
                _ = tokio::time::sleep(secs(10)) => Ok(()),
            }
        }
    })
    .lock_duration(secs(5))
    .lock_renew_interval(ms(100))
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    started_rx.await.unwrap();
    let mut c = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let base = format!("{}:{}:{}", common::TEST_PREFIX, q.name(), id);
    rec.step("another owner takes the lock", async {
        let _: () = c.set(format!("{base}:lock"), "thief").await.unwrap();
        let _: () = c.hset(&base, "lockToken", "thief").await.unwrap();
    })
    .await;
    let t = Instant::now();
    while !noticed.load(Ordering::SeqCst) {
        assert!(
            t.elapsed() < secs(3),
            "handler never learned its lock was lost"
        );
        tokio::time::sleep(ms(20)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("after shutdown").await;
    assert_eq!(
        rec.snapshot().location(id.as_str()),
        "active",
        "the new owner still holds it"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_can_wind_down_cooperatively_on_shutdown() {
    let (q, _b) = common::fresh_queue::<T>("b12c-shutdown").await;
    let mut rec = Recorder::new("b12c_cooperative_shutdown", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = std::sync::Mutex::new(Some(started_tx));
    let w = Worker::builder_with_ctx(q.clone(), move |_job: Job<T>, ctx: JobContext| {
        let tx = started_tx.lock().unwrap().take();
        async move {
            if let Some(tx) = tx {
                let _ = tx.send(());
            }
            // A long batch job that checkpoints and stops early on shutdown.
            let mut done = 0u32;
            while done < 1_000 {
                if ctx.shutdown_requested() {
                    ctx.log(format!("checkpoint at {done}, stopping for shutdown"))
                        .await?;
                    return Ok(());
                }
                tokio::time::sleep(ms(10)).await;
                done += 1;
            }
            Ok(())
        }
    })
    .shutdown_timeout(secs(30))
    .drain_delay(ms(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    started_rx.await.unwrap();
    tokio::time::sleep(ms(100)).await;
    let t = Instant::now();
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "the handler wound down in {:?}",
        t.elapsed()
    );
    rec.observe("after shutdown").await;
    assert_eq!(rec.snapshot().location(id.as_str()), "completed");
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn log_pages_read_oldest_or_newest_first() {
    let (q, b) = common::fresh_queue::<T>("logs-order").await;
    let id = q.add(T(1), JobOptions::new()).await.unwrap();
    for i in 1..=5 {
        b.log(q.name(), &id, &format!("l{i}")).await.unwrap();
    }
    let page = |start, end, descending| JobRange {
        start,
        end,
        descending,
    };
    let get = |r| {
        let (b, q, id) = (b.clone(), q.name().to_string(), id.clone());
        async move { b.get_logs(&q, &id, r).await.unwrap() }
    };
    assert_eq!(get(page(0, 1, false)).await, ["l1", "l2"]);
    assert_eq!(get(page(0, 1, true)).await, ["l5", "l4"], "newest first");
    assert_eq!(get(page(2, 3, true)).await, ["l3", "l2"]);
    assert_eq!(get(page(3, -1, true)).await, ["l2", "l1"], "to the oldest");
    assert_eq!(get(page(0, -1, true)).await, ["l5", "l4", "l3", "l2", "l1"]);
    assert!(get(page(9, 10, true)).await.is_empty());
    common::teardown(&q).await;
}
