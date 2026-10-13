//! Self-tests for the recorder: it must see a healthy run as clean, catch
//! hand-made corruption, and capture traces, events and MONITOR output.

use std::time::Duration;

use oxn::prelude::*;
use oxn_testkit::{Recorder, Severity, fresh_queue, secs, teardown};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

async fn raw() -> redis::aio::MultiplexedConnection {
    redis::Client::open(oxn_testkit::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn healthy_run_is_clean_and_fully_captured() {
    let (q, _b) = fresh_queue::<T>("selftest-ok").await;
    let mut rec = Recorder::new("selftest_healthy_run", q.name()).await;

    for i in 0..3 {
        rec.step(&format!("add job {i}"), q.add(T(i), JobOptions::new()))
            .await
            .unwrap();
    }
    assert_eq!(rec.snapshot().len("wait"), 3);

    let worker = Worker::builder(q.clone(), |_j: Job<T>| async { Ok::<_, Error>(()) })
        .drain_delay(Duration::from_millis(50))
        .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());
    assert!(
        rec.wait_until("all completed", secs(5), |s| s.len("completed") == 3)
            .await
    );
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("after shutdown").await;

    rec.assert_clean();
    assert_eq!(rec.events_named("completed").len(), 3);
    assert_eq!(rec.events_named("added").len(), 3);
    let processed = rec.traces_named("oxn.process");
    assert_eq!(processed.len(), 3, "one oxn.process span per job");
    assert!(
        processed
            .iter()
            .all(|t| t.fields.get("transition").map(String::as_str) == Some("completed"))
    );
    let scripts = rec.traces_named("oxn.script");
    assert!(
        scripts
            .iter()
            .any(|t| t.fields.get("script").map(String::as_str) == Some("move_to_finished"))
    );

    let path = rec.write_report();
    let md = std::fs::read_to_string(&path).unwrap();
    assert!(md.contains("**PASS**"));
    assert!(md.contains("## Per-job history"));
    assert!(md.contains("### job `1`"));
    assert!(md.contains("oxn.process"));
    teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_handmade_corruption() {
    let (q, _b) = fresh_queue::<T>("selftest-bad").await;
    let mut rec = Recorder::new("selftest_corruption", q.name()).await;
    let id = rec
        .step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    let base = format!("{}:{}:", oxn_testkit::TEST_PREFIX, q.name());

    let mut c = raw().await;
    rec.step("put job in completed too", async {
        let _: () = c
            .zadd(format!("{base}completed"), id.as_str(), 1)
            .await
            .unwrap();
    })
    .await;
    rec.step("create orphan hash", async {
        let _: () = c.hset(format!("{base}999"), "data", "1").await.unwrap();
    })
    .await;
    rec.step("create unknown key", async {
        let _: () = c.set(format!("{base}weird:thing"), "x").await.unwrap();
    })
    .await;

    assert!(!rec.violations_of("single-container").is_empty());
    assert!(!rec.violations_of("orphan-hash").is_empty());
    assert!(!rec.violations_of("unknown-key").is_empty());
    assert!(rec.violations(Severity::Error).len() >= 3);
    // assert_clean must fail and point at the report.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rec.assert_clean()));
    let msg = *r.unwrap_err().downcast::<String>().unwrap();
    assert!(
        msg.contains("single-container") && msg.contains("timeline.md"),
        "{msg}"
    );
    teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn monitor_tap_sees_script_internals() {
    let (q, _b) = fresh_queue::<T>("selftest-mon").await;
    let mut rec = Recorder::new("selftest_monitor", q.name())
        .await
        .with_monitor()
        .await;
    rec.step("add", q.add(T(1), JobOptions::new()))
        .await
        .unwrap();
    // MONITOR delivery is asynchronous; give it a beat, then observe.
    tokio::time::sleep(Duration::from_millis(100)).await;
    rec.observe("drain monitor").await;
    let cmds: Vec<String> = rec
        .steps()
        .iter()
        .flat_map(|s| s.commands.iter().map(|c| c.text.clone()))
        .collect();
    assert!(
        cmds.iter().any(|c| c.starts_with("EVALSHA<add_job>")),
        "{cmds:#?}"
    );
    let lua: Vec<_> = rec
        .steps()
        .iter()
        .flat_map(|s| s.commands.iter().filter(|c| c.from_lua))
        .collect();
    assert!(
        lua.iter().any(|c| c.text.starts_with("HSET")),
        "lua commands: {lua:#?}"
    );
    teardown(&q).await;
}
