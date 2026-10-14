//! B13 — flows, fully.
//!
//! In 0.2 `FlowProducer` added each node as an ordinary job: the parent went
//! straight to `wait` and ran before its children, child names were mangled
//! to `name@child-of:{queue}:{id}`, nothing was ever put in
//! `waiting-children`, and `Job::parent` was always `None`. Now the whole
//! tree is added atomically, a parent waits for its children, receives
//! their results, and each child's failure mode decides what its failure
//! does to the parent.

#![cfg(feature = "flow")]

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use crate::common;
use oxn::backend::Backend;
use oxn::error::ScriptError;
use oxn::flow::{FlowNode, FlowProducer};
use oxn::options::Removal;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// `(job name, started, finished)` for every handler run.
type RunLog = Arc<Mutex<Vec<(String, Instant, Instant)>>>;

fn node(queue: &str, name: &str) -> FlowNode {
    FlowNode::new(queue, name, &json!({ "name": name }), JobOptions::new()).unwrap()
}

fn node_opts(queue: &str, name: &str, opts: JobOptions) -> FlowNode {
    FlowNode::new(queue, name, &json!({ "name": name }), opts).unwrap()
}

struct Running(CancellationToken, JoinHandle<oxn::Result<()>>);

impl Running {
    async fn stop(self) {
        self.0.cancel();
        self.1.await.unwrap().unwrap();
    }
}

fn start<F, Fut>(q: &Queue<Value>, handler: F) -> Running
where
    F: Fn(Job<Value>, JobContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = oxn::Result<Value>> + Send + 'static,
{
    let w: Worker<Value, Value> = Worker::builder_with_ctx(q.clone(), handler)
        .concurrency(4)
        .drain_delay(ms(20))
        .build();
    Running(w.cancellation_token(), tokio::spawn(w.run()))
}

/// A worker that logs each run and returns the job's name; jobs named in
/// `fail` fail for good (unrecoverable).
fn logging_worker(q: &Queue<Value>, log: RunLog, fail: &'static [&'static str]) -> Running {
    start(q, move |job, _ctx| {
        let log = log.clone();
        async move {
            let t = Instant::now();
            tokio::time::sleep(ms(15)).await;
            log.lock().push((job.name.clone(), t, Instant::now()));
            if fail.contains(&job.name.as_str()) {
                return Err(Error::unrecoverable(format!("{} broke", job.name)));
            }
            Ok(json!(job.name))
        }
    })
}

fn runs(log: &RunLog, name: &str) -> Vec<(Instant, Instant)> {
    log.lock()
        .iter()
        .filter(|(n, ..)| n == name)
        .map(|(_, s, e)| (*s, *e))
        .collect()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn other_queue(q: &Queue<Value>, tag: &str) -> String {
    format!("{}-{tag}", q.name())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parent_runs_once_after_all_its_children() {
    let (q, b) = common::fresh_queue::<Value>("b13-order").await;
    let mut rec = Recorder::new("b13_parent_after_children", q.name()).await;
    let qn = q.name().to_string();
    let flow = FlowProducer::new(b.clone());
    let tree = rec
        .step(
            "add root ← {a, b ← {x}, c}",
            flow.add(
                node(&qn, "root")
                    .child(node(&qn, "a"))
                    .child(node(&qn, "b").child(node(&qn, "x")))
                    .child(node(&qn, "c")),
            ),
        )
        .await
        .unwrap();
    let id = |t: &oxn::FlowJob| t.id.as_str().to_string();
    let (root, a, bb, x, c) = (
        id(&tree),
        id(&tree.children[0]),
        id(&tree.children[1]),
        id(&tree.children[1].children[0]),
        id(&tree.children[2]),
    );
    let s = rec.snapshot();
    assert_eq!(s.location(&root), "waiting-children");
    assert_eq!(s.location(&bb), "waiting-children");
    for leaf in [&a, &x, &c] {
        assert_eq!(s.location(leaf), "wait");
    }
    assert_eq!(s.jobs[&root].deps.len(), 3);
    assert_eq!(s.jobs[&bb].deps.len(), 1);
    assert_eq!(
        s.jobs[&a].fields["parentKey"],
        format!("{}:{qn}:{root}", common::TEST_PREFIX)
    );
    assert_eq!(rec.events_named("waiting-children").len(), 2);
    assert_eq!(rec.events_named("added").len(), 5);

    let log: RunLog = Arc::default();
    let l = log.clone();
    let w = start(&q, move |job, ctx| {
        let l = l.clone();
        async move {
            let t = Instant::now();
            tokio::time::sleep(ms(15)).await;
            let kids: Vec<(JobRef, Value)> = ctx.children_values().await?;
            l.lock().push((job.name.clone(), t, Instant::now()));
            Ok(
                json!({ "me": job.name, "kids": kids.into_iter().map(|(_, v)| v).collect::<Vec<_>>() }),
            )
        }
    });
    assert!(
        rec.wait_until("all 5 completed", secs(10), |s| s.len("completed") == 5)
            .await
    );
    w.stop().await;

    for name in ["root", "a", "b", "x", "c"] {
        assert_eq!(runs(&log, name).len(), 1, "{name} ran once");
    }
    let started = |n: &str| runs(&log, n)[0].0;
    let ended = |n: &str| runs(&log, n)[0].1;
    for kid in ["a", "b", "c"] {
        assert!(
            started("root") >= ended(kid),
            "root started before {kid} finished"
        );
    }
    assert!(started("b") >= ended("x"), "b started before x finished");

    let root_job = q.get(&JobId::new(&root)).await.unwrap().unwrap();
    let ret: Value = serde_json::from_str(
        &b.get(&qn, &JobId::new(&root))
            .await
            .unwrap()
            .unwrap()
            .return_value_json
            .unwrap(),
    )
    .unwrap();
    assert_eq!(ret["kids"].as_array().unwrap().len(), 3, "{ret}");
    assert!(
        ret["kids"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k["me"] == "b" && k["kids"] == json!([{"me": "x", "kids": []}])),
        "{ret}"
    );
    assert_eq!(root_job.parent, None);
    let child = q.get(&JobId::new(&a)).await.unwrap().unwrap();
    assert_eq!(child.parent, Some(JobRef::new(qn.clone(), root.as_str())));
    let deps = q.dependencies(&JobId::new(&root)).await.unwrap();
    assert!(deps.pending.is_empty() && deps.failed.is_empty());
    assert_eq!(deps.processed.len(), 3);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn children_in_other_queues_release_their_parent() {
    let (pq, b) = common::fresh_queue::<Value>("b13-xq").await;
    let c1: Queue<Value> = common::queue(b.clone(), &other_queue(&pq, "c1")).await;
    let c2: Queue<Value> = common::queue(b.clone(), &other_queue(&pq, "c2")).await;
    let mut rp = Recorder::new("b13_cross_queue_parent", pq.name()).await;
    let mut r1 = Recorder::new("b13_cross_queue_c1", c1.name()).await;
    let flow = FlowProducer::new(b.clone());
    let tree = rp
        .step(
            "add parent with children in two other queues",
            flow.add(
                node(pq.name(), "parent")
                    .child(node(c1.name(), "one"))
                    .child(node(c1.name(), "two"))
                    .child(node(c2.name(), "three")),
            ),
        )
        .await
        .unwrap();
    assert_eq!(tree.children[2].queue, c2.name());
    let log: RunLog = Arc::default();
    let ws = [
        logging_worker(&pq, log.clone(), &[]),
        logging_worker(&c1, log.clone(), &[]),
        logging_worker(&c2, log.clone(), &[]),
    ];
    assert!(
        rp.wait_until("parent completed", secs(10), |s| s.len("completed") == 1)
            .await
    );
    for w in ws {
        w.stop().await;
    }
    r1.observe("children done").await;
    assert_eq!(r1.snapshot().len("completed"), 2);
    let p = runs(&log, "parent")[0].0;
    for kid in ["one", "two", "three"] {
        assert!(p >= runs(&log, kid)[0].1, "parent before {kid}");
    }
    let deps = pq.dependencies(&tree.id).await.unwrap();
    let mut queues: Vec<_> = deps
        .processed
        .iter()
        .map(|(r, _)| r.queue.clone())
        .collect();
    queues.sort();
    assert_eq!(queues, [c1.name(), c1.name(), c2.name()]);
    rp.assert_clean();
    r1.assert_clean();
    for q in [&pq, &c1, &c2] {
        common::teardown(q).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_parent_honors_pause_delay_and_priority() {
    let (pq, b) = common::fresh_queue::<Value>("b13-release").await;
    let cq: Queue<Value> = common::queue(b.clone(), &other_queue(&pq, "kids")).await;
    let mut rec = Recorder::new("b13_release_rules", pq.name()).await;
    let flow = FlowProducer::new(b.clone());
    let delayed = flow
        .add(
            node_opts(pq.name(), "delayed", JobOptions::new().delay(secs(30)))
                .child(node(cq.name(), "k1")),
        )
        .await
        .unwrap();
    let prio = flow
        .add(
            node_opts(pq.name(), "prio", JobOptions::new().priority(5))
                .child(node(cq.name(), "k2")),
        )
        .await
        .unwrap();
    rec.observe("both parents wait for children").await;
    assert_eq!(
        rec.snapshot().len("waiting-children"),
        2,
        "a delay or priority doesn't skip waiting"
    );
    let w = logging_worker(&cq, Arc::default(), &[]);
    let t0 = now_ms();
    assert!(
        rec.wait_until("parents released", secs(5), |s| s.len("waiting-children")
            == 0)
            .await
    );
    let s = rec.snapshot();
    assert_eq!(s.location(prio.id.as_str()), "prioritized");
    assert_eq!(s.location(delayed.id.as_str()), "delayed");
    let due = s.zsets["delayed"][0].1 as i64;
    assert!(
        due >= t0 + 29_000,
        "the parent's own delay runs from its release"
    );

    // A paused parent queue: the released parent lands in `paused`.
    rec.step("pause the parent queue", pq.pause())
        .await
        .unwrap();
    let paused = flow
        .add(node(pq.name(), "paused").child(node(cq.name(), "k3")))
        .await
        .unwrap();
    assert!(
        rec.wait_until("released into paused", secs(5), |s| s
            .location(paused.id.as_str())
            == "paused")
            .await
    );
    let waiting = rec.events_named("waiting");
    assert!(
        waiting
            .iter()
            .any(|e| e.job_id.as_deref() == Some(paused.id.as_str())
                && e.fields.get("prev").map(String::as_str) == Some("waiting-children"))
    );
    w.stop().await;
    rec.assert_clean();
    common::teardown(&pq).await;
    common::teardown(&cq).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fail_parent_on_failure_propagates_up_the_tree() {
    let (q, b) = common::fresh_queue::<Value>("b13-fpof").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_fail_parent", &qn).await;
    let flow = FlowProducer::new(b.clone());
    let tree = rec
        .step(
            "add root ← mid(fpof) ← {bad(fpof), slow}",
            flow.add(
                node(&qn, "root").child(
                    node(&qn, "mid")
                        .fail_parent_on_failure()
                        .child(
                            node_opts(&qn, "bad", JobOptions::new().attempts(2))
                                .fail_parent_on_failure(),
                        )
                        .child(node(&qn, "slow")),
                ),
            ),
        )
        .await
        .unwrap();
    let mid = tree.children[0].id.clone();
    let bad = tree.children[0].children[0].id.clone();
    let log: RunLog = Arc::default();
    let l = log.clone();
    let w = start(&q, move |job, _| {
        let l = l.clone();
        async move {
            let t = Instant::now();
            if job.name == "slow" {
                tokio::time::sleep(ms(400)).await;
            }
            l.lock().push((job.name.clone(), t, Instant::now()));
            if job.name == "bad" {
                return Err(Error::handler(std::io::Error::other("disk full")));
            }
            Ok(json!(job.name))
        }
    });
    assert!(
        rec.wait_until("bad, mid and root failed", secs(10), |s| s.len("failed")
            == 3)
            .await
    );
    assert!(
        rec.wait_until("slow still completes", secs(5), |s| s.len("completed") == 1)
            .await
    );
    w.stop().await;
    assert_eq!(runs(&log, "bad").len(), 2, "bad used both attempts");
    assert!(
        runs(&log, "mid").is_empty() && runs(&log, "root").is_empty(),
        "failed parents never run"
    );
    let mid_job = q.get(&mid).await.unwrap().unwrap();
    let bad_key = format!("{}:{qn}:{bad}", common::TEST_PREFIX);
    assert_eq!(
        mid_job.failed_reason.as_deref(),
        Some(format!("child {bad_key} failed: handler error: disk full").as_str())
    );
    let root_job = q.get(&tree.id).await.unwrap().unwrap();
    let reason = root_job.failed_reason.unwrap();
    assert!(
        reason.starts_with(&format!(
            "child {}:{qn}:{mid} failed: child {bad_key}",
            common::TEST_PREFIX
        )),
        "{reason}"
    );
    assert_eq!(root_job.stacktrace.len(), 1);
    // The slow sibling finished after mid failed: recorded, mid stays failed.
    let deps = q.dependencies(&mid).await.unwrap();
    assert_eq!(deps.processed.len(), 1);
    assert_eq!(
        deps.pending.len(),
        1,
        "the failed child stays listed as pending"
    );
    assert_eq!(rec.events_named("failed").len(), 3);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ignored_failures_let_the_parent_run() {
    let (q, b) = common::fresh_queue::<Value>("b13-idof").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_ignore_failure", &qn).await;
    let flow = FlowProducer::new(b.clone());
    let tree = flow
        .add(
            node(&qn, "parent")
                .child(node(&qn, "fine"))
                .child(node(&qn, "broken").ignore_dependency_on_failure()),
        )
        .await
        .unwrap();
    let seen = Arc::new(Mutex::new(None));
    let s2 = seen.clone();
    let w = start(&q, move |job, ctx| {
        let s2 = s2.clone();
        async move {
            match job.name.as_str() {
                "broken" => Err(Error::unrecoverable("nope")),
                "parent" => {
                    *s2.lock() = Some(ctx.dependencies().await?);
                    Ok(json!("parent"))
                }
                n => Ok(json!(n)),
            }
        }
    });
    assert!(
        rec.wait_until("parent completed", secs(10), |s| s
            .location(tree.id.as_str())
            == "completed")
            .await
    );
    w.stop().await;
    let deps = seen.lock().clone().expect("parent ran");
    assert_eq!(
        deps.processed,
        vec![(tree.children[0].job_ref(), "\"fine\"".to_string())]
    );
    assert_eq!(
        deps.failed,
        vec![(tree.children[1].job_ref(), "nope".to_string())]
    );
    assert!(deps.pending.is_empty());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn by_default_a_failed_child_holds_its_parent_until_retried() {
    let (q, b) = common::fresh_queue::<Value>("b13-wait").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_default_wait", &qn).await;
    let flow = FlowProducer::new(b.clone());
    let tree = flow
        .add(node(&qn, "parent").child(node(&qn, "flaky")))
        .await
        .unwrap();
    let flaky = tree.children[0].id.clone();
    let broken = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let br = broken.clone();
    let w = start(&q, move |job, _| {
        let br = br.clone();
        async move {
            if job.name == "flaky" && br.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::unrecoverable("down"));
            }
            Ok(json!(job.name))
        }
    });
    assert!(
        rec.wait_until("child failed", secs(5), |s| s.len("failed") == 1)
            .await
    );
    tokio::time::sleep(ms(200)).await;
    rec.observe("a while later").await;
    assert_eq!(
        rec.snapshot().location(tree.id.as_str()),
        "waiting-children"
    );
    broken.store(false, std::sync::atomic::Ordering::SeqCst);
    rec.step("retry the child", q.retry(&flaky)).await.unwrap();
    assert!(
        rec.wait_until("both completed", secs(5), |s| s.len("completed") == 2)
            .await
    );
    w.stop().await;
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn removing_a_parent_removes_its_whole_tree_unless_part_of_it_is_running() {
    let (q, b) = common::fresh_queue::<Value>("b13-cascade").await;
    let cq: Queue<Value> = common::queue(b.clone(), &other_queue(&q, "kids")).await;
    let mut rec = Recorder::new("b13_cascade_remove", q.name()).await;
    let mut rc = Recorder::new("b13_cascade_remove_kids", cq.name()).await;
    let flow = FlowProducer::new(b.clone());
    let tree = flow
        .add(
            node(q.name(), "root")
                .child(
                    node(cq.name(), "a")
                        .child(node(q.name(), "a1"))
                        .child(node(cq.name(), "a2")),
                )
                .child(node(q.name(), "b")),
        )
        .await
        .unwrap();
    // A running leaf blocks the removal: nothing at all is deleted.
    let fetched = b
        .fetch_next(
            cq.name(),
            "w",
            secs(30),
            ms(10),
            &CancellationToken::new(),
            None,
        )
        .await
        .unwrap()
        .expect("a2 is ready");
    assert_eq!(fetched.job.name, "a2");
    let r = rec
        .step("remove root while a2 runs", q.remove(&tree.id))
        .await;
    assert!(
        matches!(r, Err(Error::Script(ScriptError::JobLocked))),
        "{r:?}"
    );
    rc.observe("after the refused removal").await;
    assert_eq!(rec.snapshot().jobs.len() + rc.snapshot().jobs.len(), 5);
    b.release(cq.name(), &fetched.job.id, &fetched.token)
        .await
        .unwrap();

    rec.step("remove root", q.remove(&tree.id)).await.unwrap();
    rc.observe("after").await;
    assert!(rec.snapshot().jobs.is_empty() && rc.snapshot().jobs.is_empty());
    assert!(rec.snapshot().orphan_aux.is_empty() && rc.snapshot().orphan_aux.is_empty());
    assert_eq!(
        rec.events_named("removed").len() + rc.events_named("removed").len(),
        5
    );
    rec.assert_clean();
    rc.assert_clean();
    common::teardown(&q).await;
    common::teardown(&cq).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_pending_child_resolves_the_dependency() {
    let (q, b) = common::fresh_queue::<Value>("b13-detach").await;
    let cq: Queue<Value> = common::queue(b.clone(), &other_queue(&q, "kids")).await;
    let mut rec = Recorder::new("b13_delete_child", q.name()).await;
    let flow = FlowProducer::new(b.clone());
    let tree = flow
        .add(
            node(q.name(), "parent")
                .child(node(cq.name(), "a"))
                .child(node(cq.name(), "b"))
                .child(node(cq.name(), "c")),
        )
        .await
        .unwrap();
    rec.step("remove child a", cq.remove(&tree.children[0].id))
        .await
        .unwrap();
    assert_eq!(
        rec.snapshot().location(tree.id.as_str()),
        "waiting-children"
    );
    assert_eq!(rec.snapshot().jobs[tree.id.as_str()].deps.len(), 2);
    rec.step("drain the children's queue", cq.drain(false))
        .await
        .unwrap();
    assert_eq!(
        rec.snapshot().location(tree.id.as_str()),
        "wait",
        "nothing left to wait for"
    );
    assert!(q.dependencies(&tree.id).await.unwrap().pending.is_empty());
    rec.assert_clean();
    common::teardown(&q).await;
    common::teardown(&cq).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_explicit_id_rejects_the_whole_flow() {
    let (q, b) = common::fresh_queue::<Value>("b13-exists").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_flow_id_exists", &qn).await.strict();
    rec.step(
        "add job `taken`",
        q.add(json!(1), JobOptions::new().id("taken")),
    )
    .await
    .unwrap();
    let flow = FlowProducer::new(b.clone());
    let r = rec
        .step(
            "add a flow reusing `taken`",
            flow.add(node(&qn, "p").child(node(&qn, "fresh")).child(node_opts(
                &qn,
                "dup",
                JobOptions::new().id("taken"),
            ))),
        )
        .await;
    assert!(
        matches!(r, Err(Error::Script(ScriptError::JobExists))),
        "{r:?}"
    );
    let s = rec.snapshot();
    assert_eq!(s.jobs.len(), 1, "nothing of the flow was written");
    assert_eq!(s.jobs["taken"].fields["data"], "1");
    let r = flow
        .add(
            node_opts(&qn, "p", JobOptions::new().id("x")).child(node_opts(
                &qn,
                "c",
                JobOptions::new().id("x"),
            )),
        )
        .await;
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    rec.observe("after the duplicate-in-flow attempt").await;
    assert_eq!(rec.snapshot().jobs.len(), 1);
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_can_add_children_and_wait_for_them() {
    let (q, b) = common::fresh_queue::<Value>("b13-dynamic").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_dynamic_children", &qn).await;
    let parent = rec
        .step(
            "add the parent",
            q.add(json!("p"), JobOptions::new().name("parent")),
        )
        .await
        .unwrap();
    let runs_seen = Arc::new(Mutex::new(Vec::new()));
    let rs = runs_seen.clone();
    let flow = FlowProducer::new(b.clone());
    let qn2 = qn.clone();
    let w = start(&q, move |job, ctx| {
        let (rs, flow, qn) = (rs.clone(), flow.clone(), qn2.clone());
        async move {
            if job.name != "parent" {
                tokio::time::sleep(ms(30)).await;
                return Ok(json!(format!("{}!", job.name)));
            }
            let values: Vec<(JobRef, String)> = ctx.children_values().await?;
            rs.lock().push((job.attempts_made, values.len()));
            if values.is_empty() {
                flow.add_under(&ctx.as_parent(), node(&qn, "x")).await?;
                flow.add_under(&ctx.as_parent(), node(&qn, "y")).await?;
                return Err(Error::WaitingChildren);
            }
            Ok(json!(
                values.into_iter().map(|(_, v)| v).collect::<Vec<_>>()
            ))
        }
    });
    assert!(
        rec.wait_until("parent completed", secs(10), |s| s
            .location(parent.as_str())
            == "completed")
            .await
    );
    w.stop().await;
    assert_eq!(
        *runs_seen.lock(),
        vec![(1, 0), (1, 2)],
        "ran twice, the wait refunded the attempt"
    );
    let ret = b
        .get(&qn, &parent)
        .await
        .unwrap()
        .unwrap()
        .return_value_json
        .unwrap();
    let mut ret: Vec<String> = serde_json::from_str(&ret).unwrap();
    ret.sort();
    assert_eq!(ret, ["x!", "y!"]);
    assert_eq!(
        rec.events_named("waiting-children")
            .iter()
            .filter(|e| e.job_id.as_deref() == Some(parent.as_str()))
            .count(),
        1
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn waiting_with_no_children_runs_again_and_ok_with_pending_children_fails() {
    let (q, b) = common::fresh_queue::<Value>("b13-misuse").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_wait_misuse", &qn).await;
    let again = q
        .add(json!(0), JobOptions::new().name("again"))
        .await
        .unwrap();
    let eager = q
        .add(json!(0), JobOptions::new().name("eager"))
        .await
        .unwrap();
    let calls = Arc::new(Mutex::new(0u32));
    let c = calls.clone();
    let flow = FlowProducer::new(b.clone());
    let qn2 = qn.clone();
    let w = start(&q, move |job, ctx| {
        let (c, flow, qn) = (c.clone(), flow.clone(), qn2.clone());
        async move {
            match job.name.as_str() {
                "again" => {
                    let mut n = c.lock();
                    *n += 1;
                    if *n == 1 {
                        return Err(Error::WaitingChildren);
                    }
                    Ok(json!("second run"))
                }
                "eager" => {
                    flow.add_under(&ctx.as_parent(), node(&qn, "late")).await?;
                    Ok(json!("too soon"))
                }
                _ => {
                    tokio::time::sleep(ms(50)).await;
                    Ok(json!("late done"))
                }
            }
        }
    });
    assert!(
        rec.wait_until("settled", secs(10), |s| s.location(again.as_str())
            == "completed"
            && s.location(eager.as_str()) == "failed"
            && s.len("completed") == 2)
            .await
    );
    w.stop().await;
    assert_eq!(*calls.lock(), 2);
    let eager_job = q.get(&eager).await.unwrap().unwrap();
    assert!(eager_job.failed_reason.unwrap().contains("still pending"));
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_failed_by_the_stall_limit_fails_its_parent() {
    let (q, b) = common::fresh_queue::<Value>("b13-stall").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_stall_fails_parent", &qn).await;
    let flow = FlowProducer::new(b.clone());
    let tree = flow
        .add(node(&qn, "parent").child(node(&qn, "doomed").fail_parent_on_failure()))
        .await
        .unwrap();
    b.fetch_next(&qn, "dead", ms(30), ms(10), &CancellationToken::new(), None)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(ms(60)).await;
    let moved = rec
        .step("scan with max_stalled = 0", b.scan_stalled(&qn, 0))
        .await
        .unwrap();
    assert!(moved.requeued.is_empty());
    assert_eq!(moved.failed.len(), 1, "{moved:?}");
    assert_eq!(moved.failed[0].id, tree.children[0].id);
    assert_eq!(moved.failed[0].name, "doomed");
    let s = rec.snapshot();
    assert_eq!(s.location(tree.children[0].id.as_str()), "failed");
    assert_eq!(s.location(tree.id.as_str()), "failed");
    assert!(
        s.jobs[tree.id.as_str()].fields["failedReason"]
            .ends_with("job stalled more than allowable limit")
    );
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn child_results_outlive_the_childs_own_retention() {
    let (q, b) = common::fresh_queue::<Value>("b13-retain").await;
    let qn = q.name().to_string();
    let mut rec = Recorder::new("b13_child_retention", &qn).await;
    let flow = FlowProducer::new(b.clone());
    let gone = JobOptions::new().remove_on_complete(Removal::Remove);
    let tree = flow
        .add(
            node_opts(
                &qn,
                "parent",
                JobOptions::new().remove_on_complete(Removal::KeepLast(1)),
            )
            .child(node_opts(&qn, "k1", gone.clone()))
            .child(node_opts(&qn, "k2", gone)),
        )
        .await
        .unwrap();
    let w = logging_worker(&q, Arc::default(), &[]);
    assert!(
        rec.wait_until("parent completed", secs(5), |s| s.len("completed") == 1)
            .await
    );
    w.stop().await;
    let s = rec.snapshot();
    assert_eq!(s.jobs.len(), 1, "children removed on completion");
    assert_eq!(
        s.jobs[tree.id.as_str()].processed.len(),
        2,
        "their values stay with the parent"
    );
    // A newer parent trims this one (KeepLast(1)): its flow keys go too.
    let next = flow
        .add(node_opts(
            &qn,
            "parent",
            JobOptions::new().remove_on_complete(Removal::KeepLast(1)),
        ))
        .await
        .unwrap();
    let w = logging_worker(&q, Arc::default(), &[]);
    assert!(
        rec.wait_until("old parent trimmed", secs(5), |s| s
            .location(next.id.as_str())
            == "completed"
            && s.jobs.len() == 1)
            .await
    );
    w.stop().await;
    assert!(rec.snapshot().orphan_aux.is_empty());
    rec.assert_clean();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn many_flows_under_many_workers_release_each_parent_exactly_once() {
    let (pq, b) = common::fresh_queue::<Value>("b13-stress").await;
    let cq: Queue<Value> = common::queue(b.clone(), &other_queue(&pq, "kids")).await;
    let mut rp = Recorder::new("b13_stress_parents", pq.name()).await;
    let mut rc = Recorder::new("b13_stress_kids", cq.name()).await;
    const FLOWS: usize = 150;
    const KIDS: usize = 6;
    let flow = FlowProducer::new(b.clone());
    let mut trees = Vec::new();
    for f in 0..FLOWS {
        let mut n = node(pq.name(), &format!("p{f}"));
        for k in 0..KIDS {
            n = n.child(node(cq.name(), &format!("p{f}-k{k}")));
        }
        trees.push(flow.add(n).await.unwrap());
    }
    let log: RunLog = Arc::default();
    let mut ws = Vec::new();
    for q in [&pq, &cq, &pq, &cq] {
        // separate backends, like separate processes
        let b2 = common::build_backend().await as Arc<dyn Backend>;
        let wq: Queue<Value> = Queue::from_backend(
            q.name(),
            b2,
            QueueOptions::default().prefix(common::TEST_PREFIX),
        )
        .unwrap();
        ws.push(logging_worker(&wq, log.clone(), &[]));
    }
    assert!(
        rp.wait_until("every parent completed", secs(30), |s| s.len("completed")
            == FLOWS)
            .await
    );
    for w in ws {
        w.stop().await;
    }
    rc.observe("children").await;
    assert_eq!(rc.snapshot().len("completed"), FLOWS * KIDS);
    for f in 0..FLOWS {
        let p = runs(&log, &format!("p{f}"));
        assert_eq!(p.len(), 1, "parent p{f} ran {} times", p.len());
        for k in 0..KIDS {
            let kid = runs(&log, &format!("p{f}-k{k}"));
            assert_eq!(kid.len(), 1);
            assert!(p[0].0 >= kid[0].1, "p{f} started before p{f}-k{k} finished");
        }
    }
    assert_eq!(
        rp.events_named("waiting")
            .iter()
            .filter(|e| e.fields.get("prev").map(String::as_str) == Some("waiting-children"))
            .count(),
        FLOWS
    );
    let _ = trees;
    rp.assert_clean();
    rc.assert_clean();
    common::teardown(&pq).await;
    common::teardown(&cq).await;
}
