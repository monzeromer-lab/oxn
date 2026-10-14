//! `oxn-load flow` — flow fan-out under load.
//!
//! Producers add `--flows` parents (queue `…-p`), each with `--children`
//! children (queue `…-c`), while workers on both queues run them. Every
//! handler run is recorded in a per-flow ledger, and the run fails unless,
//! for every flow:
//!
//! - **normal / retry**: the parent completed after exactly one run, which
//!   started after its last child finished and saw every child's value
//!   (retry: child 0 fails once, the harness retries it, the parent waits);
//! - **ignore**: child 0 fails with `ignore_dependency_on_failure`; the
//!   parent still runs once and sees `children − 1` values and 1 failure;
//! - **fail-parent**: child 0 fails with `fail_parent_on_failure`; the
//!   parent ends failed and never ran;
//! - **dynamic**: the parent adds its children at run time and returns
//!   `Error::WaitingChildren`; it runs again once they finish, then
//!   completes with every value;
//!
//! the queue invariants (including the flow rules) held at every check,
//! and both queues leave no key behind after cleanup.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use oxn::backend::{Backend, JobRange};
use oxn::error::ScriptError;
use oxn::flow::{FlowNode, FlowProducer};
use oxn::prelude::*;
use oxn_testkit::{Snapshot, Violation, invariants};
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::stats::{Hist, HistSummary};
use crate::workload::{PREFIX, connect, now_us};

#[derive(clap::Args, Debug, Clone, Serialize)]
pub struct FlowArgs {
    /// Redis URL (use the dedicated test Redis).
    #[arg(long, default_value = "redis://127.0.0.1:6390")]
    pub redis: String,
    /// Flows (parents) to add.
    #[arg(long, default_value_t = 2_000)]
    pub flows: u64,
    /// Children per parent.
    #[arg(long, default_value_t = 10)]
    pub children: u32,
    /// Concurrent producers.
    #[arg(long, default_value_t = 4)]
    pub producers: usize,
    /// Workers per queue (each with its own backend, like a process).
    #[arg(long, default_value_t = 4)]
    pub workers: usize,
    /// Concurrency per worker.
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
    /// Simulated work per child run, µs.
    #[arg(long, default_value_t = 0)]
    pub work_us: u64,
    /// Fraction of flows whose child 0 fails with `fail_parent_on_failure`.
    #[arg(long, default_value_t = 0.0)]
    pub fail_parent_rate: f64,
    /// Fraction of flows whose child 0 fails with `ignore_dependency_on_failure`.
    #[arg(long, default_value_t = 0.0)]
    pub ignore_rate: f64,
    /// Fraction of flows whose child 0 fails once (default mode) and is
    /// then retried by the harness.
    #[arg(long, default_value_t = 0.0)]
    pub retry_rate: f64,
    /// Fraction of flows whose parent adds its children at run time.
    #[arg(long, default_value_t = 0.0)]
    pub dynamic_rate: f64,
    /// Cancel and replace every worker this often.
    #[arg(long)]
    pub restart_every_s: Option<u64>,
    /// CLIENT KILL ~30% of connections this often.
    #[arg(long)]
    pub client_kill_every_s: Option<u64>,
    /// Snapshot + invariant check cadence (0 = off).
    #[arg(long, default_value_t = 3)]
    pub check_every_s: u64,
    /// Give up waiting for every flow to finish after this long.
    #[arg(long, default_value_t = 180)]
    pub timeout_s: u64,
    #[arg(long)]
    pub allow_shared_redis: bool,
    /// Keep the queues afterwards.
    #[arg(long)]
    pub keep: bool,
    #[arg(long)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Kind {
    Normal,
    FailParent,
    Ignore,
    Retry,
    Dynamic,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Normal => "normal",
            Kind::FailParent => "fail-parent",
            Kind::Ignore => "ignore",
            Kind::Retry => "retry",
            Kind::Dynamic => "dynamic",
        }
    }
    fn parse(s: &str) -> Kind {
        match s {
            "fail-parent" => Kind::FailParent,
            "ignore" => Kind::Ignore,
            "retry" => Kind::Retry,
            "dynamic" => Kind::Dynamic,
            _ => Kind::Normal,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Run {
    start_us: u64,
    end_us: Option<u64>,
    outcome: &'static str,
    values: usize,
    failures: usize,
}

#[derive(Debug, Clone, Serialize)]
struct FlowRec {
    kind: Kind,
    added: bool,
    parent: Vec<Run>,
    children: Vec<Vec<Run>>,
}

struct Ledger {
    flows: Vec<Mutex<FlowRec>>,
}

impl Ledger {
    fn begin(&self, f: usize, k: Option<usize>) -> usize {
        let mut r = self.flows[f].lock();
        let run = Run {
            start_us: now_us(),
            end_us: None,
            outcome: "running",
            values: 0,
            failures: 0,
        };
        let runs = match k {
            None => &mut r.parent,
            Some(k) => &mut r.children[k],
        };
        runs.push(run);
        runs.len() - 1
    }
    fn end(
        &self,
        f: usize,
        k: Option<usize>,
        i: usize,
        outcome: &'static str,
        values: usize,
        failures: usize,
    ) {
        let mut r = self.flows[f].lock();
        let runs = match k {
            None => &mut r.parent,
            Some(k) => &mut r.children[k],
        };
        let run = &mut runs[i];
        run.end_us = Some(now_us());
        run.outcome = outcome;
        run.values = values;
        run.failures = failures;
    }
    /// Child runs that already finished (any outcome) before this one.
    fn finished_child_runs(&self, f: usize, k: usize) -> usize {
        self.flows[f].lock().children[k]
            .iter()
            .filter(|r| r.end_us.is_some())
            .count()
    }
}

#[derive(Debug, Serialize)]
pub struct FlowOutcome {
    pub args: FlowArgs,
    pub parent_queue: String,
    pub child_queue: String,
    pub started_at: String,
    pub add_secs: f64,
    pub total_secs: f64,
    pub jobs_per_s: f64,
    pub flows_by_kind: BTreeMap<String, u64>,
    /// Parent's completing run start − its last child's end.
    pub release_latency: HistSummary,
    pub child_runs: u64,
    pub parent_runs: u64,
    pub interrupted_runs: u64,
    pub duplicate_child_successes: u64,
    pub add_errors: u64,
    pub violations: Vec<(f64, String, Violation)>,
    pub keys_left: u64,
    pub chaos: Vec<(f64, String)>,
    pub failures: Vec<String>,
    pub passed: bool,
}

fn kind_for(a: &FlowArgs, rng: &mut impl Rng) -> Kind {
    let x: f64 = rng.r#gen();
    let mut acc = 0.0;
    for (rate, kind) in [
        (a.fail_parent_rate, Kind::FailParent),
        (a.ignore_rate, Kind::Ignore),
        (a.retry_rate, Kind::Retry),
        (a.dynamic_rate, Kind::Dynamic),
    ] {
        acc += rate;
        if x < acc {
            return kind;
        }
    }
    Kind::Normal
}

fn child_node(cq: &str, f: usize, k: usize, kind: Kind) -> FlowNode {
    let n = FlowNode::new(
        cq,
        "child",
        &json!({ "f": f, "k": k, "kind": kind.as_str() }),
        JobOptions::new(),
    )
    .expect("json payload");
    match (kind, k) {
        (Kind::FailParent, 0) => n.fail_parent_on_failure(),
        (Kind::Ignore, 0) => n.ignore_dependency_on_failure(),
        _ => n,
    }
}

fn queue_on(b: Arc<dyn Backend>, name: &str) -> Queue<Value> {
    Queue::from_backend(name, b, QueueOptions::default().prefix(PREFIX)).expect("valid queue name")
}

#[derive(Clone)]
struct Ctx {
    ledger: Arc<Ledger>,
    children: usize,
    work_us: u64,
    child_queue: String,
}

async fn handle(
    c: Ctx,
    flow: FlowProducer,
    job: Job<Value>,
    ctx: JobContext,
) -> oxn::Result<Value> {
    let f = job.data["f"].as_u64().unwrap_or(0) as usize;
    let kind = Kind::parse(job.data["kind"].as_str().unwrap_or(""));
    if let Some(k) = job.data["k"].as_u64() {
        let k = k as usize;
        let earlier = c.ledger.finished_child_runs(f, k);
        let i = c.ledger.begin(f, Some(k));
        if c.work_us > 0 {
            tokio::time::sleep(Duration::from_micros(c.work_us)).await;
        }
        let fail = k == 0
            && match kind {
                Kind::FailParent | Kind::Ignore => true,
                Kind::Retry => earlier == 0,
                _ => false,
            };
        c.ledger
            .end(f, Some(k), i, if fail { "failed" } else { "ok" }, 0, 0);
        if fail {
            return Err(Error::unrecoverable(format!("planned failure f{f}/k{k}")));
        }
        return Ok(json!({ "f": f, "k": k }));
    }
    let i = c.ledger.begin(f, None);
    let deps = ctx.dependencies().await?;
    if kind == Kind::Dynamic {
        let none = deps.pending.is_empty() && deps.processed.is_empty() && deps.failed.is_empty();
        if none {
            for k in 0..c.children {
                flow.add_under(&ctx.as_parent(), child_node(&c.child_queue, f, k, kind))
                    .await?;
            }
        }
        if none || !deps.pending.is_empty() {
            c.ledger.end(f, None, i, "waiting", 0, 0);
            return Err(Error::WaitingChildren);
        }
    }
    let (values, failures) = (deps.processed.len(), deps.failed.len());
    c.ledger.end(f, None, i, "ok", values, failures);
    Ok(json!({ "f": f, "values": values, "failures": failures }))
}

struct Workers {
    running: Vec<(CancellationToken, tokio::task::JoinHandle<oxn::Result<()>>)>,
}

async fn start_workers(a: &FlowArgs, pq: &str, cq: &str, c: &Ctx) -> anyhow::Result<Workers> {
    let mut running = Vec::new();
    for _ in 0..a.workers {
        for q in [pq, cq] {
            let b = connect(&a.redis, a.concurrency + 4).await?;
            let flow = FlowProducer::new(b.clone());
            let c = c.clone();
            let w: Worker<Value, Value> =
                Worker::builder_with_ctx(queue_on(b, q), move |job, ctx| {
                    handle(c.clone(), flow.clone(), job, ctx)
                })
                .concurrency(a.concurrency)
                .drain_delay(Duration::from_millis(200))
                .shutdown_timeout(Duration::from_millis(500))
                .lock_duration(Duration::from_secs(5))
                .lock_renew_interval(Duration::from_millis(1500))
                .stalled_interval(Duration::from_millis(1000))
                .build();
            running.push((w.cancellation_token(), tokio::spawn(w.run())));
        }
    }
    Ok(Workers { running })
}

impl Workers {
    async fn stop(self) {
        for (c, _) in &self.running {
            c.cancel();
        }
        for (_, h) in self.running {
            let _ = h.await;
        }
    }
}

pub async fn run(a: FlowArgs) -> anyhow::Result<FlowOutcome> {
    let client = redis::Client::open(a.redis.as_str())?;
    let mut direct = redis::aio::ConnectionManager::new(client.clone()).await?;
    let foreign = crate::run::foreign_keys(&mut direct).await?;
    if foreign > 0 && !a.allow_shared_redis {
        anyhow::bail!(
            "{} holds {foreign} key(s) not created by oxn-load; use the dedicated Redis or --allow-shared-redis",
            a.redis
        );
    }
    let slug = format!(
        "flow{}",
        a.label
            .as_ref()
            .map(|l| format!("-{l}"))
            .unwrap_or_default()
    );
    let id = &crate::run::uuid_like()[..8];
    let pq = format!("{slug}-{id}-p");
    let cq = format!("{slug}-{id}-c");
    let started_at = crate::run::chrono_like_now();
    eprintln!(
        "oxn-load flow: {} flows × {} children, parents `{pq}`, children `{cq}`",
        a.flows, a.children
    );

    let mut rng = rand::rngs::StdRng::seed_from_u64(rand::random());
    let kinds: Vec<Kind> = (0..a.flows).map(|_| kind_for(&a, &mut rng)).collect();
    let ledger = Arc::new(Ledger {
        flows: kinds
            .iter()
            .map(|&kind| {
                Mutex::new(FlowRec {
                    kind,
                    added: false,
                    parent: Vec::new(),
                    children: vec![Vec::new(); a.children as usize],
                })
            })
            .collect(),
    });
    let ctx = Ctx {
        ledger: ledger.clone(),
        children: a.children as usize,
        work_us: a.work_us,
        child_queue: cq.clone(),
    };
    let t0 = Instant::now();
    let mut workers = start_workers(&a, &pq, &cq, &ctx).await?;

    // Invariant checks on both queues.
    let violations: Arc<Mutex<Vec<(f64, String, Violation)>>> = Arc::default();
    let stop = CancellationToken::new();
    let checker = (a.check_every_s > 0).then(|| {
        let (v, every, stop, client) = (
            violations.clone(),
            a.check_every_s,
            stop.clone(),
            client.clone(),
        );
        let qs = [pq.clone(), cq.clone()];
        tokio::spawn(async move {
            let Ok(mut c) = redis::aio::ConnectionManager::new(client).await else {
                return;
            };
            // Paced so checks cost at most ~10% of the time: a snapshot of a
            // big queue blocks Redis while it is taken.
            let mut wait = Duration::from_secs(every);
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(wait) => {} }
                let checking = std::time::Instant::now();
                for q in &qs {
                    if let Ok(s) = Snapshot::take(&mut c, PREFIX, q).await {
                        let t = t0.elapsed().as_secs_f64();
                        let mut v = v.lock();
                        for x in invariants::check(&s) {
                            if x.severity >= oxn_testkit::Severity::Error && v.len() < 200 {
                                eprintln!("  ✗ t={t:.1}s {q}: {x}");
                                v.push((t, q.clone(), x));
                            }
                        }
                    }
                }
                wait = Duration::from_secs(every).max(checking.elapsed() * 9);
            }
        })
    });

    // Retries child 0 of `retry` flows once it has failed.
    let retrier = {
        let (b, cq, stop, kinds) = (
            connect(&a.redis, 4).await?,
            cq.clone(),
            stop.clone(),
            kinds.clone(),
        );
        tokio::spawn(async move {
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
                // Page through *every* failed child: the permanently failed
                // ones (fail-parent / ignore flows) pile up at the front.
                let mut start = 0i64;
                loop {
                    let range = JobRange {
                        start,
                        end: start + 999,
                        descending: false,
                    };
                    let Ok(page) = b.list(&cq, JobState::Failed, range).await else {
                        break;
                    };
                    let n = page.len();
                    for j in page {
                        let v: Value = serde_json::from_str(&j.data).unwrap_or_default();
                        let f = v["f"].as_u64().unwrap_or(0) as usize;
                        if kinds.get(f) == Some(&Kind::Retry) {
                            let _ = b.retry(&cq, &j.id).await;
                        }
                    }
                    if n < 1000 {
                        break;
                    }
                    start += 1000;
                }
            }
        })
    };

    // Producers: explicit parent ids make an ambiguous add idempotent.
    let next = Arc::new(AtomicU64::new(0));
    let add_errors = Arc::new(AtomicU64::new(0));
    let mut producers = Vec::new();
    for _ in 0..a.producers {
        let b = connect(&a.redis, 4).await?;
        let flow = FlowProducer::new(b);
        let (next, add_errors, ledger, pq, cq, kinds) = (
            next.clone(),
            add_errors.clone(),
            ledger.clone(),
            pq.clone(),
            cq.clone(),
            kinds.clone(),
        );
        let (flows, children) = (a.flows, a.children as usize);
        producers.push(tokio::spawn(async move {
            loop {
                let f = next.fetch_add(1, Ordering::Relaxed);
                if f >= flows {
                    break;
                }
                let f = f as usize;
                let kind = kinds[f];
                loop {
                    let mut n = FlowNode::new(
                        &pq,
                        "parent",
                        &json!({ "f": f, "kind": kind.as_str() }),
                        JobOptions::new().id(format!("f{f}")).attempts(5),
                    )
                    .expect("json");
                    if kind != Kind::Dynamic {
                        for k in 0..children {
                            n = n.child(child_node(&cq, f, k, kind));
                        }
                    }
                    match flow.add(n).await {
                        Ok(_) | Err(Error::Script(ScriptError::JobExists)) => break,
                        Err(_) => {
                            add_errors.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
                ledger.flows[f].lock().added = true;
            }
        }));
    }

    // Monitor + chaos until every parent is finished.
    let mon = connect(&a.redis, 2).await?;
    let mut chaos = Vec::new();
    let mut last_restart = Instant::now();
    let mut last_kill = Instant::now();
    let mut add_secs = None;
    let deadline = t0 + Duration::from_secs(a.timeout_s);
    let mut done = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if add_secs.is_none() && producers.iter().all(|p| p.is_finished()) {
            add_secs = Some(t0.elapsed().as_secs_f64());
            eprintln!("  ⚑ all {} flows added", a.flows);
        }
        let (p, c) = (mon.counts(&pq).await, mon.counts(&cq).await);
        if let (Ok(p), Ok(c)) = (&p, &c) {
            eprintln!(
                "  {:>5.1}s  parents: wc {:>6} ready {:>6} active {:>4} done {:>6} failed {:>5} | children: ready {:>7} active {:>4} done {:>7} failed {:>5}",
                t0.elapsed().as_secs_f64(),
                p.waiting_children,
                p.waiting + p.prioritized + p.delayed,
                p.active,
                p.completed,
                p.failed,
                c.waiting + c.prioritized + c.delayed,
                c.active,
                c.completed,
                c.failed
            );
            if add_secs.is_some() && p.completed + p.failed == a.flows {
                done = true;
                break;
            }
        }
        if let Some(every) = a.restart_every_s
            && last_restart.elapsed() >= Duration::from_secs(every)
        {
            workers.stop().await;
            workers = start_workers(&a, &pq, &cq, &ctx).await?;
            last_restart = Instant::now();
            chaos.push((
                t0.elapsed().as_secs_f64(),
                "restarted every worker".to_string(),
            ));
            eprintln!("  ⚑ restarted every worker");
        }
        if let Some(every) = a.client_kill_every_s
            && last_kill.elapsed() >= Duration::from_secs(every)
        {
            let msg = crate::run::client_kill(&mut direct, 0.3).await;
            last_kill = Instant::now();
            eprintln!("  ⚑ {msg}");
            chaos.push((t0.elapsed().as_secs_f64(), msg));
        }
    }
    let total_secs = t0.elapsed().as_secs_f64();
    let phase = |what: &str| eprintln!("  ⚑ {:>6.1}s {what}", t0.elapsed().as_secs_f64());
    phase("stopping workers");
    workers.stop().await;
    stop.cancel();
    let _ = retrier.await;
    if let Some(c) = checker {
        let _ = c.await;
    }
    phase("final invariant check");
    // A final check on the settled state.
    for q in [&pq, &cq] {
        if let Ok(s) = Snapshot::take(&mut direct, PREFIX, q).await {
            for x in invariants::check(&s) {
                if x.severity >= oxn_testkit::Severity::Error {
                    violations.lock().push((total_secs, q.clone(), x));
                }
            }
        }
    }

    phase("verifying every flow");
    // ---- verify --------------------------------------------------------
    let mut failures = Vec::new();
    if !done {
        failures.push(format!("not every flow finished within {}s", a.timeout_s));
    }
    let pqueue = queue_on(mon.clone(), &pq);
    let release = Hist::default();
    let (mut child_runs, mut parent_runs, mut interrupted, mut dup_ok) = (0u64, 0u64, 0u64, 0u64);
    let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();
    let mut bad: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    let kids = a.children as usize;
    let chaos_on = a.restart_every_s.is_some() || a.client_kill_every_s.is_some();
    for (f, rec) in ledger.flows.iter().enumerate() {
        let r = rec.lock().clone();
        *by_kind.entry(r.kind.as_str().into()).or_default() += 1;
        let mut flag = |what: &'static str, why: String| {
            let v = bad.entry(what).or_default();
            if v.len() < 10 {
                v.push(format!("f{f} ({}): {why}", r.kind.as_str()));
            }
        };
        if !r.added {
            flag("not-added", "producer never added it".into());
            continue;
        }
        for runs in r.parent.iter().chain(r.children.iter().flatten()) {
            if runs.end_us.is_none() {
                interrupted += 1;
            }
        }
        parent_runs += r.parent.len() as u64;
        child_runs += r.children.iter().map(Vec::len).sum::<usize>() as u64;
        let state = mon
            .get(&pq, &JobId::new(format!("f{f}")))
            .await
            .ok()
            .flatten()
            .map(|j| {
                (
                    j.failed_reason.clone(),
                    j.state == Some(JobState::Completed),
                )
            });
        let last_child_end = r
            .children
            .iter()
            .map(|runs| {
                runs.iter()
                    .filter_map(|x| x.end_us)
                    .max()
                    .unwrap_or(u64::MAX)
            })
            .max()
            .unwrap_or(0);
        for (k, runs) in r.children.iter().enumerate() {
            let oks = runs.iter().filter(|x| x.outcome == "ok").count();
            let must_ok = !(k == 0 && matches!(r.kind, Kind::FailParent | Kind::Ignore));
            if must_ok && oks == 0 {
                flag("child-never-completed", format!("child {k}: {runs:?}"));
            }
            if oks > 1 {
                dup_ok += 1;
            }
        }
        let oks: Vec<&Run> = r.parent.iter().filter(|x| x.outcome == "ok").collect();
        match r.kind {
            Kind::FailParent => {
                if !r.parent.is_empty() {
                    flag("failed-parent-ran", format!("{:?}", r.parent));
                }
                match &state {
                    Some((Some(reason), _)) if reason.starts_with("child ") => {}
                    other => flag("parent-not-failed", format!("{other:?}")),
                }
            }
            _ => {
                if oks.len() != 1 {
                    flag(
                        "parent-ok-runs",
                        format!("{} ok runs: {:?}", oks.len(), r.parent),
                    );
                    continue;
                }
                let ok = oks[0];
                let (want_v, want_f) = if r.kind == Kind::Ignore {
                    (kids - 1, 1)
                } else {
                    (kids, 0)
                };
                if (ok.values, ok.failures) != (want_v, want_f) {
                    flag(
                        "parent-saw-wrong-children",
                        format!(
                            "saw {} values / {} failures, want {want_v}/{want_f}",
                            ok.values, ok.failures
                        ),
                    );
                }
                if ok.start_us < last_child_end {
                    flag(
                        "parent-ran-early",
                        format!(
                            "started {}µs before its last child finished",
                            last_child_end - ok.start_us
                        ),
                    );
                } else {
                    release.record(ok.start_us - last_child_end);
                }
                if r.kind == Kind::Dynamic && !r.parent.iter().any(|x| x.outcome == "waiting") {
                    flag("dynamic-never-waited", format!("{:?}", r.parent));
                }
                if r.kind != Kind::Dynamic && r.parent.iter().any(|x| x.outcome == "waiting") {
                    flag("static-parent-waited", format!("{:?}", r.parent));
                }
                if !matches!(state, Some((None, true))) {
                    flag("parent-not-completed", format!("{state:?}"));
                }
            }
        }
    }
    for (what, examples) in &bad {
        failures.push(format!("{what}: {}", examples.join("; ")));
    }
    if dup_ok > 0 && !chaos_on {
        failures.push(format!(
            "{dup_ok} child(ren) completed more than once without chaos"
        ));
    }
    let v = violations.lock().clone();
    if !v.is_empty() {
        failures.push(format!(
            "{} invariant violation(s); first: {} {}",
            v.len(),
            v[0].1,
            v[0].2
        ));
    }

    phase("cleanup");
    let keys_left = if a.keep {
        0
    } else {
        let _ = pqueue.obliterate(true).await;
        let _ = queue_on(mon.clone(), &cq).obliterate(true).await;
        let mut n = 0;
        for q in [&pq, &cq] {
            n += crate::run::count_keys(&mut direct, &format!("{PREFIX}:{q}:")).await;
        }
        n
    };
    if keys_left > 0 {
        failures.push(format!(
            "{keys_left} key(s) left after obliterating both queues"
        ));
    }
    phase("done");
    let jobs = child_runs + parent_runs;
    let outcome = FlowOutcome {
        args: a.clone(),
        parent_queue: pq,
        child_queue: cq,
        started_at,
        add_secs: add_secs.unwrap_or(total_secs),
        total_secs,
        jobs_per_s: jobs as f64 / total_secs.max(0.001),
        flows_by_kind: by_kind,
        release_latency: release.summary(),
        child_runs,
        parent_runs,
        interrupted_runs: interrupted,
        duplicate_child_successes: dup_ok,
        add_errors: add_errors.load(Ordering::Relaxed),
        violations: v,
        keys_left,
        chaos,
        passed: failures.is_empty(),
        failures,
    };
    Ok(outcome)
}

pub fn write(o: &FlowOutcome) -> anyhow::Result<std::path::PathBuf> {
    use std::fmt::Write as _;
    let dir = oxn_testkit::report::report_root().join("load");
    std::fs::create_dir_all(&dir)?;
    let stem = format!(
        "flow-{}-{}",
        o.args.label.as_deref().unwrap_or("run"),
        o.started_at
    );
    let mut s = String::new();
    let _ = writeln!(
        s,
        "# oxn-load flow — {}\n",
        if o.passed { "PASS" } else { "FAIL" }
    );
    let _ = writeln!(
        s,
        "{} flows × {} children · {} producers · {} workers/queue × {} · added in {:.1}s, finished in {:.1}s · **{:.0} handler runs/s**\n",
        o.args.flows,
        o.args.children,
        o.args.producers,
        o.args.workers,
        o.args.concurrency,
        o.add_secs,
        o.total_secs,
        o.jobs_per_s
    );
    let _ = writeln!(s, "- flows by kind: {:?}", o.flows_by_kind);
    let _ = writeln!(
        s,
        "- handler runs: {} parent, {} child; {} interrupted (chaos); {} duplicate child successes",
        o.parent_runs, o.child_runs, o.interrupted_runs, o.duplicate_child_successes
    );
    let _ = writeln!(s, "- add errors (retried idempotently): {}", o.add_errors);
    let h = &o.release_latency;
    let _ = writeln!(
        s,
        "- parent release latency (last child end → parent start): p50 {:.2}ms · p90 {:.2}ms · p99 {:.2}ms · max {:.2}ms ({} parents)",
        h.p50_ms, h.p90_ms, h.p99_ms, h.max_ms, h.count
    );
    let _ = writeln!(s, "- invariant violations: {}", o.violations.len());
    let _ = writeln!(s, "- keys left after cleanup: {}\n", o.keys_left);
    if !o.chaos.is_empty() {
        let _ = writeln!(s, "## Chaos\n");
        for (t, m) in &o.chaos {
            let _ = writeln!(s, "- t={t:.1}s {m}");
        }
        let _ = writeln!(s);
    }
    if !o.failures.is_empty() {
        let _ = writeln!(s, "## Failures\n");
        for f in &o.failures {
            let _ = writeln!(s, "- {f}");
        }
    }
    let md = dir.join(format!("{stem}.md"));
    std::fs::write(&md, s)?;
    std::fs::write(
        dir.join(format!("{stem}.json")),
        serde_json::to_string_pretty(o)?,
    )?;
    Ok(md)
}
