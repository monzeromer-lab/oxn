//! `oxn-load schedule` — many instances of many schedulers.
//!
//! `--instances` processes (each its own backend) all run the same
//! `--schedulers` schedulers (`every --every-ms`) while workers run the
//! ticks with `remove_on_complete = Remove` — the retention that would let
//! a lagging instance re-create a tick whose job is already gone. The run
//! fails unless, for every scheduler:
//!
//! - every tick ran exactly once (no duplicate, without chaos);
//! - no tick between its first and last is missing;
//! - removing the schedulers mid-run stops every instance and leaves no
//!   pending tick or tick index behind;
//!
//! the invariants held at every check, and cleanup leaves no key.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxn::backend::Backend;
use oxn::options::Removal;
use oxn::prelude::*;
use oxn::scheduler::{JobScheduler, RepeatOptions};
use oxn_testkit::{Snapshot, Violation, invariants};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::stats::{Hist, HistSummary};
use crate::workload::{PREFIX, connect};

#[derive(clap::Args, Debug, Clone, Serialize)]
pub struct SchedArgs {
    #[arg(long, default_value = "redis://127.0.0.1:6390")]
    pub redis: String,
    /// Distinct schedulers.
    #[arg(long, default_value_t = 50)]
    pub schedulers: usize,
    /// Instances ("processes") each running every scheduler.
    #[arg(long, default_value_t = 10)]
    pub instances: usize,
    /// Tick interval, ms.
    #[arg(long, default_value_t = 200)]
    pub every_ms: u64,
    /// How long the schedulers run before being removed.
    #[arg(long, default_value_t = 20)]
    pub duration_s: u64,
    #[arg(long, default_value_t = 4)]
    pub workers: usize,
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    /// Cancel and restart half the instances this often (others stay up).
    #[arg(long)]
    pub restart_every_s: Option<u64>,
    /// CLIENT KILL ~30% of connections this often.
    #[arg(long)]
    pub client_kill_every_s: Option<u64>,
    #[arg(long, default_value_t = 3)]
    pub check_every_s: u64,
    #[arg(long)]
    pub allow_shared_redis: bool,
    #[arg(long)]
    pub label: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SchedOutcome {
    pub args: SchedArgs,
    pub queue: String,
    pub started_at: String,
    pub ticks_run: u64,
    pub ticks_expected_min: u64,
    pub duplicate_runs: u64,
    pub missing_ticks: u64,
    /// Handler start − tick fire time.
    pub lateness: HistSummary,
    pub instances_stopped_after_remove_ms: f64,
    pub pending_after_remove: u64,
    pub violations: Vec<(f64, Violation)>,
    pub keys_left: u64,
    pub chaos: Vec<(f64, String)>,
    pub failures: Vec<String>,
    pub passed: bool,
}

type Instance = Vec<(CancellationToken, tokio::task::JoinHandle<oxn::Result<()>>)>;

async fn start_instance(a: &SchedArgs, queue: &str) -> anyhow::Result<Instance> {
    let b = connect(&a.redis, 8).await?;
    let q: Queue<u64> = Queue::from_backend(queue, b, QueueOptions::default().prefix(PREFIX))?;
    let mut tasks = Vec::new();
    for s in 0..a.schedulers {
        let sched = JobScheduler::new(
            q.clone(),
            format!("s{s}"),
            RepeatOptions::every(Duration::from_millis(a.every_ms)),
            s as u64,
        )
        .with_options(JobOptions::new().remove_on_complete(Removal::Remove));
        let c = CancellationToken::new();
        tasks.push((c.clone(), tokio::spawn(sched.run(c))));
    }
    Ok(tasks)
}

async fn stop_instance(i: Instance) {
    for (c, _) in &i {
        c.cancel();
    }
    for (_, h) in i {
        let _ = h.await;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn run(a: SchedArgs) -> anyhow::Result<SchedOutcome> {
    let client = redis::Client::open(a.redis.as_str())?;
    let mut direct = redis::aio::ConnectionManager::new(client.clone()).await?;
    let foreign = crate::run::foreign_keys(&mut direct).await?;
    if foreign > 0 && !a.allow_shared_redis {
        anyhow::bail!(
            "{} holds {foreign} foreign key(s); use the dedicated Redis",
            a.redis
        );
    }
    let queue = format!(
        "sched{}-{}",
        a.label
            .as_ref()
            .map(|l| format!("-{l}"))
            .unwrap_or_default(),
        &crate::run::uuid_like()[..8]
    );
    let started_at = crate::run::chrono_like_now();
    eprintln!(
        "oxn-load schedule: {} schedulers × {} instances, every {}ms, queue `{queue}`",
        a.schedulers, a.instances, a.every_ms
    );
    let t0 = Instant::now();

    // Workers record (job id, start ms).
    let ran: Arc<Mutex<Vec<(String, i64)>>> = Arc::default();
    let mut workers = Vec::new();
    for _ in 0..a.workers {
        let b = connect(&a.redis, a.concurrency + 4).await?;
        let q: Queue<u64> = Queue::from_backend(&queue, b, QueueOptions::default().prefix(PREFIX))?;
        let ran = ran.clone();
        let w = Worker::builder(q, move |j: Job<u64>| {
            let ran = ran.clone();
            async move {
                ran.lock().push((j.id.to_string(), now_ms()));
                Ok::<_, Error>(())
            }
        })
        .concurrency(a.concurrency)
        .build();
        workers.push((w.cancellation_token(), tokio::spawn(w.run())));
    }

    let violations: Arc<Mutex<Vec<(f64, Violation)>>> = Arc::default();
    let stop = CancellationToken::new();
    let checker = (a.check_every_s > 0).then(|| {
        let (v, every, stop, client, q) = (
            violations.clone(),
            a.check_every_s,
            stop.clone(),
            client.clone(),
            queue.clone(),
        );
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
                if let Ok(s) = Snapshot::take(&mut c, PREFIX, &q).await {
                    for x in invariants::check(&s) {
                        if x.severity >= oxn_testkit::Severity::Error && v.lock().len() < 200 {
                            eprintln!("  ✗ {x}");
                            v.lock().push((t0.elapsed().as_secs_f64(), x));
                        }
                    }
                }
                wait = Duration::from_secs(every).max(checking.elapsed() * 9);
            }
        })
    });

    let mut instances = Vec::new();
    for _ in 0..a.instances {
        instances.push(start_instance(&a, &queue).await?);
    }
    let mon = connect(&a.redis, 2).await?;
    let first_fire = now_ms();
    let mut chaos = Vec::new();
    let (mut last_restart, mut last_kill) = (Instant::now(), Instant::now());
    let run_for = Duration::from_secs(a.duration_s);
    while t0.elapsed() < run_for {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Ok(c) = mon.counts(&queue).await {
            eprintln!(
                "  {:>5.1}s  ticks run {:>7} · pending: delayed {:>5} wait {:>5} active {:>4}",
                t0.elapsed().as_secs_f64(),
                ran.lock().len(),
                c.delayed,
                c.waiting,
                c.active
            );
        }
        if let Some(every) = a.restart_every_s
            && last_restart.elapsed() >= Duration::from_secs(every)
        {
            let mut idx: Vec<usize> = (0..instances.len()).collect();
            idx.shuffle(&mut rand::thread_rng());
            let half = &idx[..instances.len() / 2];
            for &i in half {
                let old = std::mem::take(&mut instances[i]);
                stop_instance(old).await;
                instances[i] = start_instance(&a, &queue).await?;
            }
            last_restart = Instant::now();
            let msg = format!("restarted {} of {} instances", half.len(), a.instances);
            eprintln!("  ⚑ {msg}");
            chaos.push((t0.elapsed().as_secs_f64(), msg));
        }
        if let Some(every) = a.client_kill_every_s
            && last_kill.elapsed() >= Duration::from_secs(every)
        {
            let msg = crate::run::client_kill(&mut direct, 0.3).await;
            eprintln!("  ⚑ {msg}");
            chaos.push((t0.elapsed().as_secs_f64(), msg));
            last_kill = Instant::now();
        }
    }

    // Remove every scheduler while all instances still run: each loop must
    // stop by itself.
    let listed = mon.schedulers(&queue).await?.len();
    let removed_at = Instant::now();
    let last_fire = now_ms();
    for s in 0..a.schedulers {
        let _ = mon.remove_scheduler(&queue, &format!("s{s}")).await?;
    }
    let mut stopped_ms = f64::NAN;
    let deadline = Instant::now() + Duration::from_millis(a.every_ms * 3 + 2000);
    while Instant::now() < deadline {
        if instances.iter().flatten().all(|(_, h)| h.is_finished()) {
            stopped_ms = removed_at.elapsed().as_secs_f64() * 1000.0;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let still_running = instances
        .iter()
        .flatten()
        .filter(|(_, h)| !h.is_finished())
        .count();
    for i in instances {
        stop_instance(i).await;
    }
    // Ticks that were already running finish; give workers a moment.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let c = mon.counts(&queue).await?;
    let pending_after_remove = c.waiting + c.delayed + c.prioritized + c.paused;
    let snap = Snapshot::take(&mut direct, PREFIX, &queue).await?;
    let leftover_index = snap.scheduler_ticks.len();
    for (_, h) in workers.iter() {
        let _ = h;
    }
    for (c, _) in &workers {
        c.cancel();
    }
    for (_, h) in workers {
        let _ = h.await;
    }
    stop.cancel();
    if let Some(c) = checker {
        let _ = c.await;
    }
    for x in invariants::check(&snap) {
        if x.severity >= oxn_testkit::Severity::Error {
            violations.lock().push((t0.elapsed().as_secs_f64(), x));
        }
    }

    // ---- verify --------------------------------------------------------
    let chaos_on = a.restart_every_s.is_some() || a.client_kill_every_s.is_some();
    let mut failures = Vec::new();
    let ran = ran.lock().clone();
    let lateness = Hist::default();
    let mut runs: BTreeMap<String, usize> = BTreeMap::new();
    let mut ticks: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for (id, started) in &ran {
        *runs.entry(id.clone()).or_default() += 1;
        let Some(rest) = id.strip_prefix("sched-") else {
            continue;
        };
        let Some((sid, t)) = rest.rsplit_once('-') else {
            continue;
        };
        let Ok(t) = t.parse::<i64>() else { continue };
        if runs[id] == 1 {
            lateness.record(((started - t).max(0) as u64) * 1000);
        }
        ticks.entry(sid.to_string()).or_default().insert(t);
    }
    let duplicate_runs: u64 = runs.values().map(|&n| (n - 1) as u64).sum();
    if duplicate_runs > 0 && !chaos_on {
        failures.push(format!("{duplicate_runs} tick(s) ran more than once"));
    }
    let every = a.every_ms as i64;
    let mut missing = 0u64;
    let mut missing_examples = Vec::new();
    for s in 0..a.schedulers {
        let sid = format!("s{s}");
        let Some(ts) = ticks.get(&sid) else {
            failures.push(format!("scheduler {sid} never fired"));
            continue;
        };
        let (lo, hi) = (*ts.first().unwrap_or(&0), *ts.last().unwrap_or(&0));
        let mut t = lo;
        while t <= hi {
            if !ts.contains(&t) {
                missing += 1;
                if missing_examples.len() < 5 {
                    missing_examples.push(format!("{sid}@{t}"));
                }
            }
            t += every;
        }
        if ts.iter().any(|t| t % every != 0) {
            failures.push(format!("scheduler {sid}: a tick off the {every}ms grid"));
        }
    }
    if missing > 0 {
        failures.push(format!(
            "{missing} tick(s) missing, e.g. {}",
            missing_examples.join(", ")
        ));
    }
    let expected_min = ((last_fire - first_fire) / every - 2).max(0) as u64 * a.schedulers as u64;
    let unique = runs.len() as u64;
    if unique < expected_min {
        failures.push(format!(
            "only {unique} ticks ran, expected ≥ {expected_min}"
        ));
    }
    if listed != a.schedulers {
        failures.push(format!(
            "schedulers() listed {listed}, expected {}",
            a.schedulers
        ));
    }
    if still_running > 0 {
        failures.push(format!(
            "{still_running} scheduler loop(s) still running after removal"
        ));
    }
    if pending_after_remove > 0 {
        failures.push(format!(
            "{pending_after_remove} tick(s) still pending after removal"
        ));
    }
    if leftover_index > 0 {
        failures.push(format!(
            "{leftover_index} tick index(es) left after removal"
        ));
    }
    let v = violations.lock().clone();
    if !v.is_empty() {
        failures.push(format!(
            "{} invariant violation(s); first: {}",
            v.len(),
            v[0].1
        ));
    }
    let q: Queue<u64> =
        Queue::from_backend(&queue, mon.clone(), QueueOptions::default().prefix(PREFIX))?;
    let _ = q.obliterate(true).await;
    let keys_left = crate::run::count_keys(&mut direct, &format!("{PREFIX}:{queue}:")).await;
    if keys_left > 0 {
        failures.push(format!("{keys_left} key(s) left after obliterate"));
    }
    let _ = Arc::clone(&mon) as Arc<dyn Backend>;
    Ok(SchedOutcome {
        args: a,
        queue,
        started_at,
        ticks_run: unique,
        ticks_expected_min: expected_min,
        duplicate_runs,
        missing_ticks: missing,
        lateness: lateness.summary(),
        instances_stopped_after_remove_ms: stopped_ms,
        pending_after_remove,
        violations: v,
        keys_left,
        chaos,
        passed: failures.is_empty(),
        failures,
    })
}

pub fn write(o: &SchedOutcome) -> anyhow::Result<std::path::PathBuf> {
    use std::fmt::Write as _;
    let dir = oxn_testkit::report::report_root().join("load");
    std::fs::create_dir_all(&dir)?;
    let stem = format!(
        "schedule-{}-{}",
        o.args.label.as_deref().unwrap_or("run"),
        o.started_at
    );
    let mut s = String::new();
    let _ = writeln!(
        s,
        "# oxn-load schedule — {}\n",
        if o.passed { "PASS" } else { "FAIL" }
    );
    let _ = writeln!(
        s,
        "{} schedulers × {} instances, every {}ms, {}s · {} workers × {}\n",
        o.args.schedulers,
        o.args.instances,
        o.args.every_ms,
        o.args.duration_s,
        o.args.workers,
        o.args.concurrency
    );
    let _ = writeln!(
        s,
        "- ticks run: {} (expected ≥ {}), duplicate runs: {}, missing ticks: {}",
        o.ticks_run, o.ticks_expected_min, o.duplicate_runs, o.missing_ticks
    );
    let h = &o.lateness;
    let _ = writeln!(
        s,
        "- lateness (start − fire time): p50 {:.1}ms · p90 {:.1}ms · p99 {:.1}ms · max {:.1}ms",
        h.p50_ms, h.p90_ms, h.p99_ms, h.max_ms
    );
    let _ = writeln!(
        s,
        "- every instance stopped {:.0}ms after the schedulers were removed; {} tick(s) left pending",
        o.instances_stopped_after_remove_ms, o.pending_after_remove
    );
    let _ = writeln!(
        s,
        "- invariant violations: {} · keys left: {}\n",
        o.violations.len(),
        o.keys_left
    );
    for (t, m) in &o.chaos {
        let _ = writeln!(s, "- chaos t={t:.1}s {m}");
    }
    if !o.failures.is_empty() {
        let _ = writeln!(s, "\n## Failures\n");
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
