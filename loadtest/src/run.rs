//! One load run, start to finish.
//!
//! 1. **Safety** — refuse to touch a Redis that holds other software's
//!    keys (unless `--allow-shared-redis`), and never fire server-wide chaos
//!    at port 6379 without it.
//! 2. **Setup** — optional chaos proxy, stream observer, workers, monitor,
//!    periodic invariant checks, chaos scheduler.
//! 3. **Load** — producers run until the job budget / duration is spent.
//! 4. **Drain** — wait until the ledger says every job is terminal.
//! 5. **Verify** — ledger anomalies, queue sizes, retention expectations,
//!    leak estimate, a final full invariant snapshot.
//! 6. **Report** — `target/oxn-reports/load/<scenario>-<ts>.{md,json}`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use oxn_testkit::invariants::{self, Severity, Violation};
use oxn_testkit::snapshot::Snapshot;
use parking_lot::Mutex;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::chaos::{ChaosAction, Proxy, split_redis_url};
use crate::config::{Config, Retention};
use crate::ledger::{Ledger, Verdict};
use crate::stats::{self, HistSummary, RedisHealth, Row, Stats};
use crate::workload::{self, InProcPool, PREFIX, ProcPool};

/// Everything a run produced, serialized into the JSON report.
#[derive(Debug, Serialize)]
pub struct Outcome {
    pub config: Config,
    pub queue: String,
    pub started_at: String,
    pub load_secs: f64,
    pub drain_secs: f64,
    pub drained: bool,
    pub added: u64,
    pub add_errors: u64,
    pub throughput_ok_per_s: f64,
    pub pickup: HistSummary,
    pub run: HistSummary,
    pub e2e: HistSummary,
    pub add_latency: HistSummary,
    pub verdict: Verdict,
    pub stream_events: BTreeMap<String, u64>,
    pub final_sizes: BTreeMap<String, u64>,
    pub retention_findings: Vec<String>,
    pub leak: LeakReport,
    pub violations: Vec<(f64, Violation)>,
    pub chaos_log: Vec<(f64, String)>,
    pub health_before: RedisHealth,
    pub health_after_drain: RedisHealth,
    pub health_after_cleanup: Option<RedisHealth>,
    pub rows: Vec<Row>,
    pub kills: u32,
    /// Host `/proc/loadavg` (1/5/15 min) at start and end — throughput and
    /// latency numbers are only comparable between quiet runs.
    pub loadavg_start: Option<String>,
    pub loadavg_end: Option<String>,
    pub passed: bool,
    pub failures: Vec<String>,
}

/// Keys left behind relative to what retention says should remain.
#[derive(Debug, Default, Serialize)]
pub struct LeakReport {
    pub snapshot_taken: bool,
    pub job_hashes: u64,
    pub retained_entries: u64,
    pub orphan_hashes: u64,
    pub orphan_aux_keys: u64,
    pub total_queue_keys: u64,
    pub note: Option<String>,
}

pub async fn run(cfg: Config) -> anyhow::Result<Outcome> {
    let cfg = Arc::new(cfg);
    let client = redis::Client::open(cfg.redis.as_str())?;
    // Auto-reconnecting: the harness must keep observing across Redis
    // restarts and CLIENT KILLs, or it would report zeros as truth.
    let mut direct = redis::aio::ConnectionManager::new(client.clone()).await?;

    // ---- 1. safety -------------------------------------------------------
    let foreign = foreign_keys(&mut direct).await?;
    if foreign > 0 && !cfg.allow_shared_redis {
        anyhow::bail!(
            "{} holds {foreign} key(s) not created by oxn-load (prefix `{PREFIX}:`). \
             Refusing to load-test a Redis other software uses. Start the dedicated one with \
             `scripts/test-redis.sh start` or pass --allow-shared-redis.",
            cfg.redis
        );
    }
    if cfg.redis.contains(":6379")
        && cfg.chaos.iter().any(|(_, a)| a.is_server_wide())
        && !cfg.allow_shared_redis
    {
        anyhow::bail!(
            "server-wide chaos (client-kill / script-flush / redis-restart) against :6379 needs --allow-shared-redis"
        );
    }

    // ---- 2. setup --------------------------------------------------------
    let run_id = &uuid_like()[..8];
    let queue = format!("{}-{run_id}", cfg.slug());
    let base = format!("{PREFIX}:{queue}:");
    let seed: u64 = rand::random();
    let started_at = chrono_like_now();
    eprintln!(
        "oxn-load: scenario={} queue={queue} redis={}",
        cfg.slug(),
        cfg.redis
    );

    let proxy = if cfg.chaos.iter().any(|(_, a)| a.needs_proxy()) {
        let (upstream, rewrite) = split_redis_url(&cfg.redis)?;
        let p = Proxy::start(upstream).await?;
        let url = rewrite(&p.listen_addr);
        eprintln!(
            "oxn-load: oxn traffic routed through chaos proxy {}",
            p.listen_addr
        );
        Some((p, url))
    } else {
        None
    };
    let oxn_url = proxy
        .as_ref()
        .map(|(_, u)| u.clone())
        .unwrap_or_else(|| cfg.redis.clone());

    let health_before = stats::redis_health(&mut direct).await;
    let loadavg_start = loadavg();
    let history_cap = if cfg.jobs > 200_000 || cfg.duration.is_some() {
        6
    } else {
        24
    };
    let ledger = Arc::new(Ledger::new(history_cap));
    let stats = Arc::new(Stats::default());
    let stop_observer = CancellationToken::new();
    let stream_counts = Arc::new(Mutex::new(BTreeMap::new()));
    let observer = tokio::spawn(workload::observe_stream(
        cfg.redis.clone(),
        base.clone(),
        ledger.clone(),
        stream_counts.clone(),
        stop_observer.clone(),
    ));

    let mut pool = if cfg.processes {
        let mut p = ProcPool::new(
            cfg.clone(),
            oxn_url.clone(),
            queue.clone(),
            ledger.clone(),
            stats.clone(),
        );
        p.start()?;
        Pool::Proc(p)
    } else {
        let mut p = InProcPool::new(
            cfg.clone(),
            oxn_url.clone(),
            queue.clone(),
            ledger.clone(),
            stats.clone(),
        );
        p.start().await?;
        Pool::InProc(p)
    };

    let started = Instant::now();
    let rows = Arc::new(Mutex::new(Vec::new()));
    let stop_monitor = CancellationToken::new();
    let monitor = tokio::spawn(stats::monitor(
        stats.clone(),
        redis::aio::ConnectionManager::new(client.clone()).await?,
        base.clone(),
        started,
        rows.clone(),
        stop_monitor.clone(),
    ));

    let violations: Arc<Mutex<Vec<(f64, Violation)>>> = Arc::new(Mutex::new(Vec::new()));
    let checker = (cfg.check_every_s > 0).then(|| {
        let (v, every, q) = (violations.clone(), cfg.check_every_s, queue.clone());
        let stop = stop_monitor.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let Ok(mut c) = redis::aio::ConnectionManager::new(client).await else {
                return;
            };
            let mut tick = tokio::time::interval(Duration::from_secs(every));
            tick.tick().await;
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = tick.tick() => {} }
                if let Ok(s) = Snapshot::take(&mut c, PREFIX, &q).await {
                    record_violations(&v, started.elapsed().as_secs_f64(), invariants::check(&s));
                }
            }
        })
    });

    // Producers.
    let backend = workload::connect(&oxn_url, cfg.producers * 2 + 2).await?;
    let pq = workload::queue_on(backend.clone(), &queue, workload::queue_defaults(&cfg));
    let next = Arc::new(AtomicU64::new(0));
    let stop_producers = CancellationToken::new();
    let mut producers = Vec::new();
    for _ in 0..cfg.producers {
        producers.push(tokio::spawn(workload::producer(
            cfg.clone(),
            pq.clone(),
            next.clone(),
            ledger.clone(),
            stats.clone(),
            seed,
            started,
            stop_producers.clone(),
        )));
    }

    // ---- 3. load + chaos -------------------------------------------------
    let mut chaos_log: Vec<(f64, String)> = Vec::new();
    let mut schedule = cfg.chaos.clone().into_iter().peekable();
    let mut producers_done = false;
    while !producers_done || schedule.peek().is_some() {
        let next_chaos = schedule.peek().map(|(t, _)| *t);
        let wait = match next_chaos {
            Some(t) => Duration::from_millis(t).saturating_sub(started.elapsed()),
            None => Duration::from_secs(3600),
        };
        tokio::select! {
            _ = tokio::time::sleep(wait), if next_chaos.is_some() => {
                let (_, action) = schedule.next().expect("peeked");
                let t = started.elapsed().as_secs_f64();
                let msg = fire(&action, &mut pool, proxy.as_ref().map(|(p, _)| p), &cfg, &mut direct).await;
                stats.note(format!("chaos {action:?}: {msg}"));
                chaos_log.push((t, format!("{action:?} → {msg}")));
            }
            _ = futures::future::join_all(producers.iter_mut()), if !producers_done => {
                producers_done = true;
                stats.note("producers finished");
            }
        }
    }
    let load_secs = started.elapsed().as_secs_f64();

    // ---- 4. drain --------------------------------------------------------
    let drain_start = Instant::now();
    let mut drained = false;
    while drain_start.elapsed() < cfg.drain_timeout {
        if ledger.pending(cfg.attempts) == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let drain_secs = drain_start.elapsed().as_secs_f64();
    stats.note(if drained {
        "drained".to_string()
    } else {
        format!("drain TIMED OUT, {} pending", ledger.pending(cfg.attempts))
    });
    // Let completion events land in the stream and the observer.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let kills = match &mut pool {
        Pool::Proc(p) => {
            let k = p.kills;
            p.stop().await;
            k
        }
        Pool::InProc(p) => {
            p.stop().await;
            0
        }
    };
    stop_observer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), observer).await;
    stop_monitor.cancel();
    let _ = monitor.await;
    if let Some(c) = checker {
        let _ = c.await;
    }

    // ---- 5. verify -------------------------------------------------------
    let health_after_drain = stats::redis_health(&mut direct).await;
    let mut sizes = None;
    for _ in 0..10 {
        match stats::queue_sizes(&mut direct, &base).await {
            Ok(s) => {
                sizes = Some(s);
                break;
            }
            Err(e) => {
                eprintln!("oxn-load: reading final sizes failed ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    let sizes =
        sizes.ok_or_else(|| anyhow::anyhow!("could not read final queue sizes from Redis"))?;
    let final_sizes: BTreeMap<String, u64> = [
        "wait",
        "active",
        "delayed",
        "prioritized",
        "completed",
        "failed",
    ]
    .iter()
    .zip(sizes)
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let chaos_allowance = kills * cfg.concurrency as u32
        + cfg
            .chaos
            .iter()
            .filter(|(_, a)| !matches!(a, ChaosAction::KillWorker))
            .count() as u32;
    let verdict = ledger.verify(cfg.attempts, chaos_allowance.min(3));

    let mut leak = LeakReport::default();
    let total_keys = count_keys(&mut direct, &base).await;
    leak.total_queue_keys = total_keys;
    if total_keys <= 300_000 {
        match Snapshot::take(&mut direct, PREFIX, &queue).await {
            Ok(s) => {
                leak.snapshot_taken = true;
                leak.job_hashes = s.jobs.len() as u64;
                leak.retained_entries = (s.len("completed") + s.len("failed")) as u64;
                let vs = invariants::check(&s);
                leak.orphan_hashes = vs.iter().filter(|v| v.rule == "orphan-hash").count() as u64;
                leak.orphan_aux_keys = vs.iter().filter(|v| v.rule == "orphan-aux").count() as u64;
                record_violations(&violations, started.elapsed().as_secs_f64(), vs);
            }
            Err(e) => leak.note = Some(format!("final snapshot failed: {e}")),
        }
    } else {
        leak.note = Some(format!(
            "{total_keys} keys — final snapshot skipped (too large)"
        ));
    }

    let ok_jobs = stats.ok.load(std::sync::atomic::Ordering::Relaxed);
    let retention_findings = check_retention(&cfg, &final_sizes, &leak, &verdict);

    let mut failures = Vec::new();
    if !drained {
        failures.push(format!(
            "backlog did not drain within {:?}",
            cfg.drain_timeout
        ));
    }
    for (k, n) in &verdict.counts {
        failures.push(format!("{n} job(s) with anomaly {k:?}"));
    }
    let pending: u64 = ["wait", "active", "delayed", "prioritized"]
        .iter()
        .map(|k| final_sizes[*k])
        .sum();
    if pending > 0 {
        failures.push(format!(
            "{pending} job(s) still pending in Redis after drain: {final_sizes:?}"
        ));
    }
    let errs = violations
        .lock()
        .iter()
        .filter(|(_, v)| v.severity == Severity::Error)
        .count();
    if errs > 0 {
        failures.push(format!("{errs} invariant violation(s) (errors)"));
    }
    failures.extend(
        retention_findings
            .iter()
            .filter(|f| f.starts_with("FAIL"))
            .cloned(),
    );
    let pickup = stats.pickup.summary();
    if let Some(slo) = cfg.slo_pickup_p99_ms
        && pickup.p99_ms > slo
    {
        failures.push(format!(
            "pickup p99 {:.1}ms exceeds the {slo:.0}ms SLO",
            pickup.p99_ms
        ));
    }

    let health_after_cleanup = if cfg.keep {
        None
    } else {
        let _ = pq.obliterate(true).await;
        Some(stats::redis_health(&mut direct).await)
    };

    let outcome = Outcome {
        queue,
        started_at,
        load_secs,
        drain_secs,
        drained,
        added: stats.added.load(std::sync::atomic::Ordering::Relaxed),
        add_errors: stats.add_errors.load(std::sync::atomic::Ordering::Relaxed),
        throughput_ok_per_s: ok_jobs as f64 / (load_secs + drain_secs).max(0.001),
        pickup,
        run: stats.run.summary(),
        e2e: stats.e2e.summary(),
        add_latency: stats.add_latency.summary(),
        verdict,
        stream_events: stream_counts.lock().clone(),
        final_sizes,
        retention_findings,
        leak,
        violations: violations.lock().clone(),
        chaos_log,
        health_before,
        health_after_drain,
        health_after_cleanup,
        rows: rows.lock().clone(),
        kills,
        loadavg_start,
        loadavg_end: loadavg(),
        passed: failures.is_empty(),
        failures,
        config: (*cfg).clone(),
    };
    Ok(outcome)
}

async fn fire(
    action: &ChaosAction,
    pool: &mut Pool,
    proxy: Option<&Proxy>,
    cfg: &Config,
    direct: &mut redis::aio::ConnectionManager,
) -> String {
    match action {
        ChaosAction::KillWorker => pool.kill().await,
        ChaosAction::RestartWorkers => pool.restart().await,
        ChaosAction::ScriptFlush => match redis::cmd("SCRIPT")
            .arg("FLUSH")
            .query_async::<()>(direct)
            .await
        {
            Ok(()) => "script cache flushed".into(),
            Err(e) => format!("failed: {e}"),
        },
        ChaosAction::ClientKill { fraction } => client_kill(direct, *fraction).await,
        ChaosAction::RedisRestart => match &cfg.redis_restart_cmd {
            Some(cmd) => {
                let st = tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(cmd)
                    .status()
                    .await;
                format!("`{cmd}` → {st:?}")
            }
            None => "skipped: no --redis-restart-cmd".into(),
        },
        ChaosAction::Latency { delay_ms, for_ms } => match proxy {
            Some(p) => {
                p.latency(
                    Duration::from_millis(*delay_ms),
                    Duration::from_millis(*for_ms),
                );
                format!("+{delay_ms}ms per hop for {for_ms}ms")
            }
            None => "no proxy".into(),
        },
        ChaosAction::DropConns => match proxy {
            Some(p) => {
                p.drop_all();
                "all proxied connections closed".into()
            }
            None => "no proxy".into(),
        },
        ChaosAction::Blackhole { for_ms } => match proxy {
            Some(p) => {
                p.blackhole(Duration::from_millis(*for_ms));
                format!("forwarding frozen for {for_ms}ms")
            }
            None => "no proxy".into(),
        },
    }
}

/// Worker pool: in-process tasks or child processes.
enum Pool {
    InProc(InProcPool),
    Proc(ProcPool),
}

impl Pool {
    async fn kill(&mut self) -> String {
        match self {
            Pool::Proc(p) => match p.kill_random().await {
                Ok(slot) => format!("SIGKILLed worker process slot {slot}, respawned"),
                Err(e) => format!("kill failed: {e}"),
            },
            Pool::InProc(_) => "skipped: kill-worker needs --processes true".into(),
        }
    }

    async fn restart(&mut self) -> String {
        match self {
            Pool::InProc(p) => match p.restart().await {
                Ok(took) => format!("all workers cancelled and replaced in {took:?}"),
                Err(e) => format!("restart failed: {e}"),
            },
            Pool::Proc(_) => "skipped: restart-workers needs in-process workers".into(),
        }
    }
}

fn record_violations(into: &Mutex<Vec<(f64, Violation)>>, t: f64, vs: Vec<Violation>) {
    let mut g = into.lock();
    for v in vs {
        if v.severity < Severity::Warning {
            continue;
        }
        if g.iter()
            .any(|(_, x)| x.rule == v.rule && x.job_id == v.job_id)
        {
            continue;
        }
        if g.len() < 500 {
            g.push((t, v));
        }
    }
}

async fn client_kill(conn: &mut redis::aio::ConnectionManager, fraction: f64) -> String {
    let me: i64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(conn)
        .await
        .unwrap_or(-1);
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(conn)
        .await
        .unwrap_or_default();
    let ids: Vec<i64> = list
        .lines()
        .filter_map(|l| {
            l.split_whitespace()
                .find_map(|f| f.strip_prefix("id="))
                .and_then(|v| v.parse().ok())
        })
        .filter(|id| *id != me)
        .collect();
    let mut killed = 0;
    for id in &ids {
        if rand::random::<f64>() < fraction {
            let r: redis::RedisResult<()> = redis::cmd("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(id)
                .query_async(conn)
                .await;
            if r.is_ok() {
                killed += 1;
            }
        }
    }
    format!("killed {killed}/{} client connections", ids.len())
}

async fn foreign_keys(conn: &mut redis::aio::ConnectionManager) -> anyhow::Result<u64> {
    let total: u64 = redis::cmd("DBSIZE").query_async(conn).await?;
    if total == 0 {
        return Ok(0);
    }
    let ours = count_keys(conn, &format!("{PREFIX}:")).await;
    Ok(total.saturating_sub(ours))
}

async fn count_keys(conn: &mut redis::aio::ConnectionManager, prefix: &str) -> u64 {
    let mut cursor: u64 = 0;
    let mut n = 0u64;
    loop {
        let r: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("{prefix}*"))
            .arg("COUNT")
            .arg(5000)
            .query_async(conn)
            .await;
        let Ok((next, page)) = r else { break };
        n += page.len() as u64;
        if next == 0 {
            break;
        }
        cursor = next;
    }
    n
}

fn check_retention(
    cfg: &Config,
    sizes: &BTreeMap<String, u64>,
    leak: &LeakReport,
    v: &Verdict,
) -> Vec<String> {
    let mut out = Vec::new();
    let (c, f) = (sizes["completed"], sizes["failed"]);
    match cfg.retention {
        Retention::Keep => {
            if c != v.completed_jobs || f != v.failed_jobs {
                out.push(format!(
                    "FAIL keep: completed zset={c} failed zset={f}, but the ledger has {} completed and {} failed job(s)",
                    v.completed_jobs, v.failed_jobs
                ));
            } else {
                out.push(format!(
                    "ok keep: completed={c} failed={f} match the ledger"
                ));
            }
        }
        Retention::Remove => {
            if c + f > 0 {
                out.push(format!(
                    "FAIL remove: {} finished job(s) retained (completed={c} failed={f})",
                    c + f
                ));
            }
        }
        Retention::Last1000 => {
            if c > 1000 || f > 1000 {
                out.push(format!("FAIL last-1000: completed={c} failed={f}"));
            }
        }
        Retention::For30s | Retention::Mixed => {}
    }
    if leak.snapshot_taken {
        if leak.orphan_hashes > 0 {
            out.push(format!(
                "FAIL leak: {} job hash(es) in no container ({} hashes vs {} retained entries)",
                leak.orphan_hashes, leak.job_hashes, leak.retained_entries
            ));
        } else {
            out.push(format!(
                "ok: {} job hashes ↔ {} retained entries",
                leak.job_hashes, leak.retained_entries
            ));
        }
        if leak.orphan_aux_keys > 0 {
            out.push(format!(
                "FAIL leak: {} orphan auxiliary key(s)",
                leak.orphan_aux_keys
            ));
        }
    }
    out
}

fn loadavg() -> Option<String> {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .map(|s| s.split_whitespace().take(3).collect::<Vec<_>>().join(" "))
}

fn uuid_like() -> String {
    format!("{:016x}", rand::random::<u64>())
}

fn chrono_like_now() -> String {
    let s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{s}")
}
