//! Live counters, latency histograms and the once-per-second monitor.
//!
//! Queue sizes and Redis health are sampled over a **direct** connection
//! (not oxn's pool, not the chaos proxy), so the monitor keeps reporting
//! the truth even while oxn is misbehaving or the network is degraded.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use parking_lot::Mutex;
use redis::aio::ConnectionManager;
use serde::Serialize;

/// A total histogram plus a per-second window, both in µs.
#[derive(Debug)]
pub struct Hist {
    total: Mutex<Histogram<u64>>,
    window: Mutex<Histogram<u64>>,
}

impl Default for Hist {
    fn default() -> Self {
        let mk = || Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("histogram bounds");
        Self {
            total: Mutex::new(mk()),
            window: Mutex::new(mk()),
        }
    }
}

impl Hist {
    pub fn record(&self, us: u64) {
        let v = us.clamp(1, 3_600_000_000);
        let _ = self.total.lock().record(v);
        let _ = self.window.lock().record(v);
    }

    /// (p50, p99) of the window in ms, then reset the window.
    fn take_window(&self) -> (f64, f64, u64) {
        let mut w = self.window.lock();
        let r = if w.is_empty() {
            (0.0, 0.0, 0)
        } else {
            (
                w.value_at_quantile(0.5) as f64 / 1000.0,
                w.value_at_quantile(0.99) as f64 / 1000.0,
                w.len(),
            )
        };
        w.reset();
        r
    }

    pub fn summary(&self) -> HistSummary {
        let h = self.total.lock();
        let q = |x: f64| {
            if h.is_empty() {
                0.0
            } else {
                h.value_at_quantile(x) as f64 / 1000.0
            }
        };
        HistSummary {
            count: h.len(),
            p50_ms: q(0.5),
            p90_ms: q(0.9),
            p99_ms: q(0.99),
            p999_ms: q(0.999),
            max_ms: if h.is_empty() {
                0.0
            } else {
                h.max() as f64 / 1000.0
            },
            mean_ms: if h.is_empty() { 0.0 } else { h.mean() / 1000.0 },
        }
    }
}

/// Percentiles of a histogram, in ms.
#[derive(Debug, Clone, Serialize)]
pub struct HistSummary {
    pub count: u64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
}

/// Shared counters for one run.
#[derive(Debug, Default)]
pub struct Stats {
    pub added: AtomicU64,
    pub add_errors: AtomicU64,
    pub started: AtomicU64,
    pub ok: AtomicU64,
    pub err: AtomicU64,
    pub fatal: AtomicU64,
    pub postponed: AtomicU64,
    /// First-attempt pickup: start − max(enqueue, due time).
    pub pickup: Hist,
    /// Handler runtime per attempt.
    pub run: Hist,
    /// Enqueue → successful completion.
    pub e2e: Hist,
    /// Producer `Queue::add` round trip.
    pub add_latency: Hist,
    /// Notes (chaos fired, phase changes) waiting for the next row.
    pub notes: Mutex<Vec<String>>,
    /// Every handler start (µs), for the rate-limit check.
    pub start_times: Mutex<Vec<u64>>,
}

impl Stats {
    pub fn note(&self, s: impl Into<String>) {
        let s = s.into();
        eprintln!("  ⚑ {s}");
        self.notes.lock().push(s);
    }
}

/// One row of the per-second time series.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Row {
    pub t_s: f64,
    pub added_ps: u64,
    pub started_ps: u64,
    pub ok_ps: u64,
    pub err_ps: u64,
    pub wait: u64,
    pub active: u64,
    pub delayed: u64,
    pub prioritized: u64,
    pub completed: u64,
    pub failed: u64,
    pub stream_len: u64,
    pub pickup_p50_ms: f64,
    pub pickup_p99_ms: f64,
    pub run_p99_ms: f64,
    pub add_p99_ms: f64,
    pub redis_ops: u64,
    pub used_memory_mb: f64,
    pub keys: u64,
    pub clients: u64,
    pub blocked_clients: u64,
    pub notes: Vec<String>,
}

/// Raw Redis health numbers.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RedisHealth {
    pub ops_per_sec: u64,
    pub used_memory: u64,
    pub keys: u64,
    pub clients: u64,
    pub blocked_clients: u64,
}

pub async fn redis_health(conn: &mut ConnectionManager) -> RedisHealth {
    let info: String = redis::cmd("INFO")
        .query_async(conn)
        .await
        .unwrap_or_default();
    let field = |k: &str| -> u64 {
        info.lines()
            .find_map(|l| l.strip_prefix(k).and_then(|v| v.strip_prefix(':')))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    };
    let keys: u64 = redis::cmd("DBSIZE").query_async(conn).await.unwrap_or(0);
    RedisHealth {
        ops_per_sec: field("instantaneous_ops_per_sec"),
        used_memory: field("used_memory"),
        keys,
        clients: field("connected_clients"),
        blocked_clients: field("blocked_clients"),
    }
}

/// Container sizes, read directly with LLEN/ZCARD/XLEN:
/// `[wait, active, delayed, prioritized, completed, failed, events stream]`.
pub async fn queue_sizes(conn: &mut ConnectionManager, base: &str) -> redis::RedisResult<[u64; 7]> {
    let (a, b, c, d, e, f, g): (u64, u64, u64, u64, u64, u64, u64) = redis::pipe()
        .cmd("LLEN")
        .arg(format!("{base}wait"))
        .cmd("LLEN")
        .arg(format!("{base}active"))
        .cmd("ZCARD")
        .arg(format!("{base}delayed"))
        .cmd("ZCARD")
        .arg(format!("{base}prioritized"))
        .cmd("ZCARD")
        .arg(format!("{base}completed"))
        .cmd("ZCARD")
        .arg(format!("{base}failed"))
        .cmd("XLEN")
        .arg(format!("{base}events"))
        .query_async(conn)
        .await?;
    Ok([a, b, c, d, e, f, g])
}

/// Print a row per second and collect it; stops when `stop` fires.
pub async fn monitor(
    stats: Arc<Stats>,
    mut conn: ConnectionManager,
    base: String,
    started: Instant,
    rows: Arc<Mutex<Vec<Row>>>,
    stop: tokio_util::sync::CancellationToken,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    let mut last = [0u64; 4];
    eprintln!(
        "{:>6} {:>7} {:>7} {:>7} {:>6} | {:>7} {:>5} {:>6} {:>6} {:>8} {:>6} | {:>8} {:>8} | {:>7} {:>7} {:>8} {:>5}",
        "t",
        "add/s",
        "start/s",
        "ok/s",
        "err/s",
        "wait",
        "act",
        "delay",
        "prio",
        "done",
        "fail",
        "pick50",
        "pick99",
        "ops/s",
        "memMB",
        "keys",
        "cli"
    );
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tick.tick() => {}
        }
        let now = [
            stats.added.load(Ordering::Relaxed),
            stats.started.load(Ordering::Relaxed),
            stats.ok.load(Ordering::Relaxed),
            stats.err.load(Ordering::Relaxed) + stats.fatal.load(Ordering::Relaxed),
        ];
        let d: Vec<u64> = now.iter().zip(last).map(|(n, l)| n - l).collect();
        last = now;
        let (p50, p99, _) = stats.pickup.take_window();
        let (_, run99, _) = stats.run.take_window();
        let (_, add99, _) = stats.add_latency.take_window();
        let _ = stats.e2e.take_window();
        let sizes = match queue_sizes(&mut conn, &base).await {
            Ok(s) => s,
            Err(e) => {
                stats.note(format!("monitor: cannot read queue sizes: {e}"));
                [0; 7]
            }
        };
        let h = redis_health(&mut conn).await;
        let notes = std::mem::take(&mut *stats.notes.lock());
        let row = Row {
            t_s: started.elapsed().as_secs_f64(),
            added_ps: d[0],
            started_ps: d[1],
            ok_ps: d[2],
            err_ps: d[3],
            wait: sizes[0],
            active: sizes[1],
            delayed: sizes[2],
            prioritized: sizes[3],
            completed: sizes[4],
            failed: sizes[5],
            stream_len: sizes[6],
            pickup_p50_ms: p50,
            pickup_p99_ms: p99,
            run_p99_ms: run99,
            add_p99_ms: add99,
            redis_ops: h.ops_per_sec,
            used_memory_mb: h.used_memory as f64 / 1_048_576.0,
            keys: h.keys,
            clients: h.clients,
            blocked_clients: h.blocked_clients,
            notes,
        };
        eprintln!(
            "{:>5.0}s {:>7} {:>7} {:>7} {:>6} | {:>7} {:>5} {:>6} {:>6} {:>8} {:>6} | {:>7.1}ms {:>7.1}ms | {:>7} {:>7.1} {:>8} {:>5}",
            row.t_s,
            row.added_ps,
            row.started_ps,
            row.ok_ps,
            row.err_ps,
            row.wait,
            row.active,
            row.delayed,
            row.prioritized,
            row.completed,
            row.failed,
            row.pickup_p50_ms,
            row.pickup_p99_ms,
            row.redis_ops,
            row.used_memory_mb,
            row.keys,
            row.clients
        );
        rows.lock().push(row);
    }
}
