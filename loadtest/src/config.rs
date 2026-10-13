//! Run configuration: named scenario presets plus per-flag overrides.

use std::time::Duration;

use clap::{Args, ValueEnum};
use serde::Serialize;

use crate::chaos::ChaosAction;

/// Built-in scenario presets. Every field can still be overridden by flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Scenario {
    /// 2k jobs, 2 workers × 4, a little of everything. CI gate (~10s).
    Smoke,
    /// Max sustained throughput: 4 producers, 8 workers × 16.
    Throughput,
    /// Steady rate with idle gaps; measures pickup latency at the default
    /// `drain_delay` (catches missing worker wake-ups).
    Latency,
    /// Long run with mixed retention policies; watches memory/key growth.
    Soak,
    /// 30% of jobs fail 1–3 times before succeeding; exponential backoff.
    RetryStorm,
    /// Worker child processes are SIGKILLed mid-job and respawned.
    KillWorker,
    /// Redis is restarted mid-run (needs `--redis-restart-cmd`).
    RedisRestart,
    /// Traffic goes through the built-in proxy: latency spikes + dropped
    /// connections.
    Network,
    /// Workers are cancelled under load, then replaced.
    Shutdown,
    /// Mix of priorities, delays and LIFO.
    Mixed,
}

/// How completed / failed jobs are retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Retention {
    /// Keep every finished job.
    Keep,
    /// Delete on finish.
    Remove,
    /// Keep the most recent 1000.
    Last1000,
    /// Keep for 30 seconds.
    For30s,
    /// Each job picks one of the above at random.
    Mixed,
}

/// Flags shared by `run` and scenario presets. `None` = use the preset.
#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// Scenario preset to start from.
    #[arg(long, value_enum, default_value = "smoke")]
    pub scenario: Scenario,
    /// Redis URL. Defaults to `TEST_REDIS_URL`, else the dedicated
    /// test instance on :6390.
    #[arg(long)]
    pub redis: Option<String>,
    /// Total jobs to enqueue (ignored when `--duration` is set).
    #[arg(long)]
    pub jobs: Option<u64>,
    /// Run for this long instead of a fixed job count (e.g. `2h`, `90s`).
    #[arg(long, value_parser = parse_duration)]
    pub duration: Option<Duration>,
    /// Target enqueue rate across all producers (jobs/s); 0 = unthrottled.
    #[arg(long)]
    pub rate: Option<u64>,
    /// Producer tasks.
    #[arg(long)]
    pub producers: Option<usize>,
    /// Worker instances.
    #[arg(long)]
    pub workers: Option<usize>,
    /// Concurrency per worker.
    #[arg(long)]
    pub concurrency: Option<usize>,
    /// Run workers as separate OS processes (required for kill chaos).
    #[arg(long)]
    pub processes: Option<bool>,
    /// Simulated work per job, µs (uniformly 0..=2× this).
    #[arg(long)]
    pub work_us: Option<u64>,
    /// Fraction of jobs that fail transiently 1–3 times.
    #[arg(long)]
    pub fail_rate: Option<f64>,
    /// Fraction of jobs that fail with `Error::Unrecoverable`.
    #[arg(long)]
    pub fatal_rate: Option<f64>,
    /// Attempts per job.
    #[arg(long)]
    pub attempts: Option<u32>,
    /// Fraction of jobs added with a delay (0–2s).
    #[arg(long)]
    pub delay_rate: Option<f64>,
    /// Fraction of jobs added with a priority (1–10).
    #[arg(long)]
    pub priority_rate: Option<f64>,
    /// Retention policy for finished jobs.
    #[arg(long, value_enum)]
    pub retention: Option<Retention>,
    /// Worker `drain_delay` in ms. Omit to use oxn's default.
    #[arg(long)]
    pub drain_delay_ms: Option<u64>,
    /// Worker lock duration in ms.
    #[arg(long)]
    pub lock_ms: Option<u64>,
    /// Chaos schedule, e.g. `10s:kill-worker,20s:script-flush,30s:latency=200ms/10s`.
    #[arg(long)]
    pub chaos: Option<String>,
    /// Shell command that restarts Redis (for the redis-restart scenario).
    #[arg(long)]
    pub redis_restart_cmd: Option<String>,
    /// Run a full invariant snapshot every N seconds (0 = only at the end).
    /// Snapshots block Redis briefly; keep this off for latency runs.
    #[arg(long)]
    pub check_every_s: Option<u64>,
    /// Give up waiting for the backlog to drain after this long.
    #[arg(long, value_parser = parse_duration, default_value = "120s")]
    pub drain_timeout: Duration,
    /// Allow running against a Redis that holds keys we didn't create.
    #[arg(long)]
    pub allow_shared_redis: bool,
    /// Keep the queue's keys after the run (default: obliterate).
    #[arg(long)]
    pub keep: bool,
    /// Label appended to the report file name.
    #[arg(long)]
    pub label: Option<String>,
    /// Fail the run if first-attempt pickup p99 exceeds this (ms).
    #[arg(long)]
    pub slo_pickup_p99_ms: Option<f64>,
}

/// Fully-resolved configuration for one run.
#[derive(Debug, Clone, Serialize)]
pub struct Config {
    pub scenario: Scenario,
    pub redis: String,
    pub jobs: u64,
    #[serde(serialize_with = "ser_opt_dur")]
    pub duration: Option<Duration>,
    pub rate: u64,
    pub producers: usize,
    pub workers: usize,
    pub concurrency: usize,
    pub processes: bool,
    pub work_us: u64,
    pub fail_rate: f64,
    pub fatal_rate: f64,
    pub attempts: u32,
    pub delay_rate: f64,
    pub priority_rate: f64,
    pub retention: Retention,
    pub drain_delay_ms: Option<u64>,
    pub lock_ms: u64,
    pub chaos: Vec<(u64, ChaosAction)>,
    pub redis_restart_cmd: Option<String>,
    pub check_every_s: u64,
    #[serde(serialize_with = "ser_dur")]
    pub drain_timeout: Duration,
    pub allow_shared_redis: bool,
    pub keep: bool,
    pub label: Option<String>,
    /// Burst pattern for the latency scenario: on/off seconds.
    pub bursty: bool,
    /// Pickup p99 service level, ms.
    pub slo_pickup_p99_ms: Option<f64>,
}

fn ser_dur<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format!("{:?}", d))
}
fn ser_opt_dur<S: serde::Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
    match d {
        Some(d) => s.serialize_str(&format!("{:?}", d)),
        None => s.serialize_none(),
    }
}

impl Config {
    pub fn resolve(a: &RunArgs) -> anyhow::Result<Self> {
        use Scenario::*;
        let mut c = Config {
            scenario: a.scenario,
            redis: a
                .redis
                .clone()
                .or_else(|| std::env::var("TEST_REDIS_URL").ok())
                .unwrap_or_else(|| "redis://127.0.0.1:6390".into()),
            jobs: 2_000,
            duration: None,
            rate: 0,
            producers: 2,
            workers: 2,
            concurrency: 4,
            processes: false,
            work_us: 200,
            fail_rate: 0.05,
            fatal_rate: 0.01,
            attempts: 3,
            delay_rate: 0.05,
            priority_rate: 0.1,
            retention: Retention::Keep,
            drain_delay_ms: Some(50),
            lock_ms: 30_000,
            chaos: Vec::new(),
            redis_restart_cmd: a.redis_restart_cmd.clone(),
            check_every_s: 2,
            drain_timeout: a.drain_timeout,
            allow_shared_redis: a.allow_shared_redis,
            keep: a.keep,
            label: a.label.clone(),
            bursty: false,
            slo_pickup_p99_ms: None,
        };
        match a.scenario {
            Smoke => {}
            Throughput => {
                c.jobs = 1_000_000;
                c.producers = 4;
                c.workers = 8;
                c.concurrency = 16;
                c.work_us = 0;
                c.fail_rate = 0.0;
                c.fatal_rate = 0.0;
                c.delay_rate = 0.0;
                c.priority_rate = 0.0;
                c.retention = Retention::Remove;
                c.check_every_s = 0;
            }
            Latency => {
                c.duration = Some(Duration::from_secs(60));
                c.rate = 500;
                c.bursty = true;
                c.workers = 4;
                c.concurrency = 8;
                c.fail_rate = 0.0;
                c.fatal_rate = 0.0;
                c.delay_rate = 0.0;
                c.priority_rate = 0.2;
                c.drain_delay_ms = None; // oxn default — the point of the test
                c.retention = Retention::Remove;
                c.check_every_s = 0;
                c.slo_pickup_p99_ms = Some(50.0);
            }
            Soak => {
                c.duration = Some(Duration::from_secs(2 * 3600));
                c.rate = 300;
                c.workers = 4;
                c.concurrency = 8;
                c.retention = Retention::Mixed;
                c.check_every_s = 0;
            }
            RetryStorm => {
                c.jobs = 20_000;
                c.fail_rate = 0.3;
                c.attempts = 4;
                c.workers = 4;
                c.concurrency = 8;
                c.check_every_s = 0;
            }
            KillWorker => {
                c.jobs = 20_000;
                c.processes = true;
                c.workers = 4;
                c.concurrency = 8;
                c.work_us = 20_000;
                c.lock_ms = 2_000;
                c.chaos =
                    parse_chaos("5s:kill-worker,10s:kill-worker,15s:kill-worker,20s:kill-worker")?;
                c.check_every_s = 0;
            }
            RedisRestart => {
                c.jobs = 20_000;
                c.rate = 1_000;
                c.lock_ms = 5_000;
                c.chaos = parse_chaos("8s:redis-restart")?;
                c.check_every_s = 0;
            }
            Network => {
                c.jobs = 20_000;
                c.rate = 1_000;
                c.lock_ms = 5_000;
                c.chaos = parse_chaos("5s:latency=200ms/5s,12s:drop-conns,16s:blackhole=2s")?;
                c.check_every_s = 0;
            }
            Shutdown => {
                c.jobs = 10_000;
                c.rate = 1_000;
                c.work_us = 50_000;
                c.workers = 4;
                c.concurrency = 8;
                c.chaos = parse_chaos("4s:restart-workers,8s:restart-workers")?;
                c.check_every_s = 0;
            }
            Mixed => {
                c.jobs = 10_000;
                c.delay_rate = 0.3;
                c.priority_rate = 0.3;
                c.retention = Retention::Mixed;
            }
        }
        macro_rules! ovr {
            ($($f:ident),*) => { $( if let Some(v) = a.$f.clone() { c.$f = v; } )* };
        }
        ovr!(
            jobs,
            rate,
            producers,
            workers,
            concurrency,
            processes,
            work_us,
            fail_rate,
            fatal_rate,
            attempts,
            delay_rate,
            priority_rate,
            retention,
            lock_ms,
            check_every_s
        );
        if a.duration.is_some() {
            c.duration = a.duration;
        }
        if a.slo_pickup_p99_ms.is_some() {
            c.slo_pickup_p99_ms = a.slo_pickup_p99_ms;
        }
        if a.drain_delay_ms.is_some() {
            c.drain_delay_ms = a.drain_delay_ms;
        }
        if let Some(ch) = &a.chaos {
            c.chaos = parse_chaos(ch)?;
        }
        if c.chaos
            .iter()
            .any(|(_, a)| matches!(a, ChaosAction::KillWorker))
            && !c.processes
        {
            anyhow::bail!("kill-worker chaos needs --processes true");
        }
        Ok(c)
    }

    /// Short slug for file names.
    pub fn slug(&self) -> String {
        let s = serde_json::to_value(self.scenario)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "run".into());
        match &self.label {
            Some(l) => format!("{s}-{l}"),
            None => s,
        }
    }
}

/// `90s`, `250ms`, `2h`, `1m30s` → Duration.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let mut total = Duration::ZERO;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            continue;
        }
        let mut unit = c.to_string();
        while let Some(&n) = chars.peek() {
            if n.is_ascii_alphabetic() {
                unit.push(n);
                chars.next();
            } else {
                break;
            }
        }
        let n: f64 = num.parse().map_err(|_| format!("bad duration `{s}`"))?;
        num.clear();
        let secs = match unit.as_str() {
            "ms" => n / 1000.0,
            "s" => n,
            "m" => n * 60.0,
            "h" => n * 3600.0,
            u => return Err(format!("unknown unit `{u}` in `{s}`")),
        };
        total += Duration::from_secs_f64(secs);
    }
    if !num.is_empty() {
        let n: f64 = num.parse().map_err(|_| format!("bad duration `{s}`"))?;
        total += Duration::from_secs_f64(n);
    }
    Ok(total)
}

/// `5s:kill-worker,12s:latency=200ms/5s` → [(5000, KillWorker), …].
pub fn parse_chaos(s: &str) -> anyhow::Result<Vec<(u64, ChaosAction)>> {
    let mut out = Vec::new();
    for item in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let (at, action) = item
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("chaos item `{item}` must be `<time>:<action>`"))?;
        let at = parse_duration(at).map_err(anyhow::Error::msg)?;
        out.push((at.as_millis() as u64, ChaosAction::parse(action)?));
    }
    out.sort_by_key(|(t, _)| *t);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("1m30s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert!(parse_duration("3x").is_err());
    }

    #[test]
    fn chaos_schedule() {
        let c = parse_chaos("12s:drop-conns, 5s:latency=200ms/5s").unwrap();
        assert_eq!(c[0].0, 5000);
        assert!(matches!(c[0].1, ChaosAction::Latency { .. }));
        assert_eq!(c[1].0, 12_000);
        assert!(parse_chaos("5s:nope").is_err());
    }
}
