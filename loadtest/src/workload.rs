//! Producers, the instrumented handler, worker pools (in-process or child
//! processes) and the server-side stream observer.

use std::io::Write as _;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use oxn::backend::Backend;
use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::options::Removal;
use oxn::prelude::*;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, Retention};
use crate::ledger::{Ledger, LoadJob, Mark};
use crate::stats::Stats;

/// Key prefix for everything the harness creates.
pub const PREFIX: &str = "oxnload";

pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// What a worker reports about one handler invocation.
#[derive(Debug, Clone, Copy)]
pub enum WEvent {
    Start {
        seq: u64,
        attempt: u32,
        worker: u32,
        at_us: u64,
        due_us: u64,
    },
    Done {
        seq: u64,
        kind: DoneKind,
        worker: u32,
        at_us: u64,
        run_us: u64,
        due_us: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoneKind {
    Ok,
    Err,
    Fatal,
}

impl WEvent {
    /// Line protocol used by child workers on stdout.
    pub fn encode(&self) -> String {
        match *self {
            WEvent::Start {
                seq,
                attempt,
                worker,
                at_us,
                due_us,
            } => {
                format!("S {seq} {attempt} {worker} {at_us} {due_us}")
            }
            WEvent::Done {
                seq,
                kind,
                worker,
                at_us,
                run_us,
                due_us,
            } => {
                let k = match kind {
                    DoneKind::Ok => 'o',
                    DoneKind::Err => 'e',
                    DoneKind::Fatal => 'f',
                };
                format!("D {seq} {k} {worker} {at_us} {run_us} {due_us}")
            }
        }
    }

    pub fn decode(line: &str) -> Option<Self> {
        let mut it = line.split_ascii_whitespace();
        let tag = it.next()?;
        let mut n = || it.next();
        match tag {
            "S" => Some(WEvent::Start {
                seq: n()?.parse().ok()?,
                attempt: n()?.parse().ok()?,
                worker: n()?.parse().ok()?,
                at_us: n()?.parse().ok()?,
                due_us: n()?.parse().ok()?,
            }),
            "D" => {
                let seq = n()?.parse().ok()?;
                let kind = match n()? {
                    "o" => DoneKind::Ok,
                    "e" => DoneKind::Err,
                    "f" => DoneKind::Fatal,
                    _ => return None,
                };
                Some(WEvent::Done {
                    seq,
                    kind,
                    worker: n()?.parse().ok()?,
                    at_us: n()?.parse().ok()?,
                    run_us: n()?.parse().ok()?,
                    due_us: n()?.parse().ok()?,
                })
            }
            _ => None,
        }
    }
}

/// Fold a worker event into the ledger and stats.
pub fn apply(ledger: &Ledger, stats: &Stats, ev: WEvent) {
    match ev {
        WEvent::Start {
            seq,
            attempt,
            worker,
            at_us,
            due_us,
        } => {
            stats.started.fetch_add(1, Ordering::Relaxed);
            if attempt <= 1 {
                stats.pickup.record(at_us.saturating_sub(due_us));
            }
            ledger.start(seq, attempt, worker, at_us);
        }
        WEvent::Done {
            seq,
            kind,
            worker,
            at_us,
            run_us,
            due_us,
        } => {
            stats.run.record(run_us);
            let mark = match kind {
                DoneKind::Ok => {
                    stats.ok.fetch_add(1, Ordering::Relaxed);
                    stats.e2e.record(at_us.saturating_sub(due_us));
                    Mark::Ok { worker }
                }
                DoneKind::Err => {
                    stats.err.fetch_add(1, Ordering::Relaxed);
                    Mark::Err { worker }
                }
                DoneKind::Fatal => {
                    stats.fatal.fetch_add(1, Ordering::Relaxed);
                    Mark::Fatal { worker }
                }
            };
            ledger.finish(seq, mark, at_us);
        }
    }
}

/// Where a handler sends its events.
#[derive(Clone)]
pub enum Sink {
    Local(Arc<Ledger>, Arc<Stats>),
    Stdout(Arc<Mutex<std::io::Stdout>>),
}

impl Sink {
    fn emit(&self, ev: WEvent) {
        match self {
            Sink::Local(l, s) => apply(l, s, ev),
            Sink::Stdout(out) => {
                // Flushed per line: a SIGKILL must not lose an `Ok` that
                // already happened, or the ledger would report it as lost.
                let mut o = out.lock();
                let _ = writeln!(o, "{}", ev.encode());
                let _ = o.flush();
            }
        }
    }
}

#[derive(Debug)]
struct Transient(u32);
impl std::fmt::Display for Transient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "planned transient failure on attempt {}", self.0)
    }
}
impl std::error::Error for Transient {}

/// Build the worker for one pool slot.
pub fn build_worker(
    queue: Queue<LoadJob>,
    index: u32,
    concurrency: usize,
    lock_ms: u64,
    drain_delay_ms: Option<u64>,
    sink: Sink,
    cancel: CancellationToken,
) -> Worker<LoadJob, u64> {
    let mut b = Worker::builder(queue, move |job: Job<LoadJob>| {
        let sink = sink.clone();
        async move {
            let d = job.data;
            let attempt = job.attempts_made;
            let due_us = d.enq_us + d.delay_ms * 1000;
            let start = now_us();
            sink.emit(WEvent::Start {
                seq: d.seq,
                attempt,
                worker: index,
                at_us: start,
                due_us,
            });
            if d.work_us > 0 {
                tokio::time::sleep(Duration::from_micros(d.work_us)).await;
            }
            let kind = if d.fatal {
                DoneKind::Fatal
            } else if attempt <= d.fail_attempts {
                DoneKind::Err
            } else {
                DoneKind::Ok
            };
            let end = now_us();
            sink.emit(WEvent::Done {
                seq: d.seq,
                kind,
                worker: index,
                at_us: end,
                run_us: end - start,
                due_us,
            });
            match kind {
                DoneKind::Ok => Ok(d.seq),
                DoneKind::Err => Err(Error::handler(Transient(attempt))),
                DoneKind::Fatal => Err(Error::unrecoverable("planned fatal failure")),
            }
        }
    })
    .concurrency(concurrency)
    .lock_duration(Duration::from_millis(lock_ms))
    .lock_renew_interval(Duration::from_millis((lock_ms / 2).max(1)))
    .stalled_interval(Duration::from_millis((lock_ms / 2).clamp(250, 30_000)))
    .shutdown_timeout(Duration::from_secs(10))
    .id(format!("w{index}"))
    .cancellation_token(cancel);
    if let Some(d) = drain_delay_ms {
        b = b.drain_delay(Duration::from_millis(d));
    }
    b.build()
}

pub async fn connect(url: &str, pool: usize) -> anyhow::Result<Arc<dyn Backend>> {
    let cfg = RedisConfig::new(url)
        .prefix(PREFIX)
        .pool_size(pool)
        .max_stream_length(200_000);
    Ok(Arc::new(RedisBackend::connect(cfg).await?))
}

pub fn queue_on(backend: Arc<dyn Backend>, name: &str) -> Queue<LoadJob> {
    Queue::from_backend(name, backend, QueueOptions::default().prefix(PREFIX))
}

/// Deterministic per-job plan.
pub fn plan(cfg: &Config, seq: u64, seed: u64) -> (LoadJob, JobOptions) {
    let mut rng = StdRng::seed_from_u64(seed ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let fail_attempts = if rng.gen_bool(cfg.fail_rate.clamp(0.0, 1.0)) {
        rng.gen_range(1..=3)
    } else {
        0
    };
    let fatal = rng.gen_bool(cfg.fatal_rate.clamp(0.0, 1.0));
    let delay_ms = if rng.gen_bool(cfg.delay_rate.clamp(0.0, 1.0)) {
        rng.gen_range(100..=2000)
    } else {
        0
    };
    let priority = if rng.gen_bool(cfg.priority_rate.clamp(0.0, 1.0)) {
        rng.gen_range(1..=10)
    } else {
        0
    };
    let work_us = if cfg.work_us > 0 {
        rng.gen_range(0..=cfg.work_us * 2)
    } else {
        0
    };
    let retention = match cfg.retention {
        Retention::Mixed => [
            Retention::Keep,
            Retention::Remove,
            Retention::Last1000,
            Retention::For30s,
        ][rng.gen_range(0..4)],
        r => r,
    };
    let removal = match retention {
        Retention::Keep | Retention::Mixed => Removal::Keep,
        Retention::Remove => Removal::Remove,
        Retention::Last1000 => Removal::KeepLast(1000),
        Retention::For30s => Removal::KeepFor(Duration::from_secs(30)),
    };
    let job = LoadJob {
        seq,
        enq_us: 0, // stamped at submit time
        delay_ms,
        fail_attempts,
        fatal,
        work_us,
    };
    let backoff = if cfg.fail_rate > 0.0 {
        Backoff::Exponential {
            initial: Duration::from_millis(50),
            max: Some(Duration::from_secs(1)),
        }
    } else {
        Backoff::Fixed {
            delay: Duration::from_millis(100),
        }
    };
    let mut opts = JobOptions::new()
        .attempts(cfg.attempts)
        .backoff(backoff)
        .priority(priority)
        .remove_on_complete(removal)
        .remove_on_fail(removal);
    if delay_ms > 0 {
        opts = opts.delay(Duration::from_millis(delay_ms));
    }
    (job, opts)
}

/// Run one producer until the job budget or duration is exhausted.
#[allow(clippy::too_many_arguments)]
pub async fn producer(
    cfg: Arc<Config>,
    queue: Queue<LoadJob>,
    next: Arc<AtomicU64>,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    seed: u64,
    started: Instant,
    stop: CancellationToken,
) {
    let per_producer_rate = if cfg.rate > 0 {
        cfg.rate as f64 / cfg.producers as f64
    } else {
        0.0
    };
    let interval =
        (per_producer_rate > 0.0).then(|| Duration::from_secs_f64(1.0 / per_producer_rate));
    let mut next_at = Instant::now();
    loop {
        if stop.is_cancelled() {
            break;
        }
        if let Some(d) = cfg.duration
            && started.elapsed() >= d
        {
            break;
        }
        if cfg.bursty {
            // 2s on, 3s off: the idle gaps send workers into their blocking
            // wait, so the first job of every burst measures wake-up latency.
            let phase = started.elapsed().as_millis() % 5000;
            if phase >= 2000 {
                tokio::time::sleep(Duration::from_millis((5000 - phase) as u64)).await;
                next_at = Instant::now();
                continue;
            }
        }
        let seq = next.fetch_add(1, Ordering::Relaxed);
        if cfg.duration.is_none() && seq >= cfg.jobs {
            break;
        }
        if let Some(iv) = interval {
            next_at += iv;
            let now = Instant::now();
            if next_at > now {
                tokio::time::sleep(next_at - now).await;
            } else if now - next_at > Duration::from_secs(1) {
                next_at = now; // fell far behind; don't burst to catch up
            }
        }
        // Retry the same seq until it lands so the ledger never has holes.
        // A failed `add` may still have been applied server-side (reply
        // lost); the ledger would then see an unknown job id in the stream,
        // which the observer ignores, and this seq gets a second job.
        loop {
            let (mut job, opts) = plan(&cfg, seq, seed);
            job.enq_us = now_us();
            let t0 = Instant::now();
            match queue.add(job.clone(), opts).await {
                Ok(id) => {
                    stats.add_latency.record(t0.elapsed().as_micros() as u64);
                    stats.added.fetch_add(1, Ordering::Relaxed);
                    ledger.added(job, id.to_string(), now_us());
                    break;
                }
                Err(e) => {
                    let n = stats.add_errors.fetch_add(1, Ordering::Relaxed);
                    if n.is_multiple_of(100) {
                        stats.note(format!("add failed (#{}): {e}", n + 1));
                    }
                    if stop.is_cancelled() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

/// In-process worker pool supporting graceful replacement.
pub struct InProcPool {
    cfg: Arc<Config>,
    url: String,
    queue_name: String,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    slots: Vec<(CancellationToken, JoinHandle<()>)>,
    generation: u32,
}

impl InProcPool {
    pub fn new(
        cfg: Arc<Config>,
        url: String,
        queue_name: String,
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            cfg,
            url,
            queue_name,
            ledger,
            stats,
            slots: Vec::new(),
            generation: 0,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        for i in 0..self.cfg.workers {
            let backend = connect(&self.url, self.cfg.concurrency + 4).await?;
            let q = queue_on(backend, &self.queue_name);
            let cancel = CancellationToken::new();
            let index = self.generation * 1000 + i as u32;
            let w = build_worker(
                q,
                index,
                self.cfg.concurrency,
                self.cfg.lock_ms,
                self.cfg.drain_delay_ms,
                Sink::Local(self.ledger.clone(), self.stats.clone()),
                cancel.clone(),
            );
            let h = tokio::spawn(async move {
                let _ = w.run().await;
            });
            self.slots.push((cancel, h));
        }
        Ok(())
    }

    /// Cancel every worker, wait for `run()` to return, start new ones.
    pub async fn restart(&mut self) -> anyhow::Result<Duration> {
        let t = Instant::now();
        self.stop().await;
        self.generation += 1;
        self.start().await?;
        Ok(t.elapsed())
    }

    pub async fn stop(&mut self) {
        for (c, _) in &self.slots {
            c.cancel();
        }
        for (_, h) in self.slots.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
        }
    }
}

/// Child-process worker pool (needed for SIGKILL chaos).
pub struct ProcPool {
    cfg: Arc<Config>,
    url: String,
    queue_name: String,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    children: Vec<Option<tokio::process::Child>>,
    readers: Vec<JoinHandle<()>>,
    generation: u32,
    pub kills: u32,
}

impl ProcPool {
    pub fn new(
        cfg: Arc<Config>,
        url: String,
        queue_name: String,
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            cfg,
            url,
            queue_name,
            ledger,
            stats,
            children: Vec::new(),
            readers: Vec::new(),
            generation: 0,
            kills: 0,
        }
    }

    fn spawn_one(&mut self, slot: usize) -> anyhow::Result<tokio::process::Child> {
        let exe = std::env::current_exe()?;
        let index = self.generation * 1000 + slot as u32;
        let mut cmd = tokio::process::Command::new(exe);
        cmd.arg("worker")
            .arg("--redis")
            .arg(&self.url)
            .arg("--queue")
            .arg(&self.queue_name)
            .arg("--index")
            .arg(index.to_string())
            .arg("--concurrency")
            .arg(self.cfg.concurrency.to_string())
            .arg("--lock-ms")
            .arg(self.cfg.lock_ms.to_string());
        if let Some(d) = self.cfg.drain_delay_ms {
            cmd.arg("--drain-delay-ms").arg(d.to_string());
        }
        cmd.stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = cmd.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("no child stdout"))?;
        let (ledger, stats) = (self.ledger.clone(), self.stats.clone());
        self.readers.push(tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(l)) = lines.next_line().await {
                if let Some(ev) = WEvent::decode(&l) {
                    apply(&ledger, &stats, ev);
                }
            }
        }));
        self.generation += 1;
        Ok(child)
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        for slot in 0..self.cfg.workers {
            let c = self.spawn_one(slot)?;
            self.children.push(Some(c));
        }
        Ok(())
    }

    /// SIGKILL a random child, respawn it after 1s. Returns its slot.
    pub async fn kill_random(&mut self) -> anyhow::Result<usize> {
        let slot = rand::thread_rng().gen_range(0..self.children.len());
        if let Some(mut c) = self.children[slot].take() {
            c.start_kill()?;
            let _ = c.wait().await;
        }
        self.kills += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let c = self.spawn_one(slot)?;
        self.children[slot] = Some(c);
        Ok(slot)
    }

    /// SIGTERM-free graceful stop isn't wired for children; kill and wait.
    pub async fn stop(&mut self) {
        for c in self.children.iter_mut().flatten() {
            let _ = c.start_kill();
            let _ = c.wait().await;
        }
        self.children.clear();
        // Let readers drain buffered lines.
        for r in self.readers.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(2), r).await;
        }
    }
}

/// Entry point of `oxn-load worker` (child process mode).
pub async fn child_worker_main(
    url: String,
    queue: String,
    index: u32,
    concurrency: usize,
    lock_ms: u64,
    drain_delay_ms: Option<u64>,
) -> anyhow::Result<()> {
    let backend = connect(&url, concurrency + 4).await?;
    let q = queue_on(backend, &queue);
    let sink = Sink::Stdout(Arc::new(Mutex::new(std::io::stdout())));
    let w = build_worker(
        q,
        index,
        concurrency,
        lock_ms,
        drain_delay_ms,
        sink,
        CancellationToken::new(),
    );
    w.run().await?;
    Ok(())
}

/// Tail the queue's events stream directly (`XREAD` from the beginning,
/// never `$`) and feed server-side truth into the ledger.
pub async fn observe_stream(
    url: String,
    base: String,
    ledger: Arc<Ledger>,
    counts: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
    stop: CancellationToken,
) {
    let Ok(client) = redis::Client::open(url.as_str()) else {
        return;
    };
    let key = format!("{base}events");
    let mut cursor = "0-0".to_string();
    let mut conn: Option<redis::aio::MultiplexedConnection> = None;
    let mut draining = false;
    loop {
        if stop.is_cancelled() && !draining {
            draining = true; // one last non-blocking sweep
        }
        if conn.is_none() {
            conn = client.get_multiplexed_async_connection().await.ok();
            if conn.is_none() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        }
        let c = conn.as_mut().expect("connected above");
        let mut cmd = redis::cmd("XREAD");
        if !draining {
            cmd.arg("BLOCK").arg(500);
        }
        cmd.arg("COUNT")
            .arg(5000)
            .arg("STREAMS")
            .arg(&key)
            .arg(&cursor);
        let reply: redis::RedisResult<Option<redis::streams::StreamReadReply>> =
            cmd.query_async(c).await;
        let reply = match reply {
            Ok(r) => r,
            Err(_) => {
                conn = None;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let mut got = 0;
        if let Some(r) = reply {
            for k in r.keys {
                for e in k.ids {
                    got += 1;
                    cursor = e.id.clone();
                    let get = |f: &str| {
                        e.map
                            .get(f)
                            .and_then(|v| redis::from_redis_value::<String>(v).ok())
                    };
                    let event = get("event").unwrap_or_default();
                    *counts.lock().entry(event.clone()).or_default() += 1;
                    if let Some(job) = get("jobId") {
                        let at_us =
                            e.id.split('-')
                                .next()
                                .and_then(|m| m.parse::<u64>().ok())
                                .unwrap_or(0)
                                * 1000;
                        ledger.server_event(&job, &event, at_us);
                    }
                }
            }
        }
        if draining && got == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_line_roundtrip() {
        let evs = [
            WEvent::Start {
                seq: 7,
                attempt: 2,
                worker: 3,
                at_us: 100,
                due_us: 90,
            },
            WEvent::Done {
                seq: 7,
                kind: DoneKind::Fatal,
                worker: 3,
                at_us: 200,
                run_us: 100,
                due_us: 90,
            },
        ];
        for e in evs {
            let back = WEvent::decode(&e.encode()).unwrap();
            assert_eq!(format!("{back:?}"), format!("{e:?}"));
        }
        assert!(WEvent::decode("garbage").is_none());
    }
}
