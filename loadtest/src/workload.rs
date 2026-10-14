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
        attempt: u32,
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
    Postponed,
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
                attempt,
                worker,
                at_us,
                run_us,
                due_us,
            } => {
                let k = match kind {
                    DoneKind::Ok => 'o',
                    DoneKind::Err => 'e',
                    DoneKind::Fatal => 'f',
                    DoneKind::Postponed => 'p',
                };
                format!("D {seq} {k} {attempt} {worker} {at_us} {run_us} {due_us}")
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
                    "p" => DoneKind::Postponed,
                    _ => return None,
                };
                Some(WEvent::Done {
                    seq,
                    kind,
                    attempt: n()?.parse().ok()?,
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
            stats.start_times.lock().push(at_us);
            if attempt <= 1 {
                stats.pickup.record(at_us.saturating_sub(due_us));
            }
            ledger.start(seq, attempt, worker, at_us);
        }
        WEvent::Done {
            seq,
            kind,
            attempt,
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
                    Mark::Ok { worker, attempt }
                }
                DoneKind::Err => {
                    stats.err.fetch_add(1, Ordering::Relaxed);
                    Mark::Err { worker, attempt }
                }
                DoneKind::Fatal => {
                    stats.fatal.fetch_add(1, Ordering::Relaxed);
                    Mark::Fatal { worker, attempt }
                }
                DoneKind::Postponed => {
                    stats.postponed.fetch_add(1, Ordering::Relaxed);
                    Mark::Postponed { worker, attempt }
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
/// Tuning for one pool slot's worker.
#[derive(Debug, Clone, Copy)]
pub struct WorkerSpec {
    pub index: u32,
    pub concurrency: usize,
    pub lock_ms: u64,
    pub drain_delay_ms: Option<u64>,
    pub shutdown_timeout_ms: u64,
    pub max_stalled: u32,
    /// Report progress and a log line through `JobContext` on every run.
    pub ctx_ops: bool,
    /// Worker `rate_limit` as (max, per ms).
    pub rate_limit: Option<(u32, u64)>,
}

pub fn build_worker(
    queue: Queue<LoadJob>,
    spec: WorkerSpec,
    sink: Sink,
    cancel: CancellationToken,
) -> Worker<LoadJob, u64> {
    let WorkerSpec {
        index,
        concurrency,
        lock_ms,
        drain_delay_ms,
        shutdown_timeout_ms,
        max_stalled,
        ctx_ops,
        rate_limit,
    } = spec;
    let mut b = Worker::builder_with_ctx(queue, move |job: Job<LoadJob>, ctx: oxn::JobContext| {
        let sink = sink.clone();
        async move {
            let d = job.data;
            let attempt = job.attempts_made;
            if ctx_ops {
                ctx.progress(50u8).await?;
                ctx.log(format!("attempt {attempt}")).await?;
            }
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
            // Postpone on the first pickup only: once the postponement has
            // elapsed the job is past its window and runs normally.
            let kind = if d.postpone_ms > 0 && start < due_us + d.postpone_ms * 1000 {
                DoneKind::Postponed
            } else if d.fatal {
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
                attempt,
                worker: index,
                at_us: end,
                run_us: end - start,
                due_us,
            });
            if kind == DoneKind::Err && d.panic {
                panic!("planned panic on attempt {attempt}");
            }
            match kind {
                DoneKind::Ok => Ok(d.seq),
                DoneKind::Err => Err(Error::handler(Transient(attempt))),
                DoneKind::Fatal => Err(Error::unrecoverable("planned fatal failure")),
                DoneKind::Postponed => Err(Error::Delayed {
                    delay_ms: d.postpone_ms,
                }),
            }
        }
    })
    .concurrency(concurrency)
    .lock_duration(Duration::from_millis(lock_ms))
    .lock_renew_interval(Duration::from_millis((lock_ms / 2).max(1)))
    .stalled_interval(Duration::from_millis((lock_ms / 2).clamp(250, 30_000)))
    .shutdown_timeout(Duration::from_millis(shutdown_timeout_ms))
    .max_stalled(max_stalled)
    .id(format!("w{index}"))
    .cancellation_token(cancel);
    if let Some(d) = drain_delay_ms {
        b = b.drain_delay(Duration::from_millis(d));
    }
    if let Some((max, per_ms)) = rate_limit {
        b = b.rate_limit(max, Duration::from_millis(per_ms));
    }
    b.build()
}

/// Events-stream cap every backend in this process uses (set once from the
/// run config; child workers receive it on the command line).
pub static MAX_STREAM_LEN: AtomicU64 = AtomicU64::new(200_000);

pub async fn connect(url: &str, pool: usize) -> anyhow::Result<Arc<dyn Backend>> {
    let cfg = RedisConfig::new(url)
        .prefix(PREFIX)
        .pool_size(pool)
        .max_stream_length(MAX_STREAM_LEN.load(Ordering::Relaxed) as usize);
    Ok(Arc::new(RedisBackend::connect(cfg).await?))
}

pub fn queue_on(backend: Arc<dyn Backend>, name: &str, defaults: JobOptions) -> Queue<LoadJob> {
    Queue::from_backend(
        name,
        backend,
        QueueOptions::default()
            .prefix(PREFIX)
            .default_job_options(defaults),
    )
    .expect("harness queue names are valid")
}

fn removal_for(r: Retention) -> Removal {
    match r {
        Retention::Keep | Retention::Mixed => Removal::Keep,
        Retention::Remove => Removal::Remove,
        Retention::Last1000 => Removal::KeepLast(1000),
        Retention::For30s => Removal::KeepFor(Duration::from_secs(30)),
    }
}

fn backoff_for(cfg: &Config) -> Backoff {
    if cfg.fail_rate > 0.0 {
        Backoff::Exponential {
            initial: Duration::from_millis(50),
            max: Some(Duration::from_secs(1)),
        }
    } else {
        Backoff::Fixed {
            delay: Duration::from_millis(100),
        }
    }
}

/// Queue-level defaults for `--options-at-queue` (empty otherwise).
pub fn queue_defaults(cfg: &Config) -> JobOptions {
    if !cfg.options_at_queue {
        return JobOptions::new();
    }
    let removal = removal_for(cfg.retention);
    JobOptions::new()
        .attempts(cfg.attempts)
        .backoff(backoff_for(cfg))
        .remove_on_complete(removal)
        .remove_on_fail(removal)
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
    let postpone_ms = if rng.gen_bool(cfg.postpone_rate.clamp(0.0, 1.0)) {
        rng.gen_range(50..=300)
    } else {
        0
    };
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
    let removal = removal_for(match cfg.retention {
        Retention::Mixed => [
            Retention::Keep,
            Retention::Remove,
            Retention::Last1000,
            Retention::For30s,
        ][rng.gen_range(0..4)],
        r => r,
    });
    let job = LoadJob {
        seq,
        enq_us: 0, // stamped at submit time
        delay_ms,
        fail_attempts,
        fatal,
        work_us,
        postpone_ms,
        panic: fail_attempts > 0 && rng.gen_bool(cfg.panic_rate.clamp(0.0, 1.0)),
    };
    let mut opts = if cfg.options_at_queue {
        JobOptions::new()
    } else {
        JobOptions::new()
            .attempts(cfg.attempts)
            .backoff(backoff_for(cfg))
            .remove_on_complete(removal)
            .remove_on_fail(removal)
    };
    if priority > 0 || !cfg.options_at_queue {
        opts = opts.priority(priority);
    }
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
        // Claim `bulk` consecutive seqs (1 = single adds).
        let n = cfg.bulk.max(1) as u64;
        let first = next.fetch_add(n, Ordering::Relaxed);
        let last = if cfg.duration.is_none() {
            (first + n).min(cfg.jobs)
        } else {
            first + n
        };
        if first >= last {
            break;
        }
        if let Some(iv) = interval {
            next_at += iv * (last - first) as u32;
            let now = Instant::now();
            if next_at > now {
                tokio::time::sleep(next_at - now).await;
            } else if now - next_at > Duration::from_secs(1) {
                next_at = now; // fell far behind; don't burst to catch up
            }
        }
        // Retry the same seqs until they land so the ledger never has
        // holes. A failed `add` may still have been applied server-side
        // (reply lost); the ledger would then see unknown job ids in the
        // stream, which the observer ignores, and these seqs get new jobs.
        loop {
            let planned: Vec<(LoadJob, JobOptions)> = (first..last)
                .map(|seq| {
                    let (mut job, opts) = plan(&cfg, seq, seed);
                    job.enq_us = now_us();
                    (job, opts)
                })
                .collect();
            let jobs: Vec<LoadJob> = planned.iter().map(|(j, _)| j.clone()).collect();
            let t0 = Instant::now();
            let result = if planned.len() == 1 {
                let (job, opts) = planned.into_iter().next().expect("one job");
                queue.add(job, opts).await.map(|id| vec![id])
            } else {
                queue.add_bulk(planned).await
            };
            match result {
                Ok(ids) => {
                    stats.add_latency.record(t0.elapsed().as_micros() as u64);
                    stats.added.fetch_add(ids.len() as u64, Ordering::Relaxed);
                    let at = now_us();
                    for (job, id) in jobs.into_iter().zip(ids) {
                        ledger.added(job, id.to_string(), at);
                    }
                    break;
                }
                Err(e) => {
                    let k = stats.add_errors.fetch_add(1, Ordering::Relaxed);
                    if k.is_multiple_of(100) {
                        stats.note(format!("add failed (#{}): {e}", k + 1));
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
            let q = queue_on(backend, &self.queue_name, JobOptions::new());
            let cancel = CancellationToken::new();
            let index = self.generation * 1000 + i as u32;
            let spec = WorkerSpec {
                index,
                concurrency: self.cfg.concurrency,
                lock_ms: self.cfg.lock_ms,
                drain_delay_ms: self.cfg.drain_delay_ms,
                shutdown_timeout_ms: self.cfg.shutdown_timeout_ms,
                max_stalled: self.cfg.max_stalled,
                ctx_ops: self.cfg.ctx_ops,
                rate_limit: self.cfg.rate_limit,
            };
            let w = build_worker(
                q,
                spec,
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
            .arg(self.cfg.lock_ms.to_string())
            .arg("--max-stream-len")
            .arg(self.cfg.max_stream_len.to_string())
            .arg("--max-stalled")
            .arg(self.cfg.max_stalled.to_string());
        if self.cfg.ctx_ops {
            cmd.arg("--ctx-ops");
        }
        if let Some((max, per_ms)) = self.cfg.rate_limit {
            cmd.arg("--rate-limit").arg(format!("{max}/{per_ms}"));
        }
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
pub async fn child_worker_main(url: String, queue: String, spec: WorkerSpec) -> anyhow::Result<()> {
    let backend = connect(&url, spec.concurrency + 4).await?;
    let q = queue_on(backend, &queue, JobOptions::new());
    let sink = Sink::Stdout(Arc::new(Mutex::new(std::io::stdout())));
    let w = build_worker(q, spec, sink, CancellationToken::new());
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
                    let mut event = get("event").unwrap_or_default();
                    // A worker shutting down hands unfinished jobs back:
                    // `waiting` with prev=active is a release, not an add.
                    if event == "waiting" && get("prev").as_deref() == Some("active") {
                        event = "released".to_string();
                    }
                    // A job that ran out of stalls fails with a `stalled`
                    // count instead of emitting a `stalled` event.
                    if event == "failed" && get("stalled").is_some() {
                        event = "stall-failed".to_string();
                    }
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

/// Consume the queue through oxn's own subscriber, counting events by the
/// same names the raw observer uses (`released` for waiting-after-active).
pub async fn oxn_subscriber(
    mut records: oxn::events::RecordStream,
    counts: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
    errors: Arc<AtomicU64>,
    stop: CancellationToken,
) {
    use futures::StreamExt;
    loop {
        let next = tokio::select! {
            _ = stop.cancelled() => break,
            n = records.next() => n,
        };
        match next {
            None => break,
            Some(Err(_)) => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
            Some(Ok(r)) => {
                let name = match &r.event {
                    Event::Waiting { prev: Some(p), .. } if p == "active" => "released".to_string(),
                    e => serde_json::to_value(e)
                        .ok()
                        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_string))
                        .unwrap_or_else(|| "unknown".into()),
                };
                *counts.lock().entry(name).or_default() += 1;
            }
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
                attempt: 2,
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
