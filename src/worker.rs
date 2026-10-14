//! [`Worker<D, R>`] — the consumer side.
//!
//! Rather than a class with dozens of knobs, a worker is a small struct that
//! holds an `Arc<Backend>`, a handler closure and some timers. All async
//! control flow is expressed with `tokio::select!` — no internal event
//! emitter, no callback soup.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::backend::{Backend, Failure, Fetched};
use crate::error::{Error, Result};
use crate::job::Job;
use crate::options::WorkerOptions;
use crate::queue::{Queue, decode_job};

/// Boxed future returned by a handler.
pub type HandlerFuture<R> = Pin<Box<dyn Future<Output = Result<R>> + Send + 'static>>;

type HandlerFn<D, R> = Arc<dyn Fn(Job<D>, JobContext) -> HandlerFuture<R> + Send + Sync + 'static>;

/// Per-job handle given to handlers built with [`Worker::builder_with_ctx`].
///
/// Cheap to clone. Lets a handler report progress, write log lines, learn
/// that its lock was lost (another worker or the stalled scanner now owns
/// the job, so its result will be discarded), and wind down cooperatively
/// when the worker is shutting down.
#[derive(Clone)]
pub struct JobContext {
    inner: Arc<CtxInner>,
}

struct CtxInner {
    backend: Arc<dyn Backend>,
    queue: String,
    id: crate::job::JobId,
    attempt: u32,
    lock_lost: CancellationToken,
    shutdown: CancellationToken,
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("queue", &self.inner.queue)
            .field("job_id", &self.inner.id)
            .field("attempt", &self.inner.attempt)
            .field("lock_lost", &self.lock_lost())
            .field("shutdown_requested", &self.shutdown_requested())
            .finish()
    }
}

impl JobContext {
    /// The job's id.
    pub fn job_id(&self) -> &crate::job::JobId {
        &self.inner.id
    }

    /// The queue the job belongs to.
    pub fn queue(&self) -> &str {
        &self.inner.queue
    }

    /// Which attempt this is (1-based; postponements and stalls don't
    /// count).
    pub fn attempt(&self) -> u32 {
        self.inner.attempt
    }

    /// Record progress (a percentage via `u8`/`f64`, or any
    /// [`crate::Progress`]); emits a `progress` event.
    pub async fn progress(&self, progress: impl Into<crate::Progress>) -> Result<()> {
        let json = serde_json::to_string(&progress.into())?;
        self.inner
            .backend
            .update_progress(&self.inner.queue, &self.inner.id, &json)
            .await
    }

    /// Append a line to the job's log (shown in the dashboard).
    pub async fn log(&self, line: impl AsRef<str>) -> Result<()> {
        self.inner
            .backend
            .log(&self.inner.queue, &self.inner.id, line.as_ref())
            .await
    }

    /// `true` once a lock renewal found the lock gone or held by another
    /// token: this worker no longer owns the job and its outcome will be
    /// discarded. Stop expensive work.
    pub fn lock_lost(&self) -> bool {
        self.inner.lock_lost.is_cancelled()
    }

    /// Resolves when [`Self::lock_lost`] becomes true.
    pub async fn lock_lost_signal(&self) {
        self.inner.lock_lost.cancelled().await;
    }

    /// `true` once the worker was asked to shut down. The handler may keep
    /// going until `shutdown_timeout`, but should checkpoint and return.
    pub fn shutdown_requested(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    /// Resolves when [`Self::shutdown_requested`] becomes true.
    pub async fn cancelled(&self) {
        self.inner.shutdown.cancelled().await;
    }
}

/// The two shutdown signals a running job observes.
#[derive(Clone)]
struct Signals {
    /// Worker cancellation: stop fetching, tell handlers to wind down.
    shutdown: CancellationToken,
    /// `shutdown_timeout` elapsed: drop handlers and release their jobs.
    abort: CancellationToken,
}

/// A worker polling a queue and dispatching jobs to a handler.
pub struct Worker<D, R = ()>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    id: String,
    queue: String,
    backend: Arc<dyn Backend>,
    opts: WorkerOptions,
    handler: HandlerFn<D, R>,
    cancel: CancellationToken,
    _data: PhantomData<fn() -> (D, R)>,
}

impl<D, R> std::fmt::Debug for Worker<D, R>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("queue", &self.queue)
            .field("concurrency", &self.opts.concurrency)
            .finish_non_exhaustive()
    }
}

impl<D, R> Worker<D, R>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    /// Start a worker builder bound to `queue` with the given async handler.
    ///
    /// The handler is any `Fn(Job<D>) -> Future<Output = Result<R, Error>>`;
    /// it will be called once per fetched job, concurrently up to
    /// [`WorkerOptions::concurrency`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxn::{Queue, Worker, Job, Error};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Email { to: String }
    /// # async fn demo(queue: Queue<Email>) -> oxn::Result<()> {
    /// let worker = Worker::builder(queue, |job: Job<Email>| async move {
    ///     println!("to: {}", job.data.to);
    ///     Ok::<_, Error>(())
    /// })
    /// .concurrency(8)
    /// .build();
    ///
    /// worker.run().await
    /// # }
    /// ```
    pub fn builder<F, Fut>(queue: Queue<D>, handler: F) -> WorkerBuilder<D, R>
    where
        F: Fn(Job<D>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        WorkerBuilder::from_handler(Arc::new(move |job, _ctx| handler(job).boxed()), queue)
    }

    /// Like [`Self::builder`], but the handler also receives a
    /// [`JobContext`] for progress, logs, lost-lock and shutdown signals.
    ///
    /// ```no_run
    /// # use oxn::{Queue, Worker, Job, Error};
    /// # use oxn::worker::JobContext;
    /// # async fn demo(queue: Queue<String>) -> oxn::Result<()> {
    /// let worker = Worker::builder_with_ctx(queue, |job: Job<String>, ctx: JobContext| async move {
    ///     ctx.log(format!("processing {}", job.data)).await?;
    ///     for pct in [25u8, 50, 75, 100] {
    ///         if ctx.shutdown_requested() || ctx.lock_lost() {
    ///             break; // checkpoint and return early
    ///         }
    ///         ctx.progress(pct).await?;
    ///     }
    ///     Ok::<_, Error>(())
    /// })
    /// .build();
    /// worker.run().await
    /// # }
    /// ```
    pub fn builder_with_ctx<F, Fut>(queue: Queue<D>, handler: F) -> WorkerBuilder<D, R>
    where
        F: Fn(Job<D>, JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        WorkerBuilder::from_handler(Arc::new(move |job, ctx| handler(job, ctx).boxed()), queue)
    }

    /// Worker id, used as the prefix for lock tokens.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// A [`CancellationToken`] that signals shutdown. Stash the child token
    /// you cloned pre-`run` to signal graceful shutdown from elsewhere.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Drive the worker until its [`CancellationToken`] fires, then shut
    /// down gracefully.
    ///
    /// When the token fires the worker stops taking new jobs (an idle
    /// worker stops waiting immediately) and lets running handlers finish,
    /// renewing their locks, for up to [`WorkerOptions::shutdown_timeout`].
    /// Handlers still running after that are dropped and their jobs are
    /// **released** back to the queue — attempt refunded, priority and
    /// pause state respected — rather than failed. The stalled-job scanner
    /// is stopped and joined before this future resolves.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxn::{Queue, Worker, Job, Error};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Task;
    /// # async fn demo(queue: Queue<Task>) -> oxn::Result<()> {
    /// let worker = Worker::builder(queue, |_: Job<Task>| async { Ok::<_, Error>(()) })
    ///     .concurrency(4)
    ///     .build();
    ///
    /// let cancel = worker.cancellation_token();
    /// tokio::spawn(async move {
    ///     tokio::signal::ctrl_c().await.ok();
    ///     cancel.cancel();
    /// });
    ///
    /// worker.run().await
    /// # }
    /// ```
    pub async fn run(self) -> Result<()> {
        let Self {
            id,
            queue,
            backend,
            opts,
            handler,
            cancel,
            _data,
        } = self;
        crate::telemetry::describe();

        let semaphore = Arc::new(Semaphore::new(opts.concurrency));
        let token_seq = Arc::new(std::sync::atomic::AtomicU64::new(0));
        // `cancel` (the caller's token) means "stop taking new jobs".
        // `abort` means "stop running handlers"; it only fires once
        // `shutdown_timeout` has elapsed with handlers still running.
        let abort = CancellationToken::new();

        // Stalled-job scanner.
        let scanner = opts.run_stalled_scanner.then(|| {
            let backend = backend.clone();
            let queue = queue.clone();
            let interval = opts.stalled_interval;
            let max_stalled = opts.max_stalled;
            let c = cancel.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = c.cancelled() => break,
                        _ = tick.tick() => {
                            match backend.scan_stalled(&queue, max_stalled).await {
                                Ok(ids) => crate::telemetry::stalled(&queue, ids.len()),
                                Err(e) => tracing::warn!(queue = %queue, error = %e, "stalled scan failed"),
                            }
                        }
                    }
                }
            })
        });

        // Main fetch loop.
        'main: loop {
            let permit = tokio::select! {
                _ = cancel.cancelled() => break 'main,
                permit = semaphore.clone().acquire_owned() => match permit {
                    Ok(p) => p,
                    Err(_) => break 'main,
                },
            };
            let token = format!(
                "{}:{}",
                id,
                token_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            let fetch_span = tracing::debug_span!(
                "oxn.fetch",
                queue = %queue,
                worker = %id,
                token = %token,
                job_id = tracing::field::Empty,
                outcome = tracing::field::Empty,
            );
            // Not raced against `cancel`: the backend honors it only while
            // waiting, so a job it already took is never dropped on the
            // floor (it would stay locked until the stalled scanner).
            let fetched = backend
                .fetch_next(
                    &queue,
                    &token,
                    opts.lock_duration,
                    opts.drain_delay,
                    &cancel,
                    opts.rate_limit,
                )
                .instrument(fetch_span.clone())
                .await;
            match fetched {
                Ok(Some(Fetched { job: raw, token })) if cancel.is_cancelled() => {
                    // Shutdown began while this fetch was taking the job:
                    // give it back untouched rather than start new work.
                    fetch_span.record("job_id", raw.id.as_str());
                    fetch_span.record("outcome", "released-on-shutdown");
                    let r = backend.release(&queue, &raw.id, &token).await;
                    log_transition(&queue, "released", &r);
                    drop(permit);
                    break 'main;
                }
                Ok(Some(Fetched { job: raw, token })) => {
                    fetch_span.record("job_id", raw.id.as_str());
                    fetch_span.record("outcome", "job");
                    let backend = backend.clone();
                    let queue = queue.clone();
                    let handler = handler.clone();
                    let opts = opts.clone();
                    let signals = Signals {
                        shutdown: cancel.clone(),
                        abort: abort.clone(),
                    };
                    tokio::spawn(async move {
                        process_one::<D, R>(backend, queue, raw, token, handler, opts, signals)
                            .await;
                        drop(permit);
                    });
                }
                Ok(None) => {
                    fetch_span.record(
                        "outcome",
                        if cancel.is_cancelled() {
                            "cancelled"
                        } else {
                            "idle"
                        },
                    );
                    drop(permit);
                }
                Err(e) => {
                    fetch_span.record("outcome", "error");
                    crate::telemetry::fetch_error(&queue);
                    tracing::warn!(queue = %queue, worker = %id, error = %e, "fetch_next failed");
                    drop(permit);
                    tokio::select! {
                        _ = cancel.cancelled() => break 'main,
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                }
            }
        }

        // Graceful shutdown. Stop fetching (done), let in-flight handlers
        // finish — their locks keep being renewed — for up to
        // `shutdown_timeout`, then abort the rest. Aborted jobs are released
        // back to the queue (attempt refunded), never failed.
        let total = opts.concurrency as u32;
        let in_flight = total as usize - semaphore.available_permits();
        tracing::debug!(queue = %queue, worker = %id, in_flight, "oxn.shutdown: waiting for in-flight jobs");
        let started = std::time::Instant::now();
        let drained = tokio::time::timeout(opts.shutdown_timeout, semaphore.acquire_many(total))
            .await
            .is_ok();
        let aborted = total as usize - semaphore.available_permits();
        if !drained {
            tracing::warn!(
                queue = %queue,
                worker = %id,
                aborted,
                "oxn.shutdown: shutdown_timeout elapsed; aborting and releasing remaining jobs"
            );
            abort.cancel();
            // Releasing is a single short script per job; bound it anyway.
            let _ = tokio::time::timeout(RELEASE_GRACE, semaphore.acquire_many(total)).await;
        }
        if let Some(scanner) = scanner {
            let _ = scanner.await;
        }
        tracing::debug!(
            queue = %queue,
            worker = %id,
            drained,
            aborted = if drained { 0 } else { aborted },
            still_running = total as usize - semaphore.available_permits(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "oxn.shutdown: done"
        );
        Ok(())
    }
}

/// After aborting handlers at shutdown, how long to wait for their jobs to
/// be released before `run()` returns anyway.
const RELEASE_GRACE: Duration = Duration::from_secs(5);

async fn process_one<D, R>(
    backend: Arc<dyn Backend>,
    queue: String,
    raw: crate::backend::RawJob,
    token: String,
    handler: HandlerFn<D, R>,
    opts: WorkerOptions,
    signals: Signals,
) where
    D: DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    let span = tracing::debug_span!(
        "oxn.process",
        queue = %queue,
        job_id = %raw.id,
        attempt = raw.attempts_made,
        attempts_total = raw.opts.attempts,
        token = %token,
        outcome = tracing::field::Empty,
        transition = tracing::field::Empty,
        elapsed_us = tracing::field::Empty,
    );
    let started = std::time::Instant::now();
    let wait = (raw.attempts_made <= 1).then(|| {
        let due = raw.timestamp_ms + raw.opts.delay.map_or(0, |d| d.as_millis() as i64);
        Duration::from_millis((chrono_like_now_ms() - due).max(0) as u64)
    });
    crate::telemetry::started(&queue, wait);
    let q = queue.clone();
    process_one_inner(
        backend,
        queue,
        raw,
        token,
        handler,
        opts,
        signals,
        span.clone(),
    )
    .instrument(span.clone())
    .await;
    crate::telemetry::finished(&q);
    span.record("elapsed_us", started.elapsed().as_micros() as u64);
}

#[allow(clippy::too_many_arguments)]
async fn process_one_inner<D, R>(
    backend: Arc<dyn Backend>,
    queue: String,
    raw: crate::backend::RawJob,
    token: String,
    handler: HandlerFn<D, R>,
    opts: WorkerOptions,
    signals: Signals,
    span: tracing::Span,
) where
    D: DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    let id = raw.id.clone();
    let attempts_made = raw.attempts_made;
    let attempts_total = raw.opts.attempts;
    let backoff = raw.opts.backoff;
    // Snapshot the retention policies up front — we pass them to
    // `complete()` / `fail()` so the backend doesn't need an extra round
    // trip to re-read the job hash for them.
    let remove_on_complete = raw.opts.remove_on_complete;
    let remove_on_fail = raw.opts.remove_on_fail;
    let job: Job<D> = match decode_job::<D>(raw) {
        Ok(j) => j,
        Err(e) => {
            span.record("outcome", "decode-error");
            span.record("transition", "failed");
            tracing::warn!(error = %e, "payload decode failed");
            let reason = format!("decode error: {e}");
            let trace = format!("attempt {attempts_made}: {reason}");
            let r = backend
                .fail(
                    &queue,
                    &id,
                    &token,
                    Failure::new(&reason).removal(remove_on_fail).trace(&trace),
                )
                .await;
            log_transition(&queue, "failed", &r);
            return;
        }
    };

    let mut renew = tokio::time::interval(opts.lock_renew_interval);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renew.tick().await; // consume the zero-tick

    let lock_lost = CancellationToken::new();
    let ctx = JobContext {
        inner: Arc::new(CtxInner {
            backend: backend.clone(),
            queue: queue.clone(),
            id: id.clone(),
            attempt: attempts_made,
            lock_lost: lock_lost.clone(),
            shutdown: signals.shutdown.clone(),
        }),
    };
    let abort = signals.abort;
    tracing::trace!("handler started");
    let ran = std::time::Instant::now();
    // A panic — while building the future or while polling it — becomes
    // `Error::Panicked`, handled like any returned error. Uncaught, it would
    // unwind this task and leave the job locked until its lock expired.
    let mut handler_fut: BoxFuture<'static, Result<R>> = match std::panic::catch_unwind(
        std::panic::AssertUnwindSafe(|| (handler)(job, ctx.clone())),
    ) {
        Ok(fut) => std::panic::AssertUnwindSafe(fut)
            .catch_unwind()
            .map(|r| r.unwrap_or_else(|p| Err(Error::Panicked(panic_message(p.as_ref())))))
            .boxed(),
        Err(p) => {
            let msg = panic_message(p.as_ref());
            futures::future::ready(Err(Error::Panicked(msg))).boxed()
        }
    };
    let outcome: Result<R> = loop {
        tokio::select! {
            biased;
            _ = abort.cancelled() => {
                // shutdown_timeout elapsed: drop the handler and hand the
                // job back. It did not fail; it was interrupted.
                drop(handler_fut);
                crate::telemetry::ran(&queue, "aborted", ran.elapsed());
                span.record("outcome", "aborted");
                span.record("transition", "released");
                tracing::debug!("handler aborted by shutdown_timeout; releasing job");
                let r = backend.release(&queue, &id, &token).await;
                log_transition(&queue, "released", &r);
                return;
            }
            _ = renew.tick(), if !lock_lost.is_cancelled() => {
                match backend.extend_lock(&queue, &id, &token, opts.lock_duration).await {
                    Ok(()) => tracing::trace!("lock extended"),
                    Err(e) if e.is_lock_lost() => {
                        // Someone else owns the job now; renewing is pointless
                        // and the handler should know its result won't count.
                        tracing::warn!(job = %id, error = %e, "lock lost; signalling handler");
                        lock_lost.cancel();
                    }
                    Err(e) => tracing::warn!(job = %id, error = %e, "lock renew failed"),
                }
            }
            r = &mut handler_fut => break r,
        }
    };
    crate::telemetry::ran(&queue, outcome_label(&outcome), ran.elapsed());
    span.record("outcome", outcome_label(&outcome));
    tracing::debug!(outcome = outcome_label(&outcome), "handler finished");
    let trace = outcome
        .as_ref()
        .err()
        .map(|e| failure_trace(attempts_made, e));
    let trace = trace.as_deref();

    match outcome {
        Ok(ret) => {
            let json = serde_json::to_string(&ret).ok();
            span.record("transition", "completed");
            let r = backend
                .complete(&queue, &id, &token, json, remove_on_complete)
                .await;
            log_transition(&queue, "completed", &r);
        }
        // Postponements, not failures: the attempt is refunded and no
        // failedReason is recorded.
        Err(Error::Delayed { delay_ms }) | Err(Error::RateLimited { delay_ms }) => {
            let until = chrono_like_now_ms() + delay_ms as i64;
            span.record("transition", "postponed");
            let r = backend.delay(&queue, &id, &token, until).await;
            log_transition(&queue, "postponed", &r);
        }
        Err(Error::Unrecoverable(msg)) => {
            span.record("transition", "failed");
            let r = backend
                .fail(
                    &queue,
                    &id,
                    &token,
                    failure(&msg, None, remove_on_fail, trace),
                )
                .await;
            log_transition(&queue, "failed", &r);
        }
        Err(err) => {
            // `attempts_made` has already been incremented by `fetch_next`
            // before the handler ran, so it counts the current attempt.
            // We still have another attempt if `attempts_made < total`.
            if attempts_made < attempts_total {
                let delay = backoff
                    .map(|b| b.compute(attempts_made))
                    .unwrap_or(Duration::ZERO);
                let retry_at = chrono_like_now_ms() + delay.as_millis() as i64;
                span.record("transition", "retry-delayed");
                tracing::debug!(backoff_ms = delay.as_millis() as u64, error = %err, "scheduling retry");
                let r = backend
                    .fail(
                        &queue,
                        &id,
                        &token,
                        failure(&err.to_string(), Some(retry_at), remove_on_fail, trace),
                    )
                    .await;
                log_transition(&queue, "retry-delayed", &r);
            } else {
                span.record("transition", "failed");
                let r = backend
                    .fail(
                        &queue,
                        &id,
                        &token,
                        failure(&err.to_string(), None, remove_on_fail, trace),
                    )
                    .await;
                log_transition(&queue, "failed", &r);
            }
        }
    }
}

/// The [`Failure`] for one attempt: retry at `retry_at` or fail for good.
fn failure<'a>(
    reason: &'a str,
    retry_at: Option<i64>,
    removal: crate::options::Removal,
    trace: Option<&'a str>,
) -> Failure<'a> {
    let mut f = Failure::new(reason).removal(removal);
    f.retry_at_ms = retry_at;
    f.trace = trace;
    f
}

/// Longest stack-trace entry stored for one attempt.
const MAX_TRACE_BYTES: usize = 4096;

/// `attempt N: <error>` plus one `  caused by: …` line per source, capped
/// at [`MAX_TRACE_BYTES`].
fn failure_trace(attempt: u32, err: &Error) -> String {
    use std::fmt::Write as _;
    let mut out = format!("attempt {attempt}: {err}");
    let mut src = std::error::Error::source(err);
    // `Error::Handler` displays its inner error already; start below it.
    if let (Error::Handler(_), Some(s)) = (err, src) {
        src = s.source();
    }
    while let Some(s) = src {
        let _ = write!(out, "\n  caused by: {s}");
        src = s.source();
    }
    if out.len() > MAX_TRACE_BYTES {
        let mut cut = MAX_TRACE_BYTES;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
        out.push('…');
    }
    out
}

/// The message a panic carried, as `panic!` formats it.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "(non-string panic payload)".to_string()
    }
}

fn outcome_label<R>(r: &Result<R>) -> &'static str {
    match r {
        Err(Error::Panicked(_)) => "panicked",
        Ok(_) => "ok",
        Err(Error::Delayed { .. }) => "delayed",
        Err(Error::RateLimited { .. }) => "rate-limited",
        Err(Error::Unrecoverable(_)) => "unrecoverable",
        Err(Error::Closed) => "cancelled",
        Err(_) => "error",
    }
}

/// Report how the backend answered a state transition.
///
/// A lost lock is expected under contention (the stalled scanner or another
/// worker took the job over) and is a warning: this worker's outcome is
/// discarded and the job's new owner decides its fate. Anything else is an
/// error — the outcome was not recorded and nobody else owns the job yet.
fn log_transition(queue: &str, to: &'static str, r: &Result<()>) {
    crate::telemetry::transition(queue, to, r);
    match r {
        Ok(()) => tracing::debug!(to, "transition applied"),
        Err(e) if e.is_lock_lost() => tracing::warn!(
            to,
            error = %e,
            "transition rejected: lock lost, outcome discarded (the job now belongs to another worker or the stalled scanner)"
        ),
        Err(e) => tracing::error!(to, error = %e, "transition rejected"),
    }
}

fn chrono_like_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Builder returned by [`Worker::builder`].
pub struct WorkerBuilder<D, R = ()>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    queue: Queue<D>,
    opts: WorkerOptions,
    handler: HandlerFn<D, R>,
    cancel: Option<CancellationToken>,
    id: Option<String>,
}

impl<D, R> std::fmt::Debug for WorkerBuilder<D, R>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("queue", &self.queue.name())
            .finish_non_exhaustive()
    }
}

impl<D, R> WorkerBuilder<D, R>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + 'static,
{
    fn from_handler(handler: HandlerFn<D, R>, queue: Queue<D>) -> Self {
        Self {
            queue,
            opts: WorkerOptions::default(),
            handler,
            cancel: None,
            id: None,
        }
    }

    /// Replace the full [`WorkerOptions`] bag.
    #[must_use]
    pub fn options(mut self, opts: WorkerOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Maximum concurrent in-flight jobs. Floored at 1.
    #[must_use]
    pub fn concurrency(mut self, n: usize) -> Self {
        self.opts.concurrency = n.max(1);
        self
    }

    /// How long each job's lock is valid. Must exceed
    /// [`Self::lock_renew_interval`].
    #[must_use]
    pub fn lock_duration(mut self, d: Duration) -> Self {
        self.opts.lock_duration = d;
        self
    }

    /// How often the worker renews in-flight job locks. Typically half of
    /// [`Self::lock_duration`].
    #[must_use]
    pub fn lock_renew_interval(mut self, d: Duration) -> Self {
        self.opts.lock_renew_interval = d;
        self
    }

    /// Cadence of the stalled-job scanner.
    #[must_use]
    pub fn stalled_interval(mut self, d: Duration) -> Self {
        self.opts.stalled_interval = d;
        self
    }

    /// Longest an idle worker blocks waiting for work before re-checking.
    ///
    /// This is a safety net, not a polling interval: new, promoted,
    /// retried and resumed jobs wake blocked workers immediately, and a
    /// worker never blocks past the due time of the next delayed job.
    /// Default 5s; `0` is treated as 1ms.
    #[must_use]
    pub fn drain_delay(mut self, d: Duration) -> Self {
        self.opts.drain_delay = d;
        self
    }

    /// How many times a job may stall (its lock lapsed: the worker died,
    /// hung past `lock_duration`, or lost Redis) and still be retried. A
    /// stalled job goes back to the head of its queue without using up one
    /// of its `attempts`; one more stall than this fails it into `failed`
    /// (reason "job stalled more than allowable limit", with its
    /// `remove_on_fail` policy applied). Default 1.
    #[must_use]
    pub fn max_stalled(mut self, n: u32) -> Self {
        self.opts.max_stalled = n;
        self
    }

    /// Disable the scanner if another process handles housekeeping.
    #[must_use]
    pub fn run_stalled_scanner(mut self, yes: bool) -> Self {
        self.opts.run_stalled_scanner = yes;
        self
    }

    /// Apply a rate limit of at most `max` jobs per `per` window.
    #[must_use]
    pub fn rate_limit(mut self, max: u32, per: Duration) -> Self {
        self.opts.rate_limit = Some(crate::options::RateLimit { max, per });
        self
    }

    /// How long [`Worker::run`] lets in-flight handlers finish after
    /// cancellation; jobs still running then are released back to the
    /// queue, not failed.
    #[must_use]
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.opts.shutdown_timeout = d;
        self
    }

    /// Inject an existing [`CancellationToken`] so multiple workers can
    /// share a shutdown signal.
    #[must_use]
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Override the worker's id (otherwise a random UUID).
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Build the worker. Does **not** start the loop — call [`Worker::run`].
    pub fn build(self) -> Worker<D, R> {
        Worker {
            id: self.id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            queue: self.queue.name().to_string(),
            backend: self.queue.backend(),
            opts: self.opts,
            handler: self.handler,
            cancel: self.cancel.unwrap_or_default(),
            _data: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_messages_are_extracted_like_panic_formats_them() {
        let msg = |f: fn()| {
            let p = std::panic::catch_unwind(f).unwrap_err();
            panic_message(p.as_ref())
        };
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // keep test output quiet
        assert_eq!(msg(|| panic!("static")), "static");
        assert_eq!(msg(|| panic!("formatted {}", 7)), "formatted 7");
        assert_eq!(
            msg(|| std::panic::panic_any(1u8)),
            "(non-string panic payload)"
        );
        std::panic::set_hook(prev);
        assert_eq!(
            Error::Panicked("x".into()).to_string(),
            "handler panicked: x"
        );
        assert_eq!(
            outcome_label::<()>(&Err(Error::Panicked("x".into()))),
            "panicked"
        );
    }

    #[test]
    fn failure_traces_follow_the_source_chain_and_are_bounded() {
        #[derive(Debug)]
        struct E(&'static str, Option<Box<E>>);
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for E {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|e| e as _)
            }
        }
        let err = Error::handler(E(
            "outer",
            Some(Box::new(E("middle", Some(Box::new(E("root", None)))))),
        ));
        assert_eq!(
            failure_trace(2, &err),
            "attempt 2: handler error: outer\n  caused by: middle\n  caused by: root"
        );
        assert_eq!(
            failure_trace(1, &Error::unrecoverable("x")),
            "attempt 1: unrecoverable: x"
        );
        let long = failure_trace(1, &Error::unrecoverable("é".repeat(5000)));
        assert!(long.len() <= MAX_TRACE_BYTES + '…'.len_utf8());
        assert!(long.ends_with('…'));
    }
}
