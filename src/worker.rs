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

use crate::backend::{Backend, Fetched};
use crate::error::{Error, Result};
use crate::job::Job;
use crate::options::WorkerOptions;
use crate::queue::{Queue, decode_job};

/// Boxed future returned by a handler.
pub type HandlerFuture<R> = Pin<Box<dyn Future<Output = Result<R>> + Send + 'static>>;

type HandlerFn<D, R> = Arc<dyn Fn(Job<D>) -> HandlerFuture<R> + Send + Sync + 'static>;

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
        WorkerBuilder::new(queue, handler)
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
                            if let Err(e) = backend.scan_stalled(&queue, max_stalled).await {
                                tracing::warn!(queue = %queue, error = %e, "stalled scan failed");
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
                    log_transition("released", &r);
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
                    let abort = abort.clone();
                    tokio::spawn(async move {
                        process_one::<D, R>(backend, queue, raw, token, handler, opts, abort).await;
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
    abort: CancellationToken,
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
    process_one_inner(
        backend,
        queue,
        raw,
        token,
        handler,
        opts,
        abort,
        span.clone(),
    )
    .instrument(span.clone())
    .await;
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
    abort: CancellationToken,
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
            let r = backend
                .fail(
                    &queue,
                    &id,
                    &token,
                    &format!("decode error: {e}"),
                    None,
                    remove_on_fail,
                )
                .await;
            log_transition("failed", &r);
            return;
        }
    };

    let mut renew = tokio::time::interval(opts.lock_renew_interval);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renew.tick().await; // consume the zero-tick

    tracing::trace!("handler started");
    let mut handler_fut: BoxFuture<'static, Result<R>> = (handler)(job).boxed();
    let outcome: Result<R> = loop {
        tokio::select! {
            biased;
            _ = abort.cancelled() => {
                // shutdown_timeout elapsed: drop the handler and hand the
                // job back. It did not fail; it was interrupted.
                drop(handler_fut);
                span.record("outcome", "aborted");
                span.record("transition", "released");
                tracing::debug!("handler aborted by shutdown_timeout; releasing job");
                let r = backend.release(&queue, &id, &token).await;
                log_transition("released", &r);
                return;
            }
            _ = renew.tick() => {
                match backend.extend_lock(&queue, &id, &token, opts.lock_duration).await {
                    Ok(()) => tracing::trace!("lock extended"),
                    Err(e) => tracing::warn!(job = %id, error = %e, "lock renew failed"),
                }
            }
            r = &mut handler_fut => break r,
        }
    };
    span.record("outcome", outcome_label(&outcome));
    tracing::debug!(outcome = outcome_label(&outcome), "handler finished");

    match outcome {
        Ok(ret) => {
            let json = serde_json::to_string(&ret).ok();
            span.record("transition", "completed");
            let r = backend
                .complete(&queue, &id, &token, json, remove_on_complete)
                .await;
            log_transition("completed", &r);
        }
        // Postponements, not failures: the attempt is refunded and no
        // failedReason is recorded.
        Err(Error::Delayed { delay_ms }) | Err(Error::RateLimited { delay_ms }) => {
            let until = chrono_like_now_ms() + delay_ms as i64;
            span.record("transition", "postponed");
            let r = backend.delay(&queue, &id, &token, until).await;
            log_transition("postponed", &r);
        }
        Err(Error::Unrecoverable(msg)) => {
            span.record("transition", "failed");
            let r = backend
                .fail(&queue, &id, &token, &msg, None, remove_on_fail)
                .await;
            log_transition("failed", &r);
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
                        &err.to_string(),
                        Some(retry_at),
                        remove_on_fail,
                    )
                    .await;
                log_transition("retry-delayed", &r);
            } else {
                span.record("transition", "failed");
                let r = backend
                    .fail(&queue, &id, &token, &err.to_string(), None, remove_on_fail)
                    .await;
                log_transition("failed", &r);
            }
        }
    }
}

fn outcome_label<R>(r: &Result<R>) -> &'static str {
    match r {
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
fn log_transition(to: &'static str, r: &Result<()>) {
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
    fn new<F, Fut>(queue: Queue<D>, handler: F) -> Self
    where
        F: Fn(Job<D>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        let handler: HandlerFn<D, R> = Arc::new(move |job| handler(job).boxed());
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
