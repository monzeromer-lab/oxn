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

    /// Drive the worker until its [`CancellationToken`] fires.
    ///
    /// This future completes once the cancel token has been triggered **and**
    /// in-flight jobs have either finished or exceeded
    /// [`WorkerOptions::shutdown_timeout`].
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

        // Stalled-job scanner.
        if opts.run_stalled_scanner {
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
            });
        }

        // Main fetch loop.
        'main: loop {
            tokio::select! {
                _ = cancel.cancelled() => break 'main,
                permit = semaphore.clone().acquire_owned() => {
                    let permit = match permit {
                        Ok(p) => p,
                        Err(_) => break 'main,
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
                    let fetched = tokio::select! {
                        _ = cancel.cancelled() => {
                            fetch_span.record("outcome", "cancelled");
                            drop(permit);
                            break 'main;
                        }
                        r = backend
                            .fetch_next(&queue, &token, opts.lock_duration, opts.drain_delay)
                            .instrument(fetch_span.clone()) => r,
                    };
                    match &fetched {
                        Ok(Some(f)) => {
                            fetch_span.record("job_id", f.job.id.as_str());
                            fetch_span.record("outcome", "job");
                        }
                        Ok(None) => {
                            fetch_span.record("outcome", "idle");
                        }
                        Err(_) => {
                            fetch_span.record("outcome", "error");
                        }
                    }
                    drop(fetch_span);
                    match fetched {
                        Ok(Some(Fetched { job: raw, token })) => {
                            let backend = backend.clone();
                            let queue = queue.clone();
                            let handler = handler.clone();
                            let opts = opts.clone();
                            let cancel = cancel.clone();
                            tokio::spawn(async move {
                                process_one::<D, R>(
                                    backend,
                                    queue,
                                    raw,
                                    token,
                                    handler,
                                    opts,
                                    cancel,
                                )
                                .await;
                                drop(permit);
                            });
                        }
                        Ok(None) => {
                            // Idle: drop permit, spin. BZPOPMIN already blocked inside fetch_next.
                            drop(permit);
                        }
                        Err(e) => {
                            tracing::warn!(queue = %queue, worker = %id, error = %e, "fetch_next failed");
                            drop(permit);
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                }
            }
        }

        // Graceful shutdown: wait for in-flight jobs up to the timeout by
        // blocking on the semaphore reaching full capacity again.
        let total = opts.concurrency;
        let in_flight = total - semaphore.available_permits();
        tracing::debug!(queue = %queue, worker = %id, in_flight, "oxn.shutdown: waiting for in-flight jobs");
        let started = std::time::Instant::now();
        let drain = async {
            let _ = semaphore.acquire_many(total as u32).await;
        };
        let drained = tokio::time::timeout(opts.shutdown_timeout, drain)
            .await
            .is_ok();
        tracing::debug!(
            queue = %queue,
            worker = %id,
            drained,
            still_running = total - semaphore.available_permits(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "oxn.shutdown: done"
        );
        Ok(())
    }
}

async fn process_one<D, R>(
    backend: Arc<dyn Backend>,
    queue: String,
    raw: crate::backend::RawJob,
    token: String,
    handler: HandlerFn<D, R>,
    opts: WorkerOptions,
    cancel: CancellationToken,
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
        cancel,
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
    cancel: CancellationToken,
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
            _ = cancel.cancelled() => {
                tracing::debug!("cancellation observed while handler running");
                break Err(Error::Closed);
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
        Err(Error::Delayed { delay_ms }) => {
            let retry_at = chrono_like_now_ms() + delay_ms as i64;
            span.record("transition", "delayed");
            let r = backend
                .fail(
                    &queue,
                    &id,
                    &token,
                    "delayed by handler",
                    Some(retry_at),
                    remove_on_fail,
                )
                .await;
            log_transition("delayed", &r);
        }
        Err(Error::RateLimited { delay_ms }) => {
            let retry_at = chrono_like_now_ms() + delay_ms as i64;
            span.record("transition", "delayed");
            let r = backend
                .fail(
                    &queue,
                    &id,
                    &token,
                    "rate limited",
                    Some(retry_at),
                    remove_on_fail,
                )
                .await;
            log_transition("delayed", &r);
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

    /// How many times a job may stall before it's force-failed.
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

    /// How long [`Worker::run`] waits for in-flight jobs to drain after
    /// cancellation before returning.
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
