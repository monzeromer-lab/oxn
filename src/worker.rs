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

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::backend::{Backend, Fetched};
use crate::error::{Error, Result};
use crate::job::Job;
use crate::options::WorkerOptions;
use crate::queue::{decode_job, Queue};

/// Boxed future returned by a handler.
pub type HandlerFuture<R> = Pin<Box<dyn Future<Output = Result<R>> + Send + 'static>>;

type HandlerFn<D, R> = Arc<
    dyn Fn(Job<D>) -> HandlerFuture<R> + Send + Sync + 'static,
>;

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
                    let fetched = tokio::select! {
                        _ = cancel.cancelled() => { drop(permit); break 'main; }
                        r = backend.fetch_next(&queue, &token, opts.lock_duration, opts.drain_delay) => r,
                    };
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
                            tracing::warn!(queue = %queue, error = %e, "fetch_next failed");
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
        let drain = async {
            let _ = semaphore.acquire_many(total as u32).await;
        };
        let _ = tokio::time::timeout(opts.shutdown_timeout, drain).await;
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
    let id = raw.id.clone();
    let attempts_made = raw.attempts_made;
    let attempts_total = raw.opts.attempts;
    let backoff = raw.opts.backoff;
    let job: Job<D> = match decode_job::<D>(raw) {
        Ok(j) => j,
        Err(e) => {
            let _ = backend
                .fail(&queue, &id, &token, &format!("decode error: {e}"), None)
                .await;
            return;
        }
    };

    let mut renew = tokio::time::interval(opts.lock_renew_interval);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renew.tick().await; // consume the zero-tick

    let mut handler_fut: BoxFuture<'static, Result<R>> = (handler)(job).boxed();
    let outcome: Result<R> = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                break Err(Error::Closed);
            }
            _ = renew.tick() => {
                if let Err(e) = backend.extend_lock(&queue, &id, &token, opts.lock_duration).await {
                    tracing::warn!(job = %id, error = %e, "lock renew failed");
                }
            }
            r = &mut handler_fut => break r,
        }
    };

    match outcome {
        Ok(ret) => {
            let json = serde_json::to_string(&ret).ok();
            if let Err(e) = backend.complete(&queue, &id, &token, json).await {
                tracing::error!(job = %id, error = %e, "complete failed");
            }
        }
        Err(Error::Delayed { delay_ms }) => {
            let retry_at = chrono_like_now_ms() + delay_ms as i64;
            if let Err(e) = backend
                .fail(&queue, &id, &token, "delayed by handler", Some(retry_at))
                .await
            {
                tracing::error!(job = %id, error = %e, "delay requeue failed");
            }
        }
        Err(Error::RateLimited { delay_ms }) => {
            let retry_at = chrono_like_now_ms() + delay_ms as i64;
            if let Err(e) = backend
                .fail(&queue, &id, &token, "rate limited", Some(retry_at))
                .await
            {
                tracing::error!(job = %id, error = %e, "rate-limit requeue failed");
            }
        }
        Err(Error::Unrecoverable(msg)) => {
            if let Err(e) = backend.fail(&queue, &id, &token, &msg, None).await {
                tracing::error!(job = %id, error = %e, "fail permanent failed");
            }
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
                if let Err(e) = backend
                    .fail(&queue, &id, &token, &err.to_string(), Some(retry_at))
                    .await
                {
                    tracing::error!(job = %id, error = %e, "retry requeue failed");
                }
            } else if let Err(e) = backend
                .fail(&queue, &id, &token, &err.to_string(), None)
                .await
            {
                tracing::error!(job = %id, error = %e, "final fail failed");
            }
        }
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

    /// Maximum time a blocking fetch waits before it loops back to try
    /// another non-blocking attempt. Lower values poll more often; higher
    /// values reduce Redis chatter.
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
