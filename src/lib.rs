//! # oxn — Redis-backed async job queue for Rust
//!
//! `oxn` gives you durable background jobs with the ergonomics of native
//! Rust: **typed payloads**, **`async`/`await`**, **builder-pattern configs**,
//! and a **pluggable backend trait**. Architecturally it follows Node's
//! [BullMQ](https://github.com/taskforcesh/bullmq) (atomic Lua state
//! transitions, Redis Streams for events, stalled-lock recovery,
//! parent/child flows) but the public surface is designed for Rust, not
//! ported from JavaScript.
//!
//! ## Mental model
//!
//! - A **[`Queue<D>`]** is a cheap `Arc`-backed producer handle typed on
//!   your payload `D`. It writes jobs into Redis and is otherwise stateless.
//! - A **[`Worker<D, R>`]** polls the queue, dispatches each job to a
//!   user-supplied async handler, renews its lock, and writes the
//!   `complete`/`fail`/`delay` outcome back to Redis. The whole lifecycle
//!   is driven by `tokio::select!` — there is no callback soup.
//! - A **[`Backend`](crate::backend::Backend)** abstracts storage. The
//!   default implementation is `RedisBackend` (under
//!   `crate::backend::redis`), wired up automatically when you call
//!   `QueueBuilder::redis(url)`.
//! - **Events** flow out through a Redis Stream. Subscribe with
//!   [`Queue::events`] → [`QueueEvents::stream`] and consume [`Event`]
//!   values with `futures::Stream`.
//!
//! All state transitions (add / move-to-active / complete / fail / delay /
//! stalled-recovery / pause / resume / promote / retry / remove) are single
//! atomic Lua scripts — a job never lives in two containers at once.
//!
//! ## Quick start
//!
//! ```no_run
//! use oxn::{Queue, Worker, Job, JobOptions, Error};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Email { to: String, subject: String }
//!
//! # async fn run() -> oxn::Result<()> {
//! // Producer — build once, `Clone` freely.
//! let queue: Queue<Email> = Queue::builder("emails")
//!     .redis("redis://127.0.0.1:6379")
//!     .build()
//!     .await?;
//!
//! queue
//!     .add(
//!         Email { to: "alice@example.com".into(), subject: "hi".into() },
//!         JobOptions::new().attempts(3),
//!     )
//!     .await?;
//!
//! // Consumer — any `Fn(Job<D>) -> impl Future<Output = Result<R, Error>>`.
//! let worker = Worker::builder(queue.clone(), |job: Job<Email>| async move {
//!     println!("sending to {}", job.data.to);
//!     Ok::<_, Error>(())
//! })
//! .concurrency(4)
//! .build();
//!
//! worker.run().await // blocks until the worker's cancellation token fires
//! # }
//! ```
//!
//! ## Handler control flow
//!
//! Your handler returns `Result<R, Error>`. A few [`Error`] variants have
//! special meaning when returned from a handler:
//!
//! | Return value                            | Worker does                                                   |
//! | --------------------------------------- | ------------------------------------------------------------- |
//! | `Ok(value)`                             | Mark job `completed`, record return value                     |
//! | `Err(Error::Delayed { delay_ms })`      | Postpone: `delayed` until now + delay, attempt refunded       |
//! | `Err(Error::RateLimited { delay_ms })`  | Same postponement, reported as `rate-limited` in metrics      |
//! | `Err(Error::WaitingChildren)`           | Park in `waiting-children` until its children resolve, attempt refunded |
//! | `Err(Error::Unrecoverable(msg))`        | Skip retries, go straight to `failed`                         |
//! | `Err(other)` or a panic, attempts left  | Requeue to `delayed` using the configured `Backoff`           |
//! | `Err(other)` or a panic, none left      | Move to `failed` with `reason = msg`                          |
//!
//! Postponements record no failure. For progress, logs, lock-loss and
//! shutdown signals, build the worker with [`Worker::builder_with_ctx`] and
//! use the [`JobContext`] it passes to the handler.
//!
//! ## Feature flags
//!
//! | Feature            | Adds                                                          |
//! |--------------------|---------------------------------------------------------------|
//! | `redis-backend`    | Default. `backend::redis::RedisBackend`.                      |
//! | `tls`              | `rediss://` URL support via rustls (recommended).             |
//! | `tls-native`       | `rediss://` URL support via native-tls (OpenSSL/SChannel).    |
//! | `scheduler`        | `JobScheduler` — cron and fixed-interval repeats.             |
//! | `flow`             | `FlowProducer` — parent/child DAGs.                           |
//! | `metrics`          | `oxn_*` counters/histograms via the `metrics` crate (`oxn::metrics`). |
//! | `api-axum`         | `api::axum_router` — the `/v1` HTTP API on any axum app.      |
//! | `api-actix`        | `api::actix_scope` — the same API on any actix-web app.       |
//! | `dashboard-axum`   | `dashboard::axum_router` — the API plus the bundled UI.       |
//! | `dashboard-actix`  | `dashboard::actix_scope` — mount on any actix-web app.        |
//! | `full`             | All of the above (rustls TLS), with `dashboard-axum` selected.|
//!
//! ## TLS (managed Redis)
//!
//! Connecting to managed Redis services (DigitalOcean Managed Redis,
//! Upstash, Redis Cloud, …) typically requires TLS and a `rediss://` URL.
//! Enable the `tls` feature:
//!
//! ```toml
//! [dependencies]
//! oxn = { version = "0.3", features = ["tls"] }
//! ```
//!
//! Then build the queue exactly the same way:
//!
//! ```no_run
//! # use oxn::Queue;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Serialize, Deserialize)]
//! # struct Email;
//! # async fn run() -> oxn::Result<()> {
//! let queue: Queue<Email> = Queue::builder("emails")
//!     .redis("rediss://default:PASS@host.example.com:25061")
//!     .build()
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Add them to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! oxn = { version = "0.3", features = ["scheduler", "flow", "dashboard-axum"] }
//! ```
//!
//! ## Recent releases
//!
//! - **0.3.0** — correctness release: every bug from the 0.2.2 review
//!   fixed (queue defaults, instant pickup, graceful shutdown, no leaked
//!   keys, lossless events, stalled-job lifecycle, …); flows, schedulers,
//!   rate limiting and metrics implemented for real; a [`JobContext`] for
//!   handlers; a standalone `/v1` HTTP API (`oxn::api`) with optional auth;
//!   a redesigned, responsive dashboard. Breaking: see
//!   `docs/MIGRATION-0.3.md`.
//! - **0.2.2** — bulk dashboard ops (`clean`, `promote_all`).
//! - **0.2.0 / 0.2.1** — job-detail drawer and logs endpoint; pool
//!   keepalive; `BZPOPMIN` on a regular pool slot.
//!
//! ## Redis Cluster
//!
//! Not supported. Every key of a queue must live on one node: scripts touch
//! several keys per call and flows span queues. Cluster-mode endpoints
//! (Redis Cluster, ElastiCache Serverless, cluster-mode ElastiCache or
//! MemoryDB) will not work; use a single, non-clustered primary.
//!
//! See `CHANGELOG.md` in the repo for the full per-version history.
//!
//! ## Minimum supported Rust version
//!
//! Rust 1.85 (edition 2024).
//!
//! ## Non-goals
//!
//! - **Port the BullMQ TypeScript API verbatim.** Many BullMQ affordances
//!   (dynamic EventEmitter strings, prototype inheritance, sandboxed child
//!   processes) are things Rust already expresses better. `oxn` uses the
//!   language, not the JavaScript playbook.
//! - **Runtime reflection.** Jobs are typed; shape is checked at the
//!   boundary via `serde`.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    rustdoc::broken_intra_doc_links
)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod backend;
pub mod error;
pub mod events;
pub mod job;
pub mod options;
pub mod queue;
pub mod validate;
pub mod worker;

#[cfg(feature = "flow")]
#[cfg_attr(docsrs, doc(cfg(feature = "flow")))]
pub mod flow;

#[cfg(feature = "scheduler")]
#[cfg_attr(docsrs, doc(cfg(feature = "scheduler")))]
pub mod scheduler;

#[cfg(feature = "api")]
#[cfg_attr(docsrs, doc(cfg(feature = "api")))]
pub mod api;

#[cfg(feature = "dashboard")]
#[cfg_attr(docsrs, doc(cfg(feature = "dashboard")))]
pub mod dashboard;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;
mod telemetry;

pub mod prelude;

pub use crate::error::{Error, Result};
pub use crate::events::{Event, EventRecord, QueueEvents, StreamStart};
pub use crate::job::{Backoff, Job, JobId, JobRef, JobState, Progress};
pub use crate::options::{JobOptions, QueueOptions, ResolvedJobOptions, WorkerOptions};
pub use crate::queue::Queue;
pub use crate::worker::{JobContext, Worker};

#[cfg(feature = "flow")]
pub use crate::flow::{FlowJob, FlowNode, FlowProducer, OnFailure};

#[cfg(feature = "scheduler")]
pub use crate::scheduler::{JobScheduler, RepeatOptions};
