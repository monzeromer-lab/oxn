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
//! | Return value                                 | Worker does                                     |
//! | -------------------------------------------- | ----------------------------------------------- |
//! | `Ok(value)`                                  | Mark job `completed`, record return value       |
//! | `Err(Error::Delayed { delay_ms })`           | Move job to `delayed`, due at now + delay       |
//! | `Err(Error::RateLimited { delay_ms })`       | Same as `Delayed`, but emitted as a rate event  |
//! | `Err(Error::Unrecoverable(msg))`             | Skip retries, go straight to `failed`           |
//! | `Err(other)` with attempts remaining         | Requeue to `delayed` using the configured `Backoff` |
//! | `Err(other)` with no attempts remaining      | Move to `failed` with `reason = msg`            |
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
//! | `metrics`          | Emits counters/histograms via the `metrics` crate.            |
//! | `dashboard-axum`   | `dashboard::axum_router` — mount on any axum app.             |
//! | `dashboard-actix`  | `dashboard::actix_scope` — mount on any actix-web app.        |
//! | `full`             | All of the above (rustls TLS), with `dashboard-axum` selected.|
//!
//! ## TLS (managed Redis)
//!
//! Connecting to managed Redis services (DigitalOcean Managed Redis, AWS
//! ElastiCache Serverless, Upstash, Redis Cloud, …) typically requires TLS
//! and a `rediss://` URL. Enable the `tls` feature:
//!
//! ```toml
//! [dependencies]
//! oxn = { version = "0.1", features = ["tls"] }
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
//! oxn = { version = "0.1", features = ["scheduler", "flow", "dashboard-axum"] }
//! ```
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
pub mod worker;

#[cfg(feature = "flow")]
#[cfg_attr(docsrs, doc(cfg(feature = "flow")))]
pub mod flow;

#[cfg(feature = "scheduler")]
#[cfg_attr(docsrs, doc(cfg(feature = "scheduler")))]
pub mod scheduler;

#[cfg(feature = "dashboard")]
#[cfg_attr(docsrs, doc(cfg(feature = "dashboard")))]
pub mod dashboard;

pub mod prelude;

pub use crate::error::{Error, Result};
pub use crate::events::{Event, QueueEvents};
pub use crate::job::{Backoff, Job, JobId, JobState, Progress};
pub use crate::options::{JobOptions, QueueOptions, WorkerOptions};
pub use crate::queue::Queue;
pub use crate::worker::Worker;

#[cfg(feature = "flow")]
pub use crate::flow::{FlowNode, FlowProducer};

#[cfg(feature = "scheduler")]
pub use crate::scheduler::{JobScheduler, RepeatOptions};
