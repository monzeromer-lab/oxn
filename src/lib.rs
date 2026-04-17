//! # oxn
//!
//! A Redis-backed async job queue for Rust.
//!
//! `oxn` takes architectural inspiration from Node's BullMQ (atomic Lua state
//! transitions, Redis Streams for cross-process events, stalled-lock recovery,
//! parent/child flows) but rebuilds everything around Rust idioms: generic
//! typed jobs, `tokio::sync::broadcast` instead of EventEmitter, builder
//! patterns, `thiserror` error types, `CancellationToken` for shutdown, and a
//! trait-based backend so Redis is a default — not a hardcoded assumption.
//!
//! ## Quick start
//!
//! ```no_run
//! use oxn::{Queue, Worker, JobOptions};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct Email { to: String, subject: String }
//!
//! # async fn run() -> oxn::Result<()> {
//! let queue: Queue<Email> = Queue::builder("emails")
//!     .redis("redis://127.0.0.1:6379")
//!     .build()
//!     .await?;
//!
//! queue.add(Email { to: "a@b.c".into(), subject: "hi".into() }, JobOptions::default()).await?;
//!
//! let worker = Worker::builder(queue.clone(), |job: oxn::Job<Email>| async move {
//!         println!("sending to {}", job.data.to);
//!         oxn::Result::<()>::Ok(())
//!     })
//!     .concurrency(4)
//!     .build();
//!
//! worker.run().await?;
//! # Ok(()) }
//! ```
//!
//! ## Feature flags
//!
//! | Feature            | Adds                                                          |
//! |--------------------|---------------------------------------------------------------|
//! | `redis-backend`    | Default. [`backend::redis::RedisBackend`].                    |
//! | `scheduler`        | [`JobScheduler`] — cron and fixed-interval repeats.           |
//! | `flow`             | [`FlowProducer`] — parent/child DAGs.                         |
//! | `metrics`          | Emits counters/histograms via the `metrics` crate.            |
//! | `dashboard-axum`   | [`dashboard::axum_router`] — mount on any axum app.           |
//! | `dashboard-actix`  | [`dashboard::actix_scope`] — mount on any actix-web app.      |
//!
//! ## Non-goals
//!
//! - Port the BullMQ TypeScript API verbatim. Many BullMQ affordances
//!   (dynamic EventEmitter strings, prototype inheritance, sandboxed child
//!   processes) are things Rust already expresses better. `oxn` uses the
//!   language, not the JavaScript playbook.
//! - Embrace runtime reflection. Jobs are typed; shape is checked at the
//!   boundary via `serde`.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]
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
