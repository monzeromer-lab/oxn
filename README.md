# oxn

A Redis-backed async job queue for Rust.

`oxn` takes architectural inspiration from Node's [BullMQ][bullmq] — atomic
Lua state transitions, Redis Streams for cross-process events, stalled-lock
recovery, parent/child flows — and rebuilds them around Rust idioms:
generic typed jobs, `tokio::sync::broadcast` semantics, builder patterns,
`thiserror`-based errors, `CancellationToken` shutdown, and a pluggable
[`Backend`] trait so Redis is the default, not a hardcoded assumption.

[bullmq]: https://github.com/taskforcesh/bullmq

```toml
[dependencies]
oxn = "0.2.2"
```

See [CHANGELOG.md](CHANGELOG.md) for the per-version notes. The most
recent two:

- **0.2.2** — Bulk dashboard ops (`Backend::clean`, `Backend::promote_all`,
  matching `POST /clean/{state}` and `POST /promote-all` routes, state-aware
  buttons in the bundled UI). Subscriber now decodes `Event::Cleaned`. 18 new
  tests; suite at 116.
- **0.2.1** — Documentation refresh of the dashboard work that landed in
  0.2.0 (drawer, retention shortcuts, route table). No code changes.

## Why another queue crate?

| BullMQ pattern | `oxn` in Rust |
| --- | --- |
| `queue.add("name", {...data}, opts)` — dynamic JSON | `queue.add(Email { .. }, JobOptions::new().attempts(3))` — `Queue<Email>` checked at compile time |
| `job.moveToCompleted()` / `throw new DelayedError()` | Handler returns `Result<R, Error>`; `Error::Delayed` / `Error::RateLimited` / `Error::Unrecoverable` are return types, not exceptions |
| `queueEvents.on('completed', fn)` | `queue.events().stream()` returns a `futures::Stream<Item = Result<Event>>` |
| Sandboxed processors via `child_process.fork` | Use `tokio::spawn`, or spawn a sibling binary — the runtime already gives you isolation |
| `Queue` extends `QueueGetters` extends `QueueBase` | Composition over inheritance: `Queue<D>` holds an `Arc<dyn Backend>` |
| `opts.removeOnComplete: { count: 100 }` | Typed `Removal::KeepLast(100)` |
| Lua scripts registered with a version suffix | Lua bundled via `include_str!`, cached by `redis::Script` (EVALSHA) |
| `.close()` to wait on in-flight jobs | `CancellationToken` + `WorkerOptions::shutdown_timeout` |

Jobs are `Serialize + DeserializeOwned`; everything crossing the Redis boundary
is `serde_json`. No untyped payloads in the public API.

## Quick start

```rust
use std::time::Duration;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> oxn::Result<()> {
    // Producer
    let queue: Queue<Email> = Queue::builder("emails")
        .redis("redis://127.0.0.1:6379")
        .build()
        .await?;

    queue
        .add(
            Email { to: "alice@example.com".into(), subject: "hi".into() },
            JobOptions::new().attempts(3).delay(Duration::from_secs(2)),
        )
        .await?;

    // Consumer
    let worker = Worker::builder(queue.clone(), |job: Job<Email>| async move {
        println!("sending to {}", job.data.to);
        Ok::<_, Error>(())
    })
    .concurrency(4)
    .lock_duration(Duration::from_secs(30))
    .build();

    worker.run().await
}
```

## Feature flags

| Feature           | Adds                                                           |
| ----------------- | -------------------------------------------------------------- |
| `redis-backend`   | **Default.** `backend::redis::RedisBackend`                    |
| `scheduler`       | `JobScheduler` — cron and fixed-interval repeats               |
| `flow`            | `FlowProducer` — parent/child DAG submission                   |
| `metrics`         | Emits counters/histograms via the `metrics` crate              |
| `dashboard-axum`  | `dashboard::axum_router` — mount on any axum app               |
| `dashboard-actix` | `dashboard::actix_scope` — mount on any actix-web app          |
| `full`            | Everything above except actix (choose one dashboard)           |

```toml
[dependencies]
oxn = { version = "0.2.2", features = ["scheduler", "dashboard-axum", "tls"] }
```

## Core concepts

### Jobs

```rust
pub struct Job<D, R = ()> {
    pub id: JobId,
    pub name: String,
    pub data: D,
    pub opts: JobOptions,
    pub timestamp_ms: i64,
    pub processed_on_ms: Option<i64>,
    pub finished_on_ms: Option<i64>,
    pub attempts_made: u32,
    pub failed_reason: Option<String>,
    pub stacktrace: Vec<String>,
    pub progress: Option<Progress>,
    pub return_value: Option<R>,
    pub lock_token: Option<String>,
    // ...
}
```

`JobState` mirrors the Redis container the job lives in — `Waiting`,
`Paused`, `Active`, `Delayed`, `Prioritized`, `WaitingChildren`,
`Completed`, `Failed`. `JobState::parse("wait")` accepts BullMQ's aliases.

### Retry + backoff

```rust
JobOptions::new()
    .attempts(5)
    .backoff(Backoff::Exponential {
        initial: Duration::from_millis(200),
        max: Some(Duration::from_secs(30)),
    })
```

Handler return types drive control flow:

```rust
async fn handler(job: Job<Payload>) -> Result<(), Error> {
    match fetch(&job.data).await {
        Ok(_) => Ok(()),
        Err(RateLimit { retry_after }) => Err(Error::RateLimited {
            delay_ms: retry_after.as_millis() as u64,
        }),
        Err(Transient(e)) => Err(Error::handler(e)),     // counts as one attempt
        Err(Fatal(e)) => Err(Error::unrecoverable(e)),   // skips retries
    }
}
```

### Priority and delay

```rust
queue.add(payload, JobOptions::new().priority(1)).await?;                    // highest
queue.add(payload, JobOptions::new().delay(Duration::from_secs(30))).await?; // eligible in 30s
queue.add(payload, JobOptions::new().lifo(true)).await?;                     // push front
```

Priority jobs live in a sorted set and are drained ahead of `wait`; delayed
jobs are promoted by the worker's `move_to_active` Lua script as their
scheduled time arrives.

### Retention (delete on completion)

Completed and failed jobs are kept indefinitely by default. To drop them as
soon as they finish — or keep a bounded window — configure a [`Removal`]
policy at queue setup time:

```rust
use oxn::options::Removal;

let queue: Queue<Email> = Queue::builder("emails")
    .redis("redis://127.0.0.1:6379")
    .remove_on_complete(Removal::Remove)        // delete on success
    .remove_on_fail(Removal::KeepLast(100))     // keep last 100 failures
    .build()
    .await?;
```

The same policy can be set per-job via `JobOptions::remove_on_complete` /
`JobOptions::remove_on_fail`; per-job values override the queue default.
`Removal` variants: `Keep`, `Remove`, `KeepLast(n)`, `KeepFor(Duration)`.

### Deduplication

```rust
queue.add(
    payload,
    JobOptions::new().deduplicate("upload:abc", Duration::from_secs(300)),
).await?;
```

Within the TTL, subsequent adds with the same id return the original job
id and emit a `deduplicated` event — no second job is created.

### Events

Cross-process via Redis Streams:

```rust
let events = queue.events();
let mut stream = events.stream().await?;
while let Some(Ok(event)) = stream.next().await {
    match event {
        Event::Completed { id, .. } => println!("{id} done"),
        Event::Failed { id, reason, attempts_made } => {
            eprintln!("{id} failed after {attempts_made}: {reason}");
        }
        _ => {}
    }
}
```

### Graceful shutdown

```rust
let worker = Worker::builder(queue.clone(), handler)
    .concurrency(8)
    .shutdown_timeout(Duration::from_secs(30))
    .build();

let token = worker.cancellation_token();
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    token.cancel();
});

worker.run().await?;   // returns once in-flight jobs drain or timeout fires
```

### Scheduler (feature `scheduler`)

```rust
use oxn::{JobScheduler, RepeatOptions};

let sched = JobScheduler::new(
    queue.clone(),
    "nightly-reports",
    RepeatOptions::cron("0 0 2 * * *")?,
    ReportParams { /* ... */ },
);
tokio::spawn(sched.run(CancellationToken::new()));
```

Fire-and-forget or driven manually with `sched.enqueue_next().await?`.

### Flow (feature `flow`)

```rust
use oxn::{FlowNode, flow::FlowProducer};

let tree = FlowNode::new("reports", "generate", &pdf_task, JobOptions::default())?
    .child(FlowNode::new("extract", "a", &task_a, JobOptions::default())?)
    .child(FlowNode::new("extract", "b", &task_b, JobOptions::default())?);

FlowProducer::new(queue.backend()).add(tree).await?;
```

## Dashboard

Enable `dashboard-axum` or `dashboard-actix`. Both export a framework-native
factory that mounts the same set of endpoints and serves a zero-dependency
HTML UI at `/`.

| Method | Path                                        | Purpose                                  |
| ------ | ------------------------------------------- | ---------------------------------------- |
| GET    | `/api/queues`                               | List queues with pause flag + counts     |
| GET    | `/api/queues/{name}/counts`                 | Per-state job counts                     |
| GET    | `/api/queues/{name}/jobs?state=failed`      | Page through jobs in a state             |
| GET    | `/api/queues/{name}/jobs/{id}`              | Full job record (data, opts, stats, …)   |
| GET    | `/api/queues/{name}/jobs/{id}/logs`         | Log lines attached via `Backend::log`    |
| POST   | `/api/queues/{name}/jobs/{id}/retry`        | Move a failed job back to `wait`         |
| POST   | `/api/queues/{name}/jobs/{id}/promote`      | Skip a delayed job's wait time           |
| POST   | `/api/queues/{name}/jobs/{id}/remove`       | Delete a job and its auxiliary keys      |
| POST   | `/api/queues/{name}/pause`/`resume`/`drain` | Queue-wide controls                      |
| POST   | `/api/queues/{name}/clean/{state}`          | Bulk-delete jobs in `completed` / `failed` / `delayed` / `prioritized` / `waiting-children`. Body: `{"limit": N}` (`0` = all). Returns `{"count": N}`. |
| POST   | `/api/queues/{name}/promote-all`            | Move every delayed job to `wait` immediately. Returns `{"count": N}`. |

Clicking a row in the bundled UI opens a side drawer with the job's
**data**, **stats** (state, attempts, priority, timestamps, duration, lock
token, retention policies), **progress**, **return value**, **failed reason
+ stack trace** when present, and the captured **log lines** — all backed
by the endpoints above.

```rust
// axum
use oxn::dashboard::axum_router;
let app = axum::Router::new().nest("/admin", axum_router(queue.backend()));

// actix
use oxn::dashboard::actix_scope;
App::new().service(actix_scope(queue.backend()))
```

`cargo run --example dashboard_axum --features dashboard-axum` →
`http://localhost:8080/admin`.

## Custom backends

`Backend` is object-safe and small (~25 async methods). Implement it for an
alternative store — in-memory for tests, SQL for single-node deployments,
NATS JetStream for at-least-once delivery — and pass the `Arc<dyn Backend>`
to `Queue::builder(name).backend(b).build()`.

```rust
#[async_trait::async_trait]
impl Backend for MyBackend {
    async fn add(&self, queue: &str, insert: JobInsert) -> oxn::Result<JobId> { /* ... */ }
    async fn fetch_next(&self, queue: &str, token: &str, lock: Duration, block: Duration)
        -> oxn::Result<Option<Fetched>> { /* ... */ }
    // ...
}
```

## Redis key layout

Mirrors BullMQ so existing tooling (RedisInsight, `redis-cli KEYS`) still
works. Default prefix is `oxn`.

```
oxn:{queue}:wait              list of ready job ids
oxn:{queue}:paused            list of ids while paused
oxn:{queue}:active            list of ids currently being processed
oxn:{queue}:delayed           zset of future jobs, score = ts ms
oxn:{queue}:prioritized       zset of prioritized jobs
oxn:{queue}:completed         zset of completed jobs
oxn:{queue}:failed            zset of failed jobs
oxn:{queue}:events            stream of lifecycle events
oxn:{queue}:meta              hash (paused flag, version, ...)
oxn:{queue}:{id}              hash — the job record
oxn:{queue}:{id}:lock         string token, TTL = lockDuration
oxn:{queue}:{id}:logs         list of user-emitted log lines
oxn:{queue}:de:{dedupId}      dedup pointer, TTL = dedup ttl
oxn:queues                    registry of known queue names
```

Every state transition is a single `EVAL` — scripts live under
`src/backend/redis/lua/`.

## Testing

The crate ships with **116 tests** (5-run stress passes 580/580):

- **42 unit tests** — types, serde round-trips, key layout, option
  builders, error-code mapping, progress clamping, flow-tree building,
  dashboard API serde.
- **62 integration tests** hitting a live Redis (`redis://127.0.0.1:6379`
  by default; override with `TEST_REDIS_URL`), each isolated by a unique
  queue name and cleaned up via `obliterate(true)`. Coverage spans
  add/process/complete, delayed/priority ordering, retry+backoff, stalled
  recovery, pause/resume, drain/obliterate, dedup, flow trees, scheduler
  ticks, dashboard HTTP round-trips, script-cache preload regression,
  and the new bulk `clean`/`promote_all` ops (including event emission and
  worker-wakeup behaviour).
- **12 doctests** keep the inline examples honest.

```bash
# run everything (default: per-test backend, stable + ~4s locally)
cargo test --features "scheduler,flow,dashboard-axum"

# unit tests only (no Redis required)
cargo test --lib --no-default-features

# point at a different Redis (managed Redis CI: shared per-binary backend
# avoids connect storms and the deadpool conn-recycle race)
TEST_REDIS_URL=rediss://... \
OXN_TEST_SHARED_BACKEND=1 \
  cargo test --features "tls,scheduler,flow,dashboard-axum" -- --test-threads=1
```

## Minimum Rust version

1.85 (edition 2024).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
