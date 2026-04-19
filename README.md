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
oxn = "0.1.2"
```

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
oxn = { version = "0.1.2", features = ["scheduler", "dashboard-axum"] }
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
factory that mounts the same set of endpoints (`/api/queues`,
`/api/queues/{name}/counts`, `/api/queues/{name}/jobs?state=failed`, plus
POST `pause`/`resume`/`drain` and per-job `retry`/`promote`/`remove`) and a
zero-dependency HTML UI served at `/`.

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

The crate ships with 77 tests:

- 38 unit tests for types, serde round-trips, key layout, option builders,
  error code mapping, progress clamping, flow tree building
- 39 integration tests hitting a live Redis (`redis://127.0.0.1:6379` by
  default; override with `TEST_REDIS_URL`), each isolated by a unique queue
  name and cleaned up via `obliterate(true)`

```bash
# run everything
cargo test --features "scheduler,flow,dashboard-axum"

# unit tests only (no Redis required)
cargo test --lib --no-default-features

# point at a different Redis
TEST_REDIS_URL=redis://redis.local:6379 cargo test
```

## Minimum Rust version

1.85 (edition 2024).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
