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
| `flow`            | `FlowProducer` — parent/child trees (a parent runs after its children) |
| `metrics`         | `oxn_*` counters, histograms and a gauge via the `metrics` crate (names in `oxn::metrics`) |
| `api-axum`        | `api::axum_router` — the `/v1` HTTP API on any axum app        |
| `api-actix`       | `api::actix_scope` — the same API on any actix-web app         |
| `dashboard-axum`  | `dashboard::axum_router` — the API plus the bundled UI         |
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

The cursor is pinned when `stream()` returns, so nothing emitted in
between is missed. Each subscriber uses its own connection (named
`oxn-events:{queue}` in `CLIENT LIST`, never a pool slot) and reconnects
with backoff after a dropped connection, surfacing one `Err` item and then
continuing from where it left off. For a durable consumer, use
`records()` and persist the last id:

```rust
use oxn::StreamStart;

let mut records = queue.events().records(StreamStart::After(saved_id)).await?;
while let Some(Ok(rec)) = records.next().await {
    handle(rec.event);
    save(&rec.id); // resume point
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

Cancelling stops the worker from taking new jobs; running handlers keep
going (their locks are renewed) until they finish or `shutdown_timeout`
elapses. Anything still running then is released back to the queue with
its attempt refunded — a deploy never burns a job's retries or fails it.

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

Run it in every process of your deployment: each tick is added **once**.
All instances compute the same fire times (cron times are absolute;
`RepeatOptions::every` ticks are aligned to the Unix epoch, so every hour
fires on the hour), tick `T` is the job `sched-{id}-{T}`, and Redis keeps
each scheduler's latest tick so none is ever added twice. The next tick
is queued ahead as a delayed job, so it fires even if every scheduler
process stops; ticks missed while none ran are skipped.

`queue.schedulers()` lists the registered schedulers with their next fire
time; `queue.remove_scheduler(id)` removes one and its pending ticks, and
every running instance of it stops. `sched.enqueue_next()` queues just the
next tick.

### Flow (feature `flow`)

A flow is added atomically. The parent waits in `waiting-children` until
every child has resolved, then runs with its children's results:

```rust
use oxn::{FlowNode, FlowProducer};

let tree = FlowNode::new("reports", "generate", &pdf_task, JobOptions::default())?
    .child(FlowNode::new("extract", "a", &task_a, JobOptions::default())?)
    .child(FlowNode::new("extract", "b", &task_b, JobOptions::default())?.fail_parent_on_failure());

let added = FlowProducer::new(queue.backend()).add(tree).await?; // ids of the whole tree

// in the parent's handler (Worker::builder_with_ctx):
let parts: Vec<(JobRef, Extracted)> = ctx.children_values().await?;
```

A child that fails for good either keeps its parent waiting (the default:
retry the child to continue), fails the parent (`fail_parent_on_failure`),
or is skipped (`ignore_dependency_on_failure`). Removing a parent removes
its whole tree. A handler can also add children to its own job with
`FlowProducer::add_under(&ctx.as_parent(), node)` and return
`Error::WaitingChildren` to run again once they finish.

## HTTP API

Enable `api-axum` or `api-actix` to manage queues over HTTP: everything the
dashboard does, versioned under `/v1`. The router is plain, so you mount it
like the dashboard and put your own middleware in front of it, or use the
built-in bearer tokens.

```rust
use oxn::api::{ApiConfig, axum_router, actix_scope};

// axum — behind your middleware…
let app = axum::Router::new()
    .nest("/oxn", axum_router(queue.backend(), ApiConfig::default()).layer(my_auth));
// …or with built-in tokens (read for GET routes, admin for the rest)
let cfg = ApiConfig::default().bearer(["reader-token"], ["admin-token"]);
let app = axum::Router::new().nest("/oxn", axum_router(queue.backend(), cfg));

// actix
App::new().service(web::scope("/oxn").wrap(my_mw).service(actix_scope(queue.backend(), ApiConfig::default())))
```

| Method | Path | |
|---|---|---|
| GET | `/v1/health` | Redis reachable (no auth) |
| GET | `/v1/openapi.json` | OpenAPI 3.1 description of everything below |
| GET | `/v1/queues` · `/v1/queues/{q}` · `/v1/queues/{q}/counts` | queues, one queue, per-state counts |
| POST | `/v1/queues/{q}/pause` · `resume` · `drain` · `clean` · `promote-all` | queue operations |
| DELETE | `/v1/queues/{q}?force=true` | obliterate |
| GET · POST | `/v1/queues/{q}/jobs` | list (`?state=&offset=&limit=&order=`, with `total`) · add a job |
| POST | `/v1/queues/{q}/jobs/bulk` | add many, all or nothing |
| GET · DELETE | `/v1/queues/{q}/jobs/{id}` | one job · remove it (and its flow tree) |
| GET | `/v1/queues/{q}/jobs/{id}/logs` · `/flow` | log lines · flow tree |
| POST | `/v1/queues/{q}/jobs/{id}/retry` · `/promote` | retry a failed job · run a delayed one now |
| GET · DELETE | `/v1/queues/{q}/schedulers` · `/schedulers/{id}` | job schedulers |
| GET | `/v1/queues/{q}/events` | live events, server-sent; resumes from `Last-Event-ID` |

```bash
curl -X POST localhost:8080/oxn/v1/queues/emails/jobs \
  -H 'authorization: Bearer admin-token' -H 'content-type: application/json' \
  -d '{"data": {"to": "a@b.c"}, "opts": {"attempts": 3, "backoff": {"type": "exponential", "delay_ms": 500}}}'
```

Errors are `{"error": {"code": "...", "message": "..."}}` with `404` for a
missing job, queue or scheduler, `409` for a job in the wrong state or held
by a worker, `400` for invalid input, `401`/`403` for auth and `503` when
Redis is unreachable. Auth options: `Auth::None` (default; your middleware
decides), `Auth::Bearer` (constant-time token check), `Auth::Custom` (your
function gets the request's method, path and headers: JWT, sessions, …),
and `ApiConfig::read_only(true)` to refuse every write. For other
frameworks, `oxn::api::Api::handle` takes a plain request and returns a
plain response.

## Dashboard

Enable `dashboard-axum` or `dashboard-actix`: the HTTP API above plus a
zero-dependency HTML UI at `/`.

```rust
// axum
use oxn::dashboard::axum_router;
let app = axum::Router::new().nest("/admin", axum_router(queue.backend()));
// with API auth: oxn::dashboard::axum_router_with(backend, ApiConfig::default().bearer(...))

// actix
use oxn::dashboard::actix_scope;
App::new().service(actix_scope(queue.backend()))
```

The UI is responsive (phone, tablet, desktop; light and dark), updates
live, and links straight to a queue, state or job from the URL. Clicking a
job opens its **data**, **progress**, **return value**, **failure and stack
trace**, **log lines** and **flow tree**, with retry / run now / remove.
Admin actions only appear when the API grants admin access (`/v1/whoami`);
with `Auth::Bearer` the page asks for a token. Browser tests:
`cd e2e/ui && npm ci && npx playwright test` (uses your installed Chrome
and `TEST_REDIS_URL`).

`cargo run --example dashboard_axum --features dashboard-axum` →
`http://localhost:8080/admin`.

The UI derives its API base from the path it is served at, so it works
wherever you nest it. Axum's `nest` does not route the trailing-slash form
(`/admin/`); add `tower_http::normalize_path::NormalizePathLayer::trim_trailing_slash()`
around your app if you want both to work. The 0.2 dashboard routes
(`/api/queues/...`) still answer in 0.3, deprecated, with their 0.2 shapes.

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
