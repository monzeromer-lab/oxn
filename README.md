# oxn

A Redis-backed async job queue for Rust.

`oxn` takes architectural inspiration from Node's [BullMQ][bullmq] — atomic
Lua state transitions, Redis Streams for cross-process events, stalled-lock
recovery, parent/child flows — and rebuilds them around Rust idioms:
generic typed jobs, events as a `futures::Stream`, builder patterns,
`thiserror`-based errors, `CancellationToken` shutdown, and a pluggable
`Backend` trait so Redis is the default, not a hardcoded assumption.

[bullmq]: https://github.com/taskforcesh/bullmq

```toml
[dependencies]
oxn = "0.3"
```

**0.3.0** is a correctness release. Every bug found in a review of 0.2.2
is fixed with a regression test: queue defaults, instant pickup, graceful
shutdown, leaked keys, lost events, the stalled-job lifecycle, retried
outcomes after a dropped connection, and more. Flows, schedulers, rate
limiting and metrics now actually work. New in 0.3: a `JobContext` for
handlers, failure reporting to Sentry or any error tracker, a standalone
`/v1` HTTP API with optional auth, and a redesigned, responsive dashboard. It has breaking changes: see
**[docs/MIGRATION-0.3.md](docs/MIGRATION-0.3.md)**. Per-version notes are in
[CHANGELOG.md](CHANGELOG.md).

> **Redis Cluster is not supported.** All of a queue's keys must live on
> one node, because the Lua scripts touch several keys per call and flows
> span queues. Cluster-mode endpoints, such as Redis Cluster, ElastiCache
> Serverless, or cluster-mode ElastiCache and MemoryDB, will not work. Use a
> single primary that is not clustered. Standalone managed Redis works, over
> TLS with the `tls` feature.

## Why another queue crate?

| BullMQ pattern | `oxn` in Rust |
| --- | --- |
| `queue.add("name", {...data}, opts)` — dynamic JSON | `queue.add(Email { .. }, JobOptions::new().attempts(3))` — `Queue<Email>` checked at compile time |
| `job.moveToCompleted()` / `throw new DelayedError()` | Handler returns `Result<R, Error>`; `Error::Delayed` / `Error::RateLimited` / `Error::Unrecoverable` are return types, not exceptions |
| `queueEvents.on('completed', fn)` | `queue.events().stream()` returns a `futures::Stream<Item = Result<Event>>` |
| Sandboxed processors via `child_process.fork` | Use `tokio::spawn`, or spawn a sibling binary — the runtime already gives you isolation |
| `Queue` extends `QueueGetters` extends `QueueBase` | Composition over inheritance: `Queue<D>` holds an `Arc<dyn Backend>` |
| `opts.removeOnComplete: { count: 100 }` | Typed `Removal::KeepLast(100)` |
| Lua scripts registered with a version suffix | Lua assembled at build time, embedded with `include_str!`, run with `EVALSHA` |
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
| `tls`             | `rediss://` URLs via rustls (recommended)                      |
| `tls-native`      | `rediss://` URLs via native-tls                                |
| `scheduler`       | `JobScheduler` — cron and fixed-interval repeats               |
| `flow`            | `FlowProducer` — parent/child trees (a parent runs after its children) |
| `metrics`         | `oxn_*` counters, histograms and a gauge via the `metrics` crate (names in `oxn::metrics`) |
| `sentry`          | Job failures reported to Sentry; each handler gets its own Sentry scope (`oxn::sentry`) |
| `api-axum`        | `api::axum_router` — the `/v1` HTTP API on any axum app        |
| `api-actix`       | `api::actix_scope` — the same API on any actix-web app         |
| `dashboard-axum`  | `dashboard::axum_router` — the API plus the bundled UI         |
| `dashboard-actix` | `dashboard::actix_scope` — mount on any actix-web app          |
| `full`            | Everything above except `sentry` and actix, with rustls `tls` and the axum dashboard |

```toml
[dependencies]
oxn = { version = "0.3", features = ["scheduler", "dashboard-axum", "tls"] }
```

## Core concepts

### Jobs

```rust
pub struct Job<D, R = ()> {
    pub id: JobId,
    pub name: String,
    pub data: D,
    pub opts: ResolvedJobOptions,  // per-job options merged with queue defaults
    pub timestamp_ms: i64,
    pub processed_on_ms: Option<i64>,
    pub finished_on_ms: Option<i64>,
    pub attempts_made: u32,
    pub failed_reason: Option<String>,
    pub stacktrace: Vec<String>,
    pub progress: Option<Progress>,
    pub return_value: Option<R>,
    pub parent: Option<JobRef>,      // set for a child in a flow
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

| Handler returns | Worker does |
| --- | --- |
| `Ok(value)` | `completed`, return value recorded |
| `Err(Error::Delayed { delay_ms })` / `Err(Error::RateLimited { delay_ms })` | **postpone**: `delayed` until now + delay, attempt refunded, no failure recorded |
| `Err(Error::WaitingChildren)` | park in `waiting-children` until the children it added resolve (attempt refunded) |
| `Err(Error::Unrecoverable(msg))` | `failed` at once, no retries |
| any other `Err`, or a panic | retry after `Backoff` while attempts remain, else `failed`; each failure appends to `stacktrace` |

`JobOptions` fields are `Option`s: anything you don't set comes from the
queue's `default_job_options` (e.g. `QueueBuilder::remove_on_complete`),
then the built-in default. An explicit value always wins.

### Job context: progress, logs, lock loss, shutdown

```rust
let worker = Worker::builder_with_ctx(queue.clone(), |job: Job<Export>, ctx: JobContext| async move {
    for (i, chunk) in job.data.chunks().enumerate() {
        if ctx.shutdown_requested() || ctx.lock_lost() {
            return Err(Error::Delayed { delay_ms: 0 }); // checkpoint; run again later
        }
        write(chunk).await?;
        ctx.progress((i * 100 / job.data.len()) as u8).await?;
    }
    ctx.log("export written").await?;
    Ok::<_, Error>(())
})
.build();
```

Progress and log lines appear in the dashboard and the API. `lock_lost()`
fires when another worker took the job over (its result would be
discarded). `shutdown_requested()` / `cancelled()` fire as soon as the
worker is cancelled, before `shutdown_timeout` aborts the handler.
`Worker::builder` is unchanged for handlers that don't need any of this.

### Rate limiting

```rust
let worker = Worker::builder(queue.clone(), handler)
    .rate_limit(100, Duration::from_secs(1)) // at most 100 starts per second, queue-wide
    .build();
```

The limit is a fixed window counted in Redis (`{queue}:limiter`), so it
holds across every worker and process that sets it: up to `2 × max` can
start across a window boundary. A limited worker sleeps until the window
reopens instead of polling. Configure the same limit on every worker of the
queue; a worker without one is not limited.

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
soon as they finish — or keep a bounded window — configure a `Removal`
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
        Event::Failed { id, reason, attempts_made, .. } => {
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

let added = FlowProducer::new(queue.backend()).add(tree).await?; // FlowJob: ids of the whole tree

// in the parent's handler (Worker::builder_with_ctx):
let parts: Vec<(JobRef, Extracted)> = ctx.children_values().await?;
```

A child that fails for good either keeps its parent waiting (the default:
retry the child to continue), fails the parent (`fail_parent_on_failure`),
or is skipped (`ignore_dependency_on_failure`). Removing a parent removes
its whole tree. A handler can also add children to its own job with
`FlowProducer::add_under(&ctx.as_parent(), node)` and return
`Error::WaitingChildren` to run again once they finish.

### Metrics (feature `metrics`)

oxn records through the [`metrics`](https://docs.rs/metrics) facade. Install
any recorder, for example `metrics-exporter-prometheus`, and these series
appear, each labelled with `queue`:

- **Counters:** `oxn_jobs_{added,started,completed,failed,retried,postponed,released,stalled}_total`,
  `oxn_lock_lost_total`, `oxn_transition_errors_total{to}`,
  `oxn_transition_retries_total{script}`, `oxn_fetch_errors_total`,
  `oxn_rate_limited_total`.
- **Histograms:** `oxn_job_duration_seconds{outcome}` and
  `oxn_job_wait_seconds` (from enqueue to start).
- **Gauge:** `oxn_jobs_active`.

A transition is counted only after Redis accepts it. The names and their
meanings are documented in `oxn::metrics`. Without the feature, the
emitters compile to nothing.

### Error tracking: Sentry and others

Every failed attempt can be sent to an error tracker once Redis has
recorded it. `on_failure` works with any tracker:

```rust
let worker = Worker::builder(queue.clone(), handler)
    .on_failure(|f| {
        if f.is_final() {
            bugsnag.notify(f.queue, f.job_name, f.job_id.as_str(), f.reason);
        }
    })
    .build();
```

A `JobFailure` carries the queue, job id and name, the attempt and the
number of attempts allowed, a `kind`, the stored reason, the handler's
error (walk `source()` for its causes) and `retry_at_ms` (`None` means the
job failed for good). `kind` is one of `Error`, `Panicked`,
`Unrecoverable`, `Decode`, `PendingChildren` or `Stalled`. Postponements
are not failures. Jobs the stalled scanner fails are reported too.

**Sentry** is built in. Enable the `sentry` feature and call
`sentry::init` as usual before starting workers. Nothing else is needed:

```toml
oxn = { version = "0.3", features = ["sentry"] }
sentry = "0.46"
```

- **What becomes an event.** A job that failed for good is an `error`
  event. The exception chain starts at your own error type, so the issue
  is titled `CardDeclined: …`, not with oxn's wrapper. Outcomes Redis
  never recorded are also `error` events. Retried attempts are reported,
  as `warning` events, only with
  `.sentry(SentryOptions::default().report_retries(true))`.
- **Tags and grouping.** Events are tagged `oxn.queue`, `oxn.job_name`,
  `oxn.failure` and `oxn.final`, and carry an `oxn job` context (id,
  attempt, reason). Issues are grouped per queue and job name.
- **A scope per handler.** Each handler runs in its own Sentry scope
  holding those tags. Anything your handler reports, and any panic that
  Sentry's panic integration catches, is labelled with its job, even with
  many jobs running at once.
- **One `sentry-core`.** oxn accepts `sentry-core` 0.36 up to (not
  including) 0.47. Cargo may still resolve two copies: `cargo tree -i
  sentry-core` must show one version. If it shows two, run
  `cargo update -p sentry-core@<the newer one> --precise <your sentry version>`.
  A worker that starts without a Sentry client logs a warning that says
  so.

See `oxn::sentry` for the details.

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

`Backend` is object-safe (33 async methods, six of them optional).
Implement it for an alternative store (in-memory for tests, SQL for
single-node deployments, NATS JetStream for at-least-once delivery) and pass
the `Arc<dyn Backend>` to `Queue::builder(name).backend(b).build()`.

```rust
#[async_trait::async_trait]
impl Backend for MyBackend {
    async fn add(&self, queue: &str, insert: JobInsert) -> oxn::Result<JobId> { /* ... */ }
    async fn fetch_next(
        &self,
        queue: &str,
        worker_token: &str,
        lock_duration: Duration,
        block_for: Duration,
        cancel: &CancellationToken,          // honor only while waiting
        rate_limit: Option<RateLimit>,
    ) -> oxn::Result<Option<Fetched>> { /* ... */ }
    async fn fail(&self, queue: &str, id: &JobId, token: &str, failure: Failure<'_>) -> oxn::Result<()> { /* ... */ }
    async fn delay(&self, queue: &str, id: &JobId, token: &str, until_ms: i64) -> oxn::Result<()> { /* ... */ }
    async fn release(&self, queue: &str, id: &JobId, token: &str) -> oxn::Result<()> { /* ... */ }
    // ...
}
```

Rejected transitions must return the matching error (`NotFound`,
`Script(LockMismatch)`, …) rather than `Ok`. Flows (`add_flow`,
`wait_for_children`, `dependencies`) and schedulers (`schedule_tick`,
`schedulers`, `remove_scheduler`) have default implementations that refuse
with `Error::Config`, so a backend only implements them if it supports
those features.

## Examples

Every example reads `REDIS_URL` (default `redis://127.0.0.1:6379`).

| Example | Shows | Run |
| --- | --- | --- |
| `basic` | producer and worker in one process | `cargo run --example basic` |
| `delayed` | delayed jobs | `cargo run --example delayed` |
| `graceful_shutdown` | grace period, checkpointing with `JobContext`, release on timeout | `cargo run --example graceful_shutdown` |
| `rate_limit` | a queue-wide limit across two workers | `cargo run --example rate_limit` |
| `flow` | a parent that sums its children's results | `cargo run --example flow --features flow` |
| `api_axum` / `api_actix` | the `/v1` API with optional bearer tokens | `cargo run --example api_axum --features api-axum` |
| `dashboard_axum` | the dashboard at `/admin` | `cargo run --example dashboard_axum --features dashboard-axum` |

## Redis key layout

Mirrors BullMQ so existing tooling (RedisInsight, `redis-cli KEYS`) still
works. Default prefix is `oxn`.

```
oxn:{queue}:wait                  list of ready job ids
oxn:{queue}:paused                list of ids while paused
oxn:{queue}:active                list of ids currently being processed
oxn:{queue}:delayed               zset of future jobs, score = due time (ms)
oxn:{queue}:prioritized           zset of prioritized jobs
oxn:{queue}:waiting-children      zset of flow parents waiting for children
oxn:{queue}:completed             zset of completed jobs, score = finish time
oxn:{queue}:failed                zset of failed jobs, score = finish time
oxn:{queue}:events                stream of lifecycle events (capped)
oxn:{queue}:meta                  hash (paused flag, version, ...)
oxn:{queue}:marker                zset that wakes workers blocked in BZPOPMIN
oxn:{queue}:id                    counter for auto-generated job ids
oxn:{queue}:pc                    counter breaking priority ties
oxn:{queue}:limiter               rate-limit window counter (with rate_limit)
oxn:{queue}:stalled-check         throttle flag for the stalled scanner
oxn:{queue}:schedulers            hash: scheduler id → record (scheduler)
oxn:{queue}:schedulers:{id}       zset of a scheduler's pending ticks
oxn:{queue}:{id}                  hash — the job record
oxn:{queue}:{id}:lock             string token, TTL = lockDuration
oxn:{queue}:{id}:logs             list of user-emitted log lines
oxn:{queue}:{id}:dependencies     set of a flow parent's pending children
oxn:{queue}:{id}:processed        hash: child → return value
oxn:{queue}:{id}:failed           hash: child → failure reason
oxn:{queue}:de:{dedupId}          dedup pointer, TTL = dedup ttl
oxn:queues                        registry of known queue names
```

Every state transition is a single `EVALSHA`. The scripts live under
`src/backend/redis/lua/`, with shared helpers in `lua/includes/common.lua`,
and `build.rs` gives each script only the helpers it uses. Queue names and
explicit job ids may not contain `:` (or whitespace), so keys never alias.

## Testing

Every fix in 0.3 started as a failing regression test. The suite:

- **76 unit tests**: types, serde round-trips, key layout, option
  resolution, error-code mapping, cron and interval maths, API routing,
  auth and DTO validation.
- **203 integration tests** against a live Redis (`TEST_REDIS_URL`),
  plus one timing test that runs on its own. Each test uses a unique queue
  name. With `oxn-testkit`, tests record a timeline of backend calls, Lua
  return codes and events, and they check the queue's invariants: every
  job hash is in exactly one container, there are no orphan keys, locks
  match their owners, and counts match the containers. When a test fails,
  its timeline is written to `target/oxn-reports/`. The tests cover every
  bug fixed in 0.3, flows, schedulers running across processes, rate
  limits, metrics, failure reporting (including Sentry events and
  per-job scopes), and fault injection (a connection cut before or after a
  script runs). An HTTP contract suite runs against both the axum and
  actix adapters and validates every response against the OpenAPI
  document.
- **23 doctests**.
- **Browser tests** (`e2e/ui`, Playwright): 41 runs of the dashboard at
  375, 768 and 1280 px, covering every action, hostile XSS payloads,
  read-only and token modes, live updates, keyboard use and axe
  accessibility scans.
- **A load, soak and chaos campaign** (`oxn-load`, `scripts/load-campaign.sh`).
  It covers throughput, latency, retry storms, `kill -9` of workers,
  `CLIENT KILL`, Redis restarts, network faults, graceful shutdown, flow
  fan-out, distributed schedulers and the HTTP API under load. Each run
  checks a per-job ledger: no job lost or completed twice. Results are in
  [docs/load-results/0.3.0.md](docs/load-results/0.3.0.md).

```bash
# a throwaway Redis on :6390 for tests (docker)
scripts/test-redis.sh start
export TEST_REDIS_URL=redis://127.0.0.1:6390

# everything
cargo test --workspace --features "scheduler,flow,metrics,sentry,dashboard-axum,dashboard-actix"

# unit tests only (no Redis required)
cargo test --lib --no-default-features

# timing-sensitive test, run alone
cargo test --test it --features "scheduler,flow,metrics,sentry,dashboard-axum,dashboard-actix" -- --ignored --test-threads=1

# load campaign (SOAK=2h for the long soak)
scripts/load-campaign.sh
```

## Minimum Rust version

1.85 (edition 2024).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
