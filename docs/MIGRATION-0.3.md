# Migrating from oxn 0.2 to 0.3

0.3 fixes a long list of correctness bugs (see the
[CHANGELOG](../CHANGELOG.md)). Most fixes change only behaviour you would
have reported as a bug. This guide covers what can break your build or
change what your code observes, in rough order of how likely it is to
affect you.

Existing Redis data keeps loading: job hashes written by 0.2 are read by
0.3 unchanged. Stop every 0.2 worker before you start 0.3 workers on the
same queues. The Lua scripts changed, and the two versions must not share
a queue.

## Checklist

- [ ] Code that builds `JobOptions` and then **reads its fields** must
      handle `Option`s ([details](#joboptions-fields-are-options)).
- [ ] Calls that ignored the result of `retry`, `promote`, `remove`,
      `complete` and `fail` will now see errors
      ([details](#rejected-operations-return-errors)).
- [ ] Queue names and explicit job ids must pass validation
      ([details](#queue-names-and-job-ids-are-validated)).
- [ ] `Queue::from_backend` returns a `Result`.
- [ ] `Event::Delayed { delay_ms }` becomes `{ due_at_ms }`, and
      `Event::Waiting` gains `prev` ([details](#events)).
- [ ] `FlowProducer::add` returns a `FlowJob` instead of a `JobId`
      ([details](#flows)).
- [ ] `RepeatOptions::Cron` is a struct variant, and `every(d)` ticks are
      aligned to the Unix epoch ([details](#schedulers)).
- [ ] Custom `Backend` implementations need the new methods and
      signatures ([details](#custom-backends)).
- [ ] HTTP clients of the dashboard's `/api/*` routes should move to `/v1`
      ([details](#http-api)).
- [ ] If you run against a Redis Cluster endpoint, it was never supported,
      and 0.3 says so ([details](#redis-cluster)).

## `JobOptions` fields are `Option`s

In 0.2, `attempts`, `remove_on_complete`, `remove_on_fail` and `lifo` were
always taken from the per-job options. As a result, queue defaults such as
`QueueBuilder::remove_on_complete(Removal::Remove)` or a default
`attempts(n)` had no effect on jobs added with `JobOptions::new()`.

In 0.3 every field is an `Option`. `None` means "use the queue's
`default_job_options`, then the built-in default". An explicit value always
wins, even one equal to the built-in default, so `.priority(0)` overrides a
queue default priority. `id` is never inherited.

The builder methods are unchanged, so code like this compiles and behaves
as before:

```rust
JobOptions::new().attempts(5).delay(Duration::from_secs(10))
```

What changes:

- **Reading fields off a `JobOptions` you built** now gives you `Option`s:

  ```rust
  // 0.2
  let n: u32 = opts.attempts;
  // 0.3
  let n: u32 = opts.attempts.unwrap_or(1);
  // or, with the built-in defaults applied:
  let n: u32 = opts.resolve().attempts;
  ```

- **Reading options off a `Job`** is unchanged. `Job::opts` (and
  `RawJob::opts`, `JobInsert::opts`) is now a `ResolvedJobOptions`, with
  plain fields (`attempts: u32`, …) holding the merged result. Its JSON is
  the same as 0.2's `JobOptions`.
- **Jobs now honour your queue defaults.** If you set queue defaults and
  relied, without knowing it, on their being ignored, jobs will now be
  retried or removed according to those defaults.

## Rejected operations return errors

0.2 ignored the Lua scripts' error codes, so these calls reported `Ok`
even when nothing happened:

| Call | 0.3 error |
|---|---|
| `retry`, `promote`, `remove` on a missing job | `Error::NotFound` |
| `retry` on a job that isn't failed; `promote` on one that isn't delayed | `Error::Script(NotInExpectedState)` |
| `remove` on a running job (it used to delete the job out from under its worker) | `Error::Script(JobLocked)` |
| `complete` / `fail` / `extend_lock` with a lock token that no longer owns the job | `Error::Script(LockMismatch \| LockNotFound)` |

`Error::is_conflict()` and `Error::is_lock_lost()` group these. Calls that
used `let _ =` or `.ok()` keep compiling. Calls that used `?` now
propagate the error.

`remove` on a flow parent now removes the whole tree. It fails with
`JobLocked` if any job in the tree is running.

## Queue names and job ids are validated

Both become parts of Redis keys. In 0.2, a `:` in either one, or a job id
such as `wait` or `meta`, silently aliased another key. 0.3 returns
`Error::Config` before writing anything when:

- **a queue name** is empty, or contains `:`, whitespace or a control
  character;
- **an explicit job id** breaks the queue-name rules, is all digits
  (reserved for auto-generated ids), or is a reserved key name (`wait`,
  `meta`, `events`, …).

`oxn::validate` exposes the checks if you want to validate input
yourself. `Queue::from_backend` now returns `Result<Queue<D>>`:

```rust
let q = Queue::from_backend("emails", backend, QueueOptions::default())?;
```

**Re-adding an explicit id that exists** now returns the existing job's id
untouched and emits `Event::Duplicated`. 0.2 overwrote the job's hash and
queued it again. `Event::Duplicated` loses its meaningless `dedup_id`
field. Dedup-id collisions are reported by `Event::Deduplicated`.

## Events

- `Event::Delayed { delay_ms }` is now `Event::Delayed { due_at_ms }`. The
  field always held an absolute timestamp, so only the name changed.
- `Event::Waiting` gains `prev`. It is `Some("active")` for a job released
  by a worker that shut down.
- New `Event::WaitingChildren { id, prev }`, emitted when a parent starts
  waiting for its children.
- New `QueueEvents::records(StreamStart)` yields
  `EventRecord { id, at_ms, event }`, for consumers that must resume where
  they stopped. `StreamStart` is `Now`, `Beginning` or `After(id)`.
  `stream()` still yields plain `Event`s.

`stream()` now pins its cursor when it returns. In 0.2, events emitted
before the first poll were lost.

## Handler control flow

`Error::Delayed` and `Error::RateLimited` used to consume an attempt and
set `failedReason`. In 0.3 they are **postponements**: the attempt is
refunded and no failure is recorded. A stall no longer uses up an attempt
either, since stalls have their own `max_stalled` budget. If you sized
`attempts` to make room for postponements or stalls, you can lower it.

A panicking handler now counts as a failure (`Error::Panicked`) and is
retried or failed right away. In 0.2 it left the job locked until its lock
expired.

Cancelling a worker no longer fails its running jobs. Handlers keep
running until `shutdown_timeout`. Jobs still running at that point are
released back to the queue with the attempt refunded.

## Flows

Flows were a stub in 0.2. They are implemented in 0.3:

- `FlowProducer::add` returns a `FlowJob` tree (`id`, `queue`,
  `children`) instead of the root's `JobId`. Use `flow.id` where you used
  the returned id.
- `ParentRef` is renamed `JobRef`; a `ParentRef` alias is kept.
- `Job::parent`, `Error::WaitingChildren` and `Event::WaitingChildren`
  exist without the `flow` feature.
- `FlowNode` gains `on_failure`, set with `fail_parent_on_failure()` or
  `ignore_dependency_on_failure()`. By default a child's permanent failure
  keeps its parent waiting.

## Schedulers

- `RepeatOptions::Cron(..)` is now `RepeatOptions::Cron { expr, schedule }`.
  Build it with `RepeatOptions::cron("0 0 2 * * *")?` as before. Only code
  that matches on the variant changes.
- `every(d)` ticks are aligned to the Unix epoch, not counted from each
  enqueue. With `every(1h)`, jobs fire on the hour.
- Scheduler job ids change from `sched:{id}:{ts}` to `sched-{id}-{ts}`.
- `run` keeps the **next** tick queued ahead instead of adding each tick
  when it fires. Running the same `JobScheduler` in every process is now
  safe: each tick is added exactly once.
- New `Queue::schedulers()`, `Queue::remove_scheduler(id)` and
  `JobScheduler::remove()`.

## Custom backends

If you implement `oxn::backend::Backend` yourself:

| Change | What to do |
|---|---|
| `fetch_next(queue, token, lock, block_for, cancel, rate_limit)` | Honour `cancel` only while **waiting**: a fetch that already took a job must return it. `rate_limit: Option<RateLimit>` caps starts per window across workers. |
| `release(queue, id, token)` (new, required) | Hand an active job back without an outcome, refunding the attempt. |
| `delay(queue, id, token, until_ms)` (new, required) | Postpone without consuming an attempt. |
| `fail(queue, id, token, Failure)` | `Failure { reason, retry_at_ms, removal, trace }`. Build it with `Failure::new(reason).retry_at(ms).removal(r).trace(t)`. |
| `subscribe(queue, StreamStart) -> RecordStream` | Yield `EventRecord`s, starting where asked. |
| `JobInsert::opts` is `ResolvedJobOptions` | Resolve first: `opts.resolve_with(&defaults)`. |
| `add_flow`, `wait_for_children`, `dependencies` | Optional. The defaults refuse with `Error::Config`. |
| `schedule_tick`, `schedulers`, `remove_scheduler` | Optional. Same default. |

Rejected transitions must return the matching `Error`
(`NotFound`, `Script(LockMismatch)`, …), not `Ok`. The worker relies on
this to detect a lost lock.

## HTTP API

The dashboard is now the new HTTP API (`oxn::api`) plus the UI. You mount
it the same way:

```rust
// unchanged
let app = Router::new().nest("/admin", oxn::dashboard::axum_router(backend.clone()));
// new: built-in auth or read-only mode
use oxn::api::ApiConfig;
let cfg = ApiConfig::default().bearer(["read-token"], ["admin-token"]);
let app = Router::new().nest("/admin", oxn::dashboard::axum_router_with(backend, cfg));
```

For actix, use `oxn::dashboard::actix_scope` / `actix_scope_with`. To
serve the API without the UI, use `oxn::api::axum_router(backend, cfg)` or
`oxn::api::actix_scope(backend, cfg)` (features `api-axum` / `api-actix`).
Middleware you already put in front (`.layer(...)`, `.wrap(...)`) keeps
working. `ApiConfig::default()` has no built-in auth.

The 0.2 routes under `/api/queues/...` remain as deprecated aliases, with
their 0.2 response shapes, for 0.3 only. Two things differ from 0.2:

- Operations that did nothing now say so: **404** for a missing job, and
  **409** for a state or lock conflict, with a `{"error": "..."}` body.
  0.2 returned **204** for both.
- With `ApiConfig::bearer` or `read_only` set, the legacy routes are
  protected too.

Move clients to `/v1`:

| 0.2 | 0.3 |
|---|---|
| `GET /api/queues` | `GET /v1/queues` |
| `GET /api/queues/{q}/counts` | `GET /v1/queues/{q}/counts` |
| `GET /api/queues/{q}/jobs?state=` | `GET /v1/queues/{q}/jobs?state=&offset=&limit=&order=` (returns `total`) |
| `GET /api/queues/{q}/jobs/{id}` · `/logs` | `GET /v1/queues/{q}/jobs/{id}` · `/logs` |
| `POST /api/queues/{q}/jobs/{id}/retry` · `/promote` | `POST /v1/queues/{q}/jobs/{id}/retry` · `/promote` |
| `POST /api/queues/{q}/jobs/{id}/remove` | `DELETE /v1/queues/{q}/jobs/{id}` |
| `POST /api/queues/{q}/pause` · `/resume` · `/drain` | same under `/v1` |
| `POST /api/queues/{q}/clean/{state}` | `POST /v1/queues/{q}/clean` with `{"state": "...", "limit": 0}` |
| `POST /api/queues/{q}/promote-all` | `POST /v1/queues/{q}/promote-all` |

`/v1` errors are `{"error": {"code": "...", "message": "..."}}`. The
full description is served at `GET /v1/openapi.json`.

## Redis Cluster

oxn needs every key of a queue on one Redis node. Its Lua scripts touch
several keys per call, and flows span queues. Redis Cluster and
cluster-mode managed endpoints (ElastiCache Serverless, cluster-mode
ElastiCache or MemoryDB, …) are not supported, although 0.2's README
suggested otherwise. Use a single, non-clustered primary. Standalone
managed Redis (DigitalOcean, Upstash, Redis Cloud without clustering,
ElastiCache with cluster mode disabled) works.
