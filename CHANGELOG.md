# Changelog

All notable changes to `oxn` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [SemVer](https://semver.org/) (pre-1.0: minor bumps
may break public API, patch bumps are fixes/additions only).

## [0.3.0] — Unreleased

0.3.0 is a correctness release: every bug found in the 0.2.2 review is
fixed with a regression test, and the repo gains `oxn-testkit` (step
recorder + invariant checker) and `oxn-load` (load/soak/chaos harness).
It contains breaking changes; see the migration notes below.

### Changed (breaking)

- **Rejected operations now return errors (B1).** Lua scripts' negative
  return codes used to be ignored everywhere except `extend_lock`, so
  `retry`, `promote`, `remove`, `complete` and `fail` reported `Ok` even
  when nothing happened. They now return:
  - `Error::NotFound` — the job doesn't exist (`retry`, `promote`,
    `remove`, and `complete`/`fail` when the job was deleted mid-flight);
  - `Error::Script(NotInExpectedState)` — `retry` on a job that isn't
    failed, `promote` on a job that isn't delayed;
  - `Error::Script(LockMismatch | LockNotFound)` — `complete`/`fail`/
    `extend_lock` with a token that no longer owns the job;
  - `Error::Script(JobLocked)` (new, code `-7`) — `remove` on an active
    job whose lock is held. It used to delete the job out from under the
    worker processing it.
- **Queue defaults are honored (B2).** `JobOptions` fields are now all
  `Option`s: `None` means "use the queue's `default_job_options`, then the
  built-in default". In 0.2, `attempts`, `remove_on_complete`,
  `remove_on_fail` and `lifo` were always taken from the per-job options,
  so `QueueBuilder::remove_on_complete(Removal::Remove)` and a default
  `attempts(n)` had no effect on jobs added with `JobOptions::new()`. An
  explicit value now always wins, even one equal to the built-in default
  (`.priority(0)` overrides a queue default priority). `id` is never
  inherited.
  - New `ResolvedJobOptions` (the merged result) is what is stored and what
    `Job::opts`, `RawJob::opts` and `JobInsert::opts` expose; its fields are
    plain values (`attempts: u32`, …). Its JSON matches 0.2's `JobOptions`,
    so existing job records keep loading.
  - New `JobOptions::resolve_with(&defaults)` / `resolve()`.
  - *Migration:* reading `opts.attempts` etc. off a `Job` is unchanged;
    code that read fields off a `JobOptions` value it built must handle
    `Option`s; code constructing `JobInsert` for a custom backend call
    must resolve first (`opts.resolve()`).
- **Queue names and explicit job ids are validated (B6).** Both become parts
  of Redis keys, so a `:` in either, or a job id equal to a queue-level key
  (`wait`, `meta`, `events`, …), silently aliased another key. Queue names
  must be non-empty with no `:`, whitespace or control characters; explicit
  job ids additionally must not be all digits (reserved for auto ids) or a
  reserved key name. Violations return `Error::Config` before anything is
  written. `Queue::from_backend` now returns `Result<Queue<D>>`.
  Scheduler job ids change from `sched:{id}:{ts}` to `sched-{id}-{ts}`.
- `Event::Duplicated` loses its meaningless `dedup_id` field: it is emitted
  when an explicit job id already exists (see below), while
  `Event::Deduplicated` covers dedup-id collisions.
- **`Backend` trait (custom backends):** `fetch_next` takes a
  `&CancellationToken` and must honor it only while *waiting* (B4); new
  required method `release(queue, id, token)` hands an active job back
  without an outcome (B4).
- Dashboard (axum and actix) maps `NotFound` → **404** and state/lock
  conflicts → **409**, with `{"error": "..."}` bodies, instead of `204`.
- New `Error::is_conflict()` and `Error::is_lock_lost()` helpers.

### Fixed

- **Workers pick up new work immediately (B3).** A worker that found the
  queue empty blocked on `BZPOPMIN` for the whole `drain_delay` (5s by
  default) and nothing woke it: plain and prioritized adds never touched
  the marker, `resume`/stall requeue didn't either, a burst woke at most
  one of several blocked workers, and a delayed job woke workers only for
  them to sleep another full `drain_delay`. Under steady traffic this
  capped throughput at one burst per `drain_delay` (baseline: pickup p50
  4,047ms / p99 5,001ms). Now:
  - every path that makes a job ready (add, prioritized add, promote,
    promote-all, retry, resume, stall requeue, retry backoff) wakes a
    worker, and `move_to_active` re-arms the marker while work remains,
    so a burst cascades through every blocked worker;
  - when nothing is ready, `move_to_active` returns how long until the
    next delayed job is due and the worker blocks only that long;
  - `drain_delay(0)` is clamped to 1ms instead of sending `BZPOPMIN 0`
    (block forever).
  After: pickup p50 1.7ms / p99 2.0ms on the same scenario.
- **Graceful shutdown actually drains (B4).** Cancelling a worker used to
  abort every running handler at once with `Error::Closed`, which went down
  the normal failure path: the attempt was consumed and a job on its last
  attempt was failed for good ("backend closed"); `run()` returned within
  milliseconds whatever `shutdown_timeout` said. Now cancellation stops
  fetching, handlers keep running (locks renewed) until `shutdown_timeout`,
  and any still running are aborted and **released** back to the head of
  their ready container with the attempt refunded (new `release.lua`,
  emits `waiting` with `prev=active`). A job fetched while shutdown began
  is released without running. An idle worker blocked in `BZPOPMIN` stops
  immediately and its pool slot is detached instead of being returned with
  a block still pending; the stalled scanner is joined before `run()`
  returns.
- **Stalled jobs follow a real lifecycle (B7).** A job that stalled more
  than `max_stalled` times was removed from `active` but never added to
  `failed`: it lived in no container, could not be listed or retried, and
  its `remove_on_fail` policy never ran. Now it fails into `failed` with
  `finishedOn`, reason "job stalled more than allowable limit", its own
  retention applied, and a `failed` event carrying `attemptsMade` (plus a
  `stalled` count). A requeued stalled job goes back to the head of its
  ready container honoring priority and pause (it used to land in `wait`
  regardless).
- **A stall no longer uses up an attempt (B7, behaviour change).** Stalls
  have their own budget (`max_stalled`); the interrupted attempt is
  refunded, as BullMQ does. Before, whether a job got its full retry
  budget depended on whether a stall happened before or after its real
  failures.
- **Re-adding an explicit job id is idempotent (B6).** It used to
  overwrite the existing job's hash in place (keeping stale fields such as
  the old return value) and push the id into `wait` again — a completed job
  reappeared in two containers, and 20 concurrent adds of one id queued it
  20 times. An existing id now returns the original job untouched and emits
  `duplicated`; the id is reusable once its job is removed.
- **Nothing leaks any more (B5).** Every deletion path now goes through one
  Lua helper that removes a job's hash, every auxiliary key (`:lock`,
  `:logs`, `:dependencies`, `:processed`, …) and its dedup pointer:
  - `KeepLast(n)` / `KeepFor(d)` used to trim only the zset, leaving the
    trimmed jobs' hashes forever (baseline: 3,877 orphaned hashes out of
    5,000 jobs). Trims are capped at 1,000 jobs per finish, so switching a
    huge set to a tight policy converges over a few finishes instead of
    blocking Redis in one call.
  - `Removal::Remove` now also deletes `:logs`.
  - `drain()` deleted the lists but left every job hash; it now deletes
    the jobs, works in batches of 1,000, and emits one `drained` event.
  - `clean()` works in batches of 1,000 (no single huge script for
    `limit = 0`) and emits one `cleaned` event with the total.
  - A dedup pointer is cleared when its job is removed, so a later add with
    the same dedup id creates a new job instead of returning a dead id.
  - The `stalled` set was written and never read; it is no longer used.
  - A job requeued by the stalled scanner no longer keeps the dead
    worker's `lockToken`.
  A 3-minute mixed-retention soak keeps keys and memory flat (memory growth
  in long runs is the events stream filling up to `max_stream_length`).
- The worker no longer logs "transition applied" when the backend
  rejected the transition. A lost lock is logged as a warning ("lock
  lost, outcome discarded"); any other rejection as an error.

### Added

- `oxn-testkit` and `oxn-load` workspace crates (unpublished), CI
  workflow, `scripts/test-redis.sh`. See `docs/load-results/`.
- Tracing spans on every Lua call (`oxn.script`), fetch, blocking wait
  (`oxn.block`, with `next_due_ms`), process and shutdown. `backend::redis::script_hashes()` maps script SHAs to names.

## [0.2.2] — Unreleased (folded into 0.3.0)

### Added

- **Bulk dashboard ops**:
  - `Backend::clean(queue, state, limit) -> u64` — delete jobs from
    `completed`, `failed`, `delayed`, `prioritized`, or `waiting-children`
    along with their auxiliary keys (logs, lock, deps, processed). `limit = 0`
    means "every job in that state". Backed by a new atomic Lua script.
  - `Backend::promote_all(queue) -> u64` — move every entry in `delayed`
    into `wait` (or `paused`, if the queue is paused) in a single Lua call.
    Bumps the marker zset so workers blocked on `BZPOPMIN` wake up immediately.
  - `POST /api/queues/{queue}/clean/{state}` accepting `{ "limit": N }` —
    returns `{ "count": N }`. Wired into both axum and actix.
  - `POST /api/queues/{queue}/promote-all` — returns `{ "count": N }`.
    Wired into both axum and actix.
  - **Dashboard UI** gains state-aware buttons: a red "Clean N" button when
    `completed`/`failed` is selected, a "Promote all N" button when
    `delayed` is selected, plus an inline explanation of the five paths a
    job can take into the delayed zset (producer-asked delay, retry
    backoff, `Error::Delayed`, `Error::RateLimited`, scheduler ticks).

### Fixed

- `Event::Cleaned` was emitted by the new `clean()` Lua script but wasn't
  parsed by the Redis Streams subscriber — it surfaced as `Event::Unknown`
  and was filtered out. Now decoded correctly with its `count` field.

### Tests

- 18 new tests (lib + integration + dashboard HTTP), bringing the suite to
  116 total. Coverage for the new ops includes:
  state-by-state cleaning, `limit` semantics, isolation between queues,
  the `Cleaned` event reaching subscribers, and `promote_all` actually
  waking up a worker blocked on `BZPOPMIN` (proves the marker push works
  end-to-end).

## [0.2.1] — 2026-04-30

### Documentation

- README and crate-level docs refreshed for the dashboard work that landed
  in 0.2.0:
  - All dashboard HTTP routes listed in a single table.
  - New "Retention (delete on completion)" section showing the
    `QueueBuilder::remove_on_complete` / `remove_on_fail` shortcuts.
  - Version pin in README and module doc comments synced to 0.2.

No functional changes from 0.2.0.

## [0.2.0] — 2026-04-30

### Added

- **Job detail drawer** in the bundled dashboard UI. Clicking a job row
  opens a side panel with the full job record (data, options, progress,
  return value, stack trace), computed stats (state, attempts, priority,
  timestamps, duration, lock token, retention policies), and the captured
  log lines.
- **`GET /api/queues/{queue}/jobs/{id}/logs`** — paginated log fetch.
  Backs the drawer's logs section in both the axum and actix adapters.
- **`QueueBuilder::remove_on_complete(Removal)`** and
  **`::remove_on_fail(Removal)`** shortcuts (mirrored on `QueueOptions`)
  so callers can opt into deleting jobs as soon as they finish without
  first constructing a `JobOptions::default()`.
- **Pool keepalive task** — when `RedisConfig::keepalive_interval` is
  set (default `Some(25s)`), a background task PINGs every pool slot on
  that cadence. Prevents managed-Redis proxies (DigitalOcean,
  ElastiCache Serverless, Upstash, Redis Cloud) from FIN-ing idle TCP
  connections during quiet periods. New `RedisConfig::health_check_timeout`
  bounds the per-checkout PING so a stale slot fails fast instead of
  stalling the next user call.

### Changed

- **`BZPOPMIN` now uses a regular pool slot** instead of a single shared
  `MultiplexedConnection`. Redis is single-threaded per connection, so the
  old design serialized blocking calls across all workers sharing a backend.
  Each blocking fetch now holds one pool slot for `block_for` seconds and
  doesn't starve other commands.
- **`cfg.pool_size` is now honored.** `Config::from_url` ignored it and
  silently used `num_cpus * 4`; the pool size you pass is now what you
  get.
- **`RedisBackend::warmup` acquires every pool slot in parallel** for the
  per-slot `SCRIPT LOAD`, dropping a 16-slot warmup against managed Redis
  from ~27 s to ~10 s.

### Fixed

- `obliterate` now invalidates the in-process `registered_queues` cache
  alongside the Redis-side `SREM`, so a re-created queue under the same
  name doesn't permanently miss the registry.

## [0.1.3] — pre-0.2 release

### Added

- **`tls` feature** — `rediss://` URL support via rustls + bundled
  webpki-roots. **`tls-native`** alternative for native-tls (OpenSSL /
  SChannel / Security.framework).
- **`RedisConfig::keepalive_interval` and `health_check_timeout`** — early
  versions of the keepalive plumbing later wired up in 0.2.0.

### Changed

- `RedisBackend::connect` now eagerly preloads every Lua script on every
  pool slot before returning. Removes the cold-start `NOSCRIPT` →
  `SCRIPT LOAD` round-trip that stalled the first `Queue::add()` for
  multiple seconds against managed Redis.
- `Backend::complete` and `::fail` take a `Removal` arg, eliminating the
  per-finish `HGETALL` round trip the backend was doing to re-read the
  retention policy.
- `Queue::add` short-circuits the `SADD oxn:queues queueName` registry
  write after the first call per queue per process via an in-memory
  `DashSet` cache.

[0.2.2]: https://github.com/monzeromer-lab/oxn/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/monzeromer-lab/oxn/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/monzeromer-lab/oxn/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/monzeromer-lab/oxn/releases/tag/v0.1.3
