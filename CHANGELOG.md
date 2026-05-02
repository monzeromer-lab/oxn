# Changelog

All notable changes to `oxn` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [SemVer](https://semver.org/) (pre-1.0: minor bumps
may break public API, patch bumps are fixes/additions only).

## [0.2.2] — Unreleased

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
