# Load baseline — oxn 0.2.2 (before fixes)

Recorded 2026-10-13 with `oxn-load` against a dedicated local Redis 8.0.5
(`scripts/test-redis.sh`, :6390, no persistence unless noted), release
build, one machine. These numbers are the "before" side of every Phase 1
fix; each fix re-runs the scenario that exposed it.

| Scenario | Result | Jobs | ok/s | pickup p50 | pickup p99 | Finding |
|---|---|---|---|---|---|---|
| `smoke --retention last1000` | ❌ | 5,000 | 1,756 | 397ms¹ | 947ms¹ | **B5** 3,877 job hashes in no container — `KeepLast` trims the zset but leaks the hashes |
| `smoke --retention remove` | ✅ | 5,000 | 1,807 | 426ms¹ | 1,000ms¹ | `:logs` leak not exercised (harness writes no logs) |
| `mixed` | ❌ | 10,000 | 2,706 | 815ms¹ | 1,424ms¹ | **B5** 6,250 orphaned hashes from `KeepLast`/`KeepFor` jobs |
| `latency` (default `drain_delay`) | ❌² | 7,994 | 196 | **4,047ms** | **5,001ms** | **B3** workers sleep the full 5s `drain_delay` while 800 jobs sit in `wait`: adds never wake a blocked worker, so throughput collapses to one burst per 5s |
| `shutdown` (restart workers under load) | ❌ | 10,000 | 538 | 3,637ms | 6,795ms | **B4** in-flight jobs on their last attempt marked *failed* ("backend closed") after 27ms of a 50ms handler; `run()` returned in 12ms despite a 10s `shutdown_timeout` |
| `smoke` + `script-flush` + `client-kill` | ❌ | 20,000 | 1,619 | 12ms | 366ms | **B15 (new)** a job whose handler succeeded stayed in `active`: `complete()` hit a killed connection and was never retried — the result is lost and the job re-runs after its lock expires |
| `kill-worker` (SIGKILL ×4) | ✅ | 20,000 | 926 | 6,558ms¹ | 12,534ms¹ | 23 stalls, all recovered; B7 needs the *same* job to stall twice (covered by integration test) |
| `network` (200ms latency, dropped conns, 2s blackhole) | ✅ | 20,000 | 680 | 13ms | 102ms | — |
| `retry-storm` (30% transient, 4 attempts) | ✅ | 20,000 | 3,151 | 1.2ms | 17.9ms | — |
| `redis-restart` (AOF, restart at 8s) | ✅ | 20,000 | 895 | 13.5ms | 300ms | — (an earlier run's ❌ was a harness bug: the monitor connection didn't reconnect; fixed) |

¹ Dominated by queueing: these scenarios enqueue faster than the worker
pool drains, so "pickup" includes time spent behind the backlog.
² Recorded before the harness had a latency gate; with
`--slo-pickup-p99-ms 50` (now the latency preset's default) it fails.

## Reports

Full per-run reports (per-second timeline, per-job anomaly histories,
Redis state after drain) are written to `<cargo target dir>/oxn-reports/load/`
(`./target` unless `CARGO_TARGET_DIR` / `build.target-dir` say otherwise):

- `smoke-baseline-last1000-1791919793.md`, `mixed-baseline-1791920023.md`
- `latency-baseline-1791919811.md`
- `shutdown-baseline-1791919893.md`
- `smoke-baseline-server-chaos-1791919988.md`
- `kill-worker-baseline-1791919926.md`, `network-baseline-1791919958.md`,
  `retry-storm-baseline-1791920014.md`, `redis-restart-baseline-1791920139.md`

## Reproduce

```bash
scripts/test-redis.sh reset
export TEST_REDIS_URL=redis://127.0.0.1:6390
cargo build -p oxn-load --release
target/release/oxn-load run --scenario latency --duration 40s
target/release/oxn-load run --scenario smoke --jobs 5000 --retention last1000
target/release/oxn-load run --scenario shutdown
target/release/oxn-load run --scenario smoke --jobs 20000 --rate 2000 \
  --chaos "3s:script-flush,5s:client-kill=0.5,7s:script-flush,8s:client-kill=1.0"
OXN_REDIS_PERSIST=1 scripts/test-redis.sh reset
target/release/oxn-load run --scenario redis-restart \
  --redis-restart-cmd "OXN_REDIS_PERSIST=1 scripts/test-redis.sh restart"
```
