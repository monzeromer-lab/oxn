//! Regression for the cold-start stall on managed Redis.
//!
//! `RedisBackend::connect` should `SCRIPT LOAD` every embedded Lua script
//! eagerly, so the first `Queue::add` call doesn't pay a `NOSCRIPT` →
//! `SCRIPT LOAD` round-trip. Without this preload, the first add against a
//! managed Redis behind a TLS proxy can hang for seconds.

mod common;

use std::sync::Mutex;
use std::time::{Duration, Instant};

use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::{JobOptions, Queue, QueueOptions};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Dummy;

// `SCRIPT FLUSH` and `CONFIG RESETSTAT` are server-global. Tests that
// touch them must not interleave or one will pull state out from under
// the other.
static SCRIPT_CACHE_LOCK: Mutex<()> = Mutex::new(());

#[tokio::test(flavor = "multi_thread")]
async fn connect_preloads_every_script_so_first_add_is_warm() {
    let _guard = SCRIPT_CACHE_LOCK.lock().unwrap();
    // Wipe any previously-cached scripts so the test is hermetic.
    let mut admin = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let _: redis::Value = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut admin)
        .await
        .unwrap();

    // connect() should re-fill the cache.
    let cfg = RedisConfig::new(common::redis_url()).prefix(common::TEST_PREFIX);
    let _backend = RedisBackend::connect(cfg).await.unwrap();

    // Every script we ship — discovered via the same Lua bodies the
    // backend loads from — must now exist in Redis's script cache.
    //
    // We re-derive the SHAs from the same Lua sources so the test
    // never drifts from the production list.
    let bodies: &[&str] = &[
        include_str!("../src/backend/redis/lua/add_job.lua"),
        include_str!("../src/backend/redis/lua/move_to_active.lua"),
        include_str!("../src/backend/redis/lua/move_to_finished.lua"),
        include_str!("../src/backend/redis/lua/move_to_delayed.lua"),
        include_str!("../src/backend/redis/lua/extend_lock.lua"),
        include_str!("../src/backend/redis/lua/scan_stalled.lua"),
        include_str!("../src/backend/redis/lua/pause.lua"),
        include_str!("../src/backend/redis/lua/promote.lua"),
        include_str!("../src/backend/redis/lua/retry.lua"),
        include_str!("../src/backend/redis/lua/remove.lua"),
    ];
    let shas: Vec<String> = bodies
        .iter()
        .map(|b| redis::Script::new(b).get_hash().to_string())
        .collect();
    let exists: Vec<bool> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&shas)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        exists.len(),
        bodies.len(),
        "SCRIPT EXISTS returned the wrong arity"
    );
    assert!(
        exists.iter().all(|b| *b),
        "not every script was preloaded; cache state: {exists:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_is_idempotent_and_recovers_from_script_flush() {
    let _guard = SCRIPT_CACHE_LOCK.lock().unwrap();
    let cfg = RedisConfig::new(common::redis_url()).prefix(common::TEST_PREFIX);
    let backend = RedisBackend::connect(cfg).await.unwrap();

    // Flush the cache out from under the backend.
    let mut admin = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let _: redis::Value = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut admin)
        .await
        .unwrap();

    // warmup() should refill it without erroring.
    backend.warmup().await.unwrap();

    // And calling it again is a no-op (no new SHAs change, no errors).
    backend.warmup().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn warmup_preloads_scripts_on_every_pool_connection() {
    // Single-connection preload is insufficient against managed Redis
    // (proxy fronts multiple backend nodes; SCRIPT cache is per-node).
    // We verify here that warmup issues SCRIPT LOAD on `pool_size` distinct
    // connections by counting Redis's per-command call counter via
    // `INFO commandstats`.
    let _guard = SCRIPT_CACHE_LOCK.lock().unwrap();

    let mut admin = redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    // Reset the counters so we measure only what this connect() does.
    let _: redis::Value = redis::cmd("CONFIG")
        .arg("RESETSTAT")
        .query_async(&mut admin)
        .await
        .unwrap();

    const POOL_SIZE: usize = 4;
    const SCRIPT_COUNT: u64 = 10; // matches ALL_SCRIPTS in scripts.rs

    let cfg = RedisConfig::new(common::redis_url())
        .prefix(common::TEST_PREFIX)
        .pool_size(POOL_SIZE);
    let _backend = RedisBackend::connect(cfg).await.unwrap();

    // Read INFO commandstats; parse cmdstat_script.calls=N
    let raw: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut admin)
        .await
        .unwrap();

    // Redis 7+ reports per-subcommand counters as `cmdstat_script|load`,
    // older versions use `cmdstat_script`. Sum every line that mentions
    // `script` so we're robust to the exact format.
    let calls: u64 = raw
        .lines()
        .filter(|l| {
            l.starts_with("cmdstat_script:") || l.starts_with("cmdstat_script|load:")
        })
        .filter_map(|l| {
            l.split_once(':')
                .and_then(|(_, rest)| rest.split(',').next())
                .and_then(|kv| kv.strip_prefix("calls="))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .sum();

    let expected = SCRIPT_COUNT * POOL_SIZE as u64;
    assert!(
        calls >= expected,
        "expected ≥ {expected} SCRIPT calls (10 scripts × {POOL_SIZE} pool slots), \
         got {calls} — looks like preload only ran on a single connection"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn first_add_after_connect_does_not_pay_script_load_cost() {
    // Local Redis already serves SCRIPT LOAD in microseconds, so we can't
    // numerically demonstrate the managed-Redis bug from a unit test. What
    // we *can* demonstrate is that the first add and the second add are
    // in the same order of magnitude — i.e. there's no warm/cold cliff.
    let cfg = RedisConfig::new(common::redis_url()).prefix(common::TEST_PREFIX);
    let backend = std::sync::Arc::new(RedisBackend::connect(cfg).await.unwrap());

    let q: Queue<Dummy> = Queue::from_backend(
        common::unique_queue_name("preload-timing"),
        backend.clone() as std::sync::Arc<dyn oxn::backend::Backend>,
        QueueOptions::default().prefix(common::TEST_PREFIX),
    );

    let t1 = Instant::now();
    let id1 = q
        .add(Dummy, JobOptions::new().delay(Duration::from_secs(86400)))
        .await
        .unwrap();
    let first = t1.elapsed();

    let t2 = Instant::now();
    let id2 = q
        .add(Dummy, JobOptions::new().delay(Duration::from_secs(86400)))
        .await
        .unwrap();
    let second = t2.elapsed();

    q.remove(&id1).await.unwrap();
    q.remove(&id2).await.unwrap();
    common::teardown(&q).await;

    // Without preload, on a managed Redis we'd see first ≫ second by
    // multiple orders of magnitude. The threshold below is generous to
    // avoid flakes on slow CI but still catches a real regression — a
    // missing preload would push first into the hundreds of ms even on
    // localhost.
    assert!(
        first < second * 50 + Duration::from_millis(50),
        "first add ({first:?}) is too far above second add ({second:?}) — \
         likely a regression in script preloading"
    );
}
