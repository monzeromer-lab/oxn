//! Shared test harness: connects to a live Redis and gives every test its
//! own unique queue name so runs can execute in parallel without stomping
//! on each other.
//!
//! Set `TEST_REDIS_URL` to override the server URL
//! (defaults to `redis://127.0.0.1:6379`).

#![allow(dead_code)] // each integration test only uses a subset

use std::sync::Arc;
use std::time::Duration;

use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::backend::Backend;
use oxn::{Queue, QueueOptions};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::OnceCell;
use uuid::Uuid;

pub fn redis_url() -> String {
    std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

pub const TEST_PREFIX: &str = "oxn-test";

pub fn unique_queue_name(slug: &str) -> String {
    // simple() = 32 hex chars, no dashes → keeps key length reasonable.
    format!("{}-{}", slug, Uuid::new_v4().simple())
}

/// Build a fresh, dedicated [`RedisBackend`].
///
/// Most tests should prefer [`shared_backend`] — connect() pays a TLS
/// handshake + per-slot script preload that costs ~6s per call against
/// managed Redis. Use this only when a test specifically needs to control
/// `connect()`-time behaviour (e.g. preload regression tests).
pub async fn build_backend() -> Arc<RedisBackend> {
    let cfg = test_config();
    Arc::new(
        RedisBackend::connect(cfg)
            .await
            .expect("failed to connect to test Redis"),
    )
}

/// Tunable [`RedisConfig`] used by the test harness. Smaller pool and
/// disabled keepalive keep startup cheap and avoid background work that
/// would outlive a short-lived test process.
pub fn test_config() -> RedisConfig {
    let pool_size = std::env::var("OXN_TEST_POOL_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    RedisConfig::new(redis_url())
        .prefix(TEST_PREFIX)
        .max_stream_length(10_000)
        // 8 slots survives `BZPOPMIN`-holds-a-slot for up to four parallel
        // workers without exhausting the pool, while keeping the per-test
        // `connect()` cost bounded against managed Redis where each TLS
        // handshake is hundreds of milliseconds.
        .pool_size(pool_size)
        // Tests are short-lived; the keepalive task would just be
        // background work that outlives every test it touches.
        .keepalive_interval(None)
        .health_check_timeout(Duration::from_secs(1))
}

/// Per-binary singleton backend.
///
/// Integration test files compile to separate test binaries; within a
/// binary, `tokio::sync::OnceCell` ensures we only call `RedisBackend::connect`
/// **once**, no matter how many tests run. Each test still gets a unique
/// queue name (see [`unique_queue_name`]) so isolation is preserved.
///
/// Against DigitalOcean Managed Redis this is the difference between
/// 37 × 6 s = 220 s and ~14 × 6 s = 80 s for an integration sweep.
static SHARED_BACKEND: OnceCell<Arc<RedisBackend>> = OnceCell::const_new();

pub async fn shared_backend() -> Arc<RedisBackend> {
    SHARED_BACKEND
        .get_or_init(|| async { build_backend().await })
        .await
        .clone()
}

/// Build a typed queue sharing `backend`.
pub async fn queue<D>(backend: Arc<RedisBackend>, name: &str) -> Queue<D>
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Queue::from_backend(
        name.to_string(),
        backend.clone() as Arc<dyn Backend>,
        QueueOptions::default().prefix(TEST_PREFIX),
    )
}

/// Fully dispose of a queue's Redis footprint, including the `queues` index.
pub async fn teardown<D>(queue: &Queue<D>)
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let _ = queue.obliterate(true).await;
}

/// Convenience: build a queue with a unique name and a backend.
///
/// Defaults to a fresh backend per test — locally that costs nothing
/// (Redis on `127.0.0.1` connects in microseconds) and fully isolates
/// every test from every other.
///
/// Set `OXN_TEST_SHARED_BACKEND=1` to opt into a per-binary shared
/// backend; that's a meaningful win against managed Redis where each
/// `connect()` pays a multi-second TLS+warmup cost, but it can race in
/// edge cases (deadpool conn lifecycle interactions we don't fully
/// control), so it's opt-in.
///
/// Tests should call [`teardown`] before returning to drop Redis keys —
/// doing it on `Drop` isn't reliable with tokio's current-thread runtime.
pub async fn fresh_queue<D>(slug: &str) -> (Queue<D>, Arc<RedisBackend>)
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let backend = if std::env::var("OXN_TEST_SHARED_BACKEND").is_ok() {
        shared_backend().await
    } else {
        build_backend().await
    };
    let name = unique_queue_name(slug);
    let q: Queue<D> = queue(backend.clone(), &name).await;
    (q, backend)
}

/// Poll `check` every 25ms up to `max_wait`, succeed when it returns `true`.
/// Used instead of fixed sleeps so tests run as fast as Redis allows.
pub async fn wait_for<F, Fut>(max_wait: std::time::Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(25));
    while start.elapsed() < max_wait {
        tick.tick().await;
        if check().await {
            return true;
        }
    }
    false
}
