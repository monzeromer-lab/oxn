//! Shared test harness: connects to a live Redis and gives every test its
//! own unique queue name so runs can execute in parallel without stomping
//! on each other.
//!
//! Set `TEST_REDIS_URL` to override the server URL
//! (defaults to `redis://127.0.0.1:6379`).

#![allow(dead_code)] // each integration test only uses a subset

use std::sync::Arc;

use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::backend::Backend;
use oxn::{Queue, QueueOptions};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

pub fn redis_url() -> String {
    std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

pub const TEST_PREFIX: &str = "oxn-test";

pub fn unique_queue_name(slug: &str) -> String {
    // simple() = 32 hex chars, no dashes → keeps key length reasonable.
    format!("{}-{}", slug, Uuid::new_v4().simple())
}

pub async fn build_backend() -> Arc<RedisBackend> {
    let cfg = RedisConfig::new(redis_url())
        .prefix(TEST_PREFIX)
        .max_stream_length(10_000)
        .pool_size(8);
    Arc::new(
        RedisBackend::connect(cfg)
            .await
            .expect("failed to connect to test Redis"),
    )
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

/// Convenience: build a queue (unique name + shared backend).
///
/// Tests should call [`teardown`] before returning to drop Redis keys —
/// doing it on `Drop` isn't reliable with tokio's current-thread runtime.
pub async fn fresh_queue<D>(slug: &str) -> (Queue<D>, Arc<RedisBackend>)
where
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let backend = build_backend().await;
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
