//! Dashboard HTTP endpoints (axum variant).

#![cfg(feature = "dashboard-axum")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use oxn::backend::Backend;
use oxn::dashboard::axum_router;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T(u32);

struct ServerGuard {
    base: String,
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

async fn spawn_dashboard(backend: Arc<dyn Backend>) -> ServerGuard {
    let app = axum::Router::new().nest("/admin", axum_router(backend));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { c.cancelled().await })
            .await
            .ok();
    });
    ServerGuard {
        base: format!("http://{}/admin", addr),
        cancel,
        handle,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_lists_queues_counts_and_jobs() {
    let (q, backend) = common::fresh_queue::<T>("dash-list").await;

    // Seed some jobs.
    for i in 0..3u32 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }

    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    // Give hyper a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let http = reqwest::Client::new();

    // GET /api/queues
    let queues: serde_json::Value = http
        .get(format!("{}/api/queues", srv.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = queues.as_array().unwrap();
    assert!(arr.iter().any(|v| v["name"] == q.name()));

    // GET /api/queues/{q}/counts
    let counts: serde_json::Value = http
        .get(format!("{}/api/queues/{}/counts", srv.base, q.name()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(counts["waiting"], 3);

    // GET /api/queues/{q}/jobs?state=waiting
    let list: serde_json::Value = http
        .get(format!("{}/api/queues/{}/jobs?state=waiting", srv.base, q.name()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["total"], 3);
    assert_eq!(list["jobs"].as_array().unwrap().len(), 3);

    // POST /api/queues/{q}/pause
    let r = http
        .post(format!("{}/api/queues/{}/pause", srv.base, q.name()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(q.is_paused().await.unwrap());

    // POST /api/queues/{q}/resume
    let r = http
        .post(format!("{}/api/queues/{}/resume", srv.base, q.name()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(!q.is_paused().await.unwrap());

    // POST drain
    let r = http
        .post(format!("{}/api/queues/{}/drain", srv.base, q.name()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert_eq!(q.counts().await.unwrap().waiting, 0);

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_serves_index_html() {
    let (q, backend) = common::fresh_queue::<T>("dash-index").await;
    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // When nested via `Router::nest("/admin", ...)`, the inner "/" route
    // matches at `/admin` (no trailing slash).
    let resp = reqwest::get(srv.base.clone()).await.unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("oxn"),
        "index at {} returned status={status} body={body:?}",
        srv.base
    );

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}
