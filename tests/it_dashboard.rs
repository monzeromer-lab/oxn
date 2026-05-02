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

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_clean_endpoint_removes_completed_jobs() {
    use oxn::{Job, Worker};
    let (q, backend) = common::fresh_queue::<T>("dash-clean").await;

    // Seed completed jobs.
    for _ in 0..3u32 {
        q.add(T(0), JobOptions::new()).await.unwrap();
    }
    let worker = Worker::builder(q.clone(), |_: Job<T>| async {
        Ok::<_, oxn::Error>(())
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());
    common::wait_for(Duration::from_secs(5), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 3).unwrap_or(false) }
    })
    .await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!(
            "{}/api/queues/{}/clean/completed",
            srv.base,
            q.name()
        ))
        .header("content-type", "application/json")
        .body(r#"{"limit":0}"#)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["count"], 3);
    assert_eq!(q.counts().await.unwrap().completed, 0);

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_clean_endpoint_works_without_a_body() {
    use oxn::Job;
    let (q, backend) = common::fresh_queue::<T>("dash-clean-no-body").await;

    for _ in 0..2u32 {
        q.add(T(0), JobOptions::new()).await.unwrap();
    }
    let worker = oxn::Worker::builder(q.clone(), |_: Job<T>| async {
        Ok::<_, oxn::Error>(())
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());
    common::wait_for(Duration::from_secs(5), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 2).unwrap_or(false) }
    })
    .await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // No body — the handler should treat it as `limit: 0` (everything).
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!(
            "{}/api/queues/{}/clean/completed",
            srv.base,
            q.name()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["count"], 2);

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_clean_endpoint_rejects_unknown_state() {
    let (q, backend) = common::fresh_queue::<T>("dash-clean-bad").await;
    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/queues/{}/clean/not-a-real-state",
            srv.base,
            q.name()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "unknown state should be 400");

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_clean_endpoint_rejects_list_backed_state() {
    // Trying to clean `waiting` should hit `Error::Config` → 400.
    let (q, backend) = common::fresh_queue::<T>("dash-clean-waiting").await;
    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/queues/{}/clean/waiting",
            srv.base,
            q.name()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_promote_all_endpoint_moves_delayed_to_waiting() {
    let (q, backend) = common::fresh_queue::<T>("dash-promote-all").await;

    for _ in 0..4u32 {
        q.add(T(0), JobOptions::new().delay(Duration::from_secs(60)))
            .await
            .unwrap();
    }
    assert_eq!(q.counts().await.unwrap().delayed, 4);

    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{}/api/queues/{}/promote-all", srv.base, q.name()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["count"], 4);

    let c = q.counts().await.unwrap();
    assert_eq!(c.delayed, 0);
    assert_eq!(c.waiting, 4);

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dashboard_promote_all_endpoint_returns_zero_when_empty() {
    let (q, backend) = common::fresh_queue::<T>("dash-promote-all-empty").await;
    let srv = spawn_dashboard(backend.clone() as Arc<dyn Backend>).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{}/api/queues/{}/promote-all", srv.base, q.name()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["count"], 0);

    srv.cancel.cancel();
    let _ = srv.handle.await;
    common::teardown(&q).await;
}
