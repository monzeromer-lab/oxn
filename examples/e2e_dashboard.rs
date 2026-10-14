//! Fixture server for the dashboard's browser tests (`e2e/ui`).
//!
//! Serves the dashboard three ways and a seeding route the tests call:
//!
//! - `/admin`  — open (no auth)
//! - `/secure` — bearer tokens: `read-token` (read), `admin-token` (admin)
//! - `/ro`     — read-only
//! - `POST /fixture/seed/{queue}` — fill `queue` with jobs in every state,
//!   including hostile (XSS) payloads, a flow tree and a scheduler.
//!
//! ```text
//! TEST_REDIS_URL=redis://127.0.0.1:6390 PORT=8199 \
//!   cargo run --example e2e_dashboard --features "dashboard-axum,flow,scheduler"
//! ```
//!
//! Test tooling only: the seeding route has no auth.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use oxn::api::ApiConfig;
use oxn::backend::Backend;
use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::flow::{FlowNode, FlowProducer};
use oxn::prelude::*;
use oxn::scheduler::{JobScheduler, RepeatOptions};
use serde_json::{Value, json};

const XSS_NAME: &str = "<img src=x onerror=\"window.__xss=1\">";
const XSS_DATA: &str = "<script>window.__xss=2</script>";
const XSS_REASON: &str = "\"><svg onload=window.__xss=3>";

#[derive(Clone)]
struct Fx {
    backend: Arc<dyn Backend>,
    prefix: String,
}

fn queue(fx: &Fx, name: &str) -> Queue<Value> {
    Queue::from_backend(
        name,
        fx.backend.clone(),
        QueueOptions::default().prefix(&fx.prefix),
    )
    .expect("valid queue name")
}

async fn seed(State(fx): State<Fx>, Path(name): Path<String>) -> Result<Json<Value>, String> {
    let e = |e: oxn::Error| e.to_string();
    let q = queue(&fx, &name);
    let _ = q.obliterate(true).await;

    // finished jobs: run them through a real worker
    let mut done = Vec::new();
    for i in 0..5 {
        done.push(
            q.add(
                json!({ "kind": "ok", "n": i }),
                JobOptions::new().name("report"),
            )
            .await
            .map_err(e)?,
        );
    }
    let mut failed = Vec::new();
    for i in 0..3 {
        let opts = JobOptions::new().name(if i == 0 { XSS_NAME } else { "charge" });
        failed.push(
            q.add(json!({ "kind": "fail", "n": i }), opts)
                .await
                .map_err(e)?,
        );
    }
    let w = Worker::builder_with_ctx(q.clone(), |job: Job<Value>, ctx: JobContext| async move {
        ctx.log("starting").await?;
        ctx.log(XSS_DATA).await?;
        ctx.progress(100u8).await?;
        if job.data["kind"] == "fail" {
            return Err(Error::unrecoverable(if job.data["n"] == 0 {
                XSS_REASON
            } else {
                "card declined"
            }));
        }
        Ok::<_, Error>(json!({ "pages": 3 }))
    })
    .concurrency(4)
    .drain_delay(Duration::from_millis(20))
    .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    for _ in 0..200 {
        let c = q.counts().await.map_err(e)?;
        if c.completed == 5 && c.failed == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    let _ = h.await;

    // pending jobs
    let mut waiting = Vec::new();
    for i in 0..30 {
        waiting.push(
            q.add(
                json!({ "n": i, "html": XSS_DATA }),
                JobOptions::new().name(if i == 0 { XSS_NAME } else { "email" }),
            )
            .await
            .map_err(e)?,
        );
    }
    let mut delayed = Vec::new();
    for i in 0..3 {
        delayed.push(
            q.add(
                json!({ "n": i }),
                JobOptions::new()
                    .name("reminder")
                    .delay(Duration::from_secs(3600)),
            )
            .await
            .map_err(e)?,
        );
    }
    for i in 0..2 {
        q.add(
            json!({ "n": i }),
            JobOptions::new().name("urgent").priority(1),
        )
        .await
        .map_err(e)?;
    }
    // a flow: parent waits for two children
    let flow = FlowProducer::new(fx.backend.clone())
        .add(
            FlowNode::new(&name, "invoice", &json!({ "order": 7 }), JobOptions::new())
                .map_err(e)?
                .child(
                    FlowNode::new(
                        &name,
                        "render-pdf",
                        &json!({}),
                        JobOptions::new().delay(Duration::from_secs(3600)),
                    )
                    .map_err(e)?,
                )
                .child(
                    FlowNode::new(
                        &name,
                        "fetch-rates",
                        &json!({}),
                        JobOptions::new().delay(Duration::from_secs(3600)),
                    )
                    .map_err(e)?,
                ),
        )
        .await
        .map_err(e)?;
    JobScheduler::new(
        q.clone(),
        "nightly-digest",
        RepeatOptions::cron("0 0 2 * * *").map_err(e)?,
        json!({}),
    )
    .enqueue_next()
    .await
    .map_err(e)?;
    Ok(Json(json!({
        "completed": done, "failed": failed, "waiting": waiting, "delayed": delayed,
        "flow": { "parent": flow.id, "children": flow.children.iter().map(|c| c.id.clone()).collect::<Vec<_>>() },
        "xss": { "failed": failed[0], "waiting": waiting[0] },
    })))
}

/// Leave one job locked by a worker that never finishes (for 10 minutes).
async fn hold(State(fx): State<Fx>, Path(name): Path<String>) -> Result<Json<Value>, String> {
    let q = queue(&fx, &name);
    let id = q
        .add(
            json!({ "slow": true }),
            JobOptions::new().name("long-export"),
        )
        .await
        .map_err(|e| e.to_string())?;
    let w = Worker::builder(q.clone(), |_j: Job<Value>| async {
        tokio::time::sleep(Duration::from_secs(600)).await;
        Ok::<_, Error>(())
    })
    .concurrency(1)
    .lock_duration(Duration::from_secs(30))
    .drain_delay(Duration::from_millis(20))
    .build();
    tokio::spawn(w.run());
    // The worker takes the oldest waiting job, not necessarily this one:
    // report whichever job it holds.
    let _ = id;
    for _ in 0..100 {
        let held = q
            .list(JobState::Active, oxn::backend::JobRange::first(1))
            .await
            .map_err(|e| e.to_string())?;
        if let Some(j) = held.first() {
            return Ok(Json(json!({ "active": j.id })));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("no job became active".into())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".into());
    let port = std::env::var("PORT").unwrap_or_else(|_| "8199".into());
    let prefix = "oxn-e2e".to_string();
    let backend: Arc<dyn Backend> =
        Arc::new(RedisBackend::connect(RedisConfig::new(url).prefix(&prefix)).await?);
    let fx = Fx {
        backend: backend.clone(),
        prefix,
    };
    let secure = ApiConfig::default().bearer(["read-token"], ["admin-token"]);
    let fixture = Router::new()
        .route("/fixture/seed/:queue", post(seed))
        .route("/fixture/hold/:queue", post(hold))
        .with_state(fx);
    let app = Router::new()
        .nest("/admin", oxn::dashboard::axum_router(backend.clone()))
        .nest(
            "/secure",
            oxn::dashboard::axum_router_with(backend.clone(), secure),
        )
        .nest(
            "/ro",
            oxn::dashboard::axum_router_with(backend, ApiConfig::default().read_only(true)),
        )
        .merge(fixture);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    println!("e2e fixture on http://127.0.0.1:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}
