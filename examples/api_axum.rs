//! Serves the `/v1` HTTP API at http://127.0.0.1:8080/oxn.
//!
//! ```text
//! cargo run --example api_axum --features api-axum
//! OXN_ADMIN_TOKEN=secret OXN_READ_TOKEN=peek cargo run --example api_axum --features api-axum
//!
//! curl localhost:8080/oxn/v1/health
//! curl -H 'authorization: Bearer secret' -X POST localhost:8080/oxn/v1/queues/emails/jobs \
//!      -H 'content-type: application/json' -d '{"data": {"to": "a@b.c"}}'
//! curl -N -H 'authorization: Bearer peek' localhost:8080/oxn/v1/queues/emails/events
//! ```
//!
//! Without tokens the API is open: this example then binds to localhost
//! only (`BIND=0.0.0.0` overrides). `REDIS_URL` and `PORT` are read too.

use std::sync::Arc;

use oxn::api::{ApiConfig, axum_router};
use oxn::backend::redis::{RedisBackend, RedisConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let url = env("REDIS_URL", "redis://127.0.0.1:6379");
    let bind = env("BIND", "127.0.0.1");
    let port = env("PORT", "8080");

    let mut cfg = ApiConfig::default();
    if let Ok(admin) = std::env::var("OXN_ADMIN_TOKEN") {
        let read: Vec<String> = std::env::var("OXN_READ_TOKEN").into_iter().collect();
        cfg = cfg.bearer(read, [admin]);
    }
    let backend = Arc::new(RedisBackend::connect(RedisConfig::new(url)).await?);
    let app = axum::Router::new().nest("/oxn", axum_router(backend, cfg));

    let listener = tokio::net::TcpListener::bind(format!("{bind}:{port}")).await?;
    println!("API on http://{bind}:{port}/oxn/v1 (spec: /oxn/v1/openapi.json)");
    axum::serve(listener, app).await?;
    Ok(())
}
