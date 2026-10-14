//! Spins up the axum dashboard at http://127.0.0.1:8080/admin.
//!
//! ```text
//! cargo run --example dashboard_axum --features "redis-backend,dashboard-axum"
//! REDIS_URL=redis://127.0.0.1:6390 PORT=8099 cargo run --example dashboard_axum --features dashboard-axum
//! ```
//!
//! The dashboard has no authentication of its own: this example binds to
//! localhost only. Put it behind your own auth before exposing it
//! (`BIND=0.0.0.0` overrides the address).

use std::sync::Arc;

use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::dashboard::axum_router;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());

    let backend = Arc::new(RedisBackend::connect(RedisConfig::new(url)).await?);
    let app = axum::Router::new().nest("/admin", axum_router(backend));

    let listener = tokio::net::TcpListener::bind(format!("{bind}:{port}")).await?;
    println!("dashboard listening on http://{bind}:{port}/admin");
    axum::serve(listener, app).await?;
    Ok(())
}
