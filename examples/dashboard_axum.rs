//! Spins up the axum dashboard on port 8080.
//!
//! `cargo run --example dashboard_axum --features "redis-backend,dashboard-axum"`

use std::sync::Arc;

use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::dashboard::axum_router;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let backend = Arc::new(
        RedisBackend::connect(RedisConfig::new("redis://127.0.0.1:6379")).await?,
    );
    let app = axum::Router::new().nest("/admin", axum_router(backend));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("dashboard listening on http://localhost:8080/admin");
    axum::serve(listener, app).await?;
    Ok(())
}
