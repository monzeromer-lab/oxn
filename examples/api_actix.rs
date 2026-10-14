//! Serves the `/v1` HTTP API with actix-web at http://127.0.0.1:8080/oxn.
//!
//! ```text
//! cargo run --example api_actix --features api-actix
//! OXN_ADMIN_TOKEN=secret OXN_READ_TOKEN=peek cargo run --example api_actix --features api-actix
//!
//! curl localhost:8080/oxn/v1/health
//! curl -H 'authorization: Bearer secret' -X POST localhost:8080/oxn/v1/queues/emails/jobs \
//!      -H 'content-type: application/json' -d '{"data": {"to": "a@b.c"}}'
//! curl -N -H 'authorization: Bearer peek' localhost:8080/oxn/v1/queues/emails/events
//! ```
//!
//! Without tokens the API is open: this example then binds to localhost
//! only (`BIND=0.0.0.0` overrides). `REDIS_URL` and `PORT` are read too.
//! The scope is plain, so your own `.wrap(...)` middleware goes in front
//! of it as usual (shown here with `DefaultHeaders`).

use std::sync::Arc;

use actix_web::middleware::DefaultHeaders;
use actix_web::{App, HttpServer, web};
use oxn::api::{ApiConfig, actix_scope};
use oxn::backend::Backend;
use oxn::backend::redis::{RedisBackend, RedisConfig};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let url = env("REDIS_URL", "redis://127.0.0.1:6379");
    let bind = env("BIND", "127.0.0.1");
    let port: u16 = env("PORT", "8080").parse()?;

    let mut cfg = ApiConfig::default();
    if let Ok(admin) = std::env::var("OXN_ADMIN_TOKEN") {
        let read: Vec<String> = std::env::var("OXN_READ_TOKEN").into_iter().collect();
        cfg = cfg.bearer(read, [admin]);
    }
    let backend: Arc<dyn Backend> = Arc::new(RedisBackend::connect(RedisConfig::new(url)).await?);

    println!("API on http://{bind}:{port}/oxn/v1 (spec: /oxn/v1/openapi.json)");
    HttpServer::new(move || {
        App::new().service(
            web::scope("/oxn")
                .wrap(DefaultHeaders::new().add(("x-served-by", "oxn-api")))
                .service(actix_scope(backend.clone(), cfg.clone())),
        )
    })
    .bind((bind.as_str(), port))?
    .run()
    .await?;
    Ok(())
}
