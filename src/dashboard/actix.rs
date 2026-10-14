//! Actix-web dashboard: the [`crate::api`] routes plus the bundled UI at `/`.
//!
//! ```no_run
//! # async fn demo() -> anyhow::Result<()> {
//! use std::sync::Arc;
//! use oxn::backend::redis::{RedisBackend, RedisConfig};
//! use oxn::dashboard::actix_scope;
//! use actix_web::{App, HttpServer};
//!
//! let backend = Arc::new(
//!     RedisBackend::connect(RedisConfig::new("redis://127.0.0.1:6379")).await?,
//! );
//! HttpServer::new(move || App::new().service(actix_scope(backend.clone())))
//!     .bind("0.0.0.0:8080")?
//!     .run()
//!     .await?;
//! # Ok(()) }
//! ```

use std::sync::Arc;

use actix_web::{HttpResponse, Scope, web};

use crate::api::{Api, ApiConfig};
use crate::backend::Backend;
use crate::dashboard::DASHBOARD_HTML;

/// The dashboard with the API unauthenticated (protect it with middleware).
pub fn scope(backend: Arc<dyn Backend>) -> Scope {
    scope_with(backend, ApiConfig::default())
}

/// The dashboard with an API configuration (e.g. bearer tokens).
pub fn scope_with(backend: Arc<dyn Backend>, config: ApiConfig) -> Scope {
    let scope = web::scope("").route("/", web::get().to(index));
    crate::api::actix::with_api(scope, Api::new(backend, config))
}

async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(DASHBOARD_HTML)
}
