//! Axum dashboard: the [`crate::api`] routes plus the bundled UI at `/`.
//!
//! ```no_run
//! # async fn demo() -> anyhow::Result<()> {
//! use std::sync::Arc;
//! use oxn::backend::redis::{RedisBackend, RedisConfig};
//! use oxn::dashboard::axum_router;
//!
//! let backend = Arc::new(
//!     RedisBackend::connect(RedisConfig::new("redis://127.0.0.1:6379")).await?,
//! );
//! let app = axum::Router::new().nest("/admin", axum_router(backend));
//! # Ok(()) }
//! ```

use std::sync::Arc;

use axum::Router;
use axum::response::Html;
use axum::routing::get;

use crate::api::{Api, ApiConfig};
use crate::backend::Backend;
use crate::dashboard::DASHBOARD_HTML;

/// The dashboard with the API unauthenticated (protect it with middleware).
pub fn router(backend: Arc<dyn Backend>) -> Router {
    router_with(backend, ApiConfig::default())
}

/// The dashboard with an API configuration (e.g. bearer tokens). The page
/// itself is static; the data behind it comes from the API.
pub fn router_with(backend: Arc<dyn Backend>, config: ApiConfig) -> Router {
    crate::api::axum::router_for(Api::new(backend, config)).route("/", get(index))
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}
