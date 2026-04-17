//! Web dashboard for inspecting and managing queues.
//!
//! Enabled via the `dashboard-axum` or `dashboard-actix` feature. Both
//! scaffolds share [`api`] — a set of framework-agnostic async functions
//! that take an `Arc<dyn Backend>` and return JSON-serializable values. The
//! framework-specific modules are thin adapters that plug those functions
//! into `axum::Router` or `actix_web::Scope`.
//!
//! The bundled UI is a single static HTML page (no build step) served at
//! `/`. Production deployments will typically override the UI path or wire
//! up their own frontend.

pub mod api;

#[cfg(feature = "dashboard-axum")]
#[cfg_attr(docsrs, doc(cfg(feature = "dashboard-axum")))]
pub mod axum;

#[cfg(feature = "dashboard-actix")]
#[cfg_attr(docsrs, doc(cfg(feature = "dashboard-actix")))]
pub mod actix;

#[cfg(feature = "dashboard-axum")]
pub use self::axum::router as axum_router;

#[cfg(feature = "dashboard-actix")]
pub use self::actix::scope as actix_scope;

use std::sync::Arc;

use crate::backend::Backend;

/// Shared state passed to dashboard endpoints.
///
/// Cheap to clone; holds an `Arc<dyn Backend>`.
#[derive(Clone)]
pub struct DashboardState {
    /// The backend used for every endpoint.
    pub backend: Arc<dyn Backend>,
}

impl std::fmt::Debug for DashboardState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DashboardState").finish_non_exhaustive()
    }
}

impl DashboardState {
    /// Construct the state. Typically you'll call this implicitly through
    /// `axum_router` or `actix_scope`.
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }
}

/// Built-in single-page UI. Served from `/`.
pub(crate) const DASHBOARD_HTML: &str = include_str!("assets/index.html");
