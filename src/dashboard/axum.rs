//! Axum dashboard adapter.
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

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::backend::Backend;
use crate::dashboard::api::{self, ListQuery};
use crate::dashboard::{DashboardState, DASHBOARD_HTML};
use crate::error::Error;
use crate::job::JobId;

/// Build a [`Router`] mountable on any axum app.
pub fn router(backend: Arc<dyn Backend>) -> Router {
    let state = DashboardState::new(backend);
    Router::new()
        .route("/", get(index))
        .route("/api/queues", get(get_queues))
        .route("/api/queues/:queue/counts", get(get_counts))
        .route("/api/queues/:queue/jobs", get(get_jobs))
        .route("/api/queues/:queue/jobs/:id", get(get_job))
        .route("/api/queues/:queue/jobs/:id/retry", post(retry_job))
        .route("/api/queues/:queue/jobs/:id/promote", post(promote_job))
        .route("/api/queues/:queue/jobs/:id/remove", post(remove_job))
        .route("/api/queues/:queue/pause", post(pause_queue))
        .route("/api/queues/:queue/resume", post(resume_queue))
        .route("/api/queues/:queue/drain", post(drain_queue))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn get_queues(State(s): State<DashboardState>) -> Result<Json<serde_json::Value>, AppError> {
    let v = api::list_queues(&s).await?;
    Ok(Json(serde_json::to_value(v)?))
}

async fn get_counts(
    State(s): State<DashboardState>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let v = api::queue_counts(&s, &queue).await?;
    Ok(Json(serde_json::to_value(v)?))
}

async fn get_jobs(
    State(s): State<DashboardState>,
    Path(queue): Path<String>,
    Query(q): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let v = api::list_jobs(&s, &queue, q).await?;
    Ok(Json(serde_json::to_value(v)?))
}

async fn get_job(
    State(s): State<DashboardState>,
    Path((queue, id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let v = api::get_job(&s, &queue, &JobId::new(id)).await?;
    Ok(Json(serde_json::to_value(v)?))
}

async fn retry_job(
    State(s): State<DashboardState>,
    Path((queue, id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    api::retry_job(&s, &queue, &JobId::new(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn promote_job(
    State(s): State<DashboardState>,
    Path((queue, id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    api::promote_job(&s, &queue, &JobId::new(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_job(
    State(s): State<DashboardState>,
    Path((queue, id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    api::remove_job(&s, &queue, &JobId::new(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn pause_queue(
    State(s): State<DashboardState>,
    Path(queue): Path<String>,
) -> Result<StatusCode, AppError> {
    api::pause_queue(&s, &queue).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resume_queue(
    State(s): State<DashboardState>,
    Path(queue): Path<String>,
) -> Result<StatusCode, AppError> {
    api::resume_queue(&s, &queue).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize, Default)]
struct DrainBody {
    #[serde(default)]
    include_delayed: bool,
}

async fn drain_queue(
    State(s): State<DashboardState>,
    Path(queue): Path<String>,
    body: Option<Json<DrainBody>>,
) -> Result<StatusCode, AppError> {
    let include_delayed = body.map(|b| b.include_delayed).unwrap_or(false);
    api::drain_queue(&s, &queue, include_delayed).await?;
    Ok(StatusCode::NO_CONTENT)
}

struct AppError(Error);

impl From<Error> for AppError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self(Error::Serde(e))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Config(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({ "error": self.0.to_string() });
        (status, Json(body)).into_response()
    }
}
