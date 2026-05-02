//! Actix-web dashboard adapter.
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

use actix_web::web::{Data, Json, Path, Query};
use actix_web::{web, HttpResponse, Responder, Scope};

use crate::backend::Backend;
use crate::dashboard::api::{self, CleanBody, ListQuery, LogsQuery};
use crate::dashboard::{DashboardState, DASHBOARD_HTML};
use crate::error::Error;
use crate::job::JobId;

/// Build an actix-web [`Scope`] mounted at `/` (nest it wherever you like).
pub fn scope(backend: Arc<dyn Backend>) -> Scope {
    let state = Data::new(DashboardState::new(backend));
    web::scope("")
        .app_data(state)
        .route("/", web::get().to(index))
        .route("/api/queues", web::get().to(get_queues))
        .route("/api/queues/{queue}/counts", web::get().to(get_counts))
        .route("/api/queues/{queue}/jobs", web::get().to(get_jobs))
        .route("/api/queues/{queue}/jobs/{id}", web::get().to(get_job))
        .route("/api/queues/{queue}/jobs/{id}/logs", web::get().to(get_job_logs))
        .route("/api/queues/{queue}/jobs/{id}/retry", web::post().to(retry_job))
        .route("/api/queues/{queue}/jobs/{id}/promote", web::post().to(promote_job))
        .route("/api/queues/{queue}/jobs/{id}/remove", web::post().to(remove_job))
        .route("/api/queues/{queue}/pause", web::post().to(pause_queue))
        .route("/api/queues/{queue}/resume", web::post().to(resume_queue))
        .route("/api/queues/{queue}/drain", web::post().to(drain_queue))
        .route("/api/queues/{queue}/clean/{state}", web::post().to(clean_state))
        .route("/api/queues/{queue}/promote-all", web::post().to(promote_all_delayed))
}

async fn index() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(DASHBOARD_HTML)
}

async fn get_queues(s: Data<DashboardState>) -> ApiResult {
    let v = api::list_queues(s.get_ref()).await?;
    Ok(HttpResponse::Ok().json(v))
}

async fn get_counts(s: Data<DashboardState>, path: Path<String>) -> ApiResult {
    let v = api::queue_counts(s.get_ref(), &path).await?;
    Ok(HttpResponse::Ok().json(v))
}

async fn get_jobs(
    s: Data<DashboardState>,
    path: Path<String>,
    q: Query<ListQuery>,
) -> ApiResult {
    let v = api::list_jobs(s.get_ref(), &path, q.into_inner()).await?;
    Ok(HttpResponse::Ok().json(v))
}

async fn get_job(s: Data<DashboardState>, path: Path<(String, String)>) -> ApiResult {
    let (queue, id) = path.into_inner();
    let v = api::get_job(s.get_ref(), &queue, &JobId::new(id)).await?;
    Ok(HttpResponse::Ok().json(v))
}

async fn get_job_logs(
    s: Data<DashboardState>,
    path: Path<(String, String)>,
    q: Query<LogsQuery>,
) -> ApiResult {
    let (queue, id) = path.into_inner();
    let v = api::get_job_logs(s.get_ref(), &queue, &JobId::new(id), q.into_inner()).await?;
    Ok(HttpResponse::Ok().json(v))
}

async fn retry_job(s: Data<DashboardState>, path: Path<(String, String)>) -> ApiResult {
    let (queue, id) = path.into_inner();
    api::retry_job(s.get_ref(), &queue, &JobId::new(id)).await?;
    Ok(HttpResponse::NoContent().finish())
}

async fn promote_job(s: Data<DashboardState>, path: Path<(String, String)>) -> ApiResult {
    let (queue, id) = path.into_inner();
    api::promote_job(s.get_ref(), &queue, &JobId::new(id)).await?;
    Ok(HttpResponse::NoContent().finish())
}

async fn remove_job(s: Data<DashboardState>, path: Path<(String, String)>) -> ApiResult {
    let (queue, id) = path.into_inner();
    api::remove_job(s.get_ref(), &queue, &JobId::new(id)).await?;
    Ok(HttpResponse::NoContent().finish())
}

async fn pause_queue(s: Data<DashboardState>, path: Path<String>) -> ApiResult {
    api::pause_queue(s.get_ref(), &path).await?;
    Ok(HttpResponse::NoContent().finish())
}

async fn resume_queue(s: Data<DashboardState>, path: Path<String>) -> ApiResult {
    api::resume_queue(s.get_ref(), &path).await?;
    Ok(HttpResponse::NoContent().finish())
}

#[derive(serde::Deserialize, Default)]
struct DrainBody {
    #[serde(default)]
    include_delayed: bool,
}

async fn drain_queue(
    s: Data<DashboardState>,
    path: Path<String>,
    body: Option<Json<DrainBody>>,
) -> ApiResult {
    let include_delayed = body.map(|b| b.include_delayed).unwrap_or(false);
    api::drain_queue(s.get_ref(), &path, include_delayed).await?;
    Ok(HttpResponse::NoContent().finish())
}

async fn clean_state(
    s: Data<DashboardState>,
    path: Path<(String, String)>,
    body: Option<Json<CleanBody>>,
) -> ApiResult {
    let (queue, job_state) = path.into_inner();
    let body = body.map(|b| b.into_inner()).unwrap_or_default();
    let r = api::clean_state(s.get_ref(), &queue, &job_state, body).await?;
    Ok(HttpResponse::Ok().json(r))
}

async fn promote_all_delayed(
    s: Data<DashboardState>,
    path: Path<String>,
) -> ApiResult {
    let r = api::promote_all(s.get_ref(), &path).await?;
    Ok(HttpResponse::Ok().json(r))
}

type ApiResult = std::result::Result<HttpResponse, ApiError>;

#[derive(Debug)]
struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl actix_web::error::ResponseError for ApiError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        use actix_web::http::StatusCode;
        match &self.0 {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Config(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .json(serde_json::json!({ "error": self.0.to_string() }))
    }
}
