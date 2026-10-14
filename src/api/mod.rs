//! A standalone HTTP API for managing queues — everything the dashboard
//! does, versioned under `/v1`, mountable in your app the same way.
//!
//! ```no_run
//! # #[cfg(feature = "api-axum")]
//! # async fn demo(backend: std::sync::Arc<dyn oxn::backend::Backend>) {
//! use oxn::api::{ApiConfig, axum_router};
//!
//! // Plain router: put your own auth middleware in front of it…
//! let app = axum::Router::new().nest("/oxn", axum_router(backend.clone(), ApiConfig::default()));
//! // …or use the built-in bearer tokens.
//! let cfg = ApiConfig::default().bearer(["reader-token"], ["admin-token"]);
//! let app = axum::Router::new().nest("/oxn", axum_router(backend, cfg));
//! # }
//! ```
//!
//! # Routes
//!
//! All paths are relative to where the router is mounted. `GET` routes
//! need read access; every other method needs admin access. Bodies are JSON;
//! errors are `{"error": {"code": "...", "message": "..."}}`. The OpenAPI
//! 3.1 description is served at `GET /v1/openapi.json`.
//!
//! | Method | Path | |
//! |---|---|---|
//! | GET | `/v1/health` | Redis reachable (no auth) |
//! | GET | `/v1/openapi.json` | OpenAPI description |
//! | GET | `/v1/whoami` | the caller's access: `read` or `admin` (what a UI may offer) |
//! | GET | `/v1/queues` | every queue with counts and pause flag |
//! | GET / DELETE | `/v1/queues/{queue}` | one queue · obliterate (`?force=true` even with active jobs) |
//! | GET | `/v1/queues/{queue}/counts` | per-state counts |
//! | POST | `/v1/queues/{queue}/pause` · `/resume` | pause / resume |
//! | POST | `/v1/queues/{queue}/drain` | delete pending jobs (`{"include_delayed": bool}`) |
//! | POST | `/v1/queues/{queue}/clean` | delete finished jobs (`{"state": "completed", "limit": 0}`) |
//! | POST | `/v1/queues/{queue}/promote-all` | run every delayed job now |
//! | GET / POST | `/v1/queues/{queue}/jobs` | list (`?state=&offset=&limit=&order=`) · add one |
//! | POST | `/v1/queues/{queue}/jobs/bulk` | add many |
//! | GET / DELETE | `/v1/queues/{queue}/jobs/{id}` | one job · remove it (with its flow tree) |
//! | GET | `/v1/queues/{queue}/jobs/{id}/logs` | log lines (`?offset=&limit=&order=`) |
//! | GET | `/v1/queues/{queue}/jobs/{id}/flow` | the job's flow tree (`?depth=`) |
//! | POST | `/v1/queues/{queue}/jobs/{id}/retry` · `/promote` | retry a failed job · run a delayed one now |
//! | GET | `/v1/queues/{queue}/schedulers` | job schedulers |
//! | DELETE | `/v1/queues/{queue}/schedulers/{id}` | remove a scheduler |
//! | GET | `/v1/queues/{queue}/events` | live events, server-sent (`?from=now|beginning|<id>`, `Last-Event-ID`) |
//!
//! The 0.2 dashboard paths (`/api/queues/...`) are kept as deprecated
//! aliases for 0.3 only.
//!
//! # Authentication
//!
//! [`Auth::None`] (the default) leaves access control to your middleware.
//! [`Auth::Bearer`] checks `Authorization: Bearer <token>` against read and
//! admin token lists in constant time. [`Auth::Custom`] hands the request
//! parts to your function (JWT, sessions, IP allow-lists, …). With
//! [`ApiConfig::read_only`], every write is refused regardless.
//!
//! # Other frameworks
//!
//! [`Api::handle`] takes a framework-neutral [`ApiRequest`] and returns an
//! [`ApiResponse`]; the axum and actix adapters are thin wrappers over it,
//! and so can yours be.

mod dto;
mod routes;
mod sse;

#[cfg(feature = "api-axum")]
#[cfg_attr(docsrs, doc(cfg(feature = "api-axum")))]
pub mod axum;

#[cfg(feature = "api-actix")]
#[cfg_attr(docsrs, doc(cfg(feature = "api-actix")))]
pub mod actix;

#[cfg(feature = "api-axum")]
pub use self::axum::router as axum_router;

#[cfg(feature = "api-actix")]
pub use self::actix::scope as actix_scope;

pub use dto::{
    AddJob, BulkAdd, FlowNodeDto, HttpBackoff, HttpJobOptions, HttpRemoval, JobDto, JobPage,
    JobSummaryDto, QueueDto,
};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;

use crate::backend::Backend;

/// The OpenAPI 3.1 description of the `/v1` routes.
pub const OPENAPI_JSON: &str = include_str!("openapi.json");

/// What a request may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Access {
    /// Nothing: answered `401`.
    Denied,
    /// `GET` routes.
    Read,
    /// Everything.
    Admin,
}

/// The parts of a request an [`Auth::Custom`] function sees.
#[derive(Debug)]
pub struct RequestParts<'a> {
    /// `GET`, `POST`, …
    pub method: &'a str,
    /// The path relative to the mount point, e.g. `/v1/queues`.
    pub path: &'a str,
    headers: &'a [(String, String)],
}

impl RequestParts<'_> {
    /// A request header, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The token of an `Authorization: Bearer <token>` header.
    pub fn bearer_token(&self) -> Option<&str> {
        let v = self.header("authorization")?;
        let (scheme, token) = v.split_once(' ')?;
        scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
    }
}

/// How requests are authenticated.
#[derive(Clone, Default)]
pub enum Auth {
    /// Every request has admin access: protect the routes with your own
    /// middleware (the router is a plain router, `.layer(...)` works).
    #[default]
    None,
    /// `Authorization: Bearer <token>`: a token in `admin` grants
    /// everything, one in `read` grants `GET` routes. Compared in constant
    /// time.
    Bearer {
        /// Tokens for read access.
        read: Vec<String>,
        /// Tokens for admin access.
        admin: Vec<String>,
    },
    /// Your function decides from the request parts.
    Custom(Arc<dyn Fn(&RequestParts<'_>) -> Access + Send + Sync>),
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Bearer { read, admin } => f
                .debug_struct("Bearer")
                .field("read", &format_args!("[{} token(s)]", read.len()))
                .field("admin", &format_args!("[{} token(s)]", admin.len()))
                .finish(),
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

/// Configuration of the API.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Authentication. Default [`Auth::None`].
    pub auth: Auth,
    /// Refuse every write (`403`), whatever the credentials. Default false.
    pub read_only: bool,
    /// Largest `limit` a list accepts. Default 500.
    pub max_page: usize,
    /// Most jobs one bulk add accepts. Default 10,000.
    pub max_bulk: usize,
    /// Largest request body. Default 10 MiB.
    pub max_body_bytes: usize,
    /// Comment sent on an idle event stream so proxies keep it open.
    /// Default 15s.
    pub sse_keepalive: Duration,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            auth: Auth::None,
            read_only: false,
            max_page: 500,
            max_bulk: 10_000,
            max_body_bytes: 10 * 1024 * 1024,
            sse_keepalive: Duration::from_secs(15),
        }
    }
}

impl ApiConfig {
    /// Use bearer tokens for read and admin access.
    #[must_use]
    pub fn bearer<R, A>(mut self, read: R, admin: A) -> Self
    where
        R: IntoIterator,
        R::Item: Into<String>,
        A: IntoIterator,
        A::Item: Into<String>,
    {
        self.auth = Auth::Bearer {
            read: read.into_iter().map(Into::into).collect(),
            admin: admin.into_iter().map(Into::into).collect(),
        };
        self
    }

    /// Decide access with your own function.
    #[must_use]
    pub fn custom_auth(
        mut self,
        f: impl Fn(&RequestParts<'_>) -> Access + Send + Sync + 'static,
    ) -> Self {
        self.auth = Auth::Custom(Arc::new(f));
        self
    }

    /// Refuse every write.
    #[must_use]
    pub fn read_only(mut self, yes: bool) -> Self {
        self.read_only = yes;
        self
    }

    /// Largest page a list returns.
    #[must_use]
    pub fn max_page(mut self, n: usize) -> Self {
        self.max_page = n.max(1);
        self
    }

    /// Largest request body.
    #[must_use]
    pub fn max_body_bytes(mut self, n: usize) -> Self {
        self.max_body_bytes = n;
        self
    }

    /// Event-stream keepalive cadence.
    #[must_use]
    pub fn sse_keepalive(mut self, d: Duration) -> Self {
        self.sse_keepalive = d;
        self
    }
}

/// A request, framework-neutral.
#[derive(Debug, Clone, Default)]
pub struct ApiRequest {
    /// `GET`, `POST`, …
    pub method: String,
    /// Path relative to the mount point (percent-encoded as received),
    /// e.g. `/v1/queues/emails/jobs/42`.
    pub path: String,
    /// The raw query string, without `?`.
    pub query: String,
    /// Request headers.
    pub headers: Vec<(String, String)>,
    /// Request body.
    pub body: Vec<u8>,
}

/// A response body.
pub enum ApiBody {
    /// A complete body.
    Bytes(Vec<u8>),
    /// A body streamed in chunks (server-sent events).
    Stream(BoxStream<'static, Vec<u8>>),
}

impl fmt::Debug for ApiBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(b) => write!(f, "Bytes({} bytes)", b.len()),
            Self::Stream(_) => f.write_str("Stream"),
        }
    }
}

/// A response, framework-neutral.
#[derive(Debug)]
pub struct ApiResponse {
    /// HTTP status.
    pub status: u16,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Body.
    pub body: ApiBody,
}

impl ApiResponse {
    pub(crate) fn json(status: u16, v: &impl serde::Serialize) -> Self {
        let body = serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec());
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: ApiBody::Bytes(body),
        }
    }

    pub(crate) fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: ApiBody::Bytes(Vec::new()),
        }
    }

    pub(crate) fn error(status: u16, code: &str, message: impl Into<String>) -> Self {
        let message = message.into();
        Self::json(
            status,
            &serde_json::json!({ "error": { "code": code, "message": message } }),
        )
    }

    pub(crate) fn header(mut self, k: &str, v: impl Into<String>) -> Self {
        self.headers.push((k.to_string(), v.into()));
        self
    }
}

/// The API service: route a request, check access, run it.
#[derive(Clone)]
pub struct Api {
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) config: Arc<ApiConfig>,
}

impl fmt::Debug for Api {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Api")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The access a route needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Needs {
    Nothing,
    Read,
    Admin,
}

impl Api {
    /// An API over `backend`.
    pub fn new(backend: Arc<dyn Backend>, config: ApiConfig) -> Self {
        Self {
            backend,
            config: Arc::new(config),
        }
    }

    /// The configuration.
    pub fn config(&self) -> &ApiConfig {
        &self.config
    }

    /// Handle one request.
    pub async fn handle(&self, req: ApiRequest) -> ApiResponse {
        if req.body.len() > self.config.max_body_bytes {
            return ApiResponse::error(413, "payload_too_large", "request body too large");
        }
        let route = match routes::Route::parse(&req.method, &req.path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };
        let parts = RequestParts {
            method: &req.method,
            path: &req.path,
            headers: &req.headers,
        };
        let access = match self.authorize(route.needs(), &parts) {
            Ok(a) => a,
            Err(resp) => return resp,
        };
        if matches!(route, routes::Route::Whoami) {
            let auth = match &self.config.auth {
                Auth::None => "none",
                Auth::Bearer { .. } => "bearer",
                Auth::Custom(_) => "custom",
            };
            let can_write = access == Access::Admin && !self.config.read_only;
            return ApiResponse::json(
                200,
                &serde_json::json!({
                    "access": if can_write { "admin" } else { "read" },
                    "read_only": self.config.read_only,
                    "auth": auth,
                }),
            )
            .header("cache-control", "no-store");
        }
        let legacy = route.is_legacy();
        let read = route.needs() == Needs::Read;
        let mut resp = routes::run(self, route, &req).await;
        // A dropped connection (a killed or stale pooled one) fails only the
        // command that finds it dead: retry an idempotent read once rather
        // than hand the client a 503. The event stream is long-lived and
        // the backend's subscription reconnects by itself.
        if read && resp.status == 503 {
            if let Ok(route) = routes::Route::parse(&req.method, &req.path) {
                if !matches!(route, routes::Route::Events(_)) {
                    resp = routes::run(self, route, &req).await;
                }
            }
        }
        if legacy && resp.status >= 400 {
            resp = legacy_error(resp);
        }
        resp.header("cache-control", "no-store")
    }

    fn authorize(&self, needs: Needs, parts: &RequestParts<'_>) -> Result<Access, ApiResponse> {
        if needs == Needs::Nothing {
            return Ok(Access::Read);
        }
        let access = match &self.config.auth {
            Auth::None => Access::Admin,
            Auth::Bearer { read, admin } => match parts.bearer_token() {
                Some(t) if admin.iter().any(|a| ct_eq(a.as_bytes(), t.as_bytes())) => Access::Admin,
                Some(t) if read.iter().any(|r| ct_eq(r.as_bytes(), t.as_bytes())) => Access::Read,
                _ => Access::Denied,
            },
            Auth::Custom(f) => f(parts),
        };
        if access == Access::Denied {
            return Err(
                ApiResponse::error(401, "unauthorized", "missing or invalid credentials")
                    .header("www-authenticate", "Bearer"),
            );
        }
        if needs == Needs::Admin {
            if self.config.read_only {
                return Err(ApiResponse::error(
                    403,
                    "read_only",
                    "this API is read-only",
                ));
            }
            if access < Access::Admin {
                return Err(ApiResponse::error(
                    403,
                    "forbidden",
                    "admin access required",
                ));
            }
        }
        Ok(access)
    }
}

/// The 0.2 dashboard's error body: `{"error": "<message>"}`.
fn legacy_error(mut r: ApiResponse) -> ApiResponse {
    let msg = match &r.body {
        ApiBody::Bytes(b) => serde_json::from_slice::<serde_json::Value>(b)
            .ok()
            .and_then(|v| v.pointer("/error/message")?.as_str().map(str::to_owned)),
        _ => None,
    };
    if let Some(msg) = msg {
        r.body = ApiBody::Bytes(
            serde_json::to_vec(&serde_json::json!({ "error": msg })).unwrap_or_default(),
        );
    }
    r
}

/// Compare two byte strings without exiting early on the first difference.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() ^ b.len()) as u8 | u8::from(a.len() != b.len());
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secreT"));
        assert!(!ct_eq(b"secret", b"secret2"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn bearer_tokens_parse_case_insensitively() {
        let h = vec![("Authorization".to_string(), "bearer  abc ".to_string())];
        let p = RequestParts {
            method: "GET",
            path: "/",
            headers: &h,
        };
        assert_eq!(p.bearer_token(), Some("abc"));
        let h = vec![("authorization".to_string(), "Basic abc".to_string())];
        let p = RequestParts {
            method: "GET",
            path: "/",
            headers: &h,
        };
        assert_eq!(p.bearer_token(), None);
    }

    #[test]
    fn the_openapi_document_is_valid_json() {
        let v: serde_json::Value = serde_json::from_str(OPENAPI_JSON).unwrap();
        assert_eq!(v["openapi"], "3.1.0");
    }
}
