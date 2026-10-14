//! The API as an axum [`Router`].
//!
//! ```no_run
//! # async fn demo(backend: std::sync::Arc<dyn oxn::backend::Backend>) {
//! use oxn::api::{ApiConfig, axum_router};
//! let app = axum::Router::new().nest("/oxn", axum_router(backend, ApiConfig::default()));
//! # let _ = app;
//! # }
//! ```
//!
//! The router is plain: add `.layer(...)` middleware (auth, CORS, tracing,
//! rate limiting) as you would to any other.

use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::any;
use futures::StreamExt;

use super::{Api, ApiBody, ApiConfig, ApiRequest};
use crate::backend::Backend;

/// The `/v1` API (and the deprecated `/api` aliases) over `backend`.
pub fn router(backend: Arc<dyn Backend>, config: ApiConfig) -> Router {
    router_for(Api::new(backend, config))
}

pub(crate) fn router_for(api: Api) -> Router {
    Router::new().route("/*rest", any(handle)).with_state(api)
}

async fn handle(State(api): State<Api>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, api.config().max_body_bytes).await {
        Ok(b) => b.to_vec(),
        Err(_) => {
            return into_response(super::ApiResponse::error(
                413,
                "payload_too_large",
                "request body too large",
            ));
        }
    };
    let req = ApiRequest {
        method: parts.method.as_str().to_string(),
        path: parts.uri.path().to_string(),
        query: parts.uri.query().unwrap_or("").to_string(),
        headers: parts
            .headers
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
            .collect(),
        body,
    };
    into_response(api.handle(req).await)
}

fn into_response(r: super::ApiResponse) -> Response {
    let body = match r.body {
        ApiBody::Bytes(b) => Body::from(b),
        ApiBody::Stream(s) => Body::from_stream(s.map(Ok::<_, Infallible>)),
    };
    let mut resp = Response::new(body);
    *resp.status_mut() =
        StatusCode::from_u16(r.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (k, v) in r.headers {
        if let (Ok(k), Ok(v)) = (HeaderName::try_from(k), HeaderValue::try_from(v)) {
            resp.headers_mut().append(k, v);
        }
    }
    resp
}
