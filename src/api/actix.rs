//! The API as an actix-web [`Scope`].
//!
//! ```no_run
//! # async fn demo(backend: std::sync::Arc<dyn oxn::backend::Backend>) -> std::io::Result<()> {
//! use actix_web::{App, HttpServer, web};
//! use oxn::api::{ApiConfig, actix_scope};
//! HttpServer::new(move || {
//!     App::new().service(web::scope("/oxn").service(actix_scope(backend.clone(), ApiConfig::default())))
//! })
//! .bind("127.0.0.1:8080")?
//! .run()
//! .await
//! # }
//! ```
//!
//! The scope is plain: `.wrap(...)` middleware works as usual.

use std::sync::Arc;

use actix_web::http::StatusCode;
use actix_web::web::{self, Bytes, Data, Payload};
use actix_web::{HttpRequest, HttpResponse, Scope};
use futures::StreamExt;

use super::{Api, ApiBody, ApiConfig, ApiRequest, ApiResponse};
use crate::backend::Backend;

/// The `/v1` API (and the deprecated `/api` aliases) over `backend`.
pub fn scope(backend: Arc<dyn Backend>, config: ApiConfig) -> Scope {
    with_api(web::scope(""), Api::new(backend, config))
}

/// Register the API's catch-all route on `scope` (after any routes of its
/// own, which take precedence).
pub(crate) fn with_api(scope: Scope, api: Api) -> Scope {
    scope
        .app_data(Data::new(api))
        .route("/{tail:.*}", web::to(handle))
}

async fn handle(api: Data<Api>, req: HttpRequest, mut payload: Payload) -> HttpResponse {
    let limit = api.config().max_body_bytes;
    let mut body = Vec::new();
    while let Some(chunk) = payload.next().await {
        let Ok(chunk) = chunk else {
            return into_response(ApiResponse::error(400, "bad_request", "unreadable body"));
        };
        if body.len() + chunk.len() > limit {
            return into_response(ApiResponse::error(
                413,
                "payload_too_large",
                "request body too large",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    let tail = req.match_info().get("tail").unwrap_or("");
    let req = ApiRequest {
        method: req.method().as_str().to_string(),
        path: format!("/{tail}"),
        query: req.query_string().to_string(),
        headers: req
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
            .collect(),
        body,
    };
    into_response(api.handle(req).await)
}

fn into_response(r: ApiResponse) -> HttpResponse {
    let mut b = HttpResponse::build(
        StatusCode::from_u16(r.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    );
    for (k, v) in &r.headers {
        b.append_header((k.as_str(), v.as_str()));
    }
    match r.body {
        ApiBody::Bytes(bytes) => b.body(bytes),
        ApiBody::Stream(s) => {
            b.streaming(s.map(|chunk| Ok::<_, actix_web::Error>(Bytes::from(chunk))))
        }
    }
}
