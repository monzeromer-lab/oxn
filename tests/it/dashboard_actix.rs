//! Dashboard HTTP endpoints (actix-web variant), driven in-process with
//! `actix_web::test` against a live Redis.

#![cfg(feature = "dashboard-actix")]

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use actix_web::{App, test};
use oxn::backend::Backend;
use oxn::dashboard::actix_scope;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T(u32);

#[actix_web::test]
async fn actix_lists_queues_and_maps_errors() {
    let (q, backend) = common::fresh_queue::<T>("actix").await;
    let waiting = q.add(T(1), JobOptions::new()).await.unwrap();
    q.add(T(2), JobOptions::new()).await.unwrap();
    let active = backend
        .fetch_next(
            q.name(),
            "A",
            Duration::from_secs(30),
            Duration::from_millis(10),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap()
        .job
        .id;
    let waiting = if waiting == active {
        JobId::new("2")
    } else {
        waiting
    };

    let app =
        test::init_service(App::new().service(actix_scope(backend.clone() as Arc<dyn Backend>)))
            .await;

    let req = test::TestRequest::get().uri("/api/queues").to_request();
    let queues: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert!(
        queues
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["name"] == q.name())
    );

    let req = test::TestRequest::get()
        .uri(&format!("/api/queues/{}/counts", q.name()))
        .to_request();
    let counts: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(counts["waiting"], 1);
    assert_eq!(counts["active"], 1);

    for (path, want) in [
        (format!("{waiting}/retry"), 409),
        ("missing/promote".to_string(), 404),
        (format!("{active}/remove"), 409),
        (format!("{waiting}/remove"), 204),
    ] {
        let req = test::TestRequest::post()
            .uri(&format!("/api/queues/{}/jobs/{path}", q.name()))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status().as_u16(), want, "POST {path}");
    }
    common::teardown(&q).await;
}
