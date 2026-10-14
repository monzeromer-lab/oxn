//! Phase 3 — the `/v1` HTTP API, over real HTTP, for both adapters.
//!
//! Every case runs twice: against `oxn::api::axum_router` and against
//! `oxn::api::actix_scope`, each served on a random port and mounted under
//! `/mnt` (so prefix stripping is exercised). Every `/v1` response is
//! validated against the OpenAPI document the API serves; an undocumented
//! status fails the test.

#![cfg(feature = "api-axum")]

use std::sync::Arc;
use std::time::Duration;

use crate::common;
use futures::StreamExt;
use oxn::api::{Access, ApiConfig, OPENAPI_JSON};
use oxn::backend::{Backend, Failure};
use oxn::prelude::*;
use oxn_testkit::{ms, secs};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy)]
enum Adapter {
    Axum,
    #[cfg_attr(not(feature = "api-actix"), allow(dead_code))]
    Actix,
}

struct Server {
    base: String,
    http: reqwest::Client,
}

struct Resp {
    status: u16,
    headers: reqwest::header::HeaderMap,
    json: Value,
}

async fn serve(adapter: Adapter, cfg: ApiConfig) -> Server {
    let addr = match adapter {
        Adapter::Axum => {
            let b = common::build_backend().await as Arc<dyn Backend>;
            let app = axum::Router::new().nest("/mnt", oxn::api::axum_router(b, cfg));
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(l, app).await });
            addr
        }
        Adapter::Actix => actix_server(cfg),
    };
    Server {
        base: format!("http://{addr}/mnt"),
        http: reqwest::Client::new(),
    }
}

#[cfg(feature = "api-actix")]
fn actix_server(cfg: ApiConfig) -> std::net::SocketAddr {
    use actix_web::{App, HttpServer, web};
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        actix_web::rt::System::new().block_on(async move {
            let b = common::build_backend().await as Arc<dyn Backend>;
            let srv = HttpServer::new(move || {
                App::new().service(
                    web::scope("/mnt").service(oxn::api::actix_scope(b.clone(), cfg.clone())),
                )
            })
            .workers(1)
            .bind(("127.0.0.1", 0))
            .unwrap();
            tx.send(srv.addrs()[0]).unwrap();
            srv.run().await.unwrap();
        });
    });
    rx.recv().unwrap()
}

#[cfg(not(feature = "api-actix"))]
fn actix_server(_: ApiConfig) -> std::net::SocketAddr {
    unreachable!("actix cases only run with the api-actix feature")
}

impl Server {
    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        token: Option<&str>,
    ) -> Resp {
        let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        let mut rb = self.http.request(m, format!("{}{path}", self.base));
        if let Some(t) = token {
            rb = rb.bearer_auth(t);
        }
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let r = rb.send().await.unwrap();
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        let bytes = r.bytes().await.unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Resp {
            status,
            headers,
            json,
        }
    }

    async fn get(&self, path: &str) -> Resp {
        self.call("GET", path, None, None).await
    }
    async fn post(&self, path: &str, body: Value) -> Resp {
        self.call("POST", path, Some(body), None).await
    }
    async fn delete(&self, path: &str) -> Resp {
        self.call("DELETE", path, None, None).await
    }
}

/// Validate a response against the OpenAPI operation `method template`.
fn conforms(method: &str, template: &str, r: &Resp) {
    let spec: Value = serde_json::from_str(OPENAPI_JSON).unwrap();
    let esc = |s: &str| s.replace('~', "~0").replace('/', "~1");
    let op = format!("/paths/{}/{}", esc(template), method.to_lowercase());
    assert!(
        spec.pointer(&op).is_some(),
        "{method} {template} is not documented"
    );
    let mut at = format!("{op}/responses/{}", r.status);
    let resp = spec.pointer(&at).unwrap_or_else(|| {
        panic!(
            "{method} {template}: status {} is not documented (body {})",
            r.status, r.json
        )
    });
    if let Some(Value::String(rf)) = resp.get("$ref") {
        at = rf.trim_start_matches('#').to_string();
    }
    let Some(schema_at) = spec
        .pointer(&at)
        .and_then(|x| x.pointer("/content/application~1json/schema"))
        .map(|_| format!("{at}/content/application~1json/schema"))
    else {
        return; // no JSON body documented (204, SSE)
    };
    let mut doc = spec.clone();
    // A URI fragment can't hold raw braces: `{queue}` must be encoded.
    let fragment = schema_at.replace('{', "%7B").replace('}', "%7D");
    doc["$ref"] = Value::String(format!("#{fragment}"));
    let v = jsonschema::validator_for(&doc).unwrap();
    let errors: Vec<String> = v
        .iter_errors(&r.json)
        .map(|e| format!("{e} at {}", e.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{method} {template} {}: {errors:?}\nbody: {}",
        r.status,
        r.json
    );
}

macro_rules! contract {
    ($case:ident) => {
        mod $case {
            use super::*;
            #[tokio::test(flavor = "multi_thread")]
            async fn axum() {
                super::$case(Adapter::Axum).await
            }
            #[cfg(feature = "api-actix")]
            #[tokio::test(flavor = "multi_thread")]
            async fn actix() {
                super::$case(Adapter::Actix).await
            }
        }
    };
}

async fn health_and_spec(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let r = s.get("/v1/health").await;
    assert_eq!(r.status, 200, "{}", r.json);
    conforms("GET", "/v1/health", &r);
    let r = s.get("/v1/openapi.json").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json["openapi"], "3.1.0");
    assert_eq!(r.headers["cache-control"], "no-store");
}
contract!(health_and_spec);

async fn queues_counts_and_listing(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let (q, _b) = common::fresh_queue::<Value>("api-list").await;
    let n = q.name().to_string();
    for i in 0..5 {
        q.add(json!({ "i": i }), JobOptions::new()).await.unwrap();
    }
    let r = s.get("/v1/queues").await;
    assert_eq!(r.status, 200);
    conforms("GET", "/v1/queues", &r);
    assert!(
        r.json
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["name"] == n && x["counts"]["waiting"] == 5)
    );
    let r = s.get(&format!("/v1/queues/{n}")).await;
    assert_eq!(r.status, 200);
    conforms("GET", "/v1/queues/{queue}", &r);
    let r = s.get("/v1/queues/no-such-queue-xyz").await;
    assert_eq!(r.status, 404);
    conforms("GET", "/v1/queues/{queue}", &r);
    let r = s.get(&format!("/v1/queues/{n}/counts")).await;
    assert_eq!(r.json["waiting"], 5);
    conforms("GET", "/v1/queues/{queue}/counts", &r);

    let r = s
        .get(&format!("/v1/queues/{n}/jobs?state=waiting&limit=2"))
        .await;
    assert_eq!(r.status, 200, "{}", r.json);
    conforms("GET", "/v1/queues/{queue}/jobs", &r);
    assert_eq!(
        (
            r.json["total"].as_u64(),
            r.json["jobs"].as_array().unwrap().len()
        ),
        (Some(5), 2)
    );
    assert_eq!(
        r.json["jobs"][0]["id"], "1",
        "processing order: the next to run first"
    );
    let r = s
        .get(&format!(
            "/v1/queues/{n}/jobs?state=waiting&offset=2&limit=2&order=desc"
        ))
        .await;
    assert_eq!(r.json["jobs"][0]["id"], "3");
    for bad in [
        "state=bogus",
        "limit=0",
        "limit=100000",
        "offset=-1",
        "order=sideways",
    ] {
        let r = s.get(&format!("/v1/queues/{n}/jobs?{bad}")).await;
        assert_eq!(r.status, 400, "{bad}: {}", r.json);
        conforms("GET", "/v1/queues/{queue}/jobs", &r);
    }
    common::teardown(&q).await;
}
contract!(queues_counts_and_listing);

async fn adding_jobs(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let (q, _b) = common::fresh_queue::<Value>("api-add").await;
    let n = q.name().to_string();
    let r = s
        .post(
            &format!("/v1/queues/{n}/jobs"),
            json!({
                "data": { "to": "a@b.c" }, "name": "email",
                "opts": { "attempts": 3, "priority": 2, "backoff": { "type": "fixed", "delay_ms": 100 },
                          "remove_on_complete": { "keep_last": 10 } }
            }),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.json);
    conforms("POST", "/v1/queues/{queue}/jobs", &r);
    let id = r.json["id"].as_str().unwrap().to_string();
    assert!(
        r.headers["location"]
            .to_str()
            .unwrap()
            .ends_with(&format!("/jobs/{id}"))
    );
    let r = s.get(&format!("/v1/queues/{n}/jobs/{id}")).await;
    assert_eq!(r.status, 200);
    conforms("GET", "/v1/queues/{queue}/jobs/{id}", &r);
    assert_eq!(r.json["data"]["to"], "a@b.c");
    assert_eq!(r.json["name"], "email");
    assert_eq!(r.json["state"], "prioritized");
    assert_eq!(r.json["attempts_total"], 3);
    // The job is a normal oxn job: a typed queue reads it.
    let job = q.get(&JobId::new(&id)).await.unwrap().unwrap();
    assert_eq!(job.opts.priority, 2);

    // Explicit ids are idempotent; invalid input is refused.
    let body = json!({ "data": 1, "opts": { "id": "order-7" } });
    let first = s.post(&format!("/v1/queues/{n}/jobs"), body.clone()).await;
    let again = s.post(&format!("/v1/queues/{n}/jobs"), body).await;
    assert_eq!(
        (first.json["id"].clone(), again.json["id"].clone()),
        (json!("order-7"), json!("order-7"))
    );
    for bad in [
        json!({ "data": 1, "opts": { "id": "has:colon" } }),
        json!({ "data": 1, "opts": { "atempts": 3 } }),
        json!({ "data": 1, "opts": { "attempts": 0 } }),
        json!({ "nodata": 1 }),
    ] {
        let r = s.post(&format!("/v1/queues/{n}/jobs"), bad.clone()).await;
        assert_eq!(r.status, 400, "{bad}: {}", r.json);
        conforms("POST", "/v1/queues/{queue}/jobs", &r);
    }

    // Bulk: all or nothing.
    let jobs: Vec<Value> = (0..3).map(|i| json!({ "data": { "i": i } })).collect();
    let r = s
        .post(
            &format!("/v1/queues/{n}/jobs/bulk"),
            json!({ "jobs": jobs }),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.json);
    conforms("POST", "/v1/queues/{queue}/jobs/bulk", &r);
    assert_eq!(r.json["ids"].as_array().unwrap().len(), 3);
    let before = q.counts().await.unwrap();
    let r = s
        .post(
            &format!("/v1/queues/{n}/jobs/bulk"),
            json!({ "jobs": [ { "data": 1 }, { "data": 2, "opts": { "id": "bad:id" } } ] }),
        )
        .await;
    assert_eq!(r.status, 400);
    assert!(
        r.json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("jobs[1]")
    );
    assert_eq!(
        q.counts().await.unwrap().waiting,
        before.waiting,
        "nothing added"
    );
    let r = s
        .post(&format!("/v1/queues/{n}/jobs/bulk"), json!({ "jobs": [] }))
        .await;
    assert_eq!(r.status, 400);
    common::teardown(&q).await;
}
contract!(adding_jobs);

async fn job_operations(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let (q, b) = common::fresh_queue::<Value>("api-ops").await;
    let n = q.name().to_string();
    let delayed = q
        .add(json!(1), JobOptions::new().delay(secs(60)))
        .await
        .unwrap();
    let r = s
        .post(&format!("/v1/queues/{n}/jobs/{delayed}/promote"), json!({}))
        .await;
    assert_eq!(r.status, 204, "{}", r.json);
    let r = s
        .post(&format!("/v1/queues/{n}/jobs/{delayed}/promote"), json!({}))
        .await;
    assert_eq!(
        (r.status, r.json["error"]["code"].clone()),
        (409, json!("not_in_expected_state"))
    );
    conforms("POST", "/v1/queues/{queue}/jobs/{id}/promote", &r);

    // Fail a job (as a worker would), then retry it.
    let f = b
        .fetch_next(&n, "w", secs(30), ms(10), &CancellationToken::new(), None)
        .await
        .unwrap()
        .unwrap();
    b.log(&n, &f.job.id, "line one").await.unwrap();
    b.log(&n, &f.job.id, "line two").await.unwrap();
    // While running, it can't be removed.
    let r = s.delete(&format!("/v1/queues/{n}/jobs/{}", f.job.id)).await;
    assert_eq!(
        (r.status, r.json["error"]["code"].clone()),
        (409, json!("job_locked"))
    );
    conforms("DELETE", "/v1/queues/{queue}/jobs/{id}", &r);
    b.fail(&n, &f.job.id, &f.token, Failure::new("boom"))
        .await
        .unwrap();
    let r = s.get(&format!("/v1/queues/{n}/jobs/{}", f.job.id)).await;
    assert_eq!(
        (r.json["state"].clone(), r.json["failed_reason"].clone()),
        (json!("failed"), json!("boom"))
    );
    let r = s
        .get(&format!("/v1/queues/{n}/jobs/{}/logs", f.job.id))
        .await;
    assert_eq!(r.json["logs"], json!(["line one", "line two"]));
    conforms("GET", "/v1/queues/{queue}/jobs/{id}/logs", &r);
    let r = s
        .get(&format!(
            "/v1/queues/{n}/jobs/{}/logs?order=desc&limit=1",
            f.job.id
        ))
        .await;
    assert_eq!(r.json["logs"], json!(["line two"]));
    let r = s
        .post(
            &format!("/v1/queues/{n}/jobs/{}/retry", f.job.id),
            json!({}),
        )
        .await;
    assert_eq!(r.status, 204);
    let r = s
        .post(
            &format!("/v1/queues/{n}/jobs/{}/retry", f.job.id),
            json!({}),
        )
        .await;
    assert_eq!(r.status, 409);
    conforms("POST", "/v1/queues/{queue}/jobs/{id}/retry", &r);

    let r = s.delete(&format!("/v1/queues/{n}/jobs/{delayed}")).await;
    assert_eq!(r.status, 204);
    for (m, p) in [
        ("GET", format!("/v1/queues/{n}/jobs/{delayed}")),
        ("DELETE", format!("/v1/queues/{n}/jobs/{delayed}")),
        ("GET", format!("/v1/queues/{n}/jobs/{delayed}/logs")),
        ("POST", format!("/v1/queues/{n}/jobs/nope/retry")),
    ] {
        let r = s.call(m, &p, None, None).await;
        assert_eq!(r.status, 404, "{m} {p}: {}", r.json);
    }
    common::teardown(&q).await;
}
contract!(job_operations);

async fn queue_operations(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let (q, b) = common::fresh_queue::<Value>("api-queue").await;
    let n = q.name().to_string();
    q.add_bulk((0..4).map(|i| (json!(i), JobOptions::new())).collect())
        .await
        .unwrap();
    q.add(json!("later"), JobOptions::new().delay(secs(60)))
        .await
        .unwrap();
    assert_eq!(
        s.post(&format!("/v1/queues/{n}/pause"), json!({}))
            .await
            .status,
        204
    );
    assert!(q.is_paused().await.unwrap());
    let r = s.get(&format!("/v1/queues/{n}")).await;
    assert_eq!(r.json["paused"], true);
    assert_eq!(
        s.post(&format!("/v1/queues/{n}/resume"), json!({}))
            .await
            .status,
        204
    );
    let r = s
        .post(&format!("/v1/queues/{n}/promote-all"), json!({}))
        .await;
    assert_eq!(r.json["count"], 1);
    conforms("POST", "/v1/queues/{queue}/promote-all", &r);
    // Finish one job, then clean it.
    let f = b
        .fetch_next(&n, "w", secs(30), ms(10), &CancellationToken::new(), None)
        .await
        .unwrap()
        .unwrap();
    b.complete(&n, &f.job.id, &f.token, None, oxn::options::Removal::Keep)
        .await
        .unwrap();
    let r = s
        .post(
            &format!("/v1/queues/{n}/clean"),
            json!({ "state": "completed" }),
        )
        .await;
    assert_eq!(r.json["count"], 1, "{}", r.json);
    conforms("POST", "/v1/queues/{queue}/clean", &r);
    let r = s
        .post(&format!("/v1/queues/{n}/clean"), json!({ "state": "nope" }))
        .await;
    assert_eq!(r.status, 400);
    assert_eq!(
        s.call("POST", &format!("/v1/queues/{n}/drain"), None, None)
            .await
            .status,
        204,
        "an empty body is fine"
    );
    assert_eq!(q.counts().await.unwrap().waiting, 0);

    // Obliterate: refused while a job runs, unless forced.
    q.add(json!("x"), JobOptions::new()).await.unwrap();
    b.fetch_next(&n, "w", secs(30), ms(10), &CancellationToken::new(), None)
        .await
        .unwrap()
        .unwrap();
    let r = s.delete(&format!("/v1/queues/{n}")).await;
    assert_eq!(
        (r.status, r.json["error"]["code"].clone()),
        (409, json!("active_jobs"))
    );
    conforms("DELETE", "/v1/queues/{queue}", &r);
    assert_eq!(
        s.delete(&format!("/v1/queues/{n}?force=true")).await.status,
        204
    );
    assert_eq!(s.get(&format!("/v1/queues/{n}")).await.status, 404);
}
contract!(queue_operations);

#[cfg(feature = "flow")]
async fn flow_tree(a: Adapter) {
    use oxn::flow::{FlowNode, FlowProducer};
    let s = serve(a, ApiConfig::default()).await;
    let (q, b) = common::fresh_queue::<Value>("api-flow").await;
    let n = q.name().to_string();
    let node = |name: &str| FlowNode::new(&n, name, &json!(name), JobOptions::new()).unwrap();
    let tree = FlowProducer::new(b.clone())
        .add(
            node("root")
                .child(node("a").child(node("a1")))
                .child(node("b")),
        )
        .await
        .unwrap();
    let r = s
        .get(&format!("/v1/queues/{n}/jobs/{}/flow", tree.id))
        .await;
    assert_eq!(r.status, 200, "{}", r.json);
    conforms("GET", "/v1/queues/{queue}/jobs/{id}/flow", &r);
    assert_eq!(r.json["state"], "waiting-children");
    let kids = r.json["children"].as_array().unwrap();
    assert_eq!(kids.len(), 2);
    assert!(kids.iter().all(|k| k["dependency"] == "pending"));
    let a_node = kids.iter().find(|k| k["name"] == "a").unwrap();
    assert_eq!(a_node["children"][0]["name"], "a1");
    let r = s
        .get(&format!("/v1/queues/{n}/jobs/{}/flow?depth=1", tree.id))
        .await;
    let a_node = r.json["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["name"] == "a")
        .unwrap()
        .clone();
    assert_eq!(
        (
            a_node["children"].as_array().unwrap().len(),
            a_node["truncated"].clone()
        ),
        (0, json!(true))
    );
    let child = s
        .get(&format!("/v1/queues/{n}/jobs/{}", tree.children[1].id))
        .await;
    assert_eq!(child.json["parent"], json!({ "queue": n, "id": tree.id }));
    assert_eq!(
        s.get(&format!("/v1/queues/{n}/jobs/{}/flow?depth=99", tree.id))
            .await
            .status,
        400
    );
    common::teardown(&q).await;
}
#[cfg(feature = "flow")]
contract!(flow_tree);

#[cfg(feature = "scheduler")]
async fn schedulers(a: Adapter) {
    use oxn::scheduler::{JobScheduler, RepeatOptions};
    let s = serve(a, ApiConfig::default()).await;
    let (q, _b) = common::fresh_queue::<Value>("api-sched").await;
    let n = q.name().to_string();
    JobScheduler::new(
        q.clone(),
        "nightly",
        RepeatOptions::cron("0 0 2 * * *").unwrap(),
        json!({}),
    )
    .enqueue_next()
    .await
    .unwrap();
    let r = s.get(&format!("/v1/queues/{n}/schedulers")).await;
    assert_eq!(r.status, 200);
    conforms("GET", "/v1/queues/{queue}/schedulers", &r);
    assert_eq!(r.json[0]["id"], "nightly");
    assert_eq!(r.json[0]["repeat"], json!({ "cron": "0 0 2 * * *" }));
    let r = s
        .delete(&format!("/v1/queues/{n}/schedulers/nightly"))
        .await;
    assert_eq!(r.status, 204);
    let r = s
        .delete(&format!("/v1/queues/{n}/schedulers/nightly"))
        .await;
    assert_eq!(r.status, 404);
    conforms("DELETE", "/v1/queues/{queue}/schedulers/{id}", &r);
    assert_eq!(
        q.counts().await.unwrap().delayed,
        0,
        "its pending tick went with it"
    );
    common::teardown(&q).await;
}
#[cfg(feature = "scheduler")]
contract!(schedulers);

async fn routing_errors(a: Adapter) {
    let s = serve(a, ApiConfig::default().max_body_bytes(64 * 1024)).await;
    let r = s.get("/v1/nothing-here").await;
    assert_eq!(
        (r.status, r.json["error"]["code"].clone()),
        (404, json!("not_found"))
    );
    let r = s.call("PUT", "/v1/queues/x/jobs", None, None).await;
    assert_eq!(r.status, 405);
    assert_eq!(r.headers["allow"], "GET, POST");
    let r = s.get("/v1/queues/bad:name/counts").await;
    assert_eq!(r.status, 400);
    let r = s
        .post(
            "/v1/queues/x/jobs",
            json!({ "data": "y".repeat(70 * 1024) }),
        )
        .await;
    assert_eq!(r.status, 413, "{}", r.json);
    let r = s.call("POST", "/v1/queues/x/jobs", None, None).await;
    assert_eq!(r.status, 400, "a missing body is not a valid job");
}
contract!(routing_errors);

/// Read SSE frames until `pred` matches one; returns every frame read.
async fn read_events(
    resp: reqwest::Response,
    mut pred: impl FnMut(&str, &Value) -> bool,
) -> Vec<(String, String, Value)> {
    let mut buf = String::new();
    let mut out = Vec::new();
    let mut body = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + secs(5);
    loop {
        let chunk = tokio::time::timeout_at(deadline, body.next())
            .await
            .expect("event within 5s");
        buf.push_str(std::str::from_utf8(&chunk.unwrap().unwrap()).unwrap());
        while let Some(i) = buf.find("\n\n") {
            let frame: String = buf.drain(..i + 2).collect();
            let field = |k: &str| {
                frame
                    .lines()
                    .find_map(|l| l.strip_prefix(k))
                    .map(str::to_string)
            };
            if let (Some(id), Some(ev), Some(data)) =
                (field("id: "), field("event: "), field("data: "))
            {
                let data: Value = serde_json::from_str(&data).unwrap();
                let hit = pred(&ev, &data);
                out.push((id, ev, data));
                if hit {
                    return out;
                }
            }
        }
    }
}

async fn events_stream(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let (q, _b) = common::fresh_queue::<Value>("api-sse").await;
    let n = q.name().to_string();
    let url = format!("{}/v1/queues/{n}/events", s.base);
    let resp = s.http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["content-type"], "text/event-stream");
    let first = s
        .post(&format!("/v1/queues/{n}/jobs"), json!({ "data": 1 }))
        .await
        .json["id"]
        .clone();
    let got = read_events(resp, |ev, d| ev == "added" && d["id"] == first).await;
    let (last_id, ev, data) = got.last().unwrap().clone();
    assert_eq!(ev, "added");
    assert_eq!(data["event_id"], last_id);
    assert!(data["at_ms"].as_i64().unwrap() > 0);

    // Disconnected; a job is added; reconnecting with Last-Event-ID resumes
    // right after the last event seen.
    let second = s
        .post(&format!("/v1/queues/{n}/jobs"), json!({ "data": 2 }))
        .await
        .json["id"]
        .clone();
    let resp = s
        .http
        .get(&url)
        .header("last-event-id", &last_id)
        .send()
        .await
        .unwrap();
    let got = read_events(resp, |ev, d| ev == "added" && d["id"] == second).await;
    assert!(
        got.iter().all(|(id, ..)| id > &last_id),
        "only newer events: {got:?}"
    );
    assert!(
        got.iter().all(|(_, _, d)| d["id"] != first),
        "nothing replayed: {got:?}"
    );
    let r = s.get(&format!("/v1/queues/{n}/events?from=garbage")).await;
    assert_eq!(r.status, 400);
    common::teardown(&q).await;
}
contract!(events_stream);

async fn auth_matrix(a: Adapter) {
    let (q, _b) = common::fresh_queue::<Value>("api-auth").await;
    let n = q.name().to_string();
    let routes: Vec<(&str, String, Option<Value>, bool)> = vec![
        ("GET", "/v1/queues".into(), None, false),
        ("GET", format!("/v1/queues/{n}/counts"), None, false),
        ("GET", format!("/v1/queues/{n}/jobs"), None, false),
        ("GET", "/v1/openapi.json".into(), None, false),
        (
            "POST",
            format!("/v1/queues/{n}/pause"),
            Some(json!({})),
            true,
        ),
        (
            "POST",
            format!("/v1/queues/{n}/jobs"),
            Some(json!({ "data": 1 })),
            true,
        ),
        ("DELETE", format!("/v1/queues/{n}/jobs/nope"), None, true),
        ("POST", "/api/queues/x/pause".into(), Some(json!({})), true),
        ("GET", "/api/queues".into(), None, false),
    ];
    let s = serve(a, ApiConfig::default().bearer(["reader"], ["admin"])).await;
    for (m, p, body, admin) in &routes {
        let anon = s.call(m, p, body.clone(), None).await;
        assert_eq!(anon.status, 401, "anonymous {m} {p}");
        assert_eq!(anon.headers["www-authenticate"], "Bearer");
        let wrong = s.call(m, p, body.clone(), Some("nope")).await;
        assert_eq!(wrong.status, 401, "wrong token {m} {p}");
        let read = s.call(m, p, body.clone(), Some("reader")).await;
        if *admin {
            assert_eq!(
                (read.status, read.json["error"]["code"].clone()),
                (403, json!("forbidden")),
                "reader {m} {p}"
            );
        } else {
            assert!(read.status < 400, "reader {m} {p}: {}", read.status);
        }
        let adm = s.call(m, p, body.clone(), Some("admin")).await;
        assert!(
            ![401, 403].contains(&adm.status),
            "admin {m} {p}: {}",
            adm.status
        );
    }
    assert_eq!(
        s.get("/v1/health").await.status,
        200,
        "health needs no credentials"
    );

    let ro = serve(a, ApiConfig::default().read_only(true)).await;
    let r = ro.post(&format!("/v1/queues/{n}/pause"), json!({})).await;
    assert_eq!(
        (r.status, r.json["error"]["code"].clone()),
        (403, json!("read_only"))
    );
    assert_eq!(ro.get("/v1/queues").await.status, 200);

    let custom = serve(
        a,
        ApiConfig::default().custom_auth(|p| match p.header("x-role") {
            Some("ops") => Access::Admin,
            Some("viewer") => Access::Read,
            _ => Access::Denied,
        }),
    )
    .await;
    let with_role = |role: &'static str| {
        let c = &custom;
        let url = format!("{}/v1/queues/{n}/pause", c.base);
        async move {
            c.http
                .post(url)
                .header("x-role", role)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };
    assert_eq!(with_role("ops").await, 204);
    assert_eq!(with_role("viewer").await, 403);
    assert_eq!(with_role("intruder").await, 401);
    common::teardown(&q).await;
}
contract!(auth_matrix);

async fn legacy_dashboard_paths(a: Adapter) {
    let s = serve(a, ApiConfig::default()).await;
    let (q, _b) = common::fresh_queue::<Value>("api-legacy").await;
    let n = q.name().to_string();
    let id = q.add(json!({ "k": 1 }), JobOptions::new()).await.unwrap();
    let r = s
        .get(&format!("/api/queues/{n}/jobs?state=waiting&start=0&end=9"))
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json["total"], 1);
    assert_eq!(r.json["jobs"][0]["id"], id.as_str());
    let r = s.get(&format!("/api/queues/{n}/jobs/{id}")).await;
    assert_eq!(r.json["data"], "{\"k\":1}", "0.2 shape: the raw record");
    let r = s
        .post(&format!("/api/queues/{n}/jobs/{id}/remove"), json!({}))
        .await;
    assert_eq!(r.status, 204);
    common::teardown(&q).await;
}
contract!(legacy_dashboard_paths);

/// The routers are plain: host middleware wraps them like anything else.
#[tokio::test(flavor = "multi_thread")]
async fn axum_middleware_composes() {
    use axum::http::StatusCode;
    use axum::middleware::{self, Next};
    async fn key(
        req: axum::extract::Request,
        next: Next,
    ) -> Result<axum::response::Response, StatusCode> {
        if req.headers().get("x-key").is_some_and(|v| v == "k") {
            Ok(next.run(req).await)
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
    let b = common::build_backend().await as Arc<dyn Backend>;
    let app = axum::Router::new().nest(
        "/mnt",
        oxn::api::axum_router(b, ApiConfig::default()).layer(middleware::from_fn(key)),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await });
    let c = reqwest::Client::new();
    let url = format!("http://{addr}/mnt/v1/queues");
    assert_eq!(c.get(&url).send().await.unwrap().status(), 401);
    assert_eq!(
        c.get(&url)
            .header("x-key", "k")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[cfg(feature = "api-actix")]
#[tokio::test(flavor = "multi_thread")]
async fn actix_middleware_composes() {
    use actix_web::dev::Service;
    use actix_web::{App, HttpServer, web};
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        actix_web::rt::System::new().block_on(async move {
            let b = common::build_backend().await as Arc<dyn Backend>;
            let srv = HttpServer::new(move || {
                App::new().service(
                    web::scope("/mnt")
                        .wrap_fn(|req, srv| {
                            let ok = req.headers().get("x-key").is_some_and(|v| v == "k");
                            let fut = ok.then(|| srv.call(req));
                            async move {
                                match fut {
                                    Some(f) => f.await,
                                    None => Err(actix_web::error::ErrorUnauthorized("nope")),
                                }
                            }
                        })
                        .service(oxn::api::actix_scope(b.clone(), ApiConfig::default())),
                )
            })
            .workers(1)
            .bind(("127.0.0.1", 0))
            .unwrap();
            tx.send(srv.addrs()[0]).unwrap();
            srv.run().await.unwrap();
        });
    });
    let addr = rx.recv().unwrap();
    let c = reqwest::Client::new();
    let url = format!("http://{addr}/mnt/v1/queues");
    assert_eq!(c.get(&url).send().await.unwrap().status(), 401);
    assert_eq!(
        c.get(&url)
            .header("x-key", "k")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

/// Load: many concurrent clients through the API while workers run.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_clients_and_workers() {
    let s = Arc::new(serve(Adapter::Axum, ApiConfig::default()).await);
    let (q, _b) = common::fresh_queue::<Value>("api-load").await;
    let n = q.name().to_string();
    let w = Worker::builder(q.clone(), |_j: Job<Value>| async { Ok::<_, Error>(()) })
        .concurrency(16)
        .drain_delay(ms(20))
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    let t = std::time::Instant::now();
    let mut tasks = Vec::new();
    for c in 0..16 {
        let (s, n) = (s.clone(), n.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..50 {
                let r = s
                    .post(
                        &format!("/v1/queues/{n}/jobs"),
                        json!({ "data": { "c": c, "i": i } }),
                    )
                    .await;
                assert_eq!(r.status, 201, "{}", r.json);
                if i % 10 == 0 {
                    assert_eq!(s.get(&format!("/v1/queues/{n}/counts")).await.status, 200);
                    assert_eq!(
                        s.get(&format!("/v1/queues/{n}/jobs?state=completed&limit=20"))
                            .await
                            .status,
                        200
                    );
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let took = t.elapsed();
    let deadline = std::time::Instant::now() + secs(10);
    while q.counts().await.unwrap().completed < 800 {
        assert!(
            std::time::Instant::now() < deadline,
            "{:?}",
            q.counts().await.unwrap()
        );
        tokio::time::sleep(ms(20)).await;
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    println!("800 adds + 240 reads from 16 clients in {took:?}");
    let _ = Duration::ZERO;
    common::teardown(&q).await;
}
