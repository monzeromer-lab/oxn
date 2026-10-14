//! `--via-api`: drive the run through oxn's HTTP API only.
//!
//! The API runs in its own process (`oxn-load api-serve`, the real axum
//! router with a bearer token). Producers add every job with
//! `POST /v1/queues/{q}/jobs/bulk`; `--api-readers` clients hammer the read
//! routes meanwhile; one client follows `GET /v1/queues/{q}/events` and
//! must count exactly the events the raw stream observer saw.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use oxn::job::Backoff;
use oxn::options::{JobOptions, Removal};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;

use crate::ledger::LoadJob;
use crate::stats::{Hist, HistSummary};

/// The bearer token the API child is started with.
const TOKEN: &str = "oxn-load-admin";

/// Child entry point: serve the API on 127.0.0.1, print the address.
pub async fn serve(redis: String) -> anyhow::Result<()> {
    let backend = crate::workload::connect(&redis, 64).await?;
    let cfg = oxn::api::ApiConfig::default().bearer(Vec::<String>::new(), [TOKEN]);
    let app = axum::Router::new().nest("/oxn", oxn::api::axum_router(backend, cfg));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("listening {}", l.local_addr()?);
    axum::serve(l, app).await?;
    Ok(())
}

/// Where the API is, and the counters of everything sent to it.
pub struct ApiTarget {
    http: reqwest::Client,
    base: String,
    pub reads: Hist,
    pub read_errors: AtomicU64,
    pub add_errors: AtomicU64,
    _child: tokio::process::Child,
}

impl ApiTarget {
    /// Start `oxn-load api-serve` and wait for its address.
    pub async fn start(redis: &str) -> anyhow::Result<Self> {
        let mut child = tokio::process::Command::new(std::env::current_exe()?)
            .arg("api-serve")
            .arg("--redis")
            .arg(redis)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("no stdout"))?;
        let mut lines = BufReader::new(stdout).lines();
        let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line())
            .await??
            .ok_or_else(|| anyhow::anyhow!("api-serve exited"))?;
        let addr = line
            .strip_prefix("listening ")
            .ok_or_else(|| anyhow::anyhow!("api-serve said {line:?}"))?;
        eprintln!("oxn-load: API server process at http://{addr}/oxn");
        Ok(Self {
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(64)
                .build()?,
            base: format!("http://{addr}/oxn/v1"),
            reads: Hist::default(),
            read_errors: AtomicU64::new(0),
            add_errors: AtomicU64::new(0),
            _child: child,
        })
    }

    fn req(&self, m: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(m, format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
    }

    /// Add jobs with `POST …/jobs/bulk`; ids in order.
    pub async fn add(
        &self,
        queue: &str,
        jobs: &[(LoadJob, JobOptions)],
    ) -> anyhow::Result<Vec<String>> {
        let body = json!({
            "jobs": jobs.iter().map(|(j, o)| json!({ "data": j, "opts": http_opts(o) })).collect::<Vec<_>>()
        });
        let r = self
            .req(reqwest::Method::POST, &format!("/queues/{queue}/jobs/bulk"))
            .json(&body)
            .send()
            .await?;
        let status = r.status();
        let v: Value = r.json().await?;
        if status != 201 {
            anyhow::bail!("bulk add: {status} {v}");
        }
        Ok(v["ids"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("no ids in {v}"))?
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect())
    }
}

/// [`JobOptions`] as the API's JSON.
fn http_opts(o: &JobOptions) -> Value {
    let removal = |r: &Removal| match r {
        Removal::Keep => json!(false),
        Removal::Remove => json!(true),
        Removal::KeepLast(n) => json!({ "keep_last": n }),
        Removal::KeepFor(d) => json!({ "keep_for_ms": d.as_millis() as u64 }),
    };
    let mut m = serde_json::Map::new();
    if let Some(v) = &o.id {
        m.insert("id".into(), json!(v));
    }
    if let Some(v) = o.delay {
        m.insert("delay_ms".into(), json!(v.as_millis() as u64));
    }
    if let Some(v) = o.priority {
        m.insert("priority".into(), json!(v));
    }
    if let Some(v) = o.attempts {
        m.insert("attempts".into(), json!(v));
    }
    if let Some(b) = &o.backoff {
        m.insert(
            "backoff".into(),
            match b {
                Backoff::Fixed { delay } => {
                    json!({ "type": "fixed", "delay_ms": delay.as_millis() as u64 })
                }
                Backoff::Exponential { initial, max } => json!({
                    "type": "exponential", "delay_ms": initial.as_millis() as u64,
                    "max_ms": max.map(|d| d.as_millis() as u64),
                }),
            },
        );
    }
    if let Some(v) = o.lifo {
        m.insert("lifo".into(), json!(v));
    }
    if let Some(r) = &o.remove_on_complete {
        m.insert("remove_on_complete".into(), removal(r));
    }
    if let Some(r) = &o.remove_on_fail {
        m.insert("remove_on_fail".into(), removal(r));
    }
    Value::Object(m)
}

/// Hammer the read routes until `stop`.
pub async fn reader(t: Arc<ApiTarget>, queue: String, stop: CancellationToken) {
    let paths = [
        format!("/queues/{queue}/counts"),
        format!("/queues/{queue}"),
        format!("/queues/{queue}/jobs?state=completed&limit=50&order=desc"),
        format!("/queues/{queue}/jobs?state=waiting&limit=20"),
        "/queues".to_string(),
    ];
    // A queue exists once its first job is added: until then a 404 for it
    // is the right answer, not an error.
    while !stop.is_cancelled() {
        let r = t.req(reqwest::Method::GET, &paths[1]).send().await;
        if r.is_ok_and(|r| r.status().is_success()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut i = 0usize;
    while !stop.is_cancelled() {
        let path = &paths[i % paths.len()];
        i += 1;
        let t0 = Instant::now();
        match t.req(reqwest::Method::GET, path).send().await {
            Ok(r) if r.status().is_success() => {
                let body: Result<Value, _> = r.json().await;
                t.reads.record(t0.elapsed().as_micros() as u64);
                // Follow one listed job to its full record.
                if let Ok(v) = body
                    && let Some(id) = v["jobs"].get(0).and_then(|j| j["id"].as_str())
                {
                    let t0 = Instant::now();
                    let r = t
                        .req(reqwest::Method::GET, &format!("/queues/{queue}/jobs/{id}"))
                        .send()
                        .await;
                    t.reads.record(t0.elapsed().as_micros() as u64);
                    let ok = r
                        .as_ref()
                        .is_ok_and(|r| r.status().is_success() || r.status() == 404);
                    if !ok {
                        let n = t.read_errors.fetch_add(1, Ordering::Relaxed);
                        if n < 5 {
                            let what = match r {
                                Ok(r) => {
                                    format!("{} {}", r.status(), r.text().await.unwrap_or_default())
                                }
                                Err(e) => format!("{e:?}"),
                            };
                            eprintln!("  ⚑ API read error #{}: GET job {id}: {what}", n + 1);
                        }
                    }
                }
            }
            other => {
                let n = t.read_errors.fetch_add(1, Ordering::Relaxed);
                if n < 5 {
                    let what = match other {
                        Ok(r) => format!("{} {}", r.status(), r.text().await.unwrap_or_default()),
                        Err(e) => format!("{e:?}"),
                    };
                    eprintln!("  ⚑ API read error #{}: GET {path}: {what}", n + 1);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Count the events the API's SSE stream delivers, by type, until `stop`.
pub async fn follow_events(
    t: Arc<ApiTarget>,
    queue: String,
    counts: Arc<Mutex<BTreeMap<String, u64>>>,
    ready: tokio::sync::oneshot::Sender<()>,
    stop: CancellationToken,
) {
    let Ok(resp) = t
        .req(
            reqwest::Method::GET,
            &format!("/queues/{queue}/events?from=now"),
        )
        .send()
        .await
    else {
        return;
    };
    let _ = ready.send(());
    let mut body = resp.bytes_stream();
    let mut buf = String::new();
    loop {
        let chunk = tokio::select! {
            _ = stop.cancelled() => break,
            c = body.next() => match c { Some(Ok(c)) => c, _ => break },
        };
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(i) = buf.find("\n\n") {
            let frame: String = buf.drain(..i + 2).collect();
            if let Some(ev) = frame.lines().find_map(|l| l.strip_prefix("event: ")) {
                *counts.lock().entry(ev.to_string()).or_default() += 1;
            }
        }
    }
}

/// What the API side of the run did.
#[derive(Debug, Serialize)]
pub struct ApiReport {
    pub read_latency: HistSummary,
    pub read_errors: u64,
    pub add_errors: u64,
    pub sse_events: BTreeMap<String, u64>,
}
