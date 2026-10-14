//! Routing and handlers.

use serde::Deserialize;
use serde_json::{Value, json};

use super::dto::{self, FlowNodeDto, JobDto, JobPage, JobSummaryDto, QueueDto};
use super::{Api, ApiRequest, ApiResponse, Needs, OPENAPI_JSON};
use crate::backend::{JobInsert, JobRange, StateCounts};
use crate::error::{Error, ScriptError};
use crate::job::{JobId, JobRef, JobState};
use crate::options::JobOptions;

type Q = String;

/// A matched route.
#[derive(Debug)]
pub(crate) enum Route {
    Health,
    OpenApi,
    Whoami,
    Queues,
    Queue(Q),
    Obliterate(Q),
    Counts(Q),
    Pause(Q),
    Resume(Q),
    Drain(Q),
    Clean(Q),
    PromoteAll(Q),
    ListJobs(Q),
    AddJob(Q),
    BulkAdd(Q),
    GetJob(Q, JobId),
    RemoveJob(Q, JobId),
    Logs(Q, JobId),
    Flow(Q, JobId),
    Retry(Q, JobId),
    Promote(Q, JobId),
    Schedulers(Q),
    RemoveScheduler(Q, String),
    Events(Q),
    // 0.2 dashboard paths, deprecated.
    LegacyQueues,
    LegacyCounts(Q),
    LegacyJobs(Q),
    LegacyJob(Q, JobId),
    LegacyLogs(Q, JobId),
    LegacyRetry(Q, JobId),
    LegacyPromote(Q, JobId),
    LegacyRemove(Q, JobId),
    LegacyPause(Q),
    LegacyResume(Q),
    LegacyDrain(Q),
    LegacyClean(Q, String),
    LegacyPromoteAll(Q),
}

/// Percent-decode one path segment or query component.
fn decode(s: &str, plus_is_space: bool) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

pub(crate) fn query_pairs(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .filter_map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            Some((decode(k, true)?, decode(v, true)?))
        })
        .collect()
}

fn param<'a>(q: &'a [(String, String)], k: &str) -> Option<&'a str> {
    q.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

fn bad(msg: impl Into<String>) -> ApiResponse {
    ApiResponse::error(400, "bad_request", msg)
}

impl Route {
    /// Match `path` and `method`: `404` for an unknown path, `405` (with
    /// `Allow`) for a known path and another method, `400` for an invalid
    /// queue name.
    pub(crate) fn parse(method: &str, path: &str) -> Result<Route, ApiResponse> {
        let raw: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut segs = Vec::with_capacity(raw.len());
        for s in raw {
            segs.push(decode(s, false).ok_or_else(|| bad("malformed path"))?);
        }
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let q = |n: &str| -> Result<String, ApiResponse> {
            crate::validate::queue_name(n).map_err(|e| bad(e.to_string()))?;
            Ok(n.to_string())
        };
        let id = |n: &str| JobId::new(n);
        let not_found = || ApiResponse::error(404, "not_found", format!("no route for {path}"));
        // (allowed methods, route for the request's method)
        let pick = |options: Vec<(&'static str, Route)>| -> Result<Route, ApiResponse> {
            let allow: Vec<&str> = options.iter().map(|(m, _)| *m).collect();
            options
                .into_iter()
                .find(|(m, _)| m.eq_ignore_ascii_case(method))
                .map(|(_, r)| r)
                .ok_or_else(|| {
                    ApiResponse::error(
                        405,
                        "method_not_allowed",
                        format!("{method} not allowed here"),
                    )
                    .header("allow", allow.join(", "))
                })
        };
        use Route::*;
        match s.as_slice() {
            ["v1", "health"] => pick(vec![("GET", Health)]),
            ["v1", "openapi.json"] => pick(vec![("GET", OpenApi)]),
            ["v1", "whoami"] => pick(vec![("GET", Whoami)]),
            ["v1", "queues"] => pick(vec![("GET", Queues)]),
            ["v1", "queues", n] => pick(vec![("GET", Queue(q(n)?)), ("DELETE", Obliterate(q(n)?))]),
            ["v1", "queues", n, "counts"] => pick(vec![("GET", Counts(q(n)?))]),
            ["v1", "queues", n, "pause"] => pick(vec![("POST", Pause(q(n)?))]),
            ["v1", "queues", n, "resume"] => pick(vec![("POST", Resume(q(n)?))]),
            ["v1", "queues", n, "drain"] => pick(vec![("POST", Drain(q(n)?))]),
            ["v1", "queues", n, "clean"] => pick(vec![("POST", Clean(q(n)?))]),
            ["v1", "queues", n, "promote-all"] => pick(vec![("POST", PromoteAll(q(n)?))]),
            ["v1", "queues", n, "jobs"] => {
                pick(vec![("GET", ListJobs(q(n)?)), ("POST", AddJob(q(n)?))])
            }
            ["v1", "queues", n, "jobs", "bulk"] if method.eq_ignore_ascii_case("POST") => {
                Ok(BulkAdd(q(n)?))
            }
            ["v1", "queues", n, "jobs", j] => pick(vec![
                ("GET", GetJob(q(n)?, id(j))),
                ("DELETE", RemoveJob(q(n)?, id(j))),
            ]),
            ["v1", "queues", n, "jobs", j, "logs"] => pick(vec![("GET", Logs(q(n)?, id(j)))]),
            ["v1", "queues", n, "jobs", j, "flow"] => pick(vec![("GET", Flow(q(n)?, id(j)))]),
            ["v1", "queues", n, "jobs", j, "retry"] => pick(vec![("POST", Retry(q(n)?, id(j)))]),
            ["v1", "queues", n, "jobs", j, "promote"] => {
                pick(vec![("POST", Promote(q(n)?, id(j)))])
            }
            ["v1", "queues", n, "schedulers"] => pick(vec![("GET", Schedulers(q(n)?))]),
            ["v1", "queues", n, "schedulers", sid] => {
                pick(vec![("DELETE", RemoveScheduler(q(n)?, sid.to_string()))])
            }
            ["v1", "queues", n, "events"] => pick(vec![("GET", Events(q(n)?))]),
            ["api", "queues"] => pick(vec![("GET", LegacyQueues)]),
            ["api", "queues", n, "counts"] => pick(vec![("GET", LegacyCounts(q(n)?))]),
            ["api", "queues", n, "jobs"] => pick(vec![("GET", LegacyJobs(q(n)?))]),
            ["api", "queues", n, "jobs", j] => pick(vec![("GET", LegacyJob(q(n)?, id(j)))]),
            ["api", "queues", n, "jobs", j, "logs"] => {
                pick(vec![("GET", LegacyLogs(q(n)?, id(j)))])
            }
            ["api", "queues", n, "jobs", j, "retry"] => {
                pick(vec![("POST", LegacyRetry(q(n)?, id(j)))])
            }
            ["api", "queues", n, "jobs", j, "promote"] => {
                pick(vec![("POST", LegacyPromote(q(n)?, id(j)))])
            }
            ["api", "queues", n, "jobs", j, "remove"] => {
                pick(vec![("POST", LegacyRemove(q(n)?, id(j)))])
            }
            ["api", "queues", n, "pause"] => pick(vec![("POST", LegacyPause(q(n)?))]),
            ["api", "queues", n, "resume"] => pick(vec![("POST", LegacyResume(q(n)?))]),
            ["api", "queues", n, "drain"] => pick(vec![("POST", LegacyDrain(q(n)?))]),
            ["api", "queues", n, "clean", st] => {
                pick(vec![("POST", LegacyClean(q(n)?, st.to_string()))])
            }
            ["api", "queues", n, "promote-all"] => pick(vec![("POST", LegacyPromoteAll(q(n)?))]),
            _ => Err(not_found()),
        }
    }

    pub(crate) fn is_legacy(&self) -> bool {
        use Route::*;
        matches!(
            self,
            LegacyQueues
                | LegacyCounts(_)
                | LegacyJobs(_)
                | LegacyJob(..)
                | LegacyLogs(..)
                | LegacyRetry(..)
                | LegacyPromote(..)
                | LegacyRemove(..)
                | LegacyPause(_)
                | LegacyResume(_)
                | LegacyDrain(_)
                | LegacyClean(..)
                | LegacyPromoteAll(_)
        )
    }

    pub(crate) fn needs(&self) -> Needs {
        use Route::*;
        match self {
            Health => Needs::Nothing,
            OpenApi | Whoami | Queues | Queue(_) | Counts(_) | ListJobs(_) | GetJob(..)
            | Logs(..) | Flow(..) | Schedulers(_) | Events(_) | LegacyQueues | LegacyCounts(_)
            | LegacyJobs(_) | LegacyJob(..) | LegacyLogs(..) => Needs::Read,
            _ => Needs::Admin,
        }
    }
}

/// Map a library error onto a status and an error code.
pub(crate) fn error_response(e: &Error) -> ApiResponse {
    let msg = e.to_string();
    match e {
        Error::NotFound(_) => ApiResponse::error(404, "not_found", msg),
        Error::Config(_) => ApiResponse::error(400, "bad_request", msg),
        Error::Script(s) => {
            let (status, code) = match s {
                ScriptError::JobNotFound => (404, "not_found"),
                ScriptError::ParentNotFound => (404, "parent_not_found"),
                ScriptError::JobLocked => (409, "job_locked"),
                ScriptError::NotInExpectedState => (409, "not_in_expected_state"),
                ScriptError::PendingChildren => (409, "pending_children"),
                ScriptError::JobExists => (409, "job_exists"),
                ScriptError::LockNotFound | ScriptError::LockMismatch => (409, "lock_lost"),
                _ => (409, "conflict"),
            };
            ApiResponse::error(status, code, msg)
        }
        #[cfg(feature = "redis-backend")]
        Error::Redis(_) | Error::Pool(_) => ApiResponse::error(503, "backend_unavailable", msg),
        _ => ApiResponse::error(500, "internal", msg),
    }
}

fn body<T: for<'de> Deserialize<'de>>(req: &ApiRequest) -> Result<T, ApiResponse> {
    let raw: &[u8] = if req.body.iter().all(u8::is_ascii_whitespace) {
        b"{}"
    } else {
        &req.body
    };
    serde_json::from_slice(raw).map_err(|e| bad(format!("invalid body: {e}")))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn count_in(c: &StateCounts, s: JobState) -> u64 {
    match s {
        JobState::Waiting => c.waiting,
        JobState::Paused => c.paused,
        JobState::Active => c.active,
        JobState::Delayed => c.delayed,
        JobState::Prioritized => c.prioritized,
        JobState::WaitingChildren => c.waiting_children,
        JobState::Completed => c.completed,
        JobState::Failed => c.failed,
    }
}

/// `offset` / `limit` / `order` from the query.
fn page(
    q: &[(String, String)],
    default_limit: u64,
    max: u64,
) -> Result<(u64, u64, bool), ApiResponse> {
    let num = |k: &str, d: u64| -> Result<u64, ApiResponse> {
        param(q, k)
            .map(|v| {
                v.parse::<u64>()
                    .map_err(|_| bad(format!("`{k}` must be a non-negative integer")))
            })
            .transpose()
            .map(|v| v.unwrap_or(d))
    };
    let offset = num("offset", 0)?;
    let limit = num("limit", default_limit)?;
    if limit == 0 || limit > max {
        return Err(bad(format!("`limit` must be between 1 and {max}")));
    }
    let desc = match param(q, "order") {
        None | Some("asc") => false,
        Some("desc") => true,
        Some(o) => return Err(bad(format!("`order` must be `asc` or `desc`, not `{o}`"))),
    };
    Ok((offset, limit, desc))
}

fn insert_for(queue: &str, add: dto::AddJob) -> Result<JobInsert, String> {
    if add.opts.attempts == Some(0) {
        return Err("`attempts` must be at least 1".into());
    }
    let opts = JobOptions::from(add.opts).resolve();
    if let Some(id) = &opts.id {
        crate::validate::job_id(id).map_err(|e| e.to_string())?;
    }
    Ok(JobInsert {
        name: add.name.unwrap_or_else(|| queue.to_string()),
        data: serde_json::to_string(&add.data).map_err(|e| e.to_string())?,
        opts,
        timestamp_ms: now_ms(),
    })
}

pub(crate) async fn run(api: &Api, route: Route, req: &ApiRequest) -> ApiResponse {
    match handle(api, route, req).await {
        Ok(r) => r,
        Err(r) => r,
    }
}

async fn handle(api: &Api, route: Route, req: &ApiRequest) -> Result<ApiResponse, ApiResponse> {
    let b = &api.backend;
    let q = query_pairs(&req.query);
    let fail = |e: Error| error_response(&e);
    use Route::*;
    Ok(match route {
        Health => match b.list_queues().await {
            Ok(_) => ApiResponse::json(200, &json!({ "status": "ok" })),
            Err(e) => ApiResponse::json(
                503,
                &json!({ "status": "unavailable", "error": e.to_string() }),
            ),
        },
        Whoami => unreachable!("answered by Api::handle"),
        OpenApi => ApiResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body: super::ApiBody::Bytes(OPENAPI_JSON.as_bytes().to_vec()),
        },
        Queues | LegacyQueues => {
            let names = b.list_queues().await.map_err(fail)?;
            let mut out = Vec::with_capacity(names.len());
            for name in names {
                let counts = b.counts(&name).await.map_err(fail)?;
                let paused = b.is_paused(&name).await.map_err(fail)?;
                out.push(QueueDto {
                    name,
                    paused,
                    counts,
                });
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            ApiResponse::json(200, &out)
        }
        Queue(name) => {
            if !b.list_queues().await.map_err(fail)?.contains(&name) {
                return Err(ApiResponse::error(
                    404,
                    "not_found",
                    format!("queue `{name}` not found"),
                ));
            }
            let counts = b.counts(&name).await.map_err(fail)?;
            let paused = b.is_paused(&name).await.map_err(fail)?;
            ApiResponse::json(
                200,
                &QueueDto {
                    name,
                    paused,
                    counts,
                },
            )
        }
        Obliterate(name) => {
            let force = matches!(param(&q, "force"), Some("true" | "1"));
            match b.obliterate(&name, force).await {
                Ok(()) => ApiResponse::empty(204),
                // The non-forced refusal is a state conflict, not bad input.
                Err(Error::Config(m)) if !force => {
                    return Err(ApiResponse::error(409, "active_jobs", m));
                }
                Err(e) => return Err(fail(e)),
            }
        }
        Counts(name) | LegacyCounts(name) => {
            ApiResponse::json(200, &b.counts(&name).await.map_err(fail)?)
        }
        Pause(name) | LegacyPause(name) => {
            b.pause(&name).await.map_err(fail)?;
            ApiResponse::empty(204)
        }
        Resume(name) | LegacyResume(name) => {
            b.resume(&name).await.map_err(fail)?;
            ApiResponse::empty(204)
        }
        Drain(name) | LegacyDrain(name) => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct DrainBody {
                #[serde(default)]
                include_delayed: bool,
            }
            let d: DrainBody = body(req)?;
            b.drain(&name, d.include_delayed).await.map_err(fail)?;
            ApiResponse::empty(204)
        }
        Clean(name) => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct CleanBody {
                state: String,
                #[serde(default)]
                limit: u64,
            }
            let c: CleanBody = body(req)?;
            let state = JobState::parse(&c.state)
                .ok_or_else(|| bad(format!("unknown state `{}`", c.state)))?;
            let count = b.clean(&name, state, c.limit).await.map_err(fail)?;
            ApiResponse::json(200, &json!({ "count": count }))
        }
        LegacyClean(name, state) => {
            #[derive(Deserialize, Default)]
            struct CleanBody {
                #[serde(default)]
                limit: u64,
            }
            let c: CleanBody = body(req)?;
            let state =
                JobState::parse(&state).ok_or_else(|| bad(format!("unknown state `{state}`")))?;
            let count = b.clean(&name, state, c.limit).await.map_err(fail)?;
            ApiResponse::json(200, &json!({ "count": count }))
        }
        PromoteAll(name) | LegacyPromoteAll(name) => {
            let count = b.promote_all(&name).await.map_err(fail)?;
            ApiResponse::json(200, &json!({ "count": count }))
        }
        ListJobs(name) => {
            let state = match param(&q, "state") {
                None => JobState::Waiting,
                Some(s) => JobState::parse(s).ok_or_else(|| bad(format!("unknown state `{s}`")))?,
            };
            let (offset, limit, desc) = page(&q, 50, api.config.max_page as u64)?;
            let range = JobRange {
                start: offset as i64,
                end: (offset + limit - 1) as i64,
                descending: desc,
            };
            let jobs = b.list(&name, state, range).await.map_err(fail)?;
            let total = count_in(&b.counts(&name).await.map_err(fail)?, state);
            ApiResponse::json(
                200,
                &JobPage {
                    jobs: jobs
                        .into_iter()
                        .map(|j| JobSummaryDto::from_raw(state, j))
                        .collect(),
                    total,
                    offset,
                    limit,
                },
            )
        }
        AddJob(name) => {
            let add: dto::AddJob = body(req)?;
            let insert = insert_for(&name, add).map_err(bad)?;
            let id = b.add(&name, insert).await.map_err(fail)?;
            crate::telemetry::added(&name, 1);
            ApiResponse::json(201, &json!({ "id": id }))
                .header("location", format!("v1/queues/{name}/jobs/{id}"))
        }
        BulkAdd(name) => {
            let bulk: dto::BulkAdd = body(req)?;
            if bulk.jobs.is_empty() {
                return Err(bad("`jobs` is empty"));
            }
            if bulk.jobs.len() > api.config.max_bulk {
                return Err(ApiResponse::error(
                    413,
                    "payload_too_large",
                    format!("at most {} jobs per bulk add", api.config.max_bulk),
                ));
            }
            let mut inserts = Vec::with_capacity(bulk.jobs.len());
            for (i, add) in bulk.jobs.into_iter().enumerate() {
                inserts.push(insert_for(&name, add).map_err(|e| bad(format!("jobs[{i}]: {e}")))?);
            }
            let n = inserts.len();
            let ids = b.add_bulk(&name, inserts).await.map_err(fail)?;
            crate::telemetry::added(&name, n);
            ApiResponse::json(201, &json!({ "ids": ids }))
        }
        GetJob(name, id) => match b.get(&name, &id).await.map_err(fail)? {
            Some(raw) => ApiResponse::json(200, &JobDto::from_raw(&name, raw)),
            None => {
                return Err(ApiResponse::error(
                    404,
                    "not_found",
                    format!("job `{id}` not found"),
                ));
            }
        },
        RemoveJob(name, id) | LegacyRemove(name, id) => {
            b.remove(&name, &id).await.map_err(fail)?;
            ApiResponse::empty(204)
        }
        Logs(name, id) => {
            let (offset, limit, desc) = page(&q, 100, api.config.max_page as u64)?;
            let range = JobRange {
                start: offset as i64,
                end: (offset + limit - 1) as i64,
                descending: desc,
            };
            let logs = b.get_logs(&name, &id, range).await.map_err(fail)?;
            if logs.is_empty() && b.get(&name, &id).await.map_err(fail)?.is_none() {
                return Err(ApiResponse::error(
                    404,
                    "not_found",
                    format!("job `{id}` not found"),
                ));
            }
            ApiResponse::json(
                200,
                &json!({ "logs": logs, "offset": offset, "limit": limit }),
            )
        }
        Flow(name, id) => {
            let depth = match param(&q, "depth") {
                None => 3,
                Some(d) => d
                    .parse::<u32>()
                    .ok()
                    .filter(|d| (1..=10).contains(d))
                    .ok_or_else(|| bad("`depth` must be between 1 and 10"))?,
            };
            let root = b.get(&name, &id).await.map_err(fail)?;
            let Some(root) = root else {
                return Err(ApiResponse::error(
                    404,
                    "not_found",
                    format!("job `{id}` not found"),
                ));
            };
            let mut budget = 1000usize;
            let tree = flow_tree(
                api,
                JobRef::new(name, id),
                Some(root),
                None,
                depth,
                &mut budget,
            )
            .await
            .map_err(fail)?;
            ApiResponse::json(200, &tree)
        }
        Retry(name, id) | LegacyRetry(name, id) => {
            b.retry(&name, &id).await.map_err(fail)?;
            ApiResponse::empty(204)
        }
        Promote(name, id) | LegacyPromote(name, id) => {
            b.promote(&name, &id).await.map_err(fail)?;
            ApiResponse::empty(204)
        }
        Schedulers(name) => ApiResponse::json(200, &b.schedulers(&name).await.map_err(fail)?),
        RemoveScheduler(name, sid) => {
            if b.remove_scheduler(&name, &sid).await.map_err(fail)? {
                ApiResponse::empty(204)
            } else {
                return Err(ApiResponse::error(
                    404,
                    "not_found",
                    format!("scheduler `{sid}` not found"),
                ));
            }
        }
        Events(name) => super::sse::events(api, &name, &q, req).await?,
        LegacyJobs(name) => {
            let state = param(&q, "state")
                .and_then(JobState::parse)
                .unwrap_or(JobState::Waiting);
            let num = |k: &str, d: i64| {
                param(&q, k)
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(d)
            };
            let range = JobRange {
                start: num("start", 0),
                end: num("end", 49),
                descending: matches!(param(&q, "desc"), Some("true" | "1")),
            };
            let jobs = b.list(&name, state, range).await.map_err(fail)?;
            let total = count_in(&b.counts(&name).await.map_err(fail)?, state);
            let jobs: Vec<Value> = jobs
                .into_iter()
                .map(|j| {
                    json!({
                        "id": j.id, "name": j.name, "state": j.state,
                        "attempts_made": j.attempts_made, "attempts_total": j.opts.attempts,
                        "timestamp_ms": j.timestamp_ms, "processed_on_ms": j.processed_on_ms,
                        "finished_on_ms": j.finished_on_ms, "failed_reason": j.failed_reason,
                    })
                })
                .collect();
            ApiResponse::json(200, &json!({ "jobs": jobs, "total": total }))
        }
        LegacyJob(name, id) => ApiResponse::json(200, &b.get(&name, &id).await.map_err(fail)?),
        LegacyLogs(name, id) => {
            let num = |k: &str, d: i64| {
                param(&q, k)
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(d)
            };
            let range = JobRange {
                start: num("start", 0),
                end: num("end", -1),
                descending: matches!(param(&q, "desc"), Some("true" | "1")),
            };
            let logs = b.get_logs(&name, &id, range).await.map_err(fail)?;
            ApiResponse::json(200, &json!({ "count": logs.len(), "logs": logs }))
        }
    })
}

/// The flow tree under `job`, `depth` levels deep, at most `budget` nodes.
fn flow_tree<'a>(
    api: &'a Api,
    job: JobRef,
    raw: Option<crate::backend::RawJob>,
    dependency: Option<&'static str>,
    depth: u32,
    budget: &'a mut usize,
) -> futures::future::BoxFuture<'a, crate::Result<FlowNodeDto>> {
    Box::pin(async move {
        *budget = budget.saturating_sub(1);
        let b = &api.backend;
        let deps = b.dependencies(&job.queue, &job.id).await?;
        let kids: Vec<(JobRef, &'static str)> = deps
            .pending
            .into_iter()
            .map(|r| (r, "pending"))
            .chain(deps.processed.into_iter().map(|(r, _)| (r, "processed")))
            .chain(deps.failed.into_iter().map(|(r, _)| (r, "ignored-failure")))
            .collect();
        let mut node = FlowNodeDto {
            name: raw.as_ref().map(|r| r.name.clone()),
            state: raw.as_ref().and_then(|r| r.state),
            queue: job.queue,
            id: job.id,
            dependency: dependency.map(str::to_string),
            children: Vec::new(),
            truncated: false,
        };
        if depth == 0 || *budget == 0 {
            node.truncated = !kids.is_empty();
            return Ok(node);
        }
        for (child, how) in kids {
            if *budget == 0 {
                node.truncated = true;
                break;
            }
            let raw = b.get(&child.queue, &child.id).await?;
            node.children
                .push(flow_tree(api, child, raw, Some(how), depth - 1, budget).await?);
        }
        Ok(node)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(r: Result<Route, ApiResponse>) -> u16 {
        r.err().map(|e| e.status).unwrap_or(200)
    }

    #[test]
    fn routes_match_methods_and_report_405_and_404() {
        assert!(matches!(
            Route::parse("GET", "/v1/queues"),
            Ok(Route::Queues)
        ));
        assert!(matches!(
            Route::parse("GET", "/v1/queues/"),
            Ok(Route::Queues)
        ));
        assert!(
            matches!(Route::parse("DELETE", "/v1/queues/emails"), Ok(Route::Obliterate(q)) if q == "emails")
        );
        assert!(matches!(
            Route::parse("POST", "/v1/queues/e/jobs/bulk"),
            Ok(Route::BulkAdd(_))
        ));
        assert!(
            matches!(Route::parse("GET", "/v1/queues/e/jobs/bulk"), Ok(Route::GetJob(_, id)) if id.as_str() == "bulk")
        );
        let r = Route::parse("PUT", "/v1/queues/e/jobs");
        let e = r.err().unwrap();
        assert_eq!(e.status, 405);
        assert!(
            e.headers
                .iter()
                .any(|(k, v)| k == "allow" && v == "GET, POST")
        );
        assert_eq!(status(Route::parse("GET", "/v1/nope")), 404);
        assert_eq!(
            status(Route::parse("GET", "/v1/queues/bad:name/counts")),
            400
        );
    }

    #[test]
    fn path_segments_and_queries_are_percent_decoded() {
        assert!(
            matches!(Route::parse("GET", "/v1/queues/e/jobs/a%20b"), Ok(Route::GetJob(_, id)) if id.as_str() == "a b")
        );
        assert_eq!(
            query_pairs("state=completed&order=desc&x=a+b%2Fc"),
            vec![
                ("state".into(), "completed".into()),
                ("order".into(), "desc".into()),
                ("x".into(), "a b/c".into()),
            ]
        );
        assert_eq!(status(Route::parse("GET", "/v1/queues/%zz")), 400);
    }

    #[test]
    fn reads_need_read_writes_need_admin_health_needs_nothing() {
        assert_eq!(
            Route::parse("GET", "/v1/health").unwrap().needs(),
            Needs::Nothing
        );
        assert_eq!(
            Route::parse("GET", "/v1/queues/q/jobs").unwrap().needs(),
            Needs::Read
        );
        assert_eq!(
            Route::parse("POST", "/v1/queues/q/jobs").unwrap().needs(),
            Needs::Admin
        );
        assert_eq!(
            Route::parse("POST", "/api/queues/q/pause").unwrap().needs(),
            Needs::Admin
        );
        assert_eq!(
            Route::parse("GET", "/api/queues").unwrap().needs(),
            Needs::Read
        );
    }

    #[test]
    fn script_errors_map_to_conflicts_with_codes() {
        let r = error_response(&Error::Script(ScriptError::JobLocked));
        assert_eq!(r.status, 409);
        let r = error_response(&Error::NotFound("x".into()));
        assert_eq!(r.status, 404);
        let r = error_response(&Error::Config("x".into()));
        assert_eq!(r.status, 400);
    }
}
