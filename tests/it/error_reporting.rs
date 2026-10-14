//! Reporting failed jobs: the `on_failure` hook and the `sentry` feature.
//!
//! A hook must see each failed attempt exactly once, after Redis recorded
//! it, with the right kind, attempt and finality; postponements and
//! successes are not failures. With `sentry`, final failures (and lost
//! outcomes) become Sentry events carrying the job's tags, and each
//! handler runs in its own Sentry scope.

use std::sync::Arc;

use crate::common;
use oxn::backend::Backend;
use oxn::backend::redis::RedisBackend;
use oxn::prelude::*;
use oxn::{FailureKind, JobFailure};
use oxn_testkit::{ms, secs};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct T(u32);

/// What a hook saw, owned.
#[derive(Debug, Clone)]
struct Seen {
    queue: String,
    id: String,
    name: String,
    kind: FailureKind,
    attempt: u32,
    attempts: u32,
    is_final: bool,
    reason: String,
    has_error: bool,
}

type Log = Arc<Mutex<Vec<Seen>>>;

fn recorder() -> (Log, impl Fn(&JobFailure<'_>) + Send + Sync + 'static) {
    let log: Log = Arc::default();
    let l = log.clone();
    (log, move |f: &JobFailure<'_>| {
        assert_eq!(f.is_final(), f.retry_at_ms.is_none());
        l.lock().push(Seen {
            queue: f.queue.to_string(),
            id: f.job_id.to_string(),
            name: f.job_name.to_string(),
            kind: f.kind,
            attempt: f.attempt,
            attempts: f.attempts,
            is_final: f.is_final(),
            reason: f.reason.to_string(),
            has_error: f.error.is_some(),
        })
    })
}

#[derive(Debug)]
struct CardDeclined {
    code: u16,
}

impl std::fmt::Display for CardDeclined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "card declined (code {})", self.code)
    }
}

impl std::error::Error for CardDeclined {}

/// What each test handler does, by job name.
async fn handle(job: Job<T>) -> Result<()> {
    match job.name.as_str() {
        "charge" => Err(Error::handler(CardDeclined { code: 51 })),
        "fatal" => Err(Error::unrecoverable("account closed")),
        "boom" => panic!("index out of bounds"),
        "later" => Err(Error::Delayed { delay_ms: 60_000 }),
        _ => Ok(()),
    }
}

fn worker(q: &Queue<T>) -> oxn::worker::WorkerBuilder<T> {
    Worker::builder(q.clone(), handle)
        .concurrency(4)
        .drain_delay(ms(20))
}

async fn wait_until(what: &str, mut ok: impl FnMut() -> bool) {
    let t0 = std::time::Instant::now();
    while !ok() {
        assert!(t0.elapsed() < secs(10), "timed out waiting for {what}");
        tokio::time::sleep(ms(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_hook_sees_each_failed_attempt_once_then_the_final_one() {
    let (q, _b) = common::fresh_queue::<T>("report-retries").await;
    let id = q
        .add(T(1), JobOptions::new().name("charge").attempts(3))
        .await
        .unwrap();
    let (log, hook) = recorder();
    let w = worker(&q).on_failure(hook).build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_until("3 reports", || log.lock().len() >= 3).await;
    tokio::time::sleep(ms(100)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let seen = log.lock().clone();
    assert_eq!(seen.len(), 3, "{seen:#?}");
    for (i, s) in seen.iter().enumerate() {
        assert_eq!(s.queue, q.name());
        assert_eq!(s.id, id.as_str());
        assert_eq!(s.name, "charge");
        assert_eq!(s.kind, FailureKind::Error);
        assert_eq!((s.attempt, s.attempts), (i as u32 + 1, 3));
        assert_eq!(s.is_final, i == 2);
        assert!(s.reason.contains("card declined (code 51)"), "{}", s.reason);
        assert!(s.has_error);
    }
    assert_eq!(q.counts().await.unwrap().failed, 1);
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kinds_are_told_apart_and_postponements_are_not_failures() {
    let (q, _b) = common::fresh_queue::<T>("report-kinds").await;
    for name in ["fatal", "boom", "later", "fine"] {
        q.add(T(0), JobOptions::new().name(name).attempts(1))
            .await
            .unwrap();
    }
    let (log, hook) = recorder();
    let w = worker(&q).on_failure(hook).build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    common::wait_for(secs(10), || async {
        let c = q.counts().await.unwrap();
        c.failed == 2 && c.completed == 1 && c.delayed == 1
    })
    .await;
    wait_until("2 reports", || log.lock().len() >= 2).await;
    tokio::time::sleep(ms(100)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let mut seen = log.lock().clone();
    seen.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(seen.len(), 2, "only the two failures: {seen:#?}");
    assert_eq!(
        (seen[0].name.as_str(), seen[0].kind),
        ("boom", FailureKind::Panicked)
    );
    assert!(seen[0].reason.contains("index out of bounds"));
    assert_eq!(
        (seen[1].name.as_str(), seen[1].kind),
        ("fatal", FailureKind::Unrecoverable)
    );
    assert_eq!(seen[1].reason, "account closed");
    assert!(seen.iter().all(|s| s.is_final && s.has_error));
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_undecodable_payload_is_reported_as_a_decode_failure() {
    let (q, b) = common::fresh_queue::<T>("report-decode").await;
    let raw: Queue<serde_json::Value> = common::queue(b.clone(), q.name()).await;
    raw.add(
        serde_json::json!({"not": "a T"}),
        JobOptions::new().name("bad"),
    )
    .await
    .unwrap();
    let (log, hook) = recorder();
    let w = worker(&q).on_failure(hook).build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_until("a report", || !log.lock().is_empty()).await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let seen = log.lock().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].kind, FailureKind::Decode);
    assert_eq!(seen[0].name, "bad");
    assert!(seen[0].is_final && seen[0].has_error);
    common::teardown(&q).await;
}

/// Fetch with a 30ms lock and let it lapse: a job whose worker died.
async fn stall(b: &Arc<RedisBackend>, q: &str) -> JobId {
    let f = b
        .fetch_next(
            q,
            "dead-worker",
            ms(30),
            ms(10),
            &CancellationToken::new(),
            None,
        )
        .await
        .unwrap()
        .expect("a job to stall");
    tokio::time::sleep(ms(60)).await;
    f.job.id
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_failed_by_the_stalled_scanner_is_reported() {
    let (q, b) = common::fresh_queue::<T>("report-stalled").await;
    q.add(
        T(0),
        JobOptions::new()
            .name("export")
            .attempts(4)
            .remove_on_fail(oxn::options::Removal::Remove),
    )
    .await
    .unwrap();
    let id = stall(&b, q.name()).await;
    let (log, hook) = recorder();
    let w = worker(&q)
        .max_stalled(0)
        .stalled_interval(ms(50))
        .on_failure(hook)
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_until("a report", || !log.lock().is_empty()).await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let seen = log.lock().clone();
    assert_eq!(seen.len(), 1);
    let s = &seen[0];
    // Read before `remove_on_fail` deleted the job.
    assert_eq!((s.id.as_str(), s.name.as_str()), (id.as_str(), "export"));
    assert_eq!(
        (s.kind, s.attempt, s.attempts),
        (FailureKind::Stalled, 1, 4)
    );
    assert!(s.is_final && !s.has_error);
    assert_eq!(s.reason, "job stalled more than allowable limit");
    assert!(q.get(&id).await.unwrap().is_none(), "retention ran");
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_hook_is_contained() {
    let (q, _b) = common::fresh_queue::<T>("report-hook-panic").await;
    for _ in 0..3 {
        q.add(T(0), JobOptions::new().name("fatal")).await.unwrap();
    }
    let (log, hook) = recorder();
    let w = worker(&q)
        .on_failure(|_| panic!("hook bug"))
        .on_failure(hook)
        .build();
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    wait_until("3 reports", || log.lock().len() >= 3).await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    assert_eq!(q.counts().await.unwrap().failed, 3);
    common::teardown(&q).await;
}

#[cfg(feature = "sentry")]
mod sentry_events {
    use super::*;
    use oxn::sentry::SentryOptions;
    use sentry::protocol::{Context, Event, Level};
    use sentry::test::TestTransport;
    use sentry::{Hub, SentryFutureExt};

    /// A hub with a client that collects events in memory.
    fn sentry_hub() -> (Arc<Hub>, Arc<TestTransport>) {
        let transport = TestTransport::new();
        let opts = sentry::ClientOptions {
            dsn: Some("https://public@sentry.invalid/1".parse().unwrap()),
            transport: Some(Arc::new(transport.clone())),
            ..Default::default()
        };
        let client = Arc::new(sentry::Client::from(opts));
        let hub = Arc::new(Hub::new(Some(client), Arc::new(sentry::Scope::default())));
        (hub, transport)
    }

    fn tag<'a>(e: &'a Event<'static>, k: &str) -> Option<&'a str> {
        e.tags.get(k).map(String::as_str)
    }

    fn job_ctx(e: &Event<'static>, k: &str) -> Option<serde_json::Value> {
        match e.contexts.get("oxn job") {
            Some(Context::Other(m)) => m.get(k).cloned(),
            _ => None,
        }
    }

    /// Run `w` with `hub` as the current hub until `done` holds.
    async fn run_until(
        w: Worker<T>,
        hub: &Arc<Hub>,
        events: &Arc<Mutex<Vec<Event<'static>>>>,
        transport: &Arc<TestTransport>,
        mut done: impl FnMut(&[Event<'static>]) -> bool,
    ) {
        let cancel = w.cancellation_token();
        let h = tokio::spawn(w.run().bind_hub(hub.clone()));
        let t0 = std::time::Instant::now();
        loop {
            events.lock().extend(transport.fetch_and_clear_events());
            if done(&events.lock()) {
                break;
            }
            assert!(t0.elapsed() < secs(10), "timed out: {:#?}", events.lock());
            tokio::time::sleep(ms(10)).await;
        }
        tokio::time::sleep(ms(100)).await;
        cancel.cancel();
        h.await.unwrap().unwrap();
        events.lock().extend(transport.fetch_and_clear_events());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_final_failure_is_one_event_titled_with_the_users_error() {
        let (q, _b) = common::fresh_queue::<T>("sentry-final").await;
        let id = q
            .add(T(1), JobOptions::new().name("charge").attempts(2))
            .await
            .unwrap();
        let (hub, transport) = sentry_hub();
        let events = Arc::default();
        run_until(worker(&q).build(), &hub, &events, &transport, |e| {
            !e.is_empty()
        })
        .await;
        common::wait_for(secs(5), || async { q.counts().await.unwrap().failed == 1 }).await;

        let events = events.lock().clone();
        assert_eq!(events.len(), 1, "the retry is not reported: {events:#?}");
        let e = &events[0];
        assert_eq!(e.level, Level::Error);
        let top = e.exception.values.last().unwrap();
        assert_eq!(top.ty, "CardDeclined");
        assert_eq!(top.value.as_deref(), Some("card declined (code 51)"));
        assert_eq!(top.mechanism.as_ref().unwrap().handled, Some(true));
        assert_eq!(tag(e, "oxn.queue"), Some(q.name()));
        assert_eq!(tag(e, "oxn.job_name"), Some("charge"));
        assert_eq!(tag(e, "oxn.failure"), Some("error"));
        assert_eq!(tag(e, "oxn.final"), Some("true"));
        assert_eq!(job_ctx(e, "id"), Some(id.as_str().into()));
        assert_eq!(job_ctx(e, "attempt"), Some(2.into()));
        assert_eq!(
            e.transaction.as_deref(),
            Some(&*format!("{}/charge", q.name()))
        );
        assert!(e.fingerprint.iter().any(|f| f == q.name()));
        common::teardown(&q).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn report_retries_adds_a_warning_per_retried_attempt() {
        let (q, _b) = common::fresh_queue::<T>("sentry-retries").await;
        q.add(T(1), JobOptions::new().name("charge").attempts(2))
            .await
            .unwrap();
        let (hub, transport) = sentry_hub();
        let events = Arc::default();
        let w = worker(&q)
            .sentry(SentryOptions::default().report_retries(true))
            .build();
        run_until(w, &hub, &events, &transport, |e| e.len() >= 2).await;
        let events = events.lock().clone();
        let levels: Vec<_> = events.iter().map(|e| e.level).collect();
        assert_eq!(levels, [Level::Warning, Level::Error], "{events:#?}");
        assert_eq!(tag(&events[0], "oxn.final"), Some("false"));
        assert!(job_ctx(&events[0], "retry_at_ms").is_some());
        common::teardown(&q).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn panics_and_stalls_are_reported() {
        let (q, b) = common::fresh_queue::<T>("sentry-kinds").await;
        q.add(T(0), JobOptions::new().name("export")).await.unwrap();
        stall(&b, q.name()).await;
        q.add(T(0), JobOptions::new().name("boom")).await.unwrap();
        let (hub, transport) = sentry_hub();
        let events = Arc::default();
        let w = worker(&q).max_stalled(0).stalled_interval(ms(50)).build();
        run_until(w, &hub, &events, &transport, |e| e.len() >= 2).await;
        let events = events.lock().clone();
        let by = |kind: &str| {
            events
                .iter()
                .find(|e| tag(e, "oxn.failure") == Some(kind))
                .unwrap_or_else(|| panic!("no {kind} event in {events:#?}"))
        };
        let panicked = by("panicked");
        let top = panicked.exception.values.last().unwrap();
        assert!(
            top.value
                .as_deref()
                .unwrap()
                .contains("index out of bounds")
        );
        assert_eq!(top.mechanism.as_ref().unwrap().handled, Some(false));
        let stalled = by("stalled");
        assert_eq!(tag(stalled, "oxn.job_name"), Some("export"));
        assert_eq!(stalled.exception.values.last().unwrap().ty, "JobStalled");
        common::teardown(&q).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn each_handler_runs_in_its_own_scope() {
        let (q, _b) = common::fresh_queue::<T>("sentry-scope").await;
        for i in 0..12u32 {
            let name = if i % 2 == 0 { "even" } else { "odd" };
            q.add(T(i), JobOptions::new().name(name)).await.unwrap();
        }
        let (hub, transport) = sentry_hub();
        let events = Arc::default();
        let w = Worker::builder(q.clone(), |job: Job<T>| async move {
            tokio::time::sleep(ms(u64::from(job.data.0 % 3) * 15)).await;
            sentry::capture_message(&format!("{} {}", job.name, job.id), Level::Info);
            Ok::<_, Error>(())
        })
        .concurrency(6)
        .drain_delay(ms(20))
        .build();
        run_until(w, &hub, &events, &transport, |e| e.len() >= 12).await;
        let events = events.lock().clone();
        assert_eq!(events.len(), 12);
        for e in events.iter() {
            let msg = e.message.as_deref().unwrap();
            let (name, id) = msg.split_once(' ').unwrap();
            assert_eq!(tag(e, "oxn.job_name"), Some(name), "{msg}");
            assert_eq!(tag(e, "oxn.queue"), Some(q.name()));
            assert_eq!(job_ctx(e, "id"), Some(id.into()), "{msg}");
        }
        common::teardown(&q).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_outcome_redis_never_recorded_is_reported() {
        use oxn::backend::redis::script_hashes;
        use oxn_testkit::fault::{Drop, FaultProxy};

        let upstream = common::redis_url()
            .trim_start_matches("redis://")
            .trim_end_matches('/')
            .to_string();
        let proxy = FaultProxy::start(&upstream).await.unwrap();
        let mut cfg = common::test_config();
        cfg.url = proxy.url();
        cfg.transition_retry_for = ms(300);
        let b = Arc::new(RedisBackend::connect(cfg).await.unwrap());
        let q: Queue<T> = common::queue(b.clone(), &common::unique_queue_name("sentry-lost")).await;
        q.add(T(0), JobOptions::new().name("fine")).await.unwrap();
        let sha = script_hashes()
            .into_iter()
            .find(|(n, _)| *n == "move_to_finished")
            .unwrap()
            .1;
        for _ in 0..200 {
            proxy.arm(&sha, Drop::BeforeForward);
        }
        let (hub, transport) = sentry_hub();
        let events = Arc::default();
        let w = worker(&q).lock_duration(secs(30)).build();
        run_until(w, &hub, &events, &transport, |e| !e.is_empty()).await;
        let events = events.lock().clone();
        let e = &events[0];
        assert_eq!(tag(e, "oxn.lost_outcome"), Some("completed"), "{e:#?}");
        assert_eq!(tag(e, "oxn.job_name"), Some("fine"));
        assert_eq!(e.level, Level::Error);
        let _ = q.obliterate(true).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_disabled_worker_reports_nothing() {
        let (q, _b) = common::fresh_queue::<T>("sentry-off").await;
        q.add(T(0), JobOptions::new().name("fatal")).await.unwrap();
        let (hub, transport) = sentry_hub();
        let w = worker(&q).sentry(SentryOptions::disabled()).build();
        let cancel = w.cancellation_token();
        let h = tokio::spawn(w.run().bind_hub(hub.clone()));
        common::wait_for(secs(10), || async { q.counts().await.unwrap().failed == 1 }).await;
        tokio::time::sleep(ms(100)).await;
        cancel.cancel();
        h.await.unwrap().unwrap();
        assert!(transport.fetch_and_clear_events().is_empty());
        common::teardown(&q).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_no_client_bound_jobs_still_run() {
        let (q, _b) = common::fresh_queue::<T>("sentry-unbound").await;
        q.add(T(0), JobOptions::new().name("fatal")).await.unwrap();
        q.add(T(0), JobOptions::new().name("fine")).await.unwrap();
        let w = worker(&q).build();
        let cancel = w.cancellation_token();
        let h = tokio::spawn(w.run());
        assert!(
            common::wait_for(secs(10), || async {
                let c = q.counts().await.unwrap();
                c.failed == 1 && c.completed == 1
            })
            .await
        );
        cancel.cancel();
        h.await.unwrap().unwrap();
        common::teardown(&q).await;
    }
}
