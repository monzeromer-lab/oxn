//! The step recorder: run an action, then look at everything.
//!
//! ```ignore
//! let mut rec = Recorder::new("retry_then_complete", q.name()).await;
//! let id = rec.step("add job", q.add(T(1), JobOptions::new().attempts(3))).await?;
//! rec.wait_until("job completes", secs(5), |s| s.len("completed") == 1).await;
//! rec.assert_clean(); // fails with a timeline report path on any violation
//! ```
//!
//! After every step the recorder:
//! 1. snapshots the queue's whole Redis footprint,
//! 2. runs the invariant checker on it,
//! 3. pulls the new entries of the queue's events stream (read raw with
//!    `XRANGE`, independent of oxn's own subscriber),
//! 4. drains the oxn tracing spans/events captured for this queue,
//! 5. drains the `MONITOR` tap, if enabled.
//!
//! [`Recorder::wait_until`] keeps doing this while polling, so states that
//! exist only for a few milliseconds mid-flight are still checked.
//!
//! A report (`timeline.md` + `timeline.json`) is written when the test
//! panics, when [`Recorder::assert_clean`] fails, or always with
//! `OXN_TRACE=1`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use redis::aio::MultiplexedConnection;
use serde::Serialize;

use crate::capture::{self, TraceRecord};
use crate::invariants::{self, Severity, Violation};
use crate::monitor::{Command, MonitorTap};
use crate::report;
use crate::snapshot::{Snapshot, now_ms};

/// Track at most this many jobs' locations per step (keeps reports of
/// bulk tests readable).
const MAX_TRACKED_JOBS: usize = 200;

/// One entry of the queue's events stream.
#[derive(Debug, Clone, Serialize)]
pub struct StreamEvent {
    /// Stream entry id (`<ms>-<seq>`).
    pub id: String,
    /// Epoch-ms parsed from the entry id.
    pub at_ms: i64,
    /// The `event` field.
    pub event: String,
    /// The `jobId` field, if present.
    pub job_id: Option<String>,
    /// All other fields.
    pub fields: BTreeMap<String, String>,
}

impl StreamEvent {
    /// `active job=3 prev=waiting`.
    pub fn render(&self) -> String {
        let mut s = self.event.clone();
        if let Some(j) = &self.job_id {
            s.push_str(&format!(" job={j}"));
        }
        for (k, v) in &self.fields {
            s.push_str(&format!(" {k}={}", short(v, 80)));
        }
        s
    }
}

fn short(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(n).collect::<String>())
    }
}

/// Everything observed for one step.
#[derive(Debug, Clone, Serialize)]
pub struct Step {
    /// 1-based position.
    pub index: usize,
    /// Label given by the test.
    pub label: String,
    /// Start offset from recorder creation, ms.
    pub at_ms: f64,
    /// How long the action (or wait) took, ms.
    pub took_ms: f64,
    /// `Debug` of the action's output, if it had one.
    pub result: Option<String>,
    /// Snapshot summary after the step.
    pub summary: String,
    /// Location of each tracked job after the step.
    pub locations: BTreeMap<String, String>,
    /// Stream entries that appeared during the step.
    pub events: Vec<StreamEvent>,
    /// oxn spans/events captured during the step.
    pub traces: Vec<TraceRecord>,
    /// Redis commands (MONITOR) touching the queue during the step.
    pub commands: Vec<Command>,
    /// Invariant violations after the step (and, for waits, at any poll).
    pub violations: Vec<Violation>,
}

/// Per-test recorder bound to one queue.
pub struct Recorder {
    pub(crate) test: String,
    pub(crate) queue: String,
    pub(crate) prefix: String,
    conn: MultiplexedConnection,
    pub(crate) started_ms: i64,
    started: Instant,
    stream_cursor: Option<String>,
    pub(crate) steps: Vec<Step>,
    monitor: Option<MonitorTap>,
    url: String,
    pub(crate) last: Snapshot,
    strict: bool,
    written: Option<PathBuf>,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder")
            .field("test", &self.test)
            .field("queue", &self.queue)
            .field("steps", &self.steps.len())
            .finish_non_exhaustive()
    }
}

impl Recorder {
    /// Recorder for `queue` under the test prefix ([`crate::TEST_PREFIX`]).
    pub async fn new(test: &str, queue: &str) -> Self {
        Self::with_prefix(test, crate::TEST_PREFIX, queue).await
    }

    /// Recorder for `queue` under an explicit key prefix.
    pub async fn with_prefix(test: &str, prefix: &str, queue: &str) -> Self {
        capture::install();
        let url = crate::redis_url();
        let client = redis::Client::open(url.as_str()).expect("bad TEST_REDIS_URL");
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("recorder: cannot connect to Redis");
        let mut rec = Self {
            test: test.to_string(),
            queue: queue.to_string(),
            prefix: prefix.to_string(),
            conn,
            started_ms: now_ms(),
            started: Instant::now(),
            stream_cursor: None,
            steps: Vec::new(),
            monitor: None,
            url,
            last: Snapshot::default(),
            strict: false,
            written: None,
        };
        if std::env::var("OXN_MONITOR").is_ok() {
            rec = rec.with_monitor().await;
        }
        rec
    }

    /// Attach a `MONITOR` tap (every Redis command touching the queue,
    /// including commands run inside Lua scripts).
    pub async fn with_monitor(mut self) -> Self {
        let needle = format!("{}:{}:", self.prefix, self.queue);
        match MonitorTap::start(&self.url, needle).await {
            Ok(t) => {
                // Give the server a moment to register the monitor before
                // the test starts issuing commands.
                tokio::time::sleep(Duration::from_millis(20)).await;
                self.monitor = Some(t);
            }
            Err(e) => eprintln!("oxn-testkit: MONITOR tap unavailable: {e}"),
        }
        self
    }

    /// Treat warnings (slow leaks) as failures in [`Self::assert_clean`].
    #[must_use]
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Run `fut`, then snapshot + check + collect everything it caused.
    pub async fn step<T: Debug, F: Future<Output = T>>(&mut self, label: &str, fut: F) -> T {
        let at = self.started.elapsed();
        let out = fut.await;
        let took = self.started.elapsed() - at;
        let result = Some(short(&format!("{out:?}"), 300));
        self.record(label, at, took, result, Vec::new(), true).await;
        out
    }

    /// Record the current state without running anything.
    pub async fn observe(&mut self, label: &str) {
        let at = self.started.elapsed();
        self.record(label, at, Duration::ZERO, None, Vec::new(), true)
            .await;
    }

    /// Poll every 20ms until `pred(snapshot)` holds or `timeout` elapses.
    ///
    /// Invariants are checked at **every** poll, so a broken state that
    /// only exists mid-flight still gets reported. Returns whether `pred`
    /// was satisfied.
    pub async fn wait_until(
        &mut self,
        label: &str,
        timeout: Duration,
        mut pred: impl FnMut(&Snapshot) -> bool,
    ) -> bool {
        let at = self.started.elapsed();
        let deadline = Instant::now() + timeout;
        let mut seen: BTreeMap<(String, Option<String>), Violation> = BTreeMap::new();
        let ok = loop {
            let snap = self.snapshot_now().await;
            for v in invariants::check(&snap) {
                if v.severity >= Severity::Warning {
                    seen.entry((v.rule.to_string(), v.job_id.clone()))
                        .or_insert(v);
                }
            }
            let done = pred(&snap);
            self.last = snap;
            if done {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let took = self.started.elapsed() - at;
        let result = Some(if ok {
            format!("satisfied after {:.1}ms", took.as_secs_f64() * 1000.0)
        } else {
            format!("TIMED OUT after {:.1}ms", took.as_secs_f64() * 1000.0)
        });
        // The last poll's snapshot is already in `self.last`.
        self.record(label, at, took, result, seen.into_values().collect(), false)
            .await;
        ok
    }

    /// Latest snapshot (after the most recent step).
    pub fn snapshot(&self) -> &Snapshot {
        &self.last
    }

    /// Every step recorded so far.
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// Every stream event seen so far, in order.
    pub fn events(&self) -> Vec<&StreamEvent> {
        self.steps.iter().flat_map(|s| s.events.iter()).collect()
    }

    /// Stream events of a given type (`"completed"`, `"stalled"`, …).
    pub fn events_named(&self, name: &str) -> Vec<&StreamEvent> {
        self.events()
            .into_iter()
            .filter(|e| e.event == name)
            .collect()
    }

    /// Captured traces whose name matches (`"oxn.process"`, …).
    pub fn traces_named(&self, name: &str) -> Vec<&TraceRecord> {
        self.steps
            .iter()
            .flat_map(|s| s.traces.iter())
            .filter(|t| t.name == name)
            .collect()
    }

    /// Every violation recorded at or above `min`, deduplicated by
    /// `(rule, job)`, first occurrence wins.
    pub fn violations(&self, min: Severity) -> Vec<(usize, &Violation)> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for s in &self.steps {
            for v in &s.violations {
                if v.severity >= min && seen.insert((v.rule, v.job_id.clone())) {
                    out.push((s.index, v));
                }
            }
        }
        out
    }

    /// Violations of one rule (any severity).
    pub fn violations_of(&self, rule: &str) -> Vec<&Violation> {
        self.steps
            .iter()
            .flat_map(|s| s.violations.iter())
            .filter(|v| v.rule == rule)
            .collect()
    }

    /// Panic (after writing the report) if any step left the queue in a
    /// state that breaks an invariant.
    pub fn assert_clean(&mut self) {
        let min = if self.strict {
            Severity::Warning
        } else {
            Severity::Error
        };
        let bad: Vec<String> = self
            .violations(min)
            .into_iter()
            .map(|(i, v)| format!("  step {i}: {v}"))
            .collect();
        if !bad.is_empty() {
            let path = self.write_report();
            panic!(
                "{} invariant violation(s) in `{}`:\n{}\ntimeline: {}",
                bad.len(),
                self.test,
                bad.join("\n"),
                path.display()
            );
        }
    }

    /// Write `timeline.md` and `timeline.json`; returns the markdown path.
    pub fn write_report(&mut self) -> PathBuf {
        let path = report::write(self);
        self.written = Some(path.clone());
        path
    }

    async fn snapshot_now(&mut self) -> Snapshot {
        match Snapshot::take(&mut self.conn, &self.prefix, &self.queue).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("oxn-testkit: snapshot failed: {e}");
                Snapshot::default()
            }
        }
    }

    async fn record(
        &mut self,
        label: &str,
        at: Duration,
        took: Duration,
        result: Option<String>,
        mut extra: Vec<Violation>,
        fresh_snapshot: bool,
    ) {
        if fresh_snapshot {
            self.last = self.snapshot_now().await;
        }
        let mut violations = invariants::check(&self.last);
        for v in extra.drain(..) {
            if !violations
                .iter()
                .any(|x| x.rule == v.rule && x.job_id == v.job_id)
            {
                violations.push(v);
            }
        }
        let events = self.pull_events().await;
        let mut traces = capture::drain_for(&self.queue);
        traces.sort_by_key(|t| t.at_us);
        let commands = self
            .monitor
            .as_ref()
            .map(MonitorTap::drain)
            .unwrap_or_default();
        let locations = self
            .last
            .jobs
            .keys()
            .chain(self.last.container_ids().iter())
            .take(MAX_TRACKED_JOBS)
            .map(|id| (id.clone(), self.last.location(id)))
            .collect();
        self.steps.push(Step {
            index: self.steps.len() + 1,
            label: label.to_string(),
            at_ms: at.as_secs_f64() * 1000.0,
            took_ms: took.as_secs_f64() * 1000.0,
            result,
            summary: self.last.summary(),
            locations,
            events,
            traces,
            commands,
            violations,
        });
    }

    async fn pull_events(&mut self) -> Vec<StreamEvent> {
        let key = format!("{}:{}:events", self.prefix, self.queue);
        let start = match &self.stream_cursor {
            Some(c) => format!("({c}"),
            None => "-".to_string(),
        };
        let reply: redis::RedisResult<redis::streams::StreamRangeReply> = redis::cmd("XRANGE")
            .arg(&key)
            .arg(&start)
            .arg("+")
            .query_async(&mut self.conn)
            .await;
        let Ok(reply) = reply else { return Vec::new() };
        let mut out = Vec::with_capacity(reply.ids.len());
        for entry in reply.ids {
            self.stream_cursor = Some(entry.id.clone());
            let mut fields = BTreeMap::new();
            for (k, v) in &entry.map {
                if let Ok(s) = redis::from_redis_value::<String>(v) {
                    fields.insert(k.clone(), s);
                }
            }
            let event = fields.remove("event").unwrap_or_default();
            let job_id = fields.remove("jobId");
            let at_ms = entry
                .id
                .split('-')
                .next()
                .and_then(|m| m.parse().ok())
                .unwrap_or(0);
            out.push(StreamEvent {
                id: entry.id,
                at_ms,
                event,
                job_id,
                fields,
            });
        }
        out
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let wanted = std::thread::panicking() || std::env::var("OXN_TRACE").is_ok();
        if wanted && self.written.is_none() && !self.steps.is_empty() {
            let path = report::write(self);
            eprintln!(
                "oxn-testkit: timeline for `{}` → {}",
                self.test,
                path.display()
            );
        }
    }
}
