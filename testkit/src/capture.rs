//! In-process capture of oxn's `tracing` spans and events.
//!
//! [`install`] registers a global subscriber (once per process) with a
//! [`CaptureLayer`]. Every closed `oxn*` span and every `oxn*` event is
//! turned into a [`TraceRecord`] and filed under the `queue` field found on
//! it or on its nearest ancestor span. Tests run in parallel inside one
//! binary, so bucketing by queue is what keeps one test's timeline free of
//! another test's noise.
//!
//! Set `OXN_LOG=debug` (any `EnvFilter` directive) to additionally print
//! everything to stderr while the test runs.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, filter};

/// Per-queue cap so a long load run can't grow the store without bound.
const MAX_RECORDS_PER_QUEUE: usize = 200_000;

/// Whether a record came from a closed span or a standalone event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TraceKind {
    /// A span that has closed; `duration_us` is set.
    Span,
    /// A `tracing::event!` (debug!/warn!/…).
    Event,
}

/// One captured span or event.
#[derive(Debug, Clone, Serialize)]
pub struct TraceRecord {
    /// Wall-clock epoch-µs when the event fired or the span **opened**.
    pub at_us: u64,
    /// Span or event.
    pub kind: TraceKind,
    /// Span name (`oxn.process`) or the event's `message`.
    pub name: String,
    /// `TRACE` … `ERROR`.
    pub level: String,
    /// Module path target (`oxn::worker`).
    pub target: String,
    /// Every recorded field (own fields only; inherited ones are folded
    /// into `queue` / `job_id`).
    pub fields: BTreeMap<String, String>,
    /// `queue` field, own or inherited.
    pub queue: Option<String>,
    /// `job_id` field, own or inherited.
    pub job_id: Option<String>,
    /// Span lifetime in µs (spans only).
    pub duration_us: Option<u64>,
}

impl TraceRecord {
    /// One-line human rendering, used in timeline reports.
    pub fn render(&self) -> String {
        let fields: Vec<String> = self
            .fields
            .iter()
            .filter(|(k, v)| k.as_str() != "message" && !v.is_empty())
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        let kind = match self.kind {
            TraceKind::Span => "span",
            TraceKind::Event => "event",
        };
        let dur = self
            .duration_us
            .map(|d| format!(" ({})", fmt_us(d)))
            .unwrap_or_default();
        format!(
            "[{kind} {lvl}] {name}{dur} {f}",
            lvl = self.level.to_lowercase(),
            name = self.name,
            f = fields.join(" ")
        )
    }
}

fn fmt_us(us: u64) -> String {
    if us >= 1000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}µs")
    }
}

#[derive(Default)]
struct Store {
    by_queue: HashMap<String, Vec<TraceRecord>>,
}

fn store() -> &'static Mutex<Store> {
    static STORE: OnceLock<Mutex<Store>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(Store::default()))
}

fn push(rec: TraceRecord) {
    let key = rec.queue.clone().unwrap_or_default();
    let mut s = store().lock();
    let bucket = s.by_queue.entry(key).or_default();
    if bucket.len() < MAX_RECORDS_PER_QUEUE {
        bucket.push(rec);
    }
}

/// Take (and clear) every record captured so far for `queue`.
pub fn drain_for(queue: &str) -> Vec<TraceRecord> {
    let mut s = store().lock();
    s.by_queue.remove(queue).unwrap_or_default()
}

/// Copy every record captured so far for `queue` without clearing.
pub fn peek_for(queue: &str) -> Vec<TraceRecord> {
    let s = store().lock();
    s.by_queue.get(queue).cloned().unwrap_or_default()
}

/// Install the capture layer as the global default subscriber.
///
/// Idempotent and safe to call from every test. If something else already
/// installed a global subscriber, capture silently stays off.
pub fn install() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let capture =
            CaptureLayer.with_filter(filter::filter_fn(|m| m.target().starts_with("oxn")));
        let fmt = std::env::var("OXN_LOG").ok().map(|directive| {
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_target(true)
                .with_filter(EnvFilter::new(directive))
        });
        let _ = tracing_subscriber::registry()
            .with(capture)
            .with(fmt)
            .try_init();
    });
}

/// Span-local storage: fields seen so far + open time.
struct SpanData {
    fields: BTreeMap<String, String>,
    opened: Instant,
    opened_us: u64,
}

struct FieldVisitor<'a>(&'a mut BTreeMap<String, String>);

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// The `tracing_subscriber` layer that feeds the store.
#[derive(Debug, Clone, Copy)]
pub struct CaptureLayer;

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn inherited<'a, S>(
    own: &BTreeMap<String, String>,
    scope: impl Iterator<Item = tracing_subscriber::registry::SpanRef<'a, S>>,
) -> (Option<String>, Option<String>)
where
    S: Subscriber + for<'l> LookupSpan<'l>,
{
    let pick = |m: &BTreeMap<String, String>, k: &str| m.get(k).filter(|v| !v.is_empty()).cloned();
    let mut queue = pick(own, "queue");
    let mut job = pick(own, "job_id");
    for span in scope {
        if queue.is_some() && job.is_some() {
            break;
        }
        if let Some(d) = span.extensions().get::<SpanData>() {
            if queue.is_none() {
                queue = pick(&d.fields, "queue");
            }
            if job.is_none() {
                job = pick(&d.fields, "job_id");
            }
        }
    }
    (queue, job)
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'l> LookupSpan<'l>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut FieldVisitor(&mut fields));
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanData {
                fields,
                opened: Instant::now(),
                opened_us: now_us(),
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(d) = span.extensions_mut().get_mut::<SpanData>()
        {
            values.record(&mut FieldVisitor(&mut d.fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut FieldVisitor(&mut fields));
        let (queue, job_id) = match ctx.event_scope(event) {
            Some(scope) => inherited(&fields, scope),
            None => inherited::<S>(&fields, std::iter::empty()),
        };
        let meta = event.metadata();
        let name = fields
            .get("message")
            .cloned()
            .unwrap_or_else(|| meta.name().to_string());
        push(TraceRecord {
            at_us: now_us(),
            kind: TraceKind::Event,
            name,
            level: meta.level().to_string(),
            target: meta.target().to_string(),
            fields,
            queue,
            job_id,
            duration_us: None,
        });
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let (fields, opened, opened_us) = {
            let ext = span.extensions();
            let Some(d) = ext.get::<SpanData>() else {
                return;
            };
            (d.fields.clone(), d.opened, d.opened_us)
        };
        let (queue, job_id) = inherited(&fields, span.scope().skip(1));
        let meta = span.metadata();
        push(TraceRecord {
            at_us: opened_us,
            kind: TraceKind::Span,
            name: meta.name().to_string(),
            level: meta.level().to_string(),
            target: meta.target().to_string(),
            fields,
            queue,
            job_id,
            duration_us: Some(opened.elapsed().as_micros() as u64),
        });
    }
}
