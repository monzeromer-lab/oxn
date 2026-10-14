//! A `metrics` recorder for tests: every counter, gauge and histogram
//! sample oxn emits, keyed by name and labels, kept for the whole process.
//!
//! Unlike `metrics-util`'s debugging recorder nothing is drained on read,
//! so tests running in parallel can each look up their own queue's series
//! (filter by the `queue` label).
//!
//! ```ignore
//! let m = oxn_testkit::metrics::install();
//! // … run a worker …
//! assert_eq!(m.counter("oxn_jobs_completed_total", &[("queue", q.name())]), 10);
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use parking_lot::Mutex;

type Labels = Vec<(String, String)>;

#[derive(Default)]
struct Cell {
    counter: AtomicU64,
    gauge: Mutex<f64>,
    samples: Mutex<Vec<f64>>,
}

impl metrics::CounterFn for Cell {
    fn increment(&self, v: u64) {
        self.counter.fetch_add(v, Ordering::Relaxed);
    }
    fn absolute(&self, v: u64) {
        self.counter.fetch_max(v, Ordering::Relaxed);
    }
}

impl metrics::GaugeFn for Cell {
    fn increment(&self, v: f64) {
        self.update(|g| g + v);
    }
    fn decrement(&self, v: f64) {
        self.update(|g| g - v);
    }
    fn set(&self, v: f64) {
        *self.gauge.lock() = v;
    }
}

impl metrics::HistogramFn for Cell {
    fn record(&self, v: f64) {
        self.samples.lock().push(v);
    }
}

impl Cell {
    fn update(&self, f: impl Fn(f64) -> f64) {
        let mut g = self.gauge.lock();
        *g = f(*g);
    }
}

/// Everything recorded so far. Get it with [`install`].
#[derive(Default)]
pub struct Captured {
    series: Mutex<BTreeMap<(String, Labels), Arc<Cell>>>,
    described: Mutex<BTreeMap<String, (Option<Unit>, String)>>,
}

impl std::fmt::Debug for Captured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Captured")
            .field("series", &self.series.lock().len())
            .finish()
    }
}

impl Captured {
    fn cell(&self, key: &Key) -> Arc<Cell> {
        let mut labels: Labels = key
            .labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect();
        labels.sort();
        self.series
            .lock()
            .entry((key.name().to_string(), labels))
            .or_default()
            .clone()
    }

    fn matching(&self, name: &str, want: &[(&str, &str)]) -> Vec<(Labels, Arc<Cell>)> {
        self.series
            .lock()
            .iter()
            .filter(|((n, labels), _)| {
                n == name
                    && want
                        .iter()
                        .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|((_, l), c)| (l.clone(), c.clone()))
            .collect()
    }

    /// Sum of every counter series named `name` whose labels include `labels`.
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.matching(name, labels)
            .iter()
            .map(|(_, c)| c.counter.load(Ordering::Relaxed))
            .sum()
    }

    /// Sum of the matching gauges.
    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.matching(name, labels)
            .iter()
            .map(|(_, c)| *c.gauge.lock())
            .sum()
    }

    /// Every sample of the matching histograms.
    pub fn histogram(&self, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
        self.matching(name, labels)
            .iter()
            .flat_map(|(_, c)| c.samples.lock().clone())
            .collect()
    }

    /// Every series recorded for `queue`: `name{labels} = value` lines,
    /// for failure messages and reports.
    pub fn dump(&self, queue: &str) -> String {
        let mut out = String::new();
        for ((name, labels), c) in self.series.lock().iter() {
            if !labels.iter().any(|(k, v)| k == "queue" && v == queue) {
                continue;
            }
            let l: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
            let samples = c.samples.lock();
            let value = if !samples.is_empty() {
                format!("{} samples", samples.len())
            } else if c.counter.load(Ordering::Relaxed) > 0 {
                c.counter.load(Ordering::Relaxed).to_string()
            } else {
                c.gauge.lock().to_string()
            };
            out.push_str(&format!("{name}{{{}}} = {value}\n", l.join(",")));
        }
        out
    }

    /// Unit and description registered for `name`, if any.
    pub fn description(&self, name: &str) -> Option<(Option<Unit>, String)> {
        self.described.lock().get(name).cloned()
    }

    fn describe(&self, key: KeyName, unit: Option<Unit>, d: SharedString) {
        self.described
            .lock()
            .insert(key.as_str().to_string(), (unit, d.to_string()));
    }
}

struct Rec(&'static Captured);

impl Recorder for Rec {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, d: SharedString) {
        self.0.describe(key, unit, d);
    }
    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, d: SharedString) {
        self.0.describe(key, unit, d);
    }
    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, d: SharedString) {
        self.0.describe(key, unit, d);
    }
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.0.cell(key))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.0.cell(key))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(self.0.cell(key))
    }
}

/// Install the capture as the process-global `metrics` recorder (once) and
/// return it. Panics if another global recorder was installed first.
pub fn install() -> &'static Captured {
    static CAPTURED: OnceLock<&'static Captured> = OnceLock::new();
    CAPTURED.get_or_init(|| {
        let c: &'static Captured = Box::leak(Box::default());
        metrics::set_global_recorder(Rec(c))
            .unwrap_or_else(|_| panic!("another global metrics recorder is installed"));
        c
    })
}
