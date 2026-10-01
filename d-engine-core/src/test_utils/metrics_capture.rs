use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Store {
    gauges: HashMap<String, f64>,
    counters: HashMap<String, u64>,
    histograms: HashMap<String, Vec<f64>>,
}

fn key_string(key: &Key) -> String {
    let mut labels: Vec<String> =
        key.labels().map(|l| format!("{}={}", l.key(), l.value())).collect();
    labels.sort();
    if labels.is_empty() {
        key.name().to_string()
    } else {
        format!("{}{{{}}}", key.name(), labels.join(","))
    }
}

struct GaugeHandle {
    store: Arc<Mutex<Store>>,
    key: String,
}

impl GaugeFn for GaugeHandle {
    fn increment(
        &self,
        value: f64,
    ) {
        let mut s = self.store.lock().unwrap();
        *s.gauges.entry(self.key.clone()).or_default() += value;
    }
    fn decrement(
        &self,
        value: f64,
    ) {
        let mut s = self.store.lock().unwrap();
        *s.gauges.entry(self.key.clone()).or_default() -= value;
    }
    fn set(
        &self,
        value: f64,
    ) {
        self.store.lock().unwrap().gauges.insert(self.key.clone(), value);
    }
}

struct CounterHandle {
    store: Arc<Mutex<Store>>,
    key: String,
}

impl CounterFn for CounterHandle {
    fn increment(
        &self,
        value: u64,
    ) {
        let mut s = self.store.lock().unwrap();
        *s.counters.entry(self.key.clone()).or_default() += value;
    }
    fn absolute(
        &self,
        value: u64,
    ) {
        self.store.lock().unwrap().counters.insert(self.key.clone(), value);
    }
}

struct HistogramHandle {
    store: Arc<Mutex<Store>>,
    key: String,
}

impl HistogramFn for HistogramHandle {
    fn record(
        &self,
        value: f64,
    ) {
        self.store
            .lock()
            .unwrap()
            .histograms
            .entry(self.key.clone())
            .or_default()
            .push(value);
    }
}

/// In-memory metrics recorder for tests.
///
/// Install with `metrics::set_global_recorder(capture.clone())` or use
/// `with_recorder` to scope it to a single thread.
pub struct MetricsCapture {
    store: Arc<Mutex<Store>>,
}

impl Clone for MetricsCapture {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

impl Default for MetricsCapture {
    fn default() -> Self {
        Self {
            store: Arc::new(Mutex::new(Store::default())),
        }
    }
}

impl MetricsCapture {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the last `set()` value for a gauge identified by name + labels.
    ///
    /// `labels` is a slice of `("key", "value")` pairs; order does not matter.
    pub fn gauge(
        &self,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Option<f64> {
        let k = build_key_string(name, labels);
        self.store.lock().unwrap().gauges.get(&k).copied()
    }

    /// Returns the cumulative increment for a counter identified by name + labels.
    pub fn counter(
        &self,
        name: &str,
        labels: &[(&str, &str)],
    ) -> u64 {
        let k = build_key_string(name, labels);
        self.store.lock().unwrap().counters.get(&k).copied().unwrap_or(0)
    }

    /// Every value recorded into a histogram, in recording order.
    pub fn histogram(
        &self,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Vec<f64> {
        let k = build_key_string(name, labels);
        self.store.lock().unwrap().histograms.get(&k).cloned().unwrap_or_default()
    }
}

fn build_key_string(
    name: &str,
    labels: &[(&str, &str)],
) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let mut pairs: Vec<String> = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
    pairs.sort();
    format!("{}{{{}}}", name, pairs.join(","))
}

impl Recorder for MetricsCapture {
    fn describe_counter(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }

    fn register_counter(
        &self,
        key: &Key,
        _metadata: &Metadata<'_>,
    ) -> Counter {
        let k = key_string(key);
        // pre-insert so counter() returns 0 before first increment
        self.store.lock().unwrap().counters.entry(k.clone()).or_default();
        Counter::from_arc(Arc::new(CounterHandle {
            store: Arc::clone(&self.store),
            key: k,
        }))
    }

    fn register_gauge(
        &self,
        key: &Key,
        _metadata: &Metadata<'_>,
    ) -> Gauge {
        Gauge::from_arc(Arc::new(GaugeHandle {
            store: Arc::clone(&self.store),
            key: key_string(key),
        }))
    }

    fn register_histogram(
        &self,
        key: &Key,
        _metadata: &Metadata<'_>,
    ) -> Histogram {
        Histogram::from_arc(Arc::new(HistogramHandle {
            store: Arc::clone(&self.store),
            key: key_string(key),
        }))
    }
}
