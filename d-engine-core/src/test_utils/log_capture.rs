//! Cross-thread log capture for tests.
//!
//! `tracing-test`'s `#[traced_test]` only captures events emitted on the
//! annotated test's own thread. Some code under test runs on a dedicated OS
//! thread with its own tokio runtime (e.g. `BufferedRaftLog`'s IO thread),
//! whose log output `#[traced_test]` cannot see. This installs a
//! process-wide subscriber instead, visible from any thread.
//!
//! Safe to call once per test process: under `cargo nextest`, each test runs
//! in its own process, so "once per process" is "once per test" in practice.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use tracing::Event;
use tracing::Subscriber;
use tracing::field::Field;
use tracing::field::Visit;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;

struct CapturingLayer {
    buf: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for CapturingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &Event<'_>,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        self.buf.lock().unwrap().push(visitor.0);
    }
}

struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(
        &mut self,
        field: &Field,
        value: &dyn fmt::Debug,
    ) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// Install a process-wide tracing subscriber that captures every event's
/// message into the returned buffer, regardless of which thread emits it.
/// Panics if called more than once in the same process — under nextest that
/// means at most once per test.
pub fn capture_logs_globally() -> Arc<Mutex<Vec<String>>> {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let layer = CapturingLayer { buf: buf.clone() };
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("capture_logs_globally() must only be called once per test process");
    buf
}

/// Like `capture_logs_globally`, but scoped to `filter` (an `EnvFilter` directive
/// string) instead of capturing every event unconditionally. Use this when the
/// unfiltered version would also pull in noisy third-party crates (e.g. `h2`
/// trace-level frame logging from a real gRPC transport under test).
pub fn capture_logs_globally_filtered(filter: &str) -> Arc<Mutex<Vec<String>>> {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let layer = CapturingLayer { buf: buf.clone() };
    let subscriber = tracing_subscriber::registry().with(EnvFilter::new(filter)).with(layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("capture_logs_globally_filtered() must only be called once per test process");
    buf
}

/// Check whether any captured log line contains `needle`.
pub fn logs_contain_globally(
    buf: &Arc<Mutex<Vec<String>>>,
    needle: &str,
) -> bool {
    buf.lock().unwrap().iter().any(|line| line.contains(needle))
}
