//! Cross-thread log capture for tests.
//!
//! `tracing-test`'s `#[traced_test]` only captures events emitted on the
//! annotated test's own thread. Some code under test runs on a dedicated OS
//! thread with its own tokio runtime (e.g. `BufferedRaftLog`'s IO thread),
//! whose log output `#[traced_test]` cannot see. This installs a
//! process-wide subscriber instead, visible from any thread.
//!
//! Idempotent per process: `cargo nextest` runs each *test binary* — not each
//! test fn — in its own process, and multiple `#[tokio::test]` fns in the same
//! integration test file share that one binary/process. The first call installs
//! the global subscriber; every later call in the same process reuses the same
//! buffer instead of re-installing (which `tracing` only allows once).

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

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

/// Backs both `capture_logs_globally` and `capture_logs_globally_filtered` — only one
/// global subscriber can ever be installed per process, so both functions share one
/// slot: whichever is called first wins, later calls (from either function) just
/// return its buffer.
static CAPTURE_BUFFER: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

/// Install a process-wide tracing subscriber that captures every event's
/// message into the returned buffer, regardless of which thread emits it.
/// Idempotent: a later call in the same process — from this fn or from
/// `capture_logs_globally_filtered` — returns the buffer the first call
/// already installed, instead of re-installing (which `tracing` only allows
/// once per process).
pub fn capture_logs_globally() -> Arc<Mutex<Vec<String>>> {
    CAPTURE_BUFFER
        .get_or_init(|| {
            let buf = Arc::new(Mutex::new(Vec::new()));
            let layer = CapturingLayer { buf: buf.clone() };
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::set_global_default(subscriber)
                .expect("capture_logs_globally() must only be called once per test process");
            buf
        })
        .clone()
}

/// Like `capture_logs_globally`, but scoped to `filter` (an `EnvFilter` directive
/// string) instead of capturing every event unconditionally. Use this when the
/// unfiltered version would also pull in noisy third-party crates (e.g. `h2`
/// trace-level frame logging from a real gRPC transport under test).
///
/// Idempotent like `capture_logs_globally`: `filter` only takes effect on
/// whichever call — from this fn or `capture_logs_globally`  — installs the
/// subscriber first. Callers that share a process with other capturing tests
/// (any `#[tokio::test]` in the same integration test binary) must record
/// `buf.lock().unwrap().len()` right after calling this and only inspect
/// entries appended after that offset — the returned buffer is not
/// necessarily empty.
pub fn capture_logs_globally_filtered(filter: &str) -> Arc<Mutex<Vec<String>>> {
    CAPTURE_BUFFER
        .get_or_init(|| {
            let buf = Arc::new(Mutex::new(Vec::new()));
            let layer = CapturingLayer { buf: buf.clone() };
            let subscriber =
                tracing_subscriber::registry().with(EnvFilter::new(filter)).with(layer);
            tracing::subscriber::set_global_default(subscriber).expect(
                "capture_logs_globally_filtered() must only be called once per test process",
            );
            buf
        })
        .clone()
}

/// Check whether any captured log line contains `needle`.
pub fn logs_contain_globally(
    buf: &Arc<Mutex<Vec<String>>>,
    needle: &str,
) -> bool {
    buf.lock().unwrap().iter().any(|line| line.contains(needle))
}

/// Like `logs_contain_globally`, but ignores every line at index `< since` — the
/// offset a caller recorded (via `buf.lock().unwrap().len()`) before the event it
/// actually wants to observe. Needed because the buffer is shared process-wide
/// (see `capture_logs_globally_filtered`'s doc comment): without this, a match from
/// an earlier test, or from an earlier phase of the same test, can make an assertion
/// pass without the phase under test having produced the log line at all.
pub fn logs_contain_globally_since(
    buf: &Arc<Mutex<Vec<String>>>,
    since: usize,
    needle: &str,
) -> bool {
    buf.lock().unwrap()[since..].iter().any(|line| line.contains(needle))
}
