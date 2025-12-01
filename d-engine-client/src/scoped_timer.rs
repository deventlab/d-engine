use tokio::time::Instant;
use tracing::trace;

pub(crate) struct ScopedTimer {
    start: Instant,
    name: &'static str,
}

impl ScopedTimer {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            name,
        }
    }
}

impl Drop for ScopedTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        trace!(target: "timing", "[TIMING] {} took {} ms", self.name, elapsed.as_millis());
    }
}
