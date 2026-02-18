use std::sync::Arc;

use tokio::time::Instant;

/// Generic single-type batch buffer for accumulating items until flush.
///
/// Uses a contiguous `Vec<E>` for cache-friendly sequential access.
/// For propose (SoA two-field), see `propose_batch_buffer`.
pub struct BatchBuffer<E> {
    pub(super) buffer: Vec<E>,
    /// Last flush timestamp (used for monitoring only)
    pub(super) last_flush: Instant,
    /// Pre-allocated metric labels for zero-allocation hot path
    metrics_labels: Option<Arc<[(String, String)]>>,
    /// Runtime switch for buffer length gauge
    metrics_enabled: bool,
}

impl<E> BatchBuffer<E> {
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(initial_capacity),
            last_flush: Instant::now(),
            metrics_labels: None,
            metrics_enabled: false,
        }
    }

    /// Enable buffer length gauge with a distinguishing buffer name label.
    ///
    /// Emits: `batch.buffer_length{node_id="..", buffer="propose"|"linearizable"}`
    pub fn with_length_gauge(
        mut self,
        node_id: u32,
        buffer_name: &'static str,
        enabled: bool,
    ) -> Self {
        self.metrics_labels = Some(Arc::from(vec![
            ("node_id".to_string(), node_id.to_string()),
            ("buffer".to_string(), buffer_name.to_string()),
        ]));
        self.metrics_enabled = enabled;
        self
    }

    /// Hot path: O(1) push to the end of the contiguous buffer.
    ///
    /// Flush is driven externally by the drain loop in `raft.rs` main event loop
    /// (recv() blocks for first item, try_recv() drains up to max_batch_size, then flush).
    /// No signal returned.
    pub fn push(
        &mut self,
        request: E,
    ) {
        self.buffer.push(request);

        if self.metrics_enabled {
            if let Some(ref labels) = self.metrics_labels {
                metrics::gauge!("batch.buffer_length", labels.as_ref())
                    .set(self.buffer.len() as f64);
            }
        }
    }

    /// Take all buffered items via `mem::take` — O(1), no copy.
    pub fn take_all(&mut self) -> Vec<E> {
        self.last_flush = Instant::now();
        let items = std::mem::take(&mut self.buffer);

        if self.metrics_enabled {
            if let Some(ref labels) = self.metrics_labels {
                metrics::gauge!("batch.buffer_length", labels.as_ref()).set(0.0);
            }
        }

        items
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.last_flush = Instant::now();
    }
}
