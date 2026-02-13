use std::collections::VecDeque;
use std::sync::Arc;

use tokio::time::Instant;
use tracing::trace;

pub struct BatchBuffer<E> {
    /// Maximum number of items to drain in one batch
    pub(super) max_batch_size: usize,
    pub(super) buffer: VecDeque<E>,
    /// Last flush timestamp (used for monitoring only)
    pub(super) last_flush: Instant,
    /// Pre-allocated metric labels for zero-allocation hot path
    metrics_labels: Option<Arc<[(String, String)]>>,
    /// Runtime switch for buffer length gauge
    metrics_enabled: bool,
}

impl<E> BatchBuffer<E> {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            buffer: VecDeque::with_capacity(max_batch_size),
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

    pub fn push(
        &mut self,
        request: E,
    ) -> Option<usize> {
        self.buffer.push_back(request);
        let len = self.buffer.len();

        trace!(
            "BatchBuffer::push, max={}, len={}",
            self.max_batch_size, len
        );

        if self.metrics_enabled {
            if let Some(ref labels) = self.metrics_labels {
                metrics::gauge!("batch.buffer_length", labels.as_ref()).set(len as f64);
            }
        }

        if len >= self.max_batch_size {
            Some(len)
        } else {
            None
        }
    }

    pub fn take(&mut self) -> VecDeque<E> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.buffer)
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Take all items unconditionally (drain-based architecture).
    pub fn take_all(&mut self) -> VecDeque<E> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.buffer)
    }

    /// Returns the number of buffered items
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.last_flush = Instant::now();
    }
}
