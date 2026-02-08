use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::time::Instant;
use tracing::trace;

// ============================================================================
// Batch Metrics
// ============================================================================

/// Batch trigger type for metrics tracking in drain-based architecture
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchTriggerType {
    /// Triggered by drain-based channel processing (main path)
    Drain,
    /// Triggered by heartbeat deadline (piggyback flush)
    Heartbeat,
}

#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub node_id: u32,
    pub drain_triggered: Arc<AtomicU64>,
    pub heartbeat_triggered: Arc<AtomicU64>,
    pub total_batch_count: Arc<AtomicU64>,
    pub total_batch_size: Arc<AtomicU64>,
}

impl BatchMetrics {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            drain_triggered: Arc::new(AtomicU64::new(0)),
            heartbeat_triggered: Arc::new(AtomicU64::new(0)),
            total_batch_count: Arc::new(AtomicU64::new(0)),
            total_batch_size: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_batch(
        &self,
        size: usize,
        trigger_type: BatchTriggerType,
    ) {
        match trigger_type {
            BatchTriggerType::Drain => {
                self.drain_triggered.fetch_add(1, Ordering::Relaxed);
                metrics::counter!(
                    "batch.drain_triggered",
                    &[("node_id", self.node_id.to_string())]
                )
                .increment(1);
            }
            BatchTriggerType::Heartbeat => {
                self.heartbeat_triggered.fetch_add(1, Ordering::Relaxed);
                metrics::counter!(
                    "batch.heartbeat_triggered",
                    &[("node_id", self.node_id.to_string())]
                )
                .increment(1);
            }
        }
        self.total_batch_count.fetch_add(1, Ordering::Relaxed);
        self.total_batch_size.fetch_add(size as u64, Ordering::Relaxed);
        metrics::counter!(
            "batch.total_count",
            &[("node_id", self.node_id.to_string())]
        )
        .increment(1);
        metrics::gauge!("batch.total_size", &[("node_id", self.node_id.to_string())])
            .set(self.total_batch_size.load(Ordering::Relaxed) as f64);

        // Record batch size histogram for distribution analysis
        metrics::histogram!("batch.size", &[("node_id", self.node_id.to_string())])
            .record(size as f64);
    }
}

pub struct BatchBuffer<E> {
    /// Maximum number of items to drain in one batch (prevents IO overload in drain loop)
    pub(super) max_batch_size: usize,
    pub(super) buffer: VecDeque<E>,
    /// Last flush timestamp (used for metrics and monitoring only)
    pub(super) last_flush: Instant,
    pub metrics: Option<Arc<BatchMetrics>>,
}

impl<E> BatchBuffer<E> {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            buffer: VecDeque::with_capacity(max_batch_size),
            last_flush: Instant::now(),
            metrics: None,
        }
    }

    pub fn with_metrics(
        mut self,
        metrics: Arc<BatchMetrics>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn push(
        &mut self,
        request: E,
    ) -> Option<usize> {
        self.buffer.push_back(request);
        trace!(
            "BatchBuffer::push, self.max_batch_size={}, self.buffer.len()={}",
            self.max_batch_size,
            self.buffer.len()
        );
        if self.buffer.len() >= self.max_batch_size {
            Some(self.buffer.len())
        } else {
            None
        }
    }

    pub fn take(&mut self) -> VecDeque<E> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.buffer)
    }

    pub fn take_with_trigger(
        &mut self,
        trigger_type: BatchTriggerType,
    ) -> VecDeque<E> {
        let batch_size = self.buffer.len();
        if let Some(ref metrics) = self.metrics {
            metrics.record_batch(batch_size, trigger_type);
        }
        self.last_flush = Instant::now();
        std::mem::take(&mut self.buffer)
    }

    /// Check if buffer is empty (used in drain-based architecture)
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Take all items from buffer without checking thresholds (drain-based).
    ///
    /// In drain-based architecture, flushing is driven by channel wakeup,
    /// not by timeout or size checks. This method unconditionally takes
    /// all buffered items for immediate processing.
    pub fn take_all(&mut self) -> VecDeque<E> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.buffer)
    }

    // Test helper methods
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.last_flush = Instant::now();
    }
}
