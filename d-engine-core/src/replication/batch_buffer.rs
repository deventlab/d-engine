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

/// Batch performance metrics (simplified)
///
/// Tracks batch trigger patterns and size distribution for tuning max_batch_size.
/// Use drain_triggered ratio to validate drain-based architecture efficiency.
#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub node_id: u32,
    pub drain_triggered: Arc<AtomicU64>,
    pub heartbeat_triggered: Arc<AtomicU64>,
    /// Pre-allocated metric labels for zero-allocation hot path
    metrics_labels: Arc<[(String, String)]>,
    /// Runtime switch for batch metrics
    enabled: bool,
}

impl BatchMetrics {
    pub fn new(
        node_id: u32,
        enabled: bool,
    ) -> Self {
        let node_id_str = node_id.to_string();
        let metrics_labels = Arc::new([("node_id".to_string(), node_id_str)]);

        Self {
            node_id,
            drain_triggered: Arc::new(AtomicU64::new(0)),
            heartbeat_triggered: Arc::new(AtomicU64::new(0)),
            metrics_labels,
            enabled,
        }
    }

    /// Record batch processing event
    ///
    /// Emits:
    /// - Counter: batch.{drain|heartbeat}_triggered - trigger type frequency
    /// - Histogram: batch.size - batch size distribution (P50/P99 analysis)
    pub fn record_batch(
        &self,
        size: usize,
        trigger_type: BatchTriggerType,
    ) {
        if !self.enabled {
            return;
        }

        // Trigger type counter
        match trigger_type {
            BatchTriggerType::Drain => {
                self.drain_triggered.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("batch.drain_triggered", self.metrics_labels.as_ref())
                    .increment(1);
            }
            BatchTriggerType::Heartbeat => {
                self.heartbeat_triggered.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("batch.heartbeat_triggered", self.metrics_labels.as_ref())
                    .increment(1);
            }
        }

        // Batch size distribution (most important metric)
        metrics::histogram!("batch.size", self.metrics_labels.as_ref()).record(size as f64);
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
        let len = self.buffer.len();

        trace!(
            "BatchBuffer::push, max={}, len={}",
            self.max_batch_size, len
        );

        // Emit buffer length gauge for backpressure monitoring (with zero-allocation labels)
        if let Some(ref metrics) = self.metrics {
            if metrics.enabled {
                metrics::gauge!("batch.buffer_length", metrics.metrics_labels.as_ref())
                    .set(len as f64);
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

    pub fn take_with_trigger(
        &mut self,
        trigger_type: BatchTriggerType,
    ) -> VecDeque<E> {
        let batch_size = std::cmp::min(self.buffer.len(), self.max_batch_size);

        let batch = if batch_size == self.buffer.len() {
            // Full take: O(1)
            std::mem::take(&mut self.buffer)
        } else {
            // Partial take: split and swap
            let remaining = self.buffer.split_off(batch_size);
            std::mem::replace(&mut self.buffer, remaining)
        };

        if let Some(ref metrics) = self.metrics {
            metrics.record_batch(batch.len(), trigger_type);
        }
        self.last_flush = Instant::now();
        batch
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

    /// Returns the number of buffered items
    ///
    /// Used for backpressure enforcement and monitoring.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.last_flush = Instant::now();
    }
}
