//! SoA (Struct-of-Arrays) buffer for client propose requests.
//!
//! # Memory Layout
//!
//! Before (AoS — Array of Structs, commit b858962):
//! ```text
//! buffer: [(EntryPayload, Sender), (EntryPayload, Sender), ...]
//! flush:  unzip() → O(n) scan + two Vec allocations
//! ```
//!
//! After (SoA — this struct):
//! ```text
//! payloads: [EP, EP, EP, ...]    ← contiguous, cache-friendly for Raft log append
//! senders:  [S,  S,  S,  ...]    ← contiguous, accessed only at response time
//! flush:    mem::swap() × 2      → O(1), replacement Vecs sized to actual batch len
//! ```
//!
//! # Invariant
//!
//! `payloads.len() == senders.len()` at all times.
//! Enforced by routing all mutations through `push()` and `flush()`.

use nanoid::nanoid;
use std::sync::Arc;

use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::EntryPayload;
use tokio::time::Instant;
use tonic::Status;

use crate::MaybeCloneOneshotSender;
use crate::RaftRequestWithSignal;

/// Sender type alias for readability.
type ProposeSender = MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>;

/// SoA buffer that accumulates client propose requests until flush.
///
/// Replaces `BatchBuffer<(EntryPayload, Sender)>` to eliminate the O(n) `unzip()`
/// previously required at flush time. The `flush()` method now returns a fully
/// constructed `RaftRequestWithSignal` in O(1) via `mem::swap` (retaining capacity).
pub struct ProposeBatchBuffer {
    /// Accumulated entry payloads — contiguous for cache-friendly log append.
    payloads: Vec<EntryPayload>,
    /// Response channels, one per payload (index-aligned with `payloads`).
    senders: Vec<ProposeSender>,
    /// Timestamp of the last flush, used for heartbeat-driven flush scheduling.
    pub last_flush: Instant,
    /// Pre-allocated metric labels for zero-allocation hot path.
    metrics_labels: Option<Arc<[(String, String)]>>,
    /// Runtime switch for buffer length gauge.
    metrics_enabled: bool,
}

impl ProposeBatchBuffer {
    /// `initial_capacity` is used only for the initial Vec pre-allocation.
    /// The flush threshold is enforced externally by the caller.
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            payloads: Vec::with_capacity(initial_capacity),
            senders: Vec::with_capacity(initial_capacity),
            last_flush: Instant::now(),
            metrics_labels: None,
            metrics_enabled: false,
        }
    }

    /// Enable buffer length gauge with a distinguishing buffer name label.
    ///
    /// Emits: `batch.buffer_length{node_id="..", buffer="propose"}`
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

    /// Hot path: O(1) push, no tuple construction, no intermediate allocation.
    ///
    /// Flush is driven externally by the drain loop in `raft.rs` main event loop
    /// (recv() blocks for first item, try_recv() drains up to max_batch_size, then flush).
    /// No signal returned — caller uses `len()` to decide when to flush.
    pub fn push(
        &mut self,
        payload: EntryPayload,
        sender: ProposeSender,
    ) {
        self.payloads.push(payload);
        self.senders.push(sender);

        if self.metrics_enabled {
            if let Some(ref labels) = self.metrics_labels {
                metrics::gauge!("batch.buffer_length", labels.as_ref())
                    .set(self.payloads.len() as f64);
            }
        }
    }

    /// Flush: O(1) — swaps both Vecs out, retaining accumulated capacity for the next batch.
    ///
    /// Uses `mem::swap` so `self` keeps the filled Vecs (now handed to the caller)
    /// and receives fresh Vecs sized to the current batch length.
    /// On the next batch, `self.payloads` will grow again without a cold reallocation
    /// because the swapped-in Vec already has capacity == last batch size.
    /// Returns `None` if the buffer is empty (nothing to replicate).
    pub fn flush(&mut self) -> Option<RaftRequestWithSignal> {
        if self.payloads.is_empty() {
            return None;
        }
        self.last_flush = Instant::now();

        // Allocate replacement Vecs sized to actual batch length, not a fixed max.
        // After swap: self holds the exact-sized empty Vecs; caller gets the filled ones.
        let n = self.payloads.len();
        let mut payloads = Vec::with_capacity(n);
        let mut senders = Vec::with_capacity(n);
        std::mem::swap(&mut self.payloads, &mut payloads);
        std::mem::swap(&mut self.senders, &mut senders);

        if self.metrics_enabled {
            if let Some(ref labels) = self.metrics_labels {
                metrics::gauge!("batch.buffer_length", labels.as_ref()).set(0.0);
            }
        }

        Some(RaftRequestWithSignal {
            id: nanoid!(),
            payloads,
            senders,
            wait_for_apply_event: true,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    pub fn len(&self) -> usize {
        self.payloads.len()
    }
}
