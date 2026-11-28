//! Storage engine trait for Raft consensus.
//!
//! See the [server customization guide](https://github.com/deventlab/d-engine/blob/master/d-engine/src/docs/server_guide/customize-storage-engine.md) for details.

use std::ops::RangeInclusive;

use async_trait::async_trait;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;

use crate::Error;
use crate::HardState;

/// High-performance storage abstraction for Raft consensus
///
/// Design principles:
/// - Zero-cost abstractions through static dispatch
/// - Async-ready for I/O parallelism
/// - Minimal interface for maximum performance
#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Batch persist entries into disk (optimized for sequential writes)
    ///
    /// # Performance
    /// Implementations should use batch operations and avoid
    /// per-entry synchronization. Expected throughput: >100k ops/sec.
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error>;

    /// Get entries in range [start, end] (inclusive)
    ///
    /// # Performance
    /// Should use efficient range scans. Expected latency: <1ms for 10k entries.
    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error>;

    /// Remove logs up to specified index
    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error>;

    /// Delete every entry with index `>= from_index`. The engine calls it when a leader's entries
    /// conflict with this node's tail, then writes the leader's entries with `persist_entries`.
    ///
    /// # Contract
    /// - If `from_index` is past the last entry, nothing is removed and `last_index()` does not
    ///   change.
    /// - It need not be atomic, but it must never leave a hole: remove entries from the tail
    ///   backward, so a crash midway leaves a shorter, contiguous log.
    /// - It need not be durable when it returns: the next `flush()` makes it durable.
    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error>;

    /// Persist the hard state (current term and votedFor).
    ///
    /// It becomes durable with the next `flush()`: the engine calls `save_hard_state` and then
    /// `flush()` before it answers a vote or an AppendEntries that changed the term.
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error>;

    /// Load the persisted hard state, or `None` on a node that has never saved one.
    fn load_hard_state(&self) -> Result<Option<HardState>, Error>;

    /// Make everything written so far durable: log entries and the hard state.
    ///
    /// The engine calls it before it answers an RPC that depends on what was written. A store
    /// whose writes are already durable when they return still implements it, as `Ok(())`.
    fn flush(&self) -> Result<(), Error>;

    /// Get last log index (optimized for frequent access)
    ///
    /// # Implementation note
    /// Should maintain cached value updated on write operations
    fn last_index(&self) -> u64;

    /// Load the purge boundary persisted by the last `purge()` call.
    ///
    /// Returns the `LogId` (index + term) of the highest entry ever purged,
    /// or `None` if `purge()` has never been called. `RaftLogCore::new()`
    /// uses this to restore `last_purged_index/term` after a restart so that
    /// `entry_term(last_purged_index)` returns the correct term even though
    /// the entry has been removed from the in-memory log.
    fn load_purge_boundary(&self) -> Result<Option<LogId>, Error>;
}
