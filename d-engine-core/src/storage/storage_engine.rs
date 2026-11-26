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

    /// Get single log entry by index
    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error>;

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

    /// Truncates log from specified index onward
    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error>;

    /// Atomically truncate from `from_index` and persist `new_entries` in one write.
    ///
    /// # Contract
    /// The caller guarantees `new_entries[i].index == from_index + i`: contiguous, starting at
    /// `from_index`. Implementations do not re-check it; the Raft log rejects a malformed
    /// AppendEntries batch before it reaches storage.
    /// Where `from_index` sits relative to the existing log (below the purge boundary,
    /// beyond `last_index + 1`) is the caller's responsibility; behavior there is unspecified.
    ///
    /// Default implementation calls `truncate` then `persist_entries` sequentially
    /// (non-atomic). Override with a single WriteBatch for true crash atomicity.
    /// Returns the highest log index on disk after the operation: the last of
    /// `new_entries`, or `from_index - 1` when `new_entries` is empty.
    async fn replace_range(
        &self,
        from_index: u64,
        new_entries: Vec<Entry>,
    ) -> Result<u64, Error> {
        let new_last = new_entries.last().map(|e| e.index).unwrap_or(from_index.saturating_sub(1));
        self.truncate(from_index).await?;
        if !new_entries.is_empty() {
            self.persist_entries(new_entries).await?;
        }
        Ok(new_last)
    }

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

    /// Delete every log entry and start again from an empty log: `last_index()` becomes 0.
    ///
    /// Only the log is affected. Return only when the deletion is done.
    async fn reset(&self) -> Result<(), Error>;

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
    fn load_purge_boundary(&self) -> Result<Option<LogId>, Error> {
        Ok(None) // Default: not persisted
    }
}
