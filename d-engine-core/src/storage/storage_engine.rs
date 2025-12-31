#![doc = include_str!("../../../d-engine-docs/src/docs/server_guide/customize-storage-engine.md")]

use std::ops::RangeInclusive;
use std::sync::Arc;

use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;
use tonic::async_trait;

use crate::Error;
use crate::HardState;

/// High-performance storage abstraction for Raft consensus
///
/// Design principles:
/// - Zero-cost abstractions through static dispatch
/// - Physical separation of log and metadata stores
/// - Async-ready for I/O parallelism
/// - Minimal interface for maximum performance
pub trait StorageEngine: Send + Sync + 'static {
    /// Associated log store type
    type LogStore: LogStore;

    /// Associated metadata store type
    type MetaStore: MetaStore;

    /// Get log storage handle
    fn log_store(&self) -> Arc<Self::LogStore>;

    /// Get metadata storage handle
    fn meta_store(&self) -> Arc<Self::MetaStore>;
}

#[cfg_attr(any(test, feature = "test-utils"), automock)]
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
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

    /// Optional: Flush pending writes (use with caution)
    fn flush(&self) -> Result<(), Error> {
        Ok(()) // Default no-op for engines with auto-flush
    }

    /// Optional: Flush pending writes (use with caution)
    async fn flush_async(&self) -> Result<(), Error> {
        Ok(()) // Default no-op for engines with auto-flush
    }

    async fn reset(&self) -> Result<(), Error>;

    /// Get last log index (optimized for frequent access)
    ///
    /// # Implementation note
    /// Should maintain cached value updated on write operations
    fn last_index(&self) -> u64;
}

/// Metadata storage operations
#[cfg_attr(any(test, feature = "test-utils"), automock)]
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Atomically persist hard state (current term and votedFor)
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error>;

    /// Load persisted hard state
    fn load_hard_state(&self) -> Result<Option<HardState>, Error>;

    /// Optional: Flush pending writes (use with caution)
    fn flush(&self) -> Result<(), Error> {
        Ok(()) // Default no-op for engines with auto-flush
    }

    /// Optional: Flush pending writes (use with caution)
    async fn flush_async(&self) -> Result<(), Error> {
        Ok(()) // Default no-op for engines with auto-flush
    }
}
