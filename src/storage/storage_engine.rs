#![doc = include_str!("../docs/server_guide/customize-storage-engine.md")]

use std::ops::RangeInclusive;

use bytes::Bytes;
#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::HardState;
use crate::Result;

/// Unified storage engine handling both Raft logs and state machine
#[async_trait]
#[cfg_attr(test, automock)]
pub trait StorageEngine: Send + Sync + 'static {
    /// Persist multiple Raft log entries
    fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()>;

    fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]> + 'static,
        V: AsRef<[u8]> + 'static;

    fn get<K>(
        &self,
        key: K,
    ) -> crate::Result<Option<Bytes>>
    where
        K: AsRef<[u8]> + Send + 'static;

    /// Get single log entry by index
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>>;

    /// Get log entries within index range (inclusive)
    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>>;

    /// Remove logs up to specified index
    fn purge_logs(
        &self,
        cutoff_index: LogId,
    ) -> Result<()>;

    fn flush(&self) -> Result<()>;

    fn reset(&self) -> Result<()>;

    /// Get last log index
    fn last_index(&self) -> u64;

    /// Truncates log from specified index onward
    fn truncate(
        &self,
        from_index: u64,
    ) -> Result<()>;

    /// When node restarts, check if there is stored state from disk
    fn load_hard_state(&self) -> Result<Option<HardState>>;

    /// Persist hard state
    fn save_hard_state(
        &self,
        hard_state: HardState,
    ) -> Result<()>;
}
