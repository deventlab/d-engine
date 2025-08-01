//! Core model in Raft: RaftLog Definition

use crate::proto::common::entry_payload::Payload;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::Result;
use std::ops::RangeInclusive;
use tonic::async_trait;

#[cfg(test)]
use mockall::automock;

// /// Configurable persistence strategy for Raft logs
// #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
// pub enum PersistenceStrategy {
//     /// Persist logs to disk before replicating to followers
//     /// (Stronger durability, lower throughput)
//     DiskFirst,

//     /// Replicate logs immediately and persist asynchronously
//     /// (Higher throughput, optimized for low-latency networks)
//     MemFirst,

//     /// Persist logs in batches at fixed intervals
//     /// (Balanced approach for mixed workloads)
//     Batched(usize, u64), // (batch_size, interval_ms)
// }

// impl Default for PersistenceStrategy {
//     fn default() -> Self {
//         Self::Batched(1000, 10)
//     }
// }

#[cfg_attr(test, automock)]
#[async_trait]
pub trait RaftLog: Send + Sync + 'static {
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>>;

    fn first_entry_id(&self) -> u64;

    fn last_entry_id(&self) -> u64;

    fn last_log_id(&self) -> Option<LogId>;

    fn last_entry(&self) -> Option<Entry>;

    fn is_empty(&self) -> bool;

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64>;

    fn pre_allocate_raft_logs_next_index(&self) -> u64;

    /// Pre-allocates a contiguous range of log indices
    /// Returns inclusive range [start, end] where (end - start + 1) == count
    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64>;

    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>>;

    async fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>>;

    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()>;

    async fn insert_batch(
        &self,
        logs: Vec<Entry>,
    ) -> Result<()>;

    /// Clear out all the entries in local logs before last_applied
    /// this function will be invoked only when entries been applied to state
    /// machine successfully
    fn purge_logs_up_to(
        &self,
        cutoff_index: LogId,
    ) -> Result<()>;

    async fn flush(&self) -> Result<()>;

    /// @Write
    async fn reset(&self) -> Result<()>;

    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        matched_ids: Vec<u64>,
    ) -> Option<u64>;

    fn load_hard_state(&self) -> Result<Option<crate::HardState>>;

    fn save_hard_state(
        &self,
        hard_state: crate::HardState,
    ) -> Result<()>;

    #[cfg(test)]
    fn db_size(
        &self,
        node_id: u32,
        db_size_cache_window: u128,
    ) -> Result<u64>;

    #[cfg(test)]
    fn len(&self) -> usize;
}

impl EntryPayload {
    #[inline]
    pub fn is_config(&self) -> bool {
        match self.payload {
            Some(Payload::Command(_)) => false,
            Some(Payload::Config(_)) => true,
            Some(Payload::Noop(_)) => false,
            None => false,
        }
    }

    #[inline]
    pub fn is_command(&self) -> bool {
        match self.payload {
            Some(Payload::Command(_)) => true,
            Some(Payload::Config(_)) => false,
            Some(Payload::Noop(_)) => false,
            None => false,
        }
    }

    #[inline]
    pub fn is_noop(&self) -> bool {
        match self.payload {
            Some(Payload::Command(_)) => false,
            Some(Payload::Config(_)) => false,
            Some(Payload::Noop(_)) => true,
            None => false,
        }
    }
}
