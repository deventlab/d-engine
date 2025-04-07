//! Core model in Raft: RaftLog Definition
use std::ops::RangeInclusive;

#[cfg(test)]
use mockall::automock;
use sled::Subscriber;
use tonic::async_trait;

use crate::grpc::rpc_service::Entry;
use crate::grpc::rpc_service::LogId;
use crate::Result;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait RaftLog: Send + Sync + 'static {
    fn last_entry_id(&self) -> u64;
    fn first_entry_id(&self) -> u64;
    fn is_empty(&self) -> bool;
    fn has_log_at(
        &self,
        index: u64,
        term: u64,
    ) -> bool;

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64>;

    /// v20241105
    /// mostly used when we want to calculate the roughtly number of entry
    /// inside locallog after removing some entries. #size_of_db is not
    /// reliable.
    ///
    /// Note: the local log entry might looks like:
    ///     1,3,7,8,9,10,11
    /// So the span = 11-1+1 = 10
    /// You can not use this as distance between last and first entry
    /// because obviously some entry might missing because of the pre-allocate
    /// index feature.
    fn span_between_first_entry_and_last_entry(&self) -> u64;

    fn pre_allocate_raft_logs_next_index(&self) -> u64;

    /// Deprecated: Use `last_log_id()` instead.
    fn last(&self) -> Option<Entry>;

    fn last_log_id(&self) -> Option<LogId>;

    /// Deprecated: Use `last_log_id()` instead.
    ///
    /// Get the metadata of the last log (term, index)
    /// if no last log found, returning (0,0)
    /// Return tuple format: (last_log_term, last_log_index)
    fn get_last_entry_metadata(&self) -> (u64, u64);

    /// Upcomging feature in v0.2.0
    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    /// Upcomging feature in v0.2.0
    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    fn get_entry_by_index(
        &self,
        index: u64,
    ) -> Option<Entry>;
    fn get_entry_term_by_index(
        &self,
        index: u64,
    ) -> Option<u64>;
    fn get_entries_between(
        &self,
        range: RangeInclusive<u64>,
    ) -> Vec<Entry>;
    // fn prev_log_index(&self, follower_id: u32) -> u64;
    fn prev_log_term(
        &self,
        follower_id: u32,
        prev_log_index: u64,
    ) -> u64;

    /// used as binary rpc communication
    fn retrieve_one_entry_for_this_follower(
        &self,
        follower_id: u32,
        next_id: u64,
    ) -> Vec<u8>;

    fn prev_log_ok(
        &self,
        req_prev_log_index: u64,
        req_prev_log_term: u64,
        last_applied: u64,
    ) -> bool;

    fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>>;

    /// If an existing entry conflicts with a new one (same index
    ///     but different terms), delete the existing entry and all that
    ///     follow it (§5.3)
    fn filter_out_conflicts_and_append2(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_ones: Vec<Entry>,
    ) -> u64;

    fn insert_batch(
        &self,
        logs: Vec<Entry>,
    ) -> Result<()>;
    /// Clear out all the entries in local logs before last_applied
    /// this function will be invoked only when entries been applied to state
    /// machine successfully
    fn delete_entries_before(
        &self,
        last_applied: u64,
    ) -> Result<()>;

    fn retrieve_subscriber(
        &self,
        watch_key: &Vec<u8>,
    ) -> Subscriber;

    // fn load_from_db(&self, commit_index: u64);

    fn flush(&self) -> Result<()>;

    /// @Write
    fn reset(&self) -> Result<()>;

    /// Load entries from disk to memory.
    fn load_uncommitted_from_db_to_cache(
        &self,
        commit_id: u64,
        len: u64,
    );

    fn get_from_cache(
        &self,
        key: &Vec<u8>,
    ) -> Option<Entry>;

    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        matched_ids: Vec<u64>,
    ) -> Option<u64>;

    #[cfg(test)]
    fn db_size(
        &self,
        node_id: u32,
        db_size_cache_window: u128,
    ) -> Result<u64>;

    #[cfg(test)]
    fn len(&self) -> usize;

    #[cfg(test)]
    fn delete_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<()>;

    #[cfg(test)]
    fn cached_length(&self) -> u64;
    #[cfg(test)]
    fn cached_mapped_entries_len(&self) -> usize;
    #[cfg(test)]
    fn cached_next_id(&self) -> u64;
}
