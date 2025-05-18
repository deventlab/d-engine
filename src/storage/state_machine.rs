//! StateMachine
//!
//! Handles all database-related operations including:
//! - Applying log entries to the state machine
//! - Generating snapshot data representation(e.g. file)
//! - Applying snapshots to the underlying database
//! - Maintaining data consistency guarantees

#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use crate::proto::Entry;
use crate::Result;

//TODO
pub(crate) type StateMachineIter = sled::Iter;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;
    fn is_running(&self) -> bool;

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Vec<u8>>>;
    fn iter(&self) -> StateMachineIter;

    /// Apply log entries in chunks
    fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<()>;

    /// NOTE: This method may degrade system performance. Use with caution.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Update last applied  index
    fn update_last_applied(
        &self,
        index: u64,
        term: u64,
    );

    /// Get the index of the last applied log: (last_applied_index, last_applied_term)
    fn last_applied(&self) -> (u64, u64);

    /// Persist (last_applied_index, last_applied_term) into local storage
    fn persist_last_applied(
        &self,
        index: u64,
        term: u64,
    ) -> Result<()>;

    /// Update last included index
    fn update_last_included(
        &self,
        index: u64,
        term: u64,
    );

    /// Get snapshot metadata: (last_included_index, last_included_term)
    fn last_included(&self) -> (u64, u64);

    /// Persist (last_included_index, last_included_term) into local storage
    fn persist_last_included(
        &self,
        index: u64,
        term: u64,
    ) -> Result<()>;

    async fn apply_snapshot_from_file(
        &self,
        metadata: crate::proto::SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> Result<()>;

    /// Generates a snapshot of the state machine's current key-value entries
    /// up to the specified `last_included_index`.
    ///
    /// This function:
    /// 1. Creates a new database at `temp_snapshot_path`.
    /// 2. Copies all key-value entries from the current state machine's database where the key
    ///    (interpreted as a log index) does not exceed `last_included_index`.
    /// 3. Uses batch writes for efficiency, committing every 100 records.
    /// 4. Will update last_included_index and last_included_term in memory
    /// 5. Will persist last_included_index and last_included_term into current database and new
    ///    database specified by `temp_snapshot_path`
    ///
    /// # Arguments
    /// * `new_snapshot_dir` - Temporary path to store the snapshot data.
    /// * `last_included_index` - Last log index included in the snapshot.
    /// * `last_included_term` - Last log term included in the snapshot.
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()>;

    fn save_hard_state(&self) -> Result<()>;

    fn flush(&self) -> Result<()>;

    #[cfg(test)]
    fn clean(&self) -> Result<()>;
}
