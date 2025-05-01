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

    // fn last_entry(&self) -> Option<Entry>;

    fn flush(&self) -> Result<()>;

    #[cfg(test)]
    fn clean(&self) -> Result<()>;

    /// NOTE: This method may degrade system performance. Use with caution.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Update last applied log index
    fn update_last_applied(
        &self,
        index: u64,
        term: u64,
    );

    /// Get the index of the last applied log
    fn last_applied(&self) -> (u64, u64);

    async fn apply_snapshot_from_file(
        &self,
        metadata: crate::proto::SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> Result<()>;

    /// Generates a snapshot of the state machine's current key-value entries
    /// up to the specified `last_included_index`.
    ///
    /// This function:
    /// 1. Creates a temporary database at `temp_snapshot_path`.
    /// 2. Copies all key-value entries from the current state machine's database where the key
    ///    (interpreted as a log index) does not exceed `last_included_index`.
    /// 3. Uses batch writes for efficiency, committing every 100 records.
    ///
    /// # Arguments
    /// * `temp_snapshot_path` - Temporary path to store the snapshot data.
    /// * `last_included_index` - Maximum log index (inclusive) included in the snapshot.
    async fn generate_snapshot_data(
        &self,
        temp_snapshot_dir: &std::path::PathBuf,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()>;

    fn save_hard_state(&self) -> Result<()>;
}
