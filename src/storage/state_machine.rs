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
use crate::proto::LogId;
use crate::proto::SnapshotMetadata;
use crate::Error;
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
        last_applied: LogId,
    );

    /// Get the index of the last applied log: (last_applied_index, last_applied_term)
    fn last_applied(&self) -> LogId;

    /// Persist (last_applied_index, last_applied_term) into local storage
    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<()>;

    /// Update last included index
    fn update_last_included(
        &self,
        last_included: LogId,
        new_checksum: Option<[u8; 32]>,
    );

    /// Get snapshot metadata: (last_included_index, last_included_term, Option<checksum>)
    fn last_included(&self) -> (LogId, Option<[u8; 32]>);

    /// Persist (last_included_index, last_included_term) into local storage
    fn persist_last_included(
        &self,
        last_applied: LogId,
        last_checksum: Option<[u8; 32]>,
    ) -> Result<()>;

    async fn apply_snapshot_from_file(
        &self,
        metadata: &crate::proto::SnapshotMetadata,
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
    ///
    /// # Returns
    /// * if success, checksum will be returned
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<[u8; 32]>;

    fn save_hard_state(&self) -> Result<()>;

    fn flush(&self) -> Result<()>;

    #[cfg(test)]
    fn clean(&self) -> Result<()>;
}

impl SnapshotMetadata {
    pub fn checksum_array(&self) -> Result<[u8; 32]> {
        if self.checksum.len() == 32 {
            let mut array = [0u8; 32];
            array.copy_from_slice(&self.checksum);
            Ok(array)
        } else {
            Err(Error::Fatal("Invalid checksum length".to_string()))
        }
    }

    pub fn set_checksum_array(
        &mut self,
        array: [u8; 32],
    ) {
        self.checksum = array.to_vec();
    }
}
