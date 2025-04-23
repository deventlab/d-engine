//! Core model in Raft: StateMachine Definition
#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use crate::proto::Entry;
use crate::proto::SnapshotEntry;
use crate::Result;

//TODO
pub(crate) type StateMachineIter = sled::Iter;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;
    fn is_running(&self) -> bool;
    /// Update last applied log index
    fn update_last_applied(
        &self,
        new_id: u64,
    );
    /// Get the index of the last applied log
    fn last_applied(&self) -> u64;
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

    fn last_entry_index(&self) -> Option<u64>;

    fn flush(&self) -> Result<()>;

    fn apply_snapshot(
        &self,
        entry: SnapshotEntry,
    ) -> Result<()>;

    #[cfg(test)]
    fn clean(&self) -> Result<()>;

    /// NOTE: This method may degrade system performance. Use with caution.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
