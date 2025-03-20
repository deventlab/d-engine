//! Core model in Raft: StateMachine Definition
//!
//!
use crate::{
    grpc::rpc_service::{Entry, SnapshotEntry},
    Result,
};

#[cfg(test)]
use mockall::automock;
use sled::Batch;
use tonic::async_trait;

//TODO
pub(crate) type StateMachineIter = sled::Iter;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;
    fn is_running(&self) -> bool;
    /// Update last applied log index
    fn update_last_applied(&self, new_id: u64);
    /// Get the index of the last applied log
    fn last_applied(&self) -> u64;
    fn get(&self, key_buffer: &Vec<u8>) -> Result<Option<Vec<u8>>>;
    fn iter(&self) -> StateMachineIter;
    // fn handle_commit(&self, new_commit_index: u64) -> Result<()>;

    /// The operation is based on the underlying storage engine
    // #[deprecated(note = "This method is deprecated. Use `apply_chunk` instead.")]
    fn apply_batch(&self, batch: Batch) -> Result<()>;
    /// Apply log entries in chunks
    async fn apply_chunk(&self, chunk: Vec<Entry>) -> Result<()>;

    fn last_entry_index(&self) -> Option<u64>;
    fn flush(&self) -> Result<()>;

    // fn apply_to_state_machine_up_to_commit_index(
    //     &self,
    //     range: RangeInclusive<u64>,
    // ) -> Result<Vector<u64>>;

    fn apply_snapshot(&self, entry: SnapshotEntry) -> Result<()>;

    #[cfg(test)]
    fn clean(&self) -> Result<()>;
    #[cfg(test)]
    fn len(&self) -> usize;
}
