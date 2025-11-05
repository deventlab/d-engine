//! The `StateMachineHandler` module provides a core component for managing both
//! write operations and read requests against the `StateMachine`.
//!
//! Snapshot related responbilities:
//! - Creating/Deleting temporary snapshot files
//! - Finalizing snapshot file naming and organization
//! - Version control of snapshots
//! - File system I/O operations for snapshots
//! - Handling file locks and concurrency control
//!
//! ## Relationship Between `StateMachineHandler` and `StateMachine`
//! The `StateMachineHandler` serves as the primary interface for interacting
//! with the `StateMachine`. Its dual responsibilities are:
//! 1. Applying committed log entries to the `StateMachine` to maintain state consistency
//! 2. Directly servicing client read requests through state machine queries
//!
//! While maintaining separation from the `StateMachine` itself, the handler
//! leverages the `StateMachine` trait for both state updates and read
//! operations. This design centralizes all state access points while preserving
//! separation of concerns.
//!
//! ## Design Recommendations
//! - **Customization Focus**: Developers should prioritize extending the `StateMachine`
//!   implementation rather than modifying the `StateMachineHandler`. The handler is intentionally
//!   generic and battle-tested, serving as:
//!   - Write coordinator for log application
//!   - Read router for direct state queries
//! - **State Access Unification**: All state access (both write and read) should flow through the
//!   handler to leverage:
//!   - Consistent concurrency control
//!   - Atomic visibility guarantees
//!   - Linearizable read optimizations

mod default_state_machine_handler;
mod snapshot_assembler;
mod snapshot_guard;
mod snapshot_policy;

pub use default_state_machine_handler::*;
pub use snapshot_policy::*;

pub(crate) use snapshot_assembler::*;
pub(crate) use snapshot_guard::*;

#[cfg(test)]
mod default_state_machine_handler_test;
#[cfg(test)]
mod snapshot_assembler_test;

use futures::stream::BoxStream;
#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;
use std::sync::Arc;
use tonic::async_trait;

use super::NewCommitData;
use crate::Result;
use crate::TypeConfig;
use crate::alias::ROF;
use d_engine_proto::client::ClientResult;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::PurgeLogResponse;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;

#[cfg_attr(any(test, feature = "test-utils"), automock)]
#[async_trait]
pub trait StateMachineHandler<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    fn last_applied(&self) -> u64;

    /// Updates the highest known committed log index that hasn't been applied yet
    fn update_pending(
        &self,
        new_commit: u64,
    );

    /// Applies a batch of committed log entries to the state machine
    async fn apply_chunk(
        &self,
        chunk: Vec<d_engine_proto::common::Entry>,
    ) -> Result<()>;

    /// Reads values from the state machine for given keys
    /// Returns None if any key doesn't exist
    fn read_from_state_machine(
        &self,
        keys: Vec<bytes::Bytes>,
    ) -> Option<Vec<ClientResult>>;

    /// Receives and installs a snapshot stream pushed by the leader
    /// Used when leader proactively sends snapshot updates to followers
    async fn apply_snapshot_stream_from_leader(
        &self,
        current_term: u64,
        stream: Box<tonic::Streaming<SnapshotChunk>>,
        ack_tx: tokio::sync::mpsc::Sender<d_engine_proto::server::storage::SnapshotAck>,
        config: &crate::SnapshotConfig,
    ) -> Result<()>;

    /// Determines if a snapshot should be created based on new commit data
    fn should_snapshot(
        &self,
        new_commit_data: NewCommitData,
    ) -> bool;

    /// Asynchronously creates a state machine snapshot with the following steps:
    /// 1. Acquires a write lock to ensure exclusive access during snapshot creation
    /// 2. Prepares temporary and final snapshot file paths using:
    ///    - Last applied index/term from state machine
    /// 3. Generates snapshot data to temporary file using state machine implementation
    /// 4. Atomically renames temporary file to final snapshot file to ensure consistency
    /// 5. Cleans up old snapshots based on last_included_index, retaining only the latest snapshot
    ///    files as specified by cleanup_retain_count.
    ///
    /// Returns new Snapshot metadata and final snapshot path to indicate the new snapshot file has
    /// been successfully created
    async fn create_snapshot(&self) -> Result<(SnapshotMetadata, std::path::PathBuf)>;

    /// Cleans up old snapshots before specified version
    async fn cleanup_snapshot(
        &self,
        before_version: u64,
        snapshot_dir: &std::path::Path,
        snapshot_dir_prefix: &str,
    ) -> crate::Result<()>;

    /// Validates if a log purge request from leader is authorized
    async fn validate_purge_request(
        &self,
        current_term: u64,
        leader_id: Option<u32>,
        req: &PurgeLogRequest,
    ) -> Result<bool>;

    /// Processes log purge requests (for non-leader nodes)
    #[allow(unused)]
    async fn handle_purge_request(
        &self,
        current_term: u64,
        leader_id: Option<u32>,
        last_purged: Option<LogId>,
        req: &PurgeLogRequest,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<PurgeLogResponse>;

    /// Retrieves metadata of the latest valid snapshot
    fn get_latest_snapshot_metadata(&self) -> Option<SnapshotMetadata>;

    /// Loads snapshot data as a stream of chunks
    async fn load_snapshot_data(
        &self,
        metadata: SnapshotMetadata,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>>;

    /// Loads a specific chunk of snapshot data by sequence number
    #[allow(unused)]
    async fn load_snapshot_chunk(
        &self,
        metadata: &SnapshotMetadata,
        seq: u32,
    ) -> Result<SnapshotChunk>;

    fn pending_range(&self) -> Option<std::ops::RangeInclusive<u64>>;
}
