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

pub(crate) use default_state_machine_handler::*;
pub(crate) use snapshot_assembler::*;
pub(crate) use snapshot_guard::*;
pub(crate) use snapshot_policy::*;

#[cfg(test)]
mod default_state_machine_handler_test;
#[cfg(test)]
mod snapshot_assembler_test;

use std::sync::Arc;

#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use super::NewCommitData;
use crate::alias::ROF;
use crate::proto::ClientCommand;
use crate::proto::ClientResult;
use crate::proto::SnapshotChunk;
use crate::Result;
use crate::TypeConfig;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait StateMachineHandler<T>: Send + Sync + 'static
where T: TypeConfig
{
    fn update_pending(
        &self,
        new_commit: u64,
    );
    async fn apply_batch(
        &self,
        raft_log: Arc<ROF<T>>,
    ) -> Result<()>;

    fn read_from_state_machine(
        &self,
        client_command: Vec<ClientCommand>,
    ) -> Option<Vec<ClientResult>>;

    async fn install_snapshot_chunk(
        &self,
        current_term: u64,
        stream_request: tonic::Streaming<SnapshotChunk>,
        sender: crate::MaybeCloneOneshotSender<std::result::Result<crate::proto::SnapshotResponse, tonic::Status>>,
    ) -> Result<()>;

    /// Validate if should generate snapshot now
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
    /// Returns the path to the successfully created final snapshot file
    async fn create_snapshot(&self) -> Result<std::path::PathBuf>;

    async fn cleanup_snapshot(
        &self,
        before_version: u64,
        snapshot_dir: &std::path::PathBuf,
    ) -> crate::Result<()>;

    // fn current_snapshot_version(&self) -> u64;
}
