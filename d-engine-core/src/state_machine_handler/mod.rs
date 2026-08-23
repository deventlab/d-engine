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

mod applied_state;
mod command;
mod default_state_machine_handler;
mod snapshot_assembler;
mod snapshot_policy;
mod worker;
mod writer;

pub use command::*;
pub use default_state_machine_handler::*;
pub(crate) use snapshot_assembler::*;
pub use snapshot_policy::*;
pub use worker::*;
pub use writer::*;

#[cfg(test)]
mod command_test;
#[cfg(test)]
mod default_state_machine_handler_test;
#[cfg(test)]
mod snapshot_assembler_test;
#[cfg(test)]
mod wait_applied_test;
#[cfg(test)]
mod worker_test;

use crate::client::KvEntry;
use async_trait::async_trait;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use futures::stream::BoxStream;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;

use super::NewCommitData;
use crate::ApplyResult;
use crate::Result;
use crate::TypeConfig;

#[cfg_attr(any(test, feature = "__test_support"), automock)]
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

    /// Waits until the state machine has applied entries up to the target index.
    /// Returns error if timeout is reached before the target is applied.
    ///
    /// This is used to ensure linearizable reads: after leader confirms a log entry
    /// is committed (via quorum), we must wait for the state machine to apply it
    /// before reading to guarantee the read reflects all committed writes.
    async fn wait_applied(
        &self,
        target_index: u64,
        timeout: std::time::Duration,
    ) -> Result<()>;

    /// Reads values from the state machine for given keys
    /// Returns None if any key doesn't exist
    fn read_from_state_machine(
        &self,
        keys: Vec<bytes::Bytes>,
    ) -> Option<Vec<KvEntry>>;

    /// Receives the remaining chunks of a snapshot stream, ACKs each one, and assembles
    /// + decompresses them into a `PreparedSnapshot`.
    ///
    /// `first_chunk` must already have been validated by the caller (leader-term check,
    /// #436) — this method has **no Raft awareness** and must not gain any:
    /// it only tracks `(leader_term, leader_id)` from `first_chunk` as an internal
    /// stream-consistency anchor (every later chunk must match, or the leader changed
    /// mid-transfer). Must not mutate the state machine.
    async fn prepare_snapshot_stream(
        &self,
        first_chunk: d_engine_proto::server::storage::SnapshotChunk,
        remaining_chunks: tokio::sync::mpsc::Receiver<SnapshotChunk>,
        ack_tx: tokio::sync::mpsc::Sender<d_engine_proto::server::storage::SnapshotAck>,
        config: &crate::SnapshotConfig,
    ) -> Result<crate::PreparedSnapshot>;

    /// Determines if a snapshot should be created based on new commit data
    fn should_snapshot(
        &self,
        new_commit_data: NewCommitData,
    ) -> bool;

    /// Marks a local snapshot capture as started — errors if one is already in
    /// progress. Not RAII: the in-progress window now spans a round-trip through
    /// `StateMachineWorker` plus a background compress step, both `'static`; caller
    /// must call `end_local_snapshot_capture` exactly once when done, success or not.
    fn try_begin_local_snapshot_capture(&self) -> Result<()>;

    /// Clears the in-progress flag set by `try_begin_local_snapshot_capture`.
    fn end_local_snapshot_capture(&self);

    /// Compresses an already-captured snapshot (see `CapturedLocalSnapshot`) into its
    /// final on-disk form and runs retention cleanup. Doesn't touch live state —
    /// `captured.temp_dir` is already physically independent of it.
    async fn build_local_snapshot(
        &self,
        captured: crate::CapturedLocalSnapshot,
    ) -> Result<(SnapshotMetadata, std::path::PathBuf)>;

    /// Cleans up old snapshots before specified version
    async fn cleanup_snapshot(
        &self,
        before_version: u64,
        snapshot_dir: &std::path::Path,
        snapshot_dir_prefix: &str,
    ) -> crate::Result<()>;

    /// Retrieves metadata of the latest valid snapshot
    fn get_latest_snapshot_metadata(&self) -> Option<SnapshotMetadata>;

    /// Loads snapshot data as a stream of chunks
    async fn load_snapshot_data(
        &self,
        metadata: SnapshotMetadata,
        leader_term: u64,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>>;

    fn pending_range(&self) -> Option<std::ops::RangeInclusive<u64>>;
}

/// Destructive state-machine mutation — deliberately a **separate trait** from
/// `StateMachineHandler`, not two methods on it. `StateMachineHandler` stays importable
/// and callable from anywhere (role layer, CommitHandler, etc.); this trait is
/// module-restricted so only code inside `state_machine_handler` (i.e. `StateMachineWorker`)
/// can even name it, let alone call it. This is real compile-time enforcement, not convention.
#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait StateMachineWriterOps<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    /// Applies a batch of committed log entries to the state machine.
    /// Fire-and-forget by design — see `StateMachineCommand::ApplyEntries` doc comment.
    async fn apply_chunk(
        &self,
        chunk: Vec<d_engine_proto::common::Entry>,
    ) -> Result<Vec<ApplyResult>>;

    /// Installs an already-received, already-decompressed snapshot. Thin forward to
    /// `StateMachine::apply_snapshot_from_file` — must NOT re-run the
    /// stale/duplicate/boundary classification, that lives exactly once there.
    async fn install_prepared_snapshot(
        &self,
        metadata: SnapshotMetadata,
        dir: std::path::PathBuf,
    ) -> Result<crate::SnapshotApplyResult>;

    /// Captures a local snapshot: reads `last_applied` and exports/copies the state
    /// machine's current content to a temp location under `snapshots_dir`. Read-only
    /// against the state machine, but must run through the Worker (see
    /// `StateMachineCommand::CaptureLocalSnapshot`) so this read is ordered against
    /// concurrent `apply_chunk`/`apply_snapshot_from_file` calls.
    async fn capture_local_snapshot(&self) -> Result<crate::CapturedLocalSnapshot>;
}
