//! Raft log replication implementation (Section 5.3)
//!
//! Handles core replication mechanics including:
//! - Leader log propagation
//! - Follower log consistency checks
//! - Conflict resolution algorithms
mod replication_handler;
pub use replication_handler::*;

#[cfg(test)]
pub mod replication_handler_test;

// Client Request Extension Definition
// -----------------------------------------------------------------------------
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::ConflictResult;
use d_engine_proto::server::replication::SuccessResult;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;
use tonic::Status;

use super::LeaderStateSnapshot;
use super::StateSnapshot;
use crate::MaybeCloneOneshotSender;
use crate::Result;
use crate::TypeConfig;
use crate::alias::ROF;

/// Request with response channel that can handle all Raft payload types
#[derive(Debug)]
pub struct RaftRequestWithSignal {
    #[allow(unused)]
    pub id: String,
    pub payloads: Vec<EntryPayload>,
    /// Multiple senders for merged requests (1 sender per payload, matched by index)
    /// Invariant: senders.len() == payloads.len()
    pub senders: Vec<MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>>,

    /// Does this request need to wait for StateMachine's ApplyCompleted event?
    ///
    /// - `true`:  Command payload → must wait for state machine apply
    /// - `false`: Noop/Config payload → respond immediately after commit
    pub wait_for_apply_event: bool,
}

/// AppendEntries response with possible state changes
#[derive(Debug)]
pub struct AppendResponseWithUpdates {
    pub response: AppendEntriesResponse,
    pub commit_index_update: Option<u64>, // Commit_index to be updated
}

/// Result of `prepare_batch_requests`: peers routed to AppendEntries vs snapshot transfer.
#[derive(Debug, Default)]
pub struct PrepareResult {
    /// Per-peer AppendEntries requests for peers whose log is in range.
    pub append_requests: Vec<(u32, AppendEntriesRequest)>,
    /// Peer IDs whose `next_index` is below the leader's purge boundary;
    /// the leader must send a snapshot instead of AppendEntries.
    pub snapshot_targets: Vec<u32>,
}

/// Core replication protocol operations
#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait ReplicationCore<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    /// Prepare per-peer AppendEntries requests without sending them.
    ///
    /// Performs two operations that MUST remain in the Raft loop (serial, single-threaded):
    ///   1. Write new log entries to the local raft log (`generate_new_entries`)
    ///   2. Build per-peer request payloads (`prepare_peer_entries` + `build_append_request`)
    ///      Peers whose `next_index < first_entry_id` (below purge boundary) are routed to
    ///      `PrepareResult.snapshot_targets` instead of `append_requests`.
    ///
    /// The caller (leader Raft loop) then fires each request to a per-follower
    /// `ReplicationWorker` task and returns immediately — no blocking `.await`.
    ///
    /// # Returns
    /// - `Ok(PrepareResult)` — append requests per in-range peer + snapshot targets.
    ///   Empty `append_requests` when there are no replication targets or all need snapshots.
    /// - `Err` for log write failures or ID allocation errors
    async fn prepare_batch_requests(
        &self,
        entry_payloads: Vec<EntryPayload>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        cluster_metadata: &crate::raft_role::ClusterMetadata,
        ctx: &crate::RaftContext<T>,
    ) -> Result<PrepareResult>;

    /// Handles successful AppendEntries responses
    ///
    /// Updates peer match/next indices according to:
    /// - Last matched log index
    /// - Current leader term
    fn handle_success_response(
        &self,
        peer_id: u32,
        peer_term: u64,
        success_result: SuccessResult,
        leader_term: u64,
    ) -> Result<crate::PeerUpdate>;

    /// Resolves log conflicts from follower responses
    ///
    /// Implements conflict backtracking optimization (Section 5.3)
    fn handle_conflict_response(
        &self,
        peer_id: u32,
        conflict_result: ConflictResult,
        raft_log: &Arc<ROF<T>>,
        current_next_index: u64,
    ) -> Result<crate::PeerUpdate>;

    /// Determines follower commit index advancement
    ///
    /// Applies Leader's commit index according to:
    /// - min(leader_commit, last_local_log_index)
    fn if_update_commit_index_as_follower(
        my_commit_index: u64,
        last_raft_log_id: u64,
        leader_commit_index: u64,
    ) -> Option<u64>;

    /// Gathers legacy logs for lagging peers
    ///
    /// Performs log segmentation based on:
    /// - Peer's next_index
    /// - Max allowed historical entries
    fn retrieve_to_be_synced_logs_for_peers(
        &self,
        new_entries: &[Entry],
        leader_last_index_before_inserting_new_entries: u64,
        max_legacy_entries_per_peer: u64,
        peer_next_indices: &HashMap<u32, u64>,
        raft_log: &Arc<ROF<T>>,
    ) -> HashMap<u32, Vec<Entry>>;

    /// Handles an incoming AppendEntries RPC request (called by ALL ROLES)
    ///
    /// Core responsibilities:
    /// 1. Term validation and comparison (RFC §5.1)
    /// 2. Log consistency checking (prev_log_index/term)
    /// 3. Entry appending with conflict resolution (RFC §5.3)
    /// 4. Commit index advancement (RFC §5.3)
    ///
    /// # Critical Architecture Constraints
    /// 1. ROLE AGNOSTIC - This method contains no role-specific logic. It simply:
    ///    - Validates the RPC against local state
    ///    - Returns required state updates
    /// 2. STATE CHANGE ISOLATION - Never directly modifies:
    ///    - Current role (Leader/Follower/etc)
    ///    - Persistent state (term/votedFor)
    ///    - Volatile leader state (nextIndex/matchIndex)
    /// 3. CALLER MUST:
    ///    - Check `response.term_update` for term conflicts
    ///    - If higher term exists, transition to Follower
    ///    - Apply other state updates via role_tx
    async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
        state_snapshot: &StateSnapshot,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<AppendResponseWithUpdates>;

    /// Validates an incoming AppendEntries RPC from a Leader against Raft protocol rules.
    ///
    /// This function implements the **log consistency checks** defined in Raft paper Section 5.3.
    /// It determines whether to accept the Leader's log entries by verifying:
    /// 1. Term freshness
    /// 2. Virtual log (prev_log_index=0) handling
    /// 3. Previous log entry consistency
    ///
    /// # Raft Protocol Rules Enforced
    /// 1. **Term Check** (Raft Paper 5.1):
    ///    - Reject requests with stale terms to prevent partitioned Leaders
    /// 2. **Virtual Log Handling** (Implementation-Specific):
    ///    - Special case for empty logs (prev_log_index=0 && prev_log_term=0)
    /// 3. **Log Matching** (Raft Paper 5.3):
    ///    - Ensure Leader's prev_log_index/term matches Follower's log
    ///
    /// # Parameters
    /// - `my_term`: Current node's term
    /// - `request`: Leader's AppendEntries RPC
    /// - `raft_log`: Reference to node's log store
    ///
    /// # Return
    /// - [`AppendEntriesResponse::success`] if validation passes
    /// - [`AppendEntriesResponse::higher_term`] if Leader's term is stale
    /// - [`AppendEntriesResponse::conflict`] with debugging info for log inconsistencies
    fn check_append_entries_request_is_legal(
        &self,
        my_term: u64,
        request: &AppendEntriesRequest,
        raft_log: &Arc<ROF<T>>,
    ) -> AppendEntriesResponse;
}
