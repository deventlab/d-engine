//! Raft log replication implementation (Section 5.3)
//!
//! Handles core replication mechanics including:
//! - Leader log propagation
//! - Follower log consistency checks
//! - Conflict resolution algorithms
mod batch_buffer;
mod replication_handler;

pub use batch_buffer::*;
pub use replication_handler::*;

#[cfg(test)]
mod batch_buffer_test;
#[cfg(test)]
mod replication_handler_test;

// Client Request Extension Definition
// -----------------------------------------------------------------------------
use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
#[cfg(test)]
use mockall::automock;
use tonic::async_trait;
use tonic::Status;

use super::LeaderStateSnapshot;
use super::StateSnapshot;
use crate::alias::ROF;
use crate::proto::append_entries_response;
use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientCommand;
use crate::proto::ClientResponse;
use crate::proto::ConflictResult;
use crate::proto::Entry;
use crate::proto::LogId;
use crate::proto::SuccessResult;
use crate::AppendResults;
use crate::MaybeCloneOneshotSender;
use crate::Result;
use crate::TypeConfig;

/// Client request with response channel
#[derive(Debug)]
pub struct ClientRequestWithSignal {
    pub id: String,
    pub commands: Vec<ClientCommand>,
    pub sender: MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
}

/// AppendEntries response with possible state changes
#[derive(Debug)]
pub struct AppendResponseWithUpdates {
    pub response: AppendEntriesResponse,
    pub commit_index_update: Option<u64>, // Commit_index to be updated
}

/// Core replication protocol operations
#[cfg_attr(test, automock)]
#[async_trait]
pub trait ReplicationCore<T>: Send + Sync + 'static
where T: TypeConfig
{
    /// As Leader, send replications to peers.
    /// (combined regular heartbeat and client proposals)
    ///
    /// Each time handle_client_proposal_in_batch is called, perform peer
    /// synchronization check
    /// 1. Verify if any peer's next_id <= leader's commit_index
    /// 2. For non-synced peers meeting this condition: a. Retrieve all unsynced log entries b.
    ///    Buffer these entries before processing real entries
    /// 3. Ensure unsynced entries are prepended to the entries queue before actual entries get
    ///    pushed
    ///
    /// Leader state will be updated by LeaderState only(follows SRP).
    async fn handle_client_proposal_in_batch(
        &self,
        commands: Vec<ClientCommand>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        ctx: &crate::RaftContext<T>,
        peer_channels: Arc<crate::alias::POF<T>>,
        // replication_members: &[ChannelWithAddressAndRole],
        // raft_log: &Arc<ROF<T>>,
        // transport: &Arc<TROF<T>>,
        // raft: &RaftConfig,
        // retry: &RetryPolicies,
    ) -> Result<AppendResults>;

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
        new_entries: Vec<Entry>,
        leader_last_index_before_inserting_new_entries: u64,
        max_legacy_entries_per_peer: u64, //Maximum number of entries
        peer_next_indices: &HashMap<u32, u64>,
        raft_log: &Arc<ROF<T>>,
    ) -> DashMap<u32, Vec<Entry>>;

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

impl AppendEntriesResponse {
    /// Generate a successful response (full success)
    pub fn success(
        node_id: u32,
        term: u64,
        last_match: Option<LogId>,
    ) -> Self {
        Self {
            node_id,
            term,
            result: Some(append_entries_response::Result::Success(SuccessResult { last_match })),
        }
    }

    /// Generate conflict response (with conflict details)
    pub fn conflict(
        node_id: u32,
        term: u64,
        conflict_term: Option<u64>,
        conflict_index: Option<u64>,
    ) -> Self {
        Self {
            node_id,
            term,
            result: Some(append_entries_response::Result::Conflict(ConflictResult {
                conflict_term,
                conflict_index,
            })),
        }
    }

    /// Generate a conflict response (Higher term found)
    pub fn higher_term(
        node_id: u32,
        term: u64,
    ) -> Self {
        Self {
            node_id,
            term,
            result: Some(append_entries_response::Result::HigherTerm(term)),
        }
    }

    /// Check if it is a success response
    pub fn is_success(&self) -> bool {
        matches!(&self.result, Some(append_entries_response::Result::Success(_)))
    }

    /// Check if it is a conflict response
    pub fn is_conflict(&self) -> bool {
        matches!(&self.result, Some(append_entries_response::Result::Conflict(_conflict)))
    }

    /// Check if it is a response of a higher Term
    pub fn is_higher_term(&self) -> bool {
        matches!(&self.result, Some(append_entries_response::Result::HigherTerm(_)))
    }
}
