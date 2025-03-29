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
use crate::{
    alias::{ROF, TROF},
    grpc::rpc_service::{AppendEntriesRequest, ClientCommand, ClientResponse, Entry},
    AppendResults, ChannelWithAddressAndRole, MaybeCloneOneshotSender, RaftConfig, Result,
    RetryPolicies, TypeConfig,
};
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tonic::{async_trait, Status};

#[cfg(test)]
use mockall::automock;

use super::{LeaderStateSnapshot, StateSnapshot};
#[derive(Debug)]
pub struct ClientRequestWithSignal {
    pub id: String,
    pub commands: Vec<ClientCommand>,
    pub sender: MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
}

#[derive(Debug)]
pub struct LeaderStateUpdate {
    /// (peer_id -> (next_index, match_index))
    pub peer_index_updates: HashMap<u32, (u64, u64)>,
    // New commit index, if thre is
    pub new_commit_index: Option<u64>,
}

#[derive(Debug)]
pub struct AppendResponseWithUpdates {
    pub success: bool,        // RPC success or failure
    pub current_term: u64,    // Current node term (may have been updated)
    pub last_matched_id: u64, // Last matched log index
    // pub term_update: Option<u64>,         // Term to be updated (if a higher term is found)
    pub commit_index_update: Option<u64>, // Commit_index to be updated
}

// ------------------------------------------------------------------------------
// Trait Definition
#[cfg_attr(test, automock)]
#[async_trait]
pub trait ReplicationCore<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    async fn handle_client_proposal_in_batch(
        &self,
        commands: Vec<ClientCommand>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        replication_members: &Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        raft: &RaftConfig,
        retry: &RetryPolicies,
    ) -> Result<AppendResults>;

    fn if_update_commit_index_as_follower(
        my_commit_index: u64,
        last_raft_log_id: u64,
        leader_commit_index: u64,
    ) -> Option<u64>;

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
    ///
    async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
        state_snapshot: &StateSnapshot,
        last_applied: u64,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<AppendResponseWithUpdates>;

    // async fn upon_receive_append_request(
    //     &self,
    //     req: &AppendEntriesRequest,
    //     state_snapshot: &StateSnapshot,
    //     last_applied: u64,
    //     raft_log: &Arc<ROF<T>>,
    // ) -> AppendResponseWithUpdates;
}
