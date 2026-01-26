use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::warn;

use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use crate::AppendResponseWithUpdates;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::NewCommitData;
use crate::QuorumVerificationResult;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::Result;
use crate::RoleEvent;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::scoped_timer::ScopedTimer;
use crate::utils::cluster::error;

#[async_trait]
pub trait RaftRoleState: Send + Sync + 'static {
    type T: TypeConfig;

    //--- For sharing state behaviors
    fn shared_state(&self) -> &SharedState;
    fn shared_state_mut(&mut self) -> &mut SharedState;
    fn node_id(&self) -> u32 {
        self.shared_state().node_id
    }
    // fn role(&self) -> i32;

    // Leader states
    #[allow(dead_code)]
    fn next_index(
        &self,
        _node_id: u32,
    ) -> Option<u64> {
        warn!("next_index NotLeader error");
        None
    }
    fn update_next_index(
        &mut self,
        _node_id: u32,
        _new_next_id: u64,
    ) -> Result<()> {
        warn!("update_next_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    fn match_index(
        &self,
        _node_id: u32,
    ) -> Option<u64> {
        warn!("match_index NotLeader error");
        None
    }
    fn update_match_index(
        &mut self,
        _node_id: u32,
        _new_match_id: u64,
    ) -> Result<()> {
        warn!("update_match_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }
    fn init_peers_next_index_and_match_index(
        &mut self,
        _last_log_id: u64,
        _node_ids: Vec<u32>,
    ) -> Result<()> {
        warn!("init_peers_next_index_and_match_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    /// Initialize cluster metadata cache (only relevant for Leader)
    async fn init_cluster_metadata(
        &mut self,
        _membership: &std::sync::Arc<<Self::T as crate::TypeConfig>::M>,
    ) -> Result<()> {
        // Default: no-op for non-leader roles
        Ok(())
    }
    #[allow(dead_code)]
    fn noop_log_id(&self) -> Result<Option<u64>> {
        warn!("noop_log_id NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    /// Called when no-op entry is committed after becoming leader.
    /// Only relevant for Leader role to track linearizable read optimization.
    fn on_noop_committed(
        &mut self,
        _ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        // Default: no-op for non-leader roles
        Ok(())
    }

    async fn verify_internal_quorum(
        &mut self,
        _payloads: Vec<EntryPayload>,
        _bypass_queue: bool,
        _ctx: &RaftContext<Self::T>,
        _role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<QuorumVerificationResult> {
        warn!("verify_internal_quorum NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    /// Immidiatelly verifies leadership status using persistent retry until timeout.
    ///
    /// This function is designed for critical operations like configuration changes
    /// that must eventually succeed. It implements:
    ///   - Infinite retries with exponential backoff
    ///   - Jitter randomization to prevent synchronization
    ///   - Termination only on success, leadership loss, or global timeout
    ///
    /// # Parameters
    /// - `payloads`: Log entries to verify
    /// - `bypass_queue`: Whether to skip request queues for direct transmission
    /// - `ctx`: Raft execution context
    /// - `role_tx`: Channel for role transition events
    ///
    /// # Returns
    /// - `Ok(true)`: Quorum verification succeeded
    /// - `Ok(false)`: Leadership definitively lost during verification
    /// - `Err(_)`: Global timeout exceeded or critical failure occurred
    async fn verify_leadership_persistent(
        &mut self,
        _payloads: Vec<EntryPayload>,
        _bypass_queue: bool,
        _ctx: &RaftContext<Self::T>,
        _role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        warn!("verify_leadership NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    async fn join_cluster(
        &self,
        _ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        warn!("join_cluster NotLearner error");
        Err(MembershipError::NotLearner.into())
    }

    async fn fetch_initial_snapshot(
        &self,
        _ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        warn!("fetch_initial_snapshot NotLearner error");
        Err(MembershipError::NotLearner.into())
    }

    #[allow(dead_code)]
    fn is_follower(&self) -> bool {
        false
    }
    #[allow(dead_code)]
    fn is_candidate(&self) -> bool {
        false
    }
    #[allow(dead_code)]
    fn is_leader(&self) -> bool {
        false
    }

    fn is_learner(&self) -> bool {
        false
    }

    fn become_leader(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_leader Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_candidate(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_candidate Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_follower(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_follower Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_learner(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_learner Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }

    //--- Shared States
    fn current_term(&self) -> u64 {
        self.shared_state().current_term()
    }
    fn update_current_term(
        &mut self,
        term: u64,
    ) {
        self.shared_state_mut().update_current_term(term)
    }
    fn increase_current_term(&mut self) {
        self.shared_state_mut().increase_current_term()
    }
    fn commit_index(&self) -> u64 {
        self.shared_state().commit_index
    }

    fn update_commit_index(
        &mut self,
        new_commit_index: u64,
    ) -> Result<()> {
        if self.commit_index() != new_commit_index {
            debug!("update_commit_index to: {:?}", new_commit_index);
            self.shared_state_mut().commit_index = new_commit_index;
        }
        Ok(())
    }

    fn update_commit_index_with_signal(
        &mut self,
        role: i32,
        current_term: u64,
        new_commit_index: u64,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        if let Err(e) = self.update_commit_index(new_commit_index) {
            error!("Follower::update_commit_index: {:?}", e);
            return Err(e);
        }
        debug!(
            "[Node-{}] update_commit_index_with_signal, new_commit_index: {:?}",
            self.node_id(),
            new_commit_index
        );

        role_tx
            .send(RoleEvent::NotifyNewCommitIndex(NewCommitData {
                new_commit_index,
                role,
                current_term,
            }))
            .map_err(|e| {
                let error_str = format!("{e:?}");
                error!("Failed to send NotifyNewCommitIndex: {}", error_str);
                NetworkError::SingalSendFailed(error_str)
            })?;

        Ok(())
    }

    fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.shared_state().voted_for()
    }
    fn reset_voted_for(&mut self) -> Result<()> {
        self.shared_state_mut().reset_voted_for()
    }
    fn update_voted_for(
        &mut self,
        voted_for: VotedFor,
    ) -> Result<bool> {
        self.shared_state_mut().update_voted_for(voted_for)
    }

    //--- Timer related ---
    fn next_deadline(&self) -> Instant;
    fn is_timer_expired(&self) -> bool;

    fn reset_timer(&mut self);

    async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        event_tx: &mpsc::Sender<RaftEvent>,
        ctx: &RaftContext<Self::T>,
    ) -> Result<()>;

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        ctx: &RaftContext<Self::T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()>;

    /// Create NOT_LEADER response with leader metadata for client redirection
    ///
    /// This method queries the cluster membership to get the current leader's
    /// ID and address, then returns a ClientResponse with ErrorMetadata populated.
    /// If no leader information is available, returns a basic NOT_LEADER error.
    ///
    /// Default implementation can be overridden by specific role states if needed.
    ///
    /// # Returns
    /// ClientResponse with NOT_LEADER error code and optional leader metadata
    async fn create_not_leader_response(
        &self,
        ctx: &RaftContext<Self::T>,
    ) -> ClientResponse {
        let leader_id = self.shared_state().current_leader();

        if let Some(lid) = leader_id {
            // Get leader address from membership
            let members = ctx.membership().members().await;
            let leader_node = members.iter().find(|n| n.id == lid);

            if let Some(leader) = leader_node {
                return ClientResponse::not_leader(
                    Some(lid.to_string()),
                    Some(leader.address.clone()),
                );
            }
        }

        // No leader info available, return basic NOT_LEADER error
        ClientResponse::not_leader(None, None)
    }

    /// When a Follower receives an AppendEntries request, it performs the following logic (as
    /// described in Section 5.1 of the Raft paper):
    ///
    /// - If the term T in the request is greater than
    /// the Followers current term, the Follower updates its term and reverts to the Follower state.
    ///
    /// - If the term T in the request is less than the Followers current term, the Follower
    ///   responds with a HigherTerm reply
    async fn handle_append_entries_request_workflow(
        &mut self,
        append_entries_request: AppendEntriesRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>,
        ctx: &RaftContext<Self::T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        state_snapshot: &StateSnapshot,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("handle_append_entries_request_workflow");
        debug!(
            "handle_raft_event::RaftEvent::AppendEntries: {:?}",
            &append_entries_request
        );
        self.reset_timer();

        // Init response with success is false
        let raft_log_last_index = ctx.storage.raft_log.last_entry_id();

        let my_term = self.current_term();
        if my_term > append_entries_request.term {
            let response = AppendEntriesResponse::higher_term(self.node_id(), my_term);
            debug!("AppendEntriesResponse: {:?}", response);

            sender.send(Ok(response)).map_err(|e| {
                let error_str = format!("{e:?}");
                error!("Failed to send: {}", error_str);
                NetworkError::SingalSendFailed(error_str)
            })?;
            return Ok(());
        }

        // Important to confirm heartbeat from Leader immediatelly
        let new_leader_id = append_entries_request.leader_id;
        let request_term = append_entries_request.term;

        // CRITICAL: Check is_new_leader BEFORE setting current_leader
        // This ensures node restart scenario works correctly:
        // - After restart, current_leader is None (memory cleared)
        // - update_voted_for() detects None and returns true
        // - Triggers LeaderDiscovered event for wait_ready()
        // Mark vote as committed (follower confirms leader)
        // Returns true only on state transition (committed: false->true or leader/term change)
        let is_new_leader = self.update_voted_for(VotedFor {
            voted_for_id: new_leader_id,
            voted_for_term: request_term,
            committed: true,
        })?;

        // Keep syncing leader_id (hot-path: ~5ns atomic store vs ~50ns RwLock)
        self.shared_state().set_current_leader(new_leader_id);

        // Trigger leader discovery notification only on state transition
        // Event-driven: avoids redundant notifications on every heartbeat
        // Performance: ~9ns check overhead, saves ~100ns redundant sends
        if is_new_leader {
            role_tx.send(RoleEvent::LeaderDiscovered(new_leader_id, request_term)).map_err(
                |e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send LeaderDiscovered: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                },
            )?;
        }

        if my_term < request_term {
            self.update_current_term(request_term);
        }

        // My term might be updated, has to fetch it again
        let my_term = self.current_term();

        // Handle replication request
        match ctx
            .replication_handler()
            .handle_append_entries(append_entries_request, state_snapshot, ctx.raft_log())
            .await
        {
            Ok(AppendResponseWithUpdates {
                response,
                commit_index_update,
            }) => {
                if let Some(commit) = commit_index_update {
                    if let Err(e) = self.update_commit_index_with_signal(
                        state_snapshot.role,
                        state_snapshot.current_term,
                        commit,
                        &role_tx,
                    ) {
                        error!(
                            "update_commit_index_with_signal,commit={}, error: {:?}",
                            commit, e
                        );
                        return Err(e);
                    }
                }
                debug!("AppendEntriesResponse: {:?}", response);

                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }
            Err(e) => {
                // Conservatively fallback to a safe position, forcing the leader to retry or
                // trigger a snapshot. Return a Conflict response (conflict index =
                // current log length + 1)
                error!(
                    "Replication failed. Conservatively fallback to a safe position, forcing the leader to retry"
                );
                let response = AppendEntriesResponse::conflict(
                    self.node_id(),
                    my_term,
                    None,
                    Some(raft_log_last_index + 1),
                );
                debug!("AppendEntriesResponse: {:?}", response);

                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
                error("handle_raft_event", &e);
                return Err(e);
            }
        }
        return Ok(());
    }

    fn drain_read_buffer(&mut self) -> Result<()> {
        warn!("update_match_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }
}
