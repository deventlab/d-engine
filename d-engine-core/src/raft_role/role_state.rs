use async_trait::async_trait;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
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
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::Result;
use crate::RoleEvent;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::event::ClientCmd;
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

    /// Update per-peer snapshot push backoff state and emit an alert when persistent
    /// failures exceed the configured threshold. No-op for non-leader roles.
    fn handle_snapshot_push_completed(
        &mut self,
        _peer_id: u32,
        _success: bool,
        _policy: &crate::InstallSnapshotBackoffPolicy,
        _node_id: u32,
    ) {
        // Default: no-op for non-leader roles
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

    /// Push single client command directly to role's internal buffer (zero-copy).
    ///
    /// For Leader: push to batch_buffer (writes) or read_buffer (reads)
    /// For non-Leader:
    ///   - Writes: immediately reject with NOT_LEADER error
    ///   - Eventual reads: serve locally from state machine
    ///   - Linear/Lease reads: immediately reject with NOT_LEADER error
    fn push_client_cmd(
        &mut self,
        cmd: ClientCmd,
        ctx: &RaftContext<Self::T>,
    ) {
        use crate::ReadConsistencyPolicy as ServerReadConsistencyPolicy;
        use d_engine_proto::client::ReadConsistencyPolicy as ClientReadConsistencyPolicy;

        match cmd {
            ClientCmd::Propose(_, sender) => {
                // Writes always require leader - reject immediately
                let status = tonic::Status::failed_precondition("Not leader");
                let _ = sender.send(Err(status));
            }
            ClientCmd::Read(req, sender) => {
                // Determine effective read policy, mirroring leader_state logic:
                // 1. If client specified a policy AND server allows override, use client policy.
                // 2. Otherwise (no policy, or override disabled), use server default.
                let effective_policy = if req.has_consistency_policy()
                    && ctx.node_config().raft.read_consistency.allow_client_override
                {
                    match req.consistency_policy() {
                        ClientReadConsistencyPolicy::EventualConsistency => {
                            ServerReadConsistencyPolicy::EventualConsistency
                        }
                        _ => {
                            // Linear/Lease requires leader
                            let _ =
                                sender.send(Err(tonic::Status::failed_precondition("Not leader")));
                            return;
                        }
                    }
                } else {
                    // No client policy, or client override not allowed — use server default
                    ctx.node_config().raft.read_consistency.default_policy.clone()
                };

                match effective_policy {
                    ServerReadConsistencyPolicy::EventualConsistency => {
                        self.process_eventual_read_local(req, sender, ctx);
                    }
                    _ => {
                        // Linear/Lease requires leader
                        let _ = sender.send(Err(tonic::Status::failed_precondition("Not leader")));
                    }
                }
            }
        }
    }

    /// Process eventual consistency read locally (available on all nodes)
    ///
    /// Eventual consistency reads can be served by any node (leader, follower, candidate, learner)
    /// directly from the local state machine without additional consistency checks.
    /// May return stale data but provides best read performance and availability.
    fn process_eventual_read_local(
        &self,
        req: d_engine_proto::client::ClientReadRequest,
        sender: crate::MaybeCloneOneshotSender<
            std::result::Result<d_engine_proto::client::ClientResponse, tonic::Status>,
        >,
        ctx: &RaftContext<Self::T>,
    ) {
        use d_engine_proto::client::ClientResponse;

        // Read directly from local state machine without any consistency checks
        let results = ctx
            .state_machine_handler()
            .read_from_state_machine(req.keys)
            .unwrap_or_default();

        let response = ClientResponse::read_results(results);
        let _ = sender.send(Ok(response));
    }

    /// Flush command buffers if size or timeout thresholds are reached.
    ///
    /// Checks both write and read buffers using BatchBuffer::should_flush().
    /// For Leader: processes batches if FlushReason::SizeThreshold or FlushReason::Timeout
    /// For non-Leader: no-op (buffers are empty)
    async fn flush_cmd_buffers(
        &mut self,
        _ctx: &RaftContext<Self::T>,
        _role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        // Default implementation for non-leader: nothing to flush
        Ok(())
    }

    /// Handle ApplyCompleted: state machine has applied entries up to `last_index`.
    /// Leader: sends client responses + serves pending linearizable reads + checks snapshot.
    /// Follower/Learner: checks snapshot trigger.
    /// Default: no-op for Candidate (transient state; snapshot deferred to next stable role).
    async fn handle_apply_completed(
        &mut self,
        _last_index: u64,
        _results: Vec<crate::ApplyResult>,
        _ctx: &RaftContext<Self::T>,
        _role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        Ok(())
    }

    /// Handle LogFlushed(durable) event: entries up to `durable` are now crash-safe.
    /// Leader: recalculates commit_index (uses durable_index in quorum calculation).
    /// Default: no-op for Candidate/Follower/Learner (ACK already sent on memory write).
    async fn handle_log_flushed(
        &mut self,
        _durable: u64,
        _ctx: &RaftContext<Self::T>,
        _role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) {
        // Candidate: no-op
    }

    /// Handle AppendEntries result from a per-follower ReplicationWorker.
    /// Leader: updates match_index, recalculates commit, drains pending_client_writes.
    /// Default: no-op for all non-leader roles (stale results arriving after step-down).
    async fn handle_append_result(
        &mut self,
        _follower_id: u32,
        _result: crate::Result<d_engine_proto::server::replication::AppendEntriesResponse>,
        _ctx: &RaftContext<Self::T>,
        _role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> crate::Result<()> {
        // Non-leader: stale result arrived after step-down — ignore safely.
        Ok(())
    }

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
                if let Some(commit) = commit_index_update
                    && let Err(e) = self.update_commit_index_with_signal(
                        state_snapshot.role,
                        state_snapshot.current_term,
                        commit,
                        &role_tx,
                    )
                {
                    error!(
                        "update_commit_index_with_signal,commit={}, error: {:?}",
                        commit, e
                    );
                    return Err(e);
                }
                debug!("AppendEntriesResponse: {:?}", response);

                // MemFirst: ACK immediately after memory write. IO thread fsyncs async.
                // Safety: quorum uses last_entry_id (in-memory); crash safety is guaranteed by
                // majority replication, not per-follower durability.
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
        // No-op for non-leader roles (only Leader has read buffer to drain)
        Err(MembershipError::NotLeader.into())
    }
}

/// Send a RaftEvent back into the role event loop for reprocessing.
pub(super) fn send_replay_raft_event(
    role_tx: &mpsc::UnboundedSender<RoleEvent>,
    raft_event: RaftEvent,
) -> Result<()> {
    role_tx.send(RoleEvent::ReprocessEvent(Box::new(raft_event))).map_err(|e| {
        let error_str = format!("{e:?}");
        error!("Failed to send: {}", error_str);
        NetworkError::SingalSendFailed(error_str).into()
    })
}

/// Check snapshot condition and trigger if met. Used by all role states after SM apply.
///
/// Per Raft §7: each server takes snapshots independently. Called from ApplyCompleted
/// handlers in leader, follower, learner, and candidate states.
pub(super) fn check_and_trigger_snapshot<T: TypeConfig>(
    last_index: u64,
    role: i32,
    current_term: u64,
    ctx: &RaftContext<T>,
    role_tx: &mpsc::UnboundedSender<RoleEvent>,
) -> Result<()> {
    if ctx.node_config.raft.snapshot.enable
        && ctx.state_machine_handler().should_snapshot(NewCommitData {
            new_commit_index: last_index,
            role,
            current_term,
        })
    {
        send_replay_raft_event(role_tx, RaftEvent::CreateSnapshotEvent)?;
    }
    Ok(())
}
