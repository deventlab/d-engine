use super::candidate_state::CandidateState;
use super::role_state::RaftRoleState;
use super::LeaderStateSnapshot;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::LEADER;
use crate::alias::POF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::client_command_to_entry_payloads;
use crate::cluster::is_majority;
use crate::proto::client::ClientResponse;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::JoinRequest;
use crate::proto::cluster::JoinResponse;
use crate::proto::cluster::NodeStatus;
use crate::proto::common::membership_change::Change;
use crate::proto::common::AddNode;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::proto::election::VoteResponse;
use crate::proto::election::VotedFor;
use crate::proto::error::ErrorCode;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotMetadata;
use crate::utils::cluster::error;
use crate::AppendResults;
use crate::BatchBuffer;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::PeerChannels;
use crate::PeerUpdate;
use crate::PurgeExecutor;
use crate::QuorumVerificationResult;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::RaftOneshot;
use crate::RaftRequestWithSignal;
use crate::ReplicationConfig;
use crate::ReplicationCore;
use crate::ReplicationError;
use crate::ReplicationTimer;
use crate::Result;
use crate::RoleEvent;
use crate::StateMachine;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::Transport;
use crate::TypeConfig;
use crate::API_SLO;
use crate::LEARNER;
use autometrics::autometrics;
use nanoid::nanoid;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio::time::Instant;
use tonic::async_trait;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::warn;

/// Leader node's state in Raft consensus algorithm.
///
/// This structure maintains all state that should be persisted on leader crashes,
/// including replication progress tracking and log compaction management.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
pub struct LeaderState<T: TypeConfig> {
    // -- Core Raft Leader State --
    /// Shared cluster state with lock protection
    pub shared_state: SharedState,

    /// === Volatile State ===
    /// For each server (node_id), index of the next log entry to send to that server
    ///
    /// Raft Paper: §5.3 Figure 2 (nextIndex)
    pub(super) next_index: HashMap<u32, u64>,

    /// === Volatile State ===
    /// For each server (node_id), index of highest log entry known to be replicated
    ///
    /// Raft Paper: §5.3 Figure 2 (matchIndex)
    pub(super) match_index: HashMap<u32, u64>,

    /// === Volatile State ===
    /// Temporary storage for no-op entry log ID during leader initialization
    pub(super) noop_log_id: Option<u64>,

    // -- Log Compaction & Purge --
    /// === Volatile State ===
    /// The upper bound (exclusive) of log entries scheduled for asynchronous physical deletion.
    ///
    /// This value is set immediately after a new snapshot is successfully created.
    /// It represents the next log position that will trigger compaction.
    ///
    /// The actual log purge is performed by a background task, which may be delayed
    /// due to resource constraints or retry mechanisms.
    pub(super) scheduled_purge_upto: Option<LogId>,

    /// === Persistent State (MUST be on disk) ===
    /// The last log position that has been **physically removed** from stable storage.
    ///
    /// This value is atomically updated when:
    /// 1. A new snapshot is persisted (marking logs up to `last_included_index` as purgeable)
    /// 2. The background purge task completes successfully
    ///
    /// Raft safety invariant:
    /// Any log entry with index ≤ `last_purged_index` is guaranteed to be
    /// reflected in the latest snapshot.
    pub(super) last_purged_index: Option<LogId>,

    /// === Volatile State ===
    /// Peer purge progress tracking for flow control
    ///
    /// Key: Peer node ID  
    /// Value: Last confirmed purge index from peer
    pub(super) peer_purge_progress: HashMap<u32, u64>,

    // -- Request Processing --
    /// Batched proposal buffer for client requests
    ///
    /// Accumulates requests until either:
    /// 1. Batch reaches configured size limit
    /// 2. Explicit flush is triggered
    batch_buffer: Box<BatchBuffer<RaftRequestWithSignal>>,

    // -- Timing & Scheduling --
    /// Replication heartbeat timer manager
    ///
    /// Handles:
    /// - Heartbeat interval tracking
    /// - Election timeout prevention
    /// - Batch proposal flushing
    timer: Box<ReplicationTimer>,

    // -- Cluster Configuration --
    /// Cached Raft node configuration (shared reference)
    ///
    /// This includes:
    /// - Cluster membership
    /// - Timeout parameters
    /// - Performance tuning knobs
    pub(super) node_config: Arc<RaftNodeConfig>,

    // -- Type System Marker --
    /// Phantom data for type parameter anchoring
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for LeaderState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    ///Overwrite default behavior.
    /// As leader, I should not receive commit index,
    ///     which is lower than my current one
    #[tracing::instrument]
    #[autometrics(objective = API_SLO)]
    fn update_commit_index(
        &mut self,
        new_commit_index: u64,
    ) -> Result<()> {
        if self.commit_index() < new_commit_index {
            debug!("update_commit_index to: {:?}", new_commit_index);
            self.shared_state.commit_index = new_commit_index;
        } else {
            warn!(
                "Illegal operation, might be a bug! I am Leader old_commit_index({}) >= new_commit_index:({})",
                self.commit_index(),
                new_commit_index
            )
        }
        Ok(())
    }

    /// As Leader should not vote any more
    fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.shared_state().voted_for()
    }

    /// As Leader might also be able to vote ,
    ///     if new legal Leader found
    fn update_voted_for(
        &mut self,
        voted_for: VotedFor,
    ) -> Result<()> {
        self.shared_state_mut().update_voted_for(voted_for)
    }

    #[autometrics(objective = API_SLO)]
    fn next_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        Some(if let Some(n) = self.next_index.get(&node_id) {
            *n
        } else {
            1
        })
    }

    fn update_next_index(
        &mut self,
        node_id: u32,
        new_next_id: u64,
    ) -> Result<()> {
        debug!("update_next_index({}) to {}", node_id, new_next_id);
        self.next_index.insert(node_id, new_next_id);
        Ok(())
    }

    fn update_match_index(
        &mut self,
        node_id: u32,
        new_match_id: u64,
    ) -> Result<()> {
        self.match_index.insert(node_id, new_match_id);
        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    fn match_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        self.match_index.get(&node_id).copied()
    }

    #[autometrics(objective = API_SLO)]
    fn init_peers_next_index_and_match_index(
        &mut self,
        last_entry_id: u64,
        peer_ids: Vec<u32>,
    ) -> Result<()> {
        for peer_id in peer_ids {
            debug!("init leader state for peer_id: {:?}", peer_id);
            let new_next_id = last_entry_id + 1;
            self.update_next_index(peer_id, new_next_id)?;
            self.update_match_index(peer_id, 0)?;
        }
        Ok(())
    }
    fn noop_log_id(&self) -> Result<Option<u64>> {
        Ok(self.noop_log_id)
    }

    /// Check leadership quorum verification Immidiatelly
    ///
    /// - Bypasses all queues with direct RPC transmission
    /// - Enforces synchronous quorum validation
    /// - Guarantees real-time network visibility
    ///
    /// # Returns
    /// - `Ok(true)`: Real-time majority quorum confirmed
    /// - `Ok(false)`: Failed to verify immediate quorum
    /// - `Err(_)`: Network or processing failure during real-time verification
    async fn verify_internal_quorum_with_retry(
        &mut self,
        payloads: Vec<EntryPayload>,
        bypass_queue: bool,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        let retry_policy = ctx.node_config.retry.internal_quorum;
        let max_retries = retry_policy.max_retries;
        let initial_delay = Duration::from_millis(ctx.node_config.retry.internal_quorum.base_delay_ms);
        let max_delay = Duration::from_millis(ctx.node_config.retry.internal_quorum.max_delay_ms);

        let mut current_delay = initial_delay;
        let mut attempts = 0;

        loop {
            match self
                .verify_internal_quorum(payloads.clone(), bypass_queue, ctx, peer_channels.clone(), role_tx)
                .await
            {
                Ok(QuorumVerificationResult::Success) => return Ok(true),
                Ok(QuorumVerificationResult::LeadershipLost) => return Ok(false),
                Ok(QuorumVerificationResult::RetryRequired) => {
                    debug!(%attempts, "verify_internal_quorum");
                    if attempts >= max_retries {
                        return Err(NetworkError::TaskBackoffFailed("Max retries exceeded".to_string()).into());
                    }

                    current_delay = current_delay.checked_mul(2).unwrap_or(max_delay).min(max_delay);
                    let jitter = Duration::from_millis(rand::random::<u64>() % 500);
                    sleep(current_delay + jitter).await;

                    attempts += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Note: This method acts as the centerialized internal Raft client
    ///
    /// Check leadership quorum verification Immidiatelly
    ///
    /// - Bypasses all queues with direct RPC transmission
    /// - Enforces synchronous quorum validation
    /// - Guarantees real-time network visibility
    ///
    /// Scenario handling summary:
    ///
    /// 1. Quorum achieved:
    ///    - Return: `Ok(QuorumVerificationResult::Success)`
    ///
    /// 2. Quorum NOT achieved (verifiable):
    ///    - Return: `Ok(QuorumVerificationResult::RetryRequired)`
    ///
    /// 3. Quorum NOT achieved (non-verifiable):
    ///    - Return: `Ok(QuorumVerificationResult::LeadershipLost)`
    ///
    /// 4. Partial timeouts:
    ///    - Return: `Ok(QuorumVerificationResult::RetryRequired)`
    ///
    /// 5. All timeouts:
    ///    - Return: `Ok(QuorumVerificationResult::RetryRequired)`
    ///
    /// 6. Higher term detected:
    ///    - Return: `Err(HigherTerm)`
    ///
    /// 7. Critical failure (e.g., system or logic error):
    ///    - Return: original error
    ///
    async fn verify_internal_quorum(
        &mut self,
        payloads: Vec<EntryPayload>,
        bypass_queue: bool,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<QuorumVerificationResult> {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads,
                sender: resp_tx,
            },
            ctx,
            peer_channels,
            bypass_queue,
            role_tx,
        )
        .await?;

        // Wait for response with timeout
        let timeout_duration = Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        match timeout(timeout_duration, resp_rx).await {
            // Case 1: Response received successfully and verification passed
            Ok(Ok(Ok(response))) => {
                debug!("Leadership check response: {:?}", response);

                // Handle different response cases
                Ok(if response.is_write_success() {
                    QuorumVerificationResult::Success
                } else if response.is_retry_required() {
                    // Verifiable quorum failure
                    QuorumVerificationResult::RetryRequired
                } else {
                    // Non-verifiable failure or explicit rejection
                    QuorumVerificationResult::LeadershipLost
                })
            }

            // Case 2: Received explicit rejection status
            Ok(Ok(Err(status))) => {
                warn!("Leadership rejected by follower: {status:?}");
                Ok(QuorumVerificationResult::LeadershipLost)
            }

            // Case 3: Channel communication failure (unrecoverable error)
            Ok(Err(e)) => {
                error!("Channel error during leadership check: {:?}", e);
                Err(NetworkError::SingalReceiveFailed(e.to_string()).into())
            }

            // Case 4: Waiting for response timeout
            Err(_) => {
                warn!("Leadership check timed out after {:?}", timeout_duration);
                Err(NetworkError::Timeout {
                    node_id: self.node_id(),
                    duration: timeout_duration,
                }
                .into())
            }
        }
    }

    fn is_leader(&self) -> bool {
        true
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        warn!("I am leader already");

        Err(StateTransitionError::InvalidTransition.into())
    }

    fn become_candidate(&self) -> Result<RaftRole<T>> {
        error!("Leader can not become Candidate");

        Err(StateTransitionError::InvalidTransition.into())
    }

    fn become_follower(&self) -> Result<RaftRole<T>> {
        info!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Follower now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Follower now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        Ok(RaftRole::Follower(Box::new(self.into())))
    }

    fn become_learner(&self) -> Result<RaftRole<T>> {
        error!("Leader can not become Learner");

        Err(StateTransitionError::InvalidTransition.into())
    }

    fn is_timer_expired(&self) -> bool {
        self.timer.is_expired()
    }

    /// Raft starts, we will check if we need reset all timer
    fn reset_timer(&mut self) {
        self.timer.reset_batch();
        self.timer.reset_replication();
    }

    fn next_deadline(&self) -> Instant {
        self.timer.next_deadline()
    }

    /// Trigger heartbeat now
    async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        _raft_tx: &mpsc::Sender<RaftEvent>,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        let now = Instant::now();
        // Keep syncing leader_id
        ctx.membership_ref().mark_leader_id(self.node_id())?;

        // Batch trigger check (should be prioritized before heartbeat check)
        if now >= self.timer.batch_deadline() {
            trace!("reset_batch timer");
            self.timer.reset_batch();

            if self.batch_buffer.should_flush() {
                self.timer.reset_replication();

                // Take out the batched messages and send them immediately
                // Do not move batch out of this block
                let batch = self.batch_buffer.take();
                self.process_batch(batch, role_tx, ctx, peer_channels.clone()).await?;
            }
        }

        // Heartbeat trigger check
        // Send heartbeat if the replication timer expires
        if now >= self.timer.replication_deadline() {
            debug!("reset_replication timer");
            self.timer.reset_replication();

            // Do not move batch out of this block
            let batch = self.batch_buffer.take();
            self.process_batch(batch, role_tx, ctx, peer_channels).await?;
        }

        Ok(())
    }

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let state_machine = ctx.state_machine();
        let last_applied_index = state_machine.last_applied().index;
        let my_id = self.shared_state.node_id;
        let my_term = self.current_term();

        match raft_event {
            // Leader receives RequestVote(term=X, candidate=Y)
            // 1. If X > currentTerm:
            // - Leader → Follower, currentTerm = X
            // - Replay event
            // 2. Else:
            // - Reply with VoteGranted=false, currentTerm=currentTerm
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                debug!("handle_raft_event::RaftEvent::ReceiveVoteRequest: {:?}", &vote_request);

                let my_term = self.current_term();
                if my_term < vote_request.term {
                    self.update_current_term(vote_request.term);
                    // Step down as Follower
                    self.send_become_follower_event(None, &role_tx)?;

                    info!("Leader will not process Vote request, it should let Follower do it.");
                    self.send_replay_raft_event(&role_tx, RaftEvent::ReceiveVoteRequest(vote_request, sender))?;
                } else {
                    let (last_log_index, last_log_term) = ctx.raft_log().get_last_entry_metadata();
                    let response = VoteResponse {
                        term: my_term,
                        vote_granted: false,
                        last_log_index,
                        last_log_term,
                    };
                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx.membership().retrieve_cluster_membership_config();
                debug!("Leader receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
                let current_conf_version = ctx.membership().get_cluster_conf_version();
                debug!(%current_conf_version, ?cluste_conf_change_request,
                    "handle_raft_event::RaftEvent::ClusterConfUpdate",
                );

                // Reject the fake Leader append entries request
                if my_term >= cluste_conf_change_request.term {
                    let response = ClusterConfUpdateResponse::higher_term(my_id, my_term, current_conf_version);

                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                } else {
                    // Step down as Follower as new Leader found
                    info!("my({}) term < request one, now I will step down to Follower", my_id);
                    //TODO: if there is a bug?  self.update_current_term(vote_request.term);
                    self.send_become_follower_event(Some(cluste_conf_change_request.id), &role_tx)?;

                    info!("Leader will not process append_entries_request, it should let Follower do it.");
                    self.send_replay_raft_event(
                        &role_tx,
                        RaftEvent::ClusterConfUpdate(cluste_conf_change_request, sender),
                    )?;
                }
            }

            RaftEvent::AppendEntries(append_entries_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::AppendEntries: {:?}",
                    &append_entries_request
                );

                // Reject the fake Leader append entries request
                if my_term >= append_entries_request.term {
                    let response = AppendEntriesResponse::higher_term(my_id, my_term);

                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                } else {
                    // Step down as Follower as new Leader found
                    info!("my({}) term < request one, now I will step down to Follower", my_id);
                    //TODO: if there is a bug?  self.update_current_term(vote_request.term);
                    self.send_become_follower_event(Some(append_entries_request.leader_id), &role_tx)?;

                    info!("Leader will not process append_entries_request, it should let Follower do it.");
                    self.send_replay_raft_event(&role_tx, RaftEvent::AppendEntries(append_entries_request, sender))?;
                }
            }

            RaftEvent::ClientPropose(client_write_request, sender) => {
                if let Err(e) = self
                    .process_raft_request(
                        RaftRequestWithSignal {
                            id: nanoid!(),
                            payloads: client_command_to_entry_payloads(client_write_request.commands),
                            sender,
                        },
                        ctx,
                        peer_channels,
                        false,
                        &role_tx,
                    )
                    .await
                {
                    error("Leader::process_raft_request", &e);
                    return Err(e);
                }
            }

            RaftEvent::ClientReadRequest(client_read_request, sender) => {
                debug!(
                    "Leader::ClientReadRequest client_read_request:{:?}",
                    &client_read_request
                );

                let response: std::result::Result<ClientResponse, tonic::Status> = {
                    let read_operation = || -> std::result::Result<ClientResponse, tonic::Status> {
                        let results = ctx
                            .handlers
                            .state_machine_handler
                            .read_from_state_machine(client_read_request.keys)
                            .unwrap_or_default();
                        debug!("handle_client_read results: {:?}", results);
                        Ok(ClientResponse::read_results(results))
                    };

                    if client_read_request.linear {
                        if !self
                            .verify_internal_quorum_with_retry(vec![], true, ctx, peer_channels, &role_tx)
                            .await
                            .unwrap_or(false)
                        {
                            warn!("enforce_quorum_consensus failed for linear read request");

                            Err(tonic::Status::failed_precondition(
                                "enforce_quorum_consensus failed".to_string(),
                            ))
                        } else if let Err(e) = self.ensure_state_machine_upto_commit_index(
                            &ctx.handlers.state_machine_handler,
                            last_applied_index,
                        ) {
                            warn!("ensure_state_machine_upto_commit_index failed for linear read request");
                            Err(tonic::Status::failed_precondition(format!(
                                "ensure_state_machine_upto_commit_index failed: {e:?}"
                            )))
                        } else {
                            read_operation()
                        }
                    } else {
                        read_operation()
                    }
                };

                debug!("Leader::ClientReadRequest is going to response: {:?}", &response);
                sender.send(response).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::InstallSnapshotChunk(_streaming, sender) => {
                sender
                    .send(Err(Status::permission_denied("Not Follower or Learner. ")))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Leader",
                    required_role: "Follower or Learner",
                    context: format!("Leader node {} receives RaftEvent::InstallSnapshotChunk", ctx.node_id),
                }
                .into());
            }

            RaftEvent::RaftLogCleanUp(_purchase_log_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Leader should not receive RaftLogCleanUp event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Leader",
                    required_role: "None Leader",
                    context: format!("Leader node {} receives RaftEvent::RaftLogCleanUp", ctx.node_id),
                }
                .into());
            }

            RaftEvent::CreateSnapshotEvent => {
                let state_machine_handler = ctx.state_machine_handler();

                match state_machine_handler.create_snapshot().await {
                    Err(e) => {
                        error!(%e,"self.state_machine_handler.create_snapshot with error.");
                    }
                    Ok((
                        SnapshotMetadata {
                            last_included: last_included_option,
                            checksum,
                        },
                        _final_path,
                    )) => {
                        info!("Purge Leader local raft logs");

                        if let Some(last_included) = last_included_option {
                            // ----------------------
                            // Phase 1: Update the scheduled purge state
                            // ----------------------
                            if self.can_purge_logs(self.last_purged_index, last_included) {
                                self.scheduled_purge_upto(last_included);
                            }

                            // ----------------------
                            // Phase 2.1: Pre-Checks before sending Purge request
                            // ----------------------
                            let peers = ctx.voting_members(peer_channels);
                            if peers.is_empty() {
                                warn!("no peer found for leader({})", my_id);
                                return Err(MembershipError::NoPeersAvailable.into());
                            }

                            // ----------------------
                            // Phase 2.2: Send Purge request to the other nodes
                            // ----------------------
                            let transport = ctx.transport();
                            match transport
                                .send_purge_requests(
                                    peers,
                                    PurgeLogRequest {
                                        term: my_term,
                                        leader_id: my_id,
                                        last_included: Some(last_included),
                                        snapshot_checksum: checksum.clone(),
                                        leader_commit: self.commit_index(),
                                    },
                                    &self.node_config.retry,
                                )
                                .await
                            {
                                Ok(result) => {
                                    info!(?result, "receive PurgeLogResult");

                                    self.peer_purge_progress(result, &role_tx)?;
                                }
                                Err(e) => {
                                    error!(?e, "RaftEvent::CreateSnapshotEvent");
                                    return Err(e);
                                }
                            }

                            // ----------------------
                            // Phase 3: Execute scheduled purge task
                            // ----------------------
                            debug!(?last_included, "Execute scheduled purge task");
                            if let Some(scheduled) = self.scheduled_purge_upto {
                                if let Err(e) = ctx.purge_executor().execute_purge(scheduled).await {
                                    error!(?e, ?scheduled, "raft_log.purge_logs_up_to");
                                }
                                self.last_purged_index = Some(scheduled);
                            }
                        }
                    }
                }
            }

            RaftEvent::JoinCluster(join_request, sender) => {
                debug!(?join_request, "Leader::RaftEvent::JoinCluster");
                self.handle_join_cluster(join_request, sender, ctx, peer_channels.clone(), &role_tx)
                    .await?;
            }
        }
        return Ok(());
    }
}

impl<T: TypeConfig> LeaderState<T> {
    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
            role: LEADER,
        }
    }

    /// The fun will retrieve current Leader state snapshot
    #[tracing::instrument]
    pub fn leader_state_snapshot(&self) -> LeaderStateSnapshot {
        LeaderStateSnapshot {
            next_index: self.next_index.clone(),
            match_index: self.match_index.clone(),
            noop_log_id: self.noop_log_id,
        }
    }

    fn send_replay_raft_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        raft_event: RaftEvent,
    ) -> Result<()> {
        role_tx
            .send(RoleEvent::ReprocessEvent(Box::new(raft_event)))
            .map_err(|e| {
                let error_str = format!("{e:?}");
                error!("Failed to send: {}", error_str);
                NetworkError::SingalSendFailed(error_str).into()
            })
    }
    /// # Params
    /// - `execute_now`: should this propose been executed immediatelly. e.g.
    ///   enforce_quorum_consensus expected to be executed immediatelly
    pub(crate) async fn process_raft_request(
        &mut self,
        raft_request_with_signal: RaftRequestWithSignal,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        execute_now: bool,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!(?raft_request_with_signal, "Leader::process_raft_request");

        let push_result = self.batch_buffer.push(raft_request_with_signal);
        // only buffer exceeds the max, the size will return
        if execute_now || push_result.is_some() {
            let batch = self.batch_buffer.take();

            trace!(
                "replication_handler.handle_raft_request_in_batch: batch size:{:?}",
                batch.len()
            );

            self.process_batch(batch, role_tx, ctx, peer_channels).await?;
        }

        Ok(())
    }

    /// Scenario handling summary:
    ///
    /// 1. Quorum achieved:
    ///    - Client response: `write_success()`
    ///    - State update: update peer indexes and commit index
    ///    - Return: `Ok(())`
    ///
    /// 2. Quorum NOT achieved (verifiable):
    ///    - Client response: `RetryRequired`
    ///    - State update: update peer indexes
    ///    - Return: `Ok(())`
    ///
    /// 3. Quorum NOT achieved (non-verifiable):
    ///    - Client response: `ProposeFailed`
    ///    - State update: update peer indexes
    ///    - Return: `Ok(())`
    ///
    /// 4. Partial timeouts:
    ///    - Client response: `ProposeFailed`
    ///    - State update: update only the peer indexes that responded
    ///    - Return: `Ok(())`
    ///
    /// 5. All timeouts:
    ///    - Client response: `ProposeFailed`
    ///    - State update: no update
    ///    - Return: `Ok(())`
    ///
    /// 6. Higher term detected:
    ///    - Client response: `TermOutdated`
    ///    - State update: update term and convert to follower
    ///    - Return: `Err(HigherTerm)`
    ///
    /// 7. Critical failure (e.g., system or logic error):
    ///    - Client response: `ProposeFailed`
    ///    - State update: none
    ///    - Return: original error
    pub(super) async fn process_batch(
        &mut self,
        batch: VecDeque<RaftRequestWithSignal>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
    ) -> Result<()> {
        // 1. Prepare batch data
        let entry_payloads: Vec<EntryPayload> = batch.iter().flat_map(|req| &req.payloads).cloned().collect();
        trace!(?entry_payloads, "process_batch..",);

        // 2. Execute the copy
        let replication_members = ctx.voting_members(peer_channels);
        let cluster_size = replication_members.len() + 1;
        trace!(%cluster_size);

        let result = ctx
            .replication_handler()
            .handle_raft_request_in_batch(
                entry_payloads,
                self.state_snapshot(),
                self.leader_state_snapshot(),
                ctx,
                replication_members,
            )
            .await;
        debug!(?result, "replication_handler::handle_raft_request_in_batch");

        // 3. Unify the processing results
        match result {
            // Case 1: Successfully reached majority
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates,
            }) => {
                self.update_peer_indexes(&peer_updates);

                // Update commit index
                if let Some(new_commit_index) = self.calculate_new_commit_index(ctx.raft_log(), &peer_updates) {
                    self.update_commit_index_with_signal(LEADER, self.current_term(), new_commit_index, role_tx)?;
                }

                // Notify all clients of success
                for request in batch {
                    let _ = request.sender.send(Ok(ClientResponse::write_success()));
                }
            }

            // Case 2: Failed to reach majority
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates,
            }) => {
                self.update_peer_indexes(&peer_updates);

                // Determine error code based on verifiability
                let responses_received = peer_updates.len();
                let error_code = if is_majority(responses_received, cluster_size) {
                    ErrorCode::RetryRequired
                } else {
                    ErrorCode::ProposeFailed
                };

                // Notify all clients of failure
                for request in batch {
                    let _ = request.sender.send(Ok(ClientResponse::client_error(error_code)));
                }
            }

            // Case 3: High term found
            Err(Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(higher_term)))) => {
                warn!("Higher term detected: {}", higher_term);
                self.update_current_term(higher_term);
                self.send_become_follower_event(None, role_tx)?;

                // Notify client of term expiration
                for request in batch {
                    let _ = request
                        .sender
                        .send(Ok(ClientResponse::client_error(ErrorCode::TermOutdated)));
                }

                return Err(ReplicationError::HigherTerm(higher_term).into());
            }

            // Case 4: Other errors
            Err(e) => {
                error!("Batch processing failed: {:?}", e);

                // Notify all clients of failure
                for request in batch {
                    let _ = request
                        .sender
                        .send(Ok(ClientResponse::client_error(ErrorCode::ProposeFailed)));
                }

                return Err(e);
            }
        }

        Ok(())
    }

    /// Update peer node index
    #[instrument]
    fn update_peer_indexes(
        &mut self,
        peer_updates: &HashMap<u32, PeerUpdate>,
    ) {
        for (peer_id, update) in peer_updates {
            if let Err(e) = self.update_next_index(*peer_id, update.next_index) {
                error!("Failed to update next index: {:?}", e);
            }
            if let Err(e) = self.update_match_index(*peer_id, update.match_index) {
                error!("Failed to update match index: {:?}", e);
            }
        }
    }

    /// Calculate new submission index
    #[instrument]
    fn calculate_new_commit_index(
        &mut self,
        raft_log: &Arc<ROF<T>>,
        peer_updates: &HashMap<u32, PeerUpdate>,
    ) -> Option<u64> {
        let old_commit_index = self.commit_index();
        let current_term = self.current_term();

        let matched_ids: Vec<u64> = peer_updates.keys().filter_map(|&id| self.match_index(id)).collect();

        let new_commit_index = raft_log.calculate_majority_matched_index(current_term, old_commit_index, matched_ids);

        if new_commit_index.is_some() && new_commit_index.unwrap() > old_commit_index {
            new_commit_index
        } else {
            None
        }
    }

    /// Processes the result of batched client proposals and updates leader
    /// state accordingly.
    ///
    /// This function is placed in `LeaderState` rather than
    /// `ReplicationHandler` because:
    /// 1. **Single Responsibility Principle**: The handling of proposal results directly impacts
    ///    core leader state (e.g., peer indexes, commit index, leader status). State mutations
    ///    should be centralized in the component that owns the state - `LeaderState` is the
    ///    authoritative source for leader-specific state management.
    /// 2. **State Encapsulation**: The logic requires deep access to leader state fields
    ///    (`next_index`, `match_index`, etc.). Keeping this in `LeaderState` maintains
    ///    encapsulation and prevents exposing internal state details to the replication handler
    ///    layer.
    /// 3. **Decision Centralization**: Leadership-specific reactions to proposal outcomes (e.g.,
    ///    stepping down on term conflicts) are inherently tied to leader state management and
    ///    should be colocated with state ownership.
    ///
    /// The `ReplicationHandler` remains focused on protocol mechanics, while
    /// state-aware result processing naturally belongs to the state owner.
    pub(crate) fn process_raft_request_in_batch_result(
        &mut self,
        batch: VecDeque<RaftRequestWithSignal>,
        append_result: Result<AppendResults>,
        raft_log: &Arc<ROF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        match append_result {
            Ok(AppendResults {
                commit_quorum_achieved,
                peer_updates,
            }) => {
                let peer_ids: Vec<u32> = peer_updates.keys().cloned().collect();

                // Converate peer_updates to peer_index_updates
                for (peer_id, peer_update) in peer_updates {
                    if let Err(e) = self.update_next_index(peer_id, peer_update.next_index) {
                        error!(
                            "update_next_index({:?}, {:?}), {:?}",
                            peer_id, peer_update.next_index, e
                        );
                    }
                    if let Err(e) = self.update_match_index(peer_id, peer_update.match_index) {
                        error!(
                            "update_match_index({:?}, {:?}), {:?}",
                            peer_id, peer_update.match_index, e
                        );
                    }
                }

                // Check if quorum achieved
                if commit_quorum_achieved {
                    let old_commit_index = self.commit_index();
                    let current_term = self.current_term();
                    let matched_ids: Vec<u64> = peer_ids.iter().map(|&id| self.match_index(id).unwrap_or(0)).collect();

                    debug!("collected matched_ids:{:?}", &matched_ids);
                    let calculated_matched_index =
                        raft_log.calculate_majority_matched_index(current_term, old_commit_index, matched_ids);
                    debug!("calculated_matched_index: {:?}", &calculated_matched_index);
                    let (updated, commit_index) = self.if_update_commit_index(calculated_matched_index);

                    debug!("old commit: {:?} , new commit: {:?}", old_commit_index, commit_index);
                    //notify commit_success_receiver, new commit is ready to conver to KV store.
                    if updated {
                        if let Err(e) =
                            self.update_commit_index_with_signal(LEADER, self.current_term(), commit_index, role_tx)
                        {
                            error!(
                                "update_commit_index_with_signal,commit={}, error: {:?}",
                                commit_index, e
                            );
                            return Err(e);
                        }
                    } else {
                        debug!("no need to update commit index");
                    }
                }

                for r in batch {
                    if commit_quorum_achieved {
                        if let Err(e) = r.sender.send(Ok(ClientResponse::write_success())) {
                            error!("[{}]/append_result_signal_sender failed to send signal after receive majority confirmation: {:?}", r.id, e);
                        } else {
                            debug!("execute the client command successfully. (id: {})", r.id);
                        }
                    } else {
                        debug!("notify client that replication failed.");
                        if let Err(e) = r
                            .sender
                            .send(Ok(ClientResponse::client_error(ErrorCode::ProposeFailed)))
                        {
                            error!("r.sender.send response failed: {:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Execute the client command failed with error: {:?}", e);
                match e {
                    Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(higher_term))) => {
                        warn!("found higher term");
                        self.update_current_term(higher_term);

                        // if let Err(e) = role_tx.send(RoleEvent::BecomeFollower(None)) {
                        //     error!("Send conflict leader signal failed with error: {:?}", e);
                        // }
                        if let Err(e) = self.send_become_follower_event(None, role_tx) {
                            error!(?e, "Send conflict leader signal failed.");
                        }

                        for r in batch {
                            if let Err(e) = r.sender.send(Ok(ClientResponse::client_error(ErrorCode::TermOutdated))) {
                                error!("r.sender.send response failed: {:?}", e);
                            }
                        }
                    }
                    _ => {
                        error("process_raft_request_in_batch_result", &e);
                        for r in batch {
                            if let Err(e) = r
                                .sender
                                .send(Ok(ClientResponse::client_error(ErrorCode::ProposeFailed)))
                            {
                                error!("r.sender.send response failed: {:?}", e);
                            }
                        }
                        return Err(e);
                    }
                }

                return Err(e);
            }
        }
        Ok(())
    }

    #[tracing::instrument]
    fn if_update_commit_index(
        &self,
        new_commit_index_option: Option<u64>,
    ) -> (bool, u64) {
        let current_commit_index = self.commit_index();
        if let Some(new_commit_index) = new_commit_index_option {
            debug!("Leader::update_commit_index: {:?}", new_commit_index);
            if current_commit_index < new_commit_index {
                return (true, new_commit_index);
            }
        }
        debug!("Leader::update_commit_index: false");
        (false, current_commit_index)
    }

    pub(crate) fn ensure_state_machine_upto_commit_index(
        &self,
        state_machine_handler: &Arc<SMHOF<T>>,
        last_applied: u64,
    ) -> Result<()> {
        let commit_index = self.commit_index();

        debug!(
            "ensure_state_machine_upto_commit_index: last_applied:{} < commit_index:{} ?",
            last_applied, commit_index
        );
        if last_applied < commit_index {
            state_machine_handler.update_pending(commit_index);

            debug!("ensure_state_machine_upto_commit_index success");
        }
        Ok(())
    }

    #[instrument(skip(self))]
    fn scheduled_purge_upto(
        &mut self,
        received_last_included: LogId,
    ) {
        if let Some(existing) = self.scheduled_purge_upto {
            if existing.index >= received_last_included.index {
                warn!(
                    ?received_last_included,
                    ?existing,
                    "Will not update scheduled_purge_upto, received invalid last_included log"
                );
                return;
            }
        }
        info!(?self.scheduled_purge_upto, ?received_last_included, "Updte scheduled_purge_upto.");
        self.scheduled_purge_upto = Some(received_last_included);
    }

    fn peer_purge_progress(
        &mut self,
        responses: Vec<Result<PurgeLogResponse>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        if responses.is_empty() {
            return Ok(());
        }
        for r in responses.iter().flatten() {
            if r.term > self.current_term() {
                self.update_current_term(r.term);
                self.send_become_follower_event(None, role_tx)?;
            }

            if let Some(last_purged) = r.last_purged {
                self.peer_purge_progress
                    .entry(r.node_id)
                    .and_modify(|v| *v = last_purged.index)
                    .or_insert(last_purged.index);
            }
        }

        Ok(())
    }

    fn send_become_follower_event(
        &self,
        new_leader_id: Option<u32>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        info!(?new_leader_id, "Leader is going to step down as Follower...");
        role_tx.send(RoleEvent::BecomeFollower(new_leader_id)).map_err(|e| {
            let error_str = format!("{e:?}");
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str)
        })?;

        Ok(())
    }

    /// Determines if logs prior to `last_included_in_snapshot` can be permanently discarded.
    ///
    /// Implements Leader-side log compaction safety checks per Raft paper §7.2:
    /// > "The leader uses a new RPC called InstallSnapshot to send snapshots to followers that are
    /// > too far behind"
    ///
    /// # Safety Invariants (ALL must hold)
    ///
    /// 1. **Committed Entry Guarantee**   `last_included_in_snapshot.index < self.commit_index`
    ///    - Ensures we never discard uncommitted entries (Raft §5.4.2)
    ///    - Maintains at least one committed entry after purge for log matching property
    ///
    /// 2. **Monotonic Snapshot Advancement**   `last_purge_index < last_included_in_snapshot.index`
    ///    - Enforces snapshot indices strictly increase (prevents rollback attacks)
    ///    - Maintains sequential purge ordering (FSM safety requirement)
    ///
    /// 3. **Cluster-wide Progress Validation**   `peer_purge_progress.values().all(≥
    ///    snapshot.index)`
    ///    - Ensures ALL followers have confirmed ability to reach this snapshot
    ///    - Prevents leadership changes from causing log inconsistencies
    ///
    /// 4. **Operation Atomicity**   `pending_purge.is_none()`
    ///    - Ensures only one concurrent purge operation
    ///    - Critical for linearizable state machine semantics
    ///
    /// # Implementation Notes
    /// - Leader must maintain `peer_purge_progress` through AppendEntries responses
    /// - Actual log discard should be deferred until storage confirms snapshot persistence
    /// - Design differs from followers by requiring full cluster confirmation (Raft extension for
    ///   enhanced durability)
    #[instrument(skip(self))]
    pub(super) fn can_purge_logs(
        &self,
        last_purge_index: Option<LogId>,
        last_included_in_snapshot: LogId,
    ) -> bool {
        let monotonic_check = last_purge_index
            .map(|lid| lid.index < last_included_in_snapshot.index)
            .unwrap_or(true);

        last_included_in_snapshot.index < self.commit_index()
            && monotonic_check
            && self
                .peer_purge_progress
                .values()
                .all(|&v| v >= last_included_in_snapshot.index)
    }

    pub(super) async fn handle_join_cluster(
        &mut self,
        join_request: JoinRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let node_id = join_request.node_id;
        let address = join_request.address;

        // 1. Validate join request
        if ctx.membership().contains_node(node_id) {
            let error_msg = format!("Node {} already exists in cluster", node_id);
            warn!(%error_msg);
            return self
                .send_join_error(sender, MembershipError::NodeAlreadyExists(node_id))
                .await;
        }

        // 2. Add new peer to connection manager
        peer_channels
            .add_peer(node_id, address.clone(), LEARNER, NodeStatus::Active)
            .await?;

        // 3. Add the node as Learner
        ctx.membership().add_learner(node_id, address.clone()).await?;

        // 4. Create configuration change payload
        let config_change = Change::AddNode(AddNode {
            node_id,
            address: address.clone(),
        });

        // 5. Wait for quorum confirmation
        match self
            .verify_internal_quorum_with_retry(
                vec![EntryPayload::config(config_change)],
                false,
                ctx,
                peer_channels.clone(),
                role_tx,
            )
            .await
        {
            Ok(true) => {
                info!("Join config committed for node {}", node_id);
                self.send_join_success(node_id, &address, sender, ctx).await?;

                // AFTER join success: Trigger snapshot transfer
                if let Some(lastest_snapshot_metadata) = ctx.state_machine().snapshot_metadata() {
                    self.trigger_snapshot_transfer(node_id, lastest_snapshot_metadata, ctx, peer_channels.clone())
                        .await?;
                }
            }
            Ok(false) => {
                warn!("Failed to commit join config for node {}", node_id);
                self.send_join_error(sender, MembershipError::CommitTimeout).await?
            }
            Err(e) => {
                error!("Error waiting for commit: {:?}", e);
                self.send_join_error(sender, e).await?
            }
        }
        Ok(())
    }

    async fn send_join_success(
        &self,
        node_id: u32,
        address: &str,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // Retrieve latest snapshot metadata, if there is
        let snapshot_metadata = ctx.state_machine_handler().get_latest_snapshot_metadata();

        // Prepare response
        let response = JoinResponse {
            success: true,
            error: String::new(),
            config: Some(ctx.membership().retrieve_cluster_membership_config()),
            config_version: ctx.membership().get_cluster_conf_version(),
            snapshot_metadata,
            leader_id: self.node_id(),
        };

        sender.send(Ok(response)).map_err(|e| {
            error!("Failed to send join response: {:?}", e);
            NetworkError::SingalSendFailed(format!("{e:?}"))
        })?;

        info!("Node {} ({}) successfully added as learner", node_id, address);
        Ok(())
    }

    async fn send_join_error(
        &self,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
        error: impl Into<Error>,
    ) -> Result<()> {
        let error = error.into();
        let status = Status::internal(error.to_string());

        sender.send(Err(status)).map_err(|e| {
            error!("Failed to send join error: {:?}", e);
            NetworkError::SingalSendFailed(format!("{e:?}"))
        })?;

        Err(error)
    }

    #[cfg(test)]
    pub(crate) fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        let ReplicationConfig {
            rpc_append_entries_in_batch_threshold,
            rpc_append_entries_batch_process_delay_in_ms,
            rpc_append_entries_clock_in_ms,
            ..
        } = node_config.raft.replication;

        LeaderState {
            shared_state: SharedState::new(node_id, None, None),
            timer: Box::new(ReplicationTimer::new(
                rpc_append_entries_clock_in_ms,
                rpc_append_entries_batch_process_delay_in_ms,
            )),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            batch_buffer: Box::new(BatchBuffer::new(
                rpc_append_entries_in_batch_threshold,
                Duration::from_millis(rpc_append_entries_batch_process_delay_in_ms),
            )),

            node_config,
            _marker: PhantomData,
            scheduled_purge_upto: None,
            last_purged_index: None, //TODO
            peer_purge_progress: HashMap::new(),
        }
    }

    pub(super) async fn trigger_snapshot_transfer(
        &self,
        node_id: u32,
        metadata: SnapshotMetadata,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
    ) -> Result<()> {
        let transport = ctx.transport().clone();

        // Get existing channel from peer manager
        let channel_with_address = peer_channels
            .get_peer_channel(node_id)
            .ok_or(NetworkError::PeerConnectionNotFound(node_id))?;

        let data_stream = ctx.state_machine_handler().load_snapshot_data(metadata.clone()).await?;
        let retry = ctx.node_config.retry.install_snapshot.clone();
        tokio::spawn(async move {
            match transport
                .install_snapshot(channel_with_address.channel, metadata, data_stream, &retry)
                .await
            {
                Ok(_) => info!("Snapshot transferred successfully to node {}", node_id),
                Err(e) => error!("Snapshot transfer failed to node {}: {:?}", node_id, e),
            }
        });

        Ok(())
    }
}

impl<T: TypeConfig> From<&CandidateState<T>> for LeaderState<T> {
    fn from(candidate: &CandidateState<T>) -> Self {
        let ReplicationConfig {
            rpc_append_entries_in_batch_threshold,
            rpc_append_entries_batch_process_delay_in_ms,
            rpc_append_entries_clock_in_ms,
            ..
        } = candidate.node_config.raft.replication;

        Self {
            shared_state: candidate.shared_state.clone(),
            timer: Box::new(ReplicationTimer::new(
                rpc_append_entries_clock_in_ms,
                rpc_append_entries_batch_process_delay_in_ms,
            )),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            batch_buffer: Box::new(BatchBuffer::new(
                rpc_append_entries_in_batch_threshold,
                Duration::from_millis(rpc_append_entries_batch_process_delay_in_ms),
            )),

            node_config: candidate.node_config.clone(),
            _marker: PhantomData,

            scheduled_purge_upto: None,
            last_purged_index: candidate.last_purged_index,
            peer_purge_progress: HashMap::new(),
        }
    }
}

impl<T: TypeConfig> Debug for LeaderState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LeaderState")
            .field("shared_state", &self.shared_state)
            .field("next_index", &self.next_index)
            .field("match_index", &self.match_index)
            .field("noop_log_id", &self.noop_log_id)
            .finish()
    }
}
