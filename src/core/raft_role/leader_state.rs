use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use autometrics::autometrics;
use nanoid::nanoid;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio::time::Instant;
use tonic::async_trait;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

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
use crate::constants::INTERNAL_CLIENT_ID;
use crate::proto::client::ClientCommand;
use crate::proto::client::ClientProposeRequest;
use crate::proto::client::ClientResponse;
use crate::proto::cluster::ClusterConfUpdateResponse;
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
use crate::ClientRequestWithSignal;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotReceiver;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::PurgeExecutor;
use crate::QuorumStatus;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::RaftOneshot;
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
    batch_buffer: Box<BatchBuffer<ClientRequestWithSignal>>,

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

    async fn verify_leadership_in_new_term(
        &mut self,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!("verify_leadership_in_new_term...");
        let command = ClientCommand::no_op();

        let client_propose_request = ClientProposeRequest {
            client_id: INTERNAL_CLIENT_ID,
            commands: vec![command],
        };

        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();

        self.process_client_propose(client_propose_request, resp_tx, ctx, peer_channels, false, &role_tx)
            .await?;
        Ok(())
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
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx.membership().retrieve_cluster_membership_config();
                debug!("Leader receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::ClusterConfUpdate(cluste_membership_change_request, sender) => {
                debug!(
                    "Leader::update_cluster_conf::Received req: {:?}",
                    cluste_membership_change_request
                );

                let my_id = self.node_id();
                let my_current_term = self.current_term();
                let success = ctx
                    .membership()
                    .update_cluster_conf_from_leader(my_current_term, &cluste_membership_change_request)
                    .await
                    .is_ok();

                let response = ClusterConfUpdateResponse {
                    id: my_id,
                    term: my_current_term,
                    version: ctx.membership().get_cluster_conf_version(),
                    success,
                };

                debug!("[peer-{}] update_cluster_conf response: {:?}", my_id, &response);
                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
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
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                } else {
                    // Step down as Follower as new Leader found
                    info!("my({}) term < request one, now I will step down to Follower", my_id);

                    // role_tx
                    //     .send(RoleEvent::BecomeFollower(Some(append_entries_request.leader_id)))
                    //     .map_err(|e| {
                    //         let error_str = format!("{:?}", e);
                    //         error!("Failed to send: {}", error_str);
                    //         NetworkError::SingalSendFailed(error_str)
                    //     })?;
                    self.send_become_follower_event(Some(append_entries_request.leader_id), &role_tx)?;

                    info!("Leader will not process append_entries_request, it should let Follower do it.");
                    self.send_replay_raft_event(&role_tx, RaftEvent::AppendEntries(append_entries_request, sender))?;
                }
            }

            RaftEvent::ClientPropose(client_propose_request, sender) => {
                if let Err(e) = self
                    .process_client_propose(client_propose_request, sender, ctx, peer_channels, false, &role_tx)
                    .await
                {
                    error("Leader::process_client_propose", &e);
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
                            .read_from_state_machine(client_read_request.commands)
                            .unwrap_or_default();
                        debug!("handle_client_read results: {:?}", results);
                        Ok(ClientResponse::read_results(results))
                    };

                    if client_read_request.linear {
                        let quorum_result = self.enforce_quorum_consensus(ctx, peer_channels, &role_tx).await;

                        let quorum_succeeded = matches!(quorum_result, Ok(true));

                        if !quorum_succeeded {
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
                                "ensure_state_machine_upto_commit_index failed: {:?}",
                                e
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
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::InstallSnapshotChunk(_streaming, sender) => {
                sender
                    .send(Err(Status::permission_denied("Not Follower or Learner. ")))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }

            RaftEvent::RaftLogCleanUp(_purchase_log_request, sender) => {
                sender
                    .send(Err(Status::permission_denied("Leader should not receive this event.")))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
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

            RaftEvent::StartScheduledPurgeLogEvent => {
                warn!("Leader should not receive StartScheduledPurgeLogEvent.");
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
                let error_str = format!("{:?}", e);
                error!("Failed to send: {}", error_str);
                NetworkError::SingalSendFailed(error_str).into()
            })
    }

    /// # Params
    /// - `execute_now`: should this propose been executed immediatelly. e.g.
    ///   enforce_quorum_consensus expected to be executed immediatelly
    pub(crate) async fn process_client_propose(
        &mut self,
        client_propose_request: ClientProposeRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        execute_now: bool,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!(
            "Leader::process_client_propose, client_propose_request={:?}",
            &client_propose_request
        );

        let push_result = self.batch_buffer.push(ClientRequestWithSignal {
            id: nanoid!(),
            commands: client_propose_request.commands,
            sender,
        });

        // only buffer exceeds the max, the size will return
        if execute_now || push_result.is_some() {
            let batch = self.batch_buffer.take();

            trace!(
                "replication_handler.handle_client_proposal_in_batch: batch size:{:?}",
                batch.len()
            );

            self.process_batch(batch, role_tx, ctx, peer_channels).await?;
        }

        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: VecDeque<ClientRequestWithSignal>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
    ) -> Result<()> {
        let commands: Vec<ClientCommand> = batch.iter().flat_map(|req| &req.commands).cloned().collect();

        trace!("process_batch.., commands:{:?}", &commands);

        let append_result = ctx
            .replication_handler()
            .handle_client_proposal_in_batch(
                commands,
                self.state_snapshot(),
                self.leader_state_snapshot(),
                ctx,
                peer_channels,
            )
            .await;

        debug!("process_client_proposal_in_batch_result: {:?}", &append_result);

        self.process_client_proposal_in_batch_result(batch, append_result, ctx.raft_log(), role_tx)?;

        Ok(())
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
    pub(crate) fn process_client_proposal_in_batch_result(
        &mut self,
        batch: VecDeque<ClientRequestWithSignal>,
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
                    }
                    _ => {
                        error("process_client_proposal_in_batch_result", &e);
                    }
                }
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

    /// The enforce_quorum_consensus should be executed immediatelly
    ///  
    #[tracing::instrument]
    pub async fn enforce_quorum_consensus(
        &mut self,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        let client_propose_request = ClientProposeRequest {
            client_id: self.node_config.raft.election.internal_rpc_client_request_id,
            commands: vec![],
        };

        let status = self
            .check_leadership_quorum_immediate(client_propose_request, ctx, peer_channels, role_tx)
            .await?;
        debug!("enforce_quorum_consensus:status = {:?}", status);
        match status {
            QuorumStatus::Confirmed => Ok(true),
            QuorumStatus::LostQuorum => Ok(false),
            QuorumStatus::NetworkError => Ok(false),
        }
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
        for res in responses.iter() {
            if let Ok(r) = res {
                if r.term > self.current_term() {
                    self.send_become_follower_event(None, role_tx)?;
                }

                if let Some(last_purged) = r.last_purged {
                    self.peer_purge_progress
                        .entry(r.node_id)
                        .and_modify(|v| *v = last_purged.index)
                        .or_insert(last_purged.index);
                }
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
            let error_str = format!("{:?}", e);
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

impl<T: TypeConfig> LeaderState<T> {
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
    async fn check_leadership_quorum_immediate(
        &mut self,
        request: ClientProposeRequest,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<QuorumStatus> {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.process_client_propose(request, resp_tx, ctx, peer_channels, true, role_tx)
            .await?;

        self.wait_quorum_response(
            resp_rx,
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms),
        )
        .await
    }

    async fn wait_quorum_response(
        &self,
        receiver: MaybeCloneOneshotReceiver<std::result::Result<ClientResponse, Status>>,
        timeout_duration: Duration,
    ) -> Result<QuorumStatus> {
        // Wait for response with timeout
        match timeout(timeout_duration, receiver).await {
            // Case 1: Response received successfully and verification passed
            Ok(Ok(Ok(response))) => {
                debug!("Leadership check response: {:?}", response);
                Ok(if response.validate_error().is_ok() {
                    QuorumStatus::Confirmed
                } else {
                    QuorumStatus::LostQuorum
                })
            }

            // Case 2: Received explicit rejection status
            Ok(Ok(Err(status))) => {
                warn!("Leadership check failed with status: {:?}", status);
                Ok(QuorumStatus::LostQuorum)
            }

            // Case 3: Channel communication failure (unrecoverable error)
            Ok(Err(e)) => {
                error!("Channel error during leadership check: {:?}", e);
                Err(NetworkError::SingalReceiveFailed(e.to_string()).into())
            }

            // Case 4: Waiting for response timeout
            Err(_) => {
                warn!("Leadership check timed out after {:?}", timeout_duration);
                Ok(QuorumStatus::NetworkError)
            }
        }
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
