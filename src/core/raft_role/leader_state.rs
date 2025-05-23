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
use crate::alias::POF;
use crate::alias::REPOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::alias::TROF;
use crate::constants::INTERNAL_CLIENT_ID;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientCommand;
use crate::proto::ClientProposeRequest;
use crate::proto::ClientResponse;
use crate::proto::ClusterConfUpdateResponse;
use crate::proto::ErrorCode;
use crate::proto::VoteResponse;
use crate::proto::VotedFor;
use crate::utils::cluster::error;
use crate::AppendResults;
use crate::BatchBuffer;
use crate::ChannelWithAddressAndRole;
use crate::ClientRequestWithSignal;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotReceiver;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::NetworkError;
use crate::QuorumStatus;
use crate::RaftConfig;
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
use crate::RetryPolicies;
use crate::RoleEvent;
use crate::StateMachine;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::API_SLO;

pub struct LeaderState<T: TypeConfig> {
    // Leader State
    pub shared_state: SharedState,

    pub(super) next_index: HashMap<u32, u64>,
    pub(super) match_index: HashMap<u32, u64>,
    pub(super) noop_log_id: Option<u64>,

    // Leader batched proposal buffer
    batch_buffer: BatchBuffer<ClientRequestWithSignal>,

    timer: ReplicationTimer,

    // Shared global settings
    pub(super) settings: Arc<RaftNodeConfig>,

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
    fn prev_log_index(
        &self,
        follower_id: u32,
    ) -> Option<u64> {
        if let Some(next_id) = self.next_index(follower_id) {
            debug!("follower({})s next_id is: {}", follower_id, next_id);
            Some(if next_id > 0 { next_id - 1 } else { 0 })
        } else {
            None
        }
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

        let settings = ctx.settings();

        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();

        self.process_client_propose(
            client_propose_request,
            resp_tx,
            ctx.replication_handler(),
            &ctx.voting_members(peer_channels),
            ctx.raft_log(),
            ctx.transport(),
            &settings.raft,
            &settings.retry,
            false,
            &role_tx,
        )
        .await?;
        Ok(())
    }
    /// Decrease next id for node(node_id) by 1
    #[cfg(test)]
    fn decr_next_index(
        &mut self,
        node_id: u32,
    ) -> Result<()> {
        self.next_index.entry(node_id).and_modify(|v| {
            if *v > 1 {
                *v -= 1;
            }
        });
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
        Ok(RaftRole::Follower(self.into()))
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
        let voting_members = ctx.voting_members(peer_channels);
        let raft_log = ctx.raft_log();
        let transport = ctx.transport();
        let replication_handler = ctx.replication_handler();
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
                self.process_batch(
                    replication_handler,
                    batch,
                    role_tx,
                    &voting_members,
                    raft_log,
                    transport,
                    &ctx.settings.raft,
                    &ctx.settings.retry,
                )
                .await?;
            }
        }

        // Heartbeat trigger check
        // Send heartbeat if the replication timer expires
        if now >= self.timer.replication_deadline() {
            debug!("reset_replication timer");
            self.timer.reset_replication();

            // Do not move batch out of this block
            let batch = self.batch_buffer.take();
            self.process_batch(
                replication_handler,
                batch,
                role_tx,
                &voting_members,
                raft_log,
                transport,
                &ctx.settings.raft,
                &ctx.settings.retry,
            )
            .await?;
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
        let raft_log = ctx.raft_log();
        let transport = ctx.transport();
        let settings = ctx.settings();
        let state_machine = ctx.state_machine();
        let last_applied = state_machine.last_applied();
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
                    self.send_become_follower_event(&role_tx)?;

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

                    role_tx
                        .send(RoleEvent::BecomeFollower(Some(append_entries_request.leader_id)))
                        .map_err(|e| {
                            let error_str = format!("{:?}", e);
                            error!("Failed to send: {}", error_str);
                            NetworkError::SingalSendFailed(error_str)
                        })?;

                    info!("Leader will not process append_entries_request, it should let Follower do it.");
                    self.send_replay_raft_event(&role_tx, RaftEvent::AppendEntries(append_entries_request, sender))?;
                }
            }

            RaftEvent::ClientPropose(client_propose_request, sender) => {
                let voting_members = ctx.voting_members(peer_channels);
                if let Err(e) = self
                    .process_client_propose(
                        client_propose_request,
                        sender,
                        ctx.replication_handler(),
                        &voting_members,
                        raft_log,
                        transport,
                        &settings.raft,
                        &settings.retry,
                        false,
                        &role_tx,
                    )
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
                            .state_machine_handler
                            .read_from_state_machine(client_read_request.commands)
                            .unwrap_or_default();
                        debug!("handle_client_read results: {:?}", results);
                        Ok(ClientResponse::read_results(results))
                    };

                    if client_read_request.linear {
                        let voting_members = ctx.voting_members(peer_channels);
                        let quorum_result = self
                            .enforce_quorum_consensus(
                                ctx.replication_handler(),
                                &voting_members,
                                raft_log,
                                transport,
                                &settings.raft,
                                &settings.retry,
                                &role_tx,
                            )
                            .await;

                        let quorum_succeeded = matches!(quorum_result, Ok(true));

                        if !quorum_succeeded {
                            warn!("enforce_quorum_consensus failed for linear read request");

                            Err(tonic::Status::failed_precondition(
                                "enforce_quorum_consensus failed".to_string(),
                            ))
                        } else if let Err(e) =
                            self.ensure_state_machine_upto_commit_index(&ctx.state_machine_handler, last_applied)
                        {
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

    fn send_become_follower_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
            let error_str = format!("{:?}", e);
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str).into()
        })
    }

    fn send_replay_raft_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        raft_event: RaftEvent,
    ) -> Result<()> {
        role_tx.send(RoleEvent::ReprocessEvent(raft_event)).map_err(|e| {
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
        replication_handler: &REPOF<T>,
        voting_members: &Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        raft_config: &RaftConfig,
        retry_policies: &RetryPolicies,
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

            self.process_batch(
                replication_handler,
                batch,
                role_tx,
                voting_members,
                raft_log,
                transport,
                raft_config,
                retry_policies,
            )
            .await?;
        }

        Ok(())
    }

    async fn process_batch(
        &mut self,
        replication_handler: &REPOF<T>,
        batch: VecDeque<ClientRequestWithSignal>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        voting_members: &Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        raft_config: &RaftConfig,
        retry_policies: &RetryPolicies,
    ) -> Result<()> {
        let commands: Vec<ClientCommand> = batch.iter().flat_map(|req| &req.commands).cloned().collect();

        trace!("process_batch.., commands:{:?}", &commands);

        let append_result = replication_handler
            .handle_client_proposal_in_batch(
                commands,
                self.state_snapshot(),
                self.leader_state_snapshot(),
                voting_members,
                raft_log,
                transport,
                raft_config,
                retry_policies,
            )
            .await;

        debug!("process_client_proposal_in_batch_result: {:?}", &append_result);

        self.process_client_proposal_in_batch_result(batch, append_result, raft_log, role_tx)?;

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
                        if let Err(e) = self.update_commit_index_with_signal(commit_index, role_tx) {
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

                        if let Err(e) = role_tx.send(RoleEvent::BecomeFollower(None)) {
                            error!("Send conflict leader signal failed with error: {:?}", e);
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
        replication_handler: &REPOF<T>,
        voting_members: &Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        raft_config: &RaftConfig,
        retry_policies: &RetryPolicies,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        let client_propose_request = ClientProposeRequest {
            client_id: self.settings.raft.election.internal_rpc_client_request_id,
            commands: vec![],
        };

        let status = self
            .check_leadership_quorum_immediate(
                client_propose_request,
                replication_handler,
                voting_members,
                raft_log,
                transport,
                raft_config,
                retry_policies,
                role_tx,
            )
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
}

impl<T: TypeConfig> From<&CandidateState<T>> for LeaderState<T> {
    fn from(candidate: &CandidateState<T>) -> Self {
        let ReplicationConfig {
            rpc_append_entries_in_batch_threshold,
            rpc_append_entries_batch_process_delay_in_ms,
            rpc_append_entries_clock_in_ms,
            ..
        } = candidate.settings.raft.replication;

        Self {
            shared_state: candidate.shared_state.clone(),
            timer: ReplicationTimer::new(
                rpc_append_entries_clock_in_ms,
                rpc_append_entries_batch_process_delay_in_ms,
            ),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            batch_buffer: BatchBuffer::new(
                rpc_append_entries_in_batch_threshold,
                Duration::from_millis(rpc_append_entries_batch_process_delay_in_ms),
            ),

            settings: candidate.settings.clone(),
            _marker: PhantomData,
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
        replication_handler: &REPOF<T>,
        voting_members: &Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        raft_config: &RaftConfig,
        retry_policies: &RetryPolicies,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<QuorumStatus> {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.process_client_propose(
            request,
            resp_tx,
            replication_handler,
            voting_members,
            raft_log,
            transport,
            raft_config,
            retry_policies,
            true,
            role_tx,
        )
        .await?;

        self.wait_quorum_response(
            resp_rx,
            Duration::from_millis(self.settings.raft.general_raft_timeout_duration_in_ms),
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
        settings: Arc<RaftNodeConfig>,
    ) -> Self {
        let ReplicationConfig {
            rpc_append_entries_in_batch_threshold,
            rpc_append_entries_batch_process_delay_in_ms,
            rpc_append_entries_clock_in_ms,
            ..
        } = settings.raft.replication;

        LeaderState {
            shared_state: SharedState::new(node_id, None, None),
            timer: ReplicationTimer::new(
                rpc_append_entries_clock_in_ms,
                rpc_append_entries_batch_process_delay_in_ms,
            ),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            batch_buffer: BatchBuffer::new(
                rpc_append_entries_in_batch_threshold,
                Duration::from_millis(rpc_append_entries_batch_process_delay_in_ms),
            ),

            settings,
            _marker: PhantomData,
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
