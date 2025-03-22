use super::{
    follower_state::FollowerState, role_state::RaftRoleState, RaftRole, Result, SharedState,
    StateSnapshot,
};
use crate::{
    alias::POF,
    grpc::rpc_service::{AppendEntriesResponse, ClientResponse, VoteResponse, VotedFor},
    utils::util::error,
    AppendResponseWithUpdates, ElectionCore, ElectionTimer, Error, Membership, RaftContext,
    RaftEvent, ReplicationCore, RoleEvent, Settings, StateMachine, StateMachineHandler, TypeConfig,
};
use log::{debug, error, info, warn};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tokio::{sync::mpsc, time::Instant};
use tonic::{async_trait, Status};

#[derive(Clone, Debug)]
pub struct CandidateState<T: TypeConfig> {
    pub shared_state: SharedState,

    pub(super) timer: ElectionTimer,
    pub(super) votes: HashMap<u32, bool>,

    // Shared global settings
    pub(super) settings: Arc<Settings>,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for CandidateState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    fn is_candidate(&self) -> bool {
        true
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        info!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Leader now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Leader now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        Ok(RaftRole::Leader(self.into()))
    }

    fn become_candidate(&self) -> Result<RaftRole<T>> {
        warn!("I am candidate already");
        Err(Error::Illegal)
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
        info!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Learner now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Learner now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        Ok(RaftRole::Learner(self.into()))
    }

    fn reset_timer(&mut self) {
        self.timer.reset()
    }

    fn is_timer_expired(&self) -> bool {
        self.timer.is_expired()
    }
    fn next_deadline(&self) -> Instant {
        self.timer.next_deadline()
    }

    // fn tick_interval(&self) -> Duration {
    //     self.timer.tick_interval()
    // }

    /// Election Timeout: as candidate, it should send vote requests now
    async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        _raft_event_tx: &mpsc::Sender<RaftEvent>,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        debug!("reset timer");
        self.timer.reset();

        debug!("candidate::start_election...");

        self.increase_current_term();
        self.reset_voted_for()?;

        // if self.can_vote_myself() {
        debug!("candidate new term: {}", self.current_term());

        self.vote_myself()?;

        if let Ok(_) = ctx
            .election_handler()
            .broadcast_vote_requests(
                self.current_term(),
                ctx.voting_members(peer_channels),
                ctx.raft_log(),
                ctx.transport(),
                &ctx.settings(),
            )
            .await
        {
            debug!("BecomeLeader");
            if let Err(e) = role_tx.send(RoleEvent::BecomeLeader) {
                error!(
                    "self.my_role_change_event_sender.send(RaftRole::Leader) failed: {:?}",
                    e
                );
            }
        }
        // }
        Ok(())
    }

    async fn recv_heartbeat(&mut self, leader_id: u32, ctx: &RaftContext<T>) -> Result<()> {
        self.reset_timer();

        // Keep syncing leader_id
        ctx.membership_ref().mark_leader_id(leader_id);

        Ok(())
    }

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        _peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let raft_log = ctx.raft_log();
        let state_snapshot = self.state_snapshot();
        let state_machine = ctx.state_machine();
        let last_applied = state_machine.last_applied();
        let my_id = self.shared_state.node_id;
        let my_term = self.current_term();

        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                let candidate_id = vote_request.candidate_id;
                match ctx
                    .election_handler()
                    .handle_vote_request(
                        vote_request,
                        self.current_term(),
                        self.voted_for().unwrap(),
                        raft_log,
                    )
                    .await
                {
                    Ok(state_update) => {
                        debug!(
                            "candidate::handle_vote_request success with state_update: {:?}",
                            &state_update
                        );

                        // 1. If switch to Follower
                        if state_update.step_to_follower {
                            role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
                                let error_str = format!("{:?}", e);
                                error!("Failed to send: {}", error_str);
                                Error::TokioSendStatusError(error_str)
                            })?;
                        }

                        // 3. If update my voted_for
                        let new_voted_for = state_update.new_voted_for;
                        if let Some(v) = new_voted_for {
                            if let Err(e) = self.update_voted_for(v) {
                                error("candidate::update_voted_for", &e);
                                return Err(e);
                            }
                        }

                        let response = VoteResponse {
                            term: my_term,
                            vote_granted: new_voted_for.is_some(),
                        };
                        debug!(
                            "Response candidate_{:?} with response: {:?}",
                            candidate_id, response
                        );

                        sender.send(Ok(response)).map_err(|e| {
                            let error_str = format!("{:?}", e);
                            error!("Failed to send: {}", error_str);
                            Error::TokioSendStatusError(error_str)
                        })?;
                        return Ok(());
                    }
                    Err(e) => {
                        error(
                            "candidate::handle_raft_event::RaftEvent::ReceiveVoteRequest",
                            &e,
                        );
                        return Err(e);
                    }
                };
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx.membership().retrieve_cluster_membership_config();
                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    Error::TokioSendStatusError(error_str)
                })?;
                return Ok(());
            }
            RaftEvent::ClusterConfUpdate(_cluste_membership_change_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Not able to update cluster conf, as node is not Leader",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        Error::TokioSendStatusError(error_str)
                    })?;
                return Err(Error::NotLeader);
            }
            RaftEvent::AppendEntries(append_entries_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::AppendEntries: {:?}",
                    &append_entries_request
                );

                // Important to confirm heartbeat from Leader immediatelly
                if let Err(e) = self
                    .recv_heartbeat(append_entries_request.leader_id, ctx)
                    .await
                {
                    error!("recv_heartbeat: {:?}", e);
                }

                // Step down as Follower
                if let Err(e) = role_tx.send(RoleEvent::BecomeFollower(Some(
                    append_entries_request.leader_id,
                ))) {
                    error!(
                        "self.my_role_change_event_sender.send(RaftRole::Follower) failed: {:?}",
                        e
                    );
                } else {
                    debug!(
                        "my term is smaller than Append Request one, so I({}) become follower.",
                        my_id
                    );
                }

                // Handle replication request
                match ctx
                    .replication_handler()
                    .handle_append_entries(
                        append_entries_request,
                        &state_snapshot,
                        last_applied,
                        // sender,
                        raft_log,
                    )
                    .await
                {
                    Ok(AppendResponseWithUpdates {
                        success,
                        current_term,
                        last_matched_id,
                        term_update,
                        commit_index_update,
                    }) => {
                        if let Some(term) = term_update {
                            self.update_current_term(term);
                        }
                        if let Some(commit) = commit_index_update {
                            if let Err(e) = self.update_commit_index_with_signal(commit, &role_tx) {
                                error!(
                                    "update_commit_index_with_signal,commit={}, error: {:?}",
                                    commit, e
                                );
                                return Err(e);
                            }
                        }

                        // Create a response
                        let response = AppendEntriesResponse {
                            id: self.node_id(),
                            term: current_term,
                            success,
                            match_index: last_matched_id,
                        };

                        debug!("follower's response: {:?}", response);

                        sender.send(Ok(response)).map_err(|e| {
                            let error_str = format!("{:?}", e);
                            error!("Failed to send: {}", error_str);
                            Error::TokioSendStatusError(error_str)
                        })?;
                    }
                    Err(e) => {
                        error("Candidate::handle_raft_event", &e);
                        return Err(e);
                    }
                }
                return Ok(());
            }
            RaftEvent::ClientPropose(_client_propose_request, sender) => {
                //TODO: direct to leader
                // self.redirect_to_leader(client_propose_request).await;
                sender
                    .send(Ok(ClientResponse::write_error(
                        Error::AppendEntriesNotLeader,
                    )))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        Error::TokioSendStatusError(error_str)
                    })?;
                return Ok(());
            }
            RaftEvent::ClientReadRequest(client_read_request, sender) => {
                // If the request is linear request, ...
                if client_read_request.linear {
                    sender
                        .send(Err(Status::unauthenticated(
                            "Not leader. Send linearizable read requet to Leader only.",
                        )))
                        .map_err(|e| {
                            let error_str = format!("{:?}", e);
                            error!("Failed to send: {}", error_str);
                            Error::TokioSendStatusError(error_str)
                        })?;
                } else {
                    // Otherwise
                    let mut results = vec![];
                    if let Some(v) = ctx
                        .state_machine_handler
                        .read_from_state_machine(client_read_request.commands)
                    {
                        results = v;
                    }
                    let response = ClientResponse::read_results(results);
                    debug!("handle_client_read response: {:?}", response);
                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        Error::TokioSendStatusError(error_str)
                    })?;
                }
                return Ok(());
            }
        }
    }
}

impl<T: TypeConfig> CandidateState<T> {
    pub fn can_vote_myself(&self) -> bool {
        if let Ok(Some(vf)) = self.voted_for() {
            let current_term = self.current_term();
            debug!(
                "[candiate::can_vote_myself] vf={:?}, current_term={}",
                &vf, current_term
            );
            // voted term is smaller than mine
            vf.voted_for_term < current_term
        } else {
            // either I have not voted for anyone yet
            debug!("[candiate::can_vote_myself] true");
            return true;
        }
    }
    pub fn vote_myself(&mut self) -> Result<()> {
        info!("vote myself as candidate");
        self.update_voted_for(VotedFor {
            voted_for_id: self.node_id(),
            voted_for_term: self.current_term(),
        })
    }

    #[cfg(test)]
    pub fn new(node_id: u32, settings: Arc<Settings>) -> Self {
        Self {
            shared_state: SharedState::new(node_id, None, None),
            votes: HashMap::new(),
            timer: ElectionTimer::new((1, 2)),
            settings,
            _marker: PhantomData,
        }
    }
}

impl<T: TypeConfig> CandidateState<T> {
    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
        }
    }
}

impl<T: TypeConfig> From<&FollowerState<T>> for CandidateState<T> {
    fn from(follower: &FollowerState<T>) -> Self {
        Self {
            shared_state: follower.shared_state.clone(),
            votes: HashMap::new(),
            timer: ElectionTimer::new((
                follower.settings.raft_settings.election_timeout_min,
                follower.settings.raft_settings.election_timeout_max,
            )),
            settings: follower.settings.clone(),
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> Drop for CandidateState<T> {
    fn drop(&mut self) {
        // self.votes.clear();
    }
}
