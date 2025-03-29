use super::{
    follower_state::FollowerState, role_state::RaftRoleState, RaftRole, Result, SharedState,
    StateSnapshot,
};
use crate::{
    alias::POF,
    grpc::rpc_service::{AppendEntriesResponse, ClientResponse, VoteResponse, VotedFor},
    ElectionCore, ElectionTimer, Error, Membership, RaftContext, RaftEvent, RaftLog, RoleEvent,
    Settings, StateMachineHandler, TypeConfig,
};
use log::{debug, error, info, warn};
use std::{marker::PhantomData, sync::Arc};
use tokio::{sync::mpsc, time::Instant};
use tonic::{async_trait, Status};

#[derive(Clone, Debug)]
pub struct CandidateState<T: TypeConfig> {
    pub shared_state: SharedState,

    pub(super) timer: ElectionTimer,

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

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        _peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::ReceiveVoteRequest: {:?}",
                    &vote_request
                );
                let my_term = self.current_term();
                if my_term < vote_request.term {
                    self.update_current_term(vote_request.term);
                    // Step down as Follower
                    self.send_become_follower_event(&role_tx)?;

                    info!("Candiate will not process ReceiveVoteRequest, it should let Follower do it.");
                    self.send_replay_raft_event(
                        &role_tx,
                        RaftEvent::ReceiveVoteRequest(vote_request, sender),
                    )?;
                } else {
                    let response = VoteResponse {
                        term: my_term,
                        vote_granted: false,
                    };
                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        Error::TokioSendStatusError(error_str)
                    })?;
                }
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx.membership().retrieve_cluster_membership_config();
                debug!("Candidate receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    Error::TokioSendStatusError(error_str)
                })?;
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
            }
            RaftEvent::AppendEntries(append_entries_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::AppendEntries: {:?}",
                    &append_entries_request
                );

                // Important to reset timer immediatelly
                self.reset_timer();

                let my_term = self.current_term();
                if append_entries_request.term < my_term {
                    let raft_log_last_index = ctx.raft_log.last_entry_id();
                    let response = AppendEntriesResponse {
                        id: self.node_id(),
                        term: self.current_term(),
                        success: false,
                        match_index: raft_log_last_index,
                    };
                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        Error::TokioSendStatusError(error_str)
                    })?;
                    return Ok(());
                } else {
                    // Keep syncing leader_id
                    ctx.membership_ref()
                        .mark_leader_id(append_entries_request.leader_id)?;

                    if append_entries_request.term > my_term {
                        self.update_current_term(append_entries_request.term);
                    }
                    // Step down as Follower
                    self.send_become_follower_event(&role_tx)?;

                    info!("Candiate will not process AppendEntries request, it should let Follower do it.");
                    self.send_replay_raft_event(
                        &role_tx,
                        RaftEvent::AppendEntries(append_entries_request, sender),
                    )?;
                }
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
            }
            RaftEvent::ClientReadRequest(client_read_request, sender) => {
                // If the request is linear request, ...
                if client_read_request.linear {
                    sender
                        .send(Err(Status::permission_denied(
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
            }
        }
        return Ok(());
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

    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
        }
    }

    pub(super) fn send_become_follower_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
            let error_str = format!("{:?}", e);
            error!("Failed to send: {}", error_str);
            Error::TokioSendStatusError(error_str)
        })
    }

    pub(super) fn send_replay_raft_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        raft_event: RaftEvent,
    ) -> Result<()> {
        debug!("send_replay_raft_event, raft_event:{:?}", &raft_event);
        role_tx
            .send(RoleEvent::ReprocessEvent(raft_event))
            .map_err(|e| {
                let error_str = format!("{:?}", e);
                error!("Failed to send: {}", error_str);
                Error::TokioSendStatusError(error_str)
            })
    }

    #[cfg(test)]
    pub fn new(node_id: u32, settings: Arc<Settings>) -> Self {
        Self {
            shared_state: SharedState::new(node_id, None, None),
            timer: ElectionTimer::new((1, 2)),
            settings,
            _marker: PhantomData,
        }
    }
}

impl<T: TypeConfig> From<&FollowerState<T>> for CandidateState<T> {
    fn from(follower: &FollowerState<T>) -> Self {
        Self {
            shared_state: follower.shared_state.clone(),
            timer: ElectionTimer::new((
                follower.settings.raft.election.election_timeout_min,
                follower.settings.raft.election.election_timeout_max,
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
