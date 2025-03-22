use super::{
    candidate_state::CandidateState, follower_state::FollowerState, role_state::RaftRoleState,
    RaftRole, SharedState, StateSnapshot,
};
use crate::{
    alias::POF,
    grpc::rpc_service::{AppendEntriesResponse, ClientResponse, VotedFor},
    utils::util::error,
    AppendResponseWithUpdates, ElectionTimer, Error, Membership, RaftContext, RaftEvent,
    ReplicationCore, Result, RoleEvent, Settings, StateMachine, TypeConfig,
};
use log::{debug, error, info, warn};
use std::{marker::PhantomData, sync::Arc};
use tokio::{
    sync::mpsc::{self},
    time::Instant,
};
use tonic::{async_trait, Status};

pub struct LearnerState<T: TypeConfig> {
    pub shared_state: SharedState,
    pub(super) timer: ElectionTimer,
    // Shared global settings
    pub(super) settings: Arc<Settings>,

    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for LearnerState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    // fn role(&self) -> i32 {
    //     RaftRole::Learner(self.clone()).as_i32()
    // }

    fn is_learner(&self) -> bool {
        true
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        error!("become_leader Illegal. I am Learner");
        Err(Error::Illegal)
    }
    fn become_candidate(&self) -> Result<RaftRole<T>> {
        warn!("become_candidate Illegal. I am Learner");
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
        warn!("I am Learner already");
        Err(Error::Illegal)
    }
    /// As Leader should not vote any more
    ///
    fn voted_for(&self) -> Result<Option<VotedFor>> {
        warn!("voted_for - As Learner should not vote any more.");
        Err(Error::Illegal)
    }
    //--- None state behaviors
    fn is_timer_expired(&self) -> bool {
        self.timer.is_expired()
    }
    fn reset_timer(&mut self) {
        self.timer.reset();
    }
    fn next_deadline(&self) -> Instant {
        self.timer.next_deadline()
    }
    // fn tick_interval(&self) -> Duration {
    //     self.timer.tick_interval()
    // }

    async fn tick(
        &mut self,
        _role_event_tx: &mpsc::UnboundedSender<RoleEvent>,
        _raft_tx: &mpsc::Sender<RaftEvent>,
        _peer_channels: Arc<POF<T>>,
        _ctx: &RaftContext<T>,
    ) -> Result<()> {
        debug!("reset timer");
        self.timer.reset();

        debug!("as Learner will do nothing");

        Ok(())
    }

    async fn recv_heartbeat(&mut self, leader_id: u32, ctx: &RaftContext<T>) -> Result<()> {
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

        match raft_event {
            RaftEvent::ReceiveVoteRequest(_vote_request, _sender) => {
                info!("handle_raft_event::ReceiveVoteRequest, will ignore vote request. Learner cannot vote.");
                return Err(Error::LearnerCanNotVote);
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Not able to respond to cluster conf request as node is Learner",
                    )))
                    .map_err(|e| {
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
                return Ok(());
            }
            RaftEvent::AppendEntries(append_entries_request, sender) => {
                // Important to confirm heartbeat from Leader immediatelly
                if let Err(e) = self
                    .recv_heartbeat(append_entries_request.leader_id, ctx)
                    .await
                {
                    error!("recv_heartbeat: {:?}", e);
                }

                // Handle replication request
                match ctx
                    .replication_handler()
                    .handle_append_entries(
                        append_entries_request,
                        &state_snapshot,
                        last_applied,
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

                        debug!("learner's response: {:?}", response);

                        sender.send(Ok(response)).map_err(|e| {
                            let error_str = format!("{:?}", e);
                            error!("Failed to send: {}", error_str);
                            Error::TokioSendStatusError(error_str)
                        })?;
                    }
                    Err(e) => {
                        error("Learner::handle_raft_event", &e);
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
            RaftEvent::ClientReadRequest(_client_read_request, sender) => {
                sender
                    .send(Err(Status::unauthenticated(
                        "Learner can not process client read request",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        Error::TokioSendStatusError(error_str)
                    })?;
                return Ok(());
            }
        }
    }
}

impl<T: TypeConfig> LearnerState<T> {
    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
        }
    }
}

impl<T: TypeConfig> LearnerState<T> {
    pub fn new(node_id: u32, settings: Arc<Settings>) -> Self {
        LearnerState {
            shared_state: SharedState::new(node_id, None, None),
            timer: ElectionTimer::new((
                settings.raft_settings.election_timeout_min,
                settings.raft_settings.election_timeout_max,
            )),
            settings,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&FollowerState<T>> for LearnerState<T> {
    fn from(follower_state: &FollowerState<T>) -> Self {
        Self {
            shared_state: follower_state.shared_state.clone(),
            timer: ElectionTimer::new((
                follower_state.settings.raft_settings.election_timeout_min,
                follower_state.settings.raft_settings.election_timeout_max,
            )),
            settings: follower_state.settings.clone(),
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for LearnerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            timer: ElectionTimer::new((
                candidate_state.settings.raft_settings.election_timeout_min,
                candidate_state.settings.raft_settings.election_timeout_max,
            )),
            settings: candidate_state.settings.clone(),
            _marker: PhantomData,
        }
    }
}
