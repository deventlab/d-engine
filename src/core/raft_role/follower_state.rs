use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::async_trait;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::candidate_state::CandidateState;
use super::leader_state::LeaderState;
use super::learner_state::LearnerState;
use super::role_state::RaftRoleState;
use super::HardState;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use crate::alias::POF;
use crate::proto::ClientResponse;
use crate::proto::ErrorCode;
use crate::proto::VoteResponse;
use crate::utils::cluster::error;
use crate::ElectionCore;
use crate::ElectionTimer;
use crate::Membership;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::RoleEvent;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;

pub struct FollowerState<T: TypeConfig> {
    pub shared_state: SharedState,
    pub(super) timer: ElectionTimer,
    // Shared global settings
    pub(super) settings: Arc<RaftNodeConfig>,

    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for FollowerState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    fn is_follower(&self) -> bool {
        true
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        error!("become_leader Illegal. I am Follower");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_candidate(&self) -> Result<RaftRole<T>> {
        info!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Candidate now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "\n\n
                =================================
                [{:?}<{:?}>] >>> switch to Candidate now.\n
                =================================
                \n\n",
            self.node_id(),
            self.current_term(),
        );
        Ok(RaftRole::Candidate(self.into()))
    }
    fn become_follower(&self) -> Result<RaftRole<T>> {
        warn!("I am follower already");

        Err(StateTransitionError::InvalidTransition.into())
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

    //--- Timer releated ---
    fn is_timer_expired(&self) -> bool {
        self.timer.is_expired()
    }
    fn reset_timer(&mut self) {
        self.timer.reset()
    }
    fn next_deadline(&self) -> Instant {
        self.timer.next_deadline()
    }

    /// Election Timeout
    /// As follower,
    ///  step as Candidate
    async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        _event_tx: &mpsc::Sender<RaftEvent>,
        _peer_channels: Arc<POF<T>>,
        _ctx: &RaftContext<T>,
    ) -> Result<()> {
        debug!("reset timer");
        self.timer.reset();

        debug!("follower::start_election...");

        role_tx.send(RoleEvent::BecomeCandidate).map_err(|e| {
            let error_str = format!("{:?}", e);
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str)
        })?;

        Ok(())
    }

    #[tracing::instrument]
    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        _peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let state_snapshot = self.state_snapshot();

        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                let candidate_id = vote_request.candidate_id;
                let my_term = self.current_term();

                let (last_log_index, last_log_term) = ctx.raft_log().get_last_entry_metadata();

                match ctx
                    .election_handler()
                    .handle_vote_request(vote_request, my_term, self.voted_for().unwrap(), ctx.raft_log())
                    .await
                {
                    Ok(state_update) => {
                        debug!("handle_vote_request success with state_update: {:?}", &state_update);

                        // 1. Update term FIRST if needed
                        if let Some(new_term) = state_update.term_update {
                            self.update_current_term(new_term);
                        }
                        // 2. If update my voted_for
                        let new_voted_for = state_update.new_voted_for;
                        if let Some(v) = new_voted_for {
                            if let Err(e) = self.update_voted_for(v) {
                                error("update_voted_for", &e);
                                return Err(e);
                            }
                        }

                        let response = VoteResponse {
                            term: my_term,
                            vote_granted: new_voted_for.is_some(),
                            last_log_index,
                            last_log_term,
                        };
                        debug!("Response candidate_{:?} with response: {:?}", candidate_id, response);

                        sender.send(Ok(response)).map_err(|e| {
                            let error_str = format!("{:?}", e);
                            error!("Failed to send: {}", error_str);
                            NetworkError::SingalSendFailed(error_str)
                        })?;
                    }
                    Err(e) => {
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
                        error("handle_raft_event::RaftEvent::ReceiveVoteRequest", &e);
                        return Err(e);
                    }
                }
            }
            // RaftEvent::ReceiveVoteResponse(_, vote_response) => todo!(),
            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx.membership().retrieve_cluster_membership_config();
                debug!("Follower receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
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
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }
            RaftEvent::AppendEntries(append_entries_request, sender) => {
                self.handle_append_entries_request_workflow(
                    append_entries_request,
                    sender,
                    ctx,
                    role_tx,
                    &state_snapshot,
                )
                .await?;
            }
            RaftEvent::ClientPropose(_client_propose_request, sender) => {
                //TODO: direct to leader
                // self.redirect_to_leader(client_propose_request).await;
                sender
                    .send(Ok(ClientResponse::client_error(ErrorCode::NotLeader)))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
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
                            NetworkError::SingalSendFailed(error_str)
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
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }
            }
        }

        return Ok(());
    }
}

impl<T: TypeConfig> FollowerState<T> {
    pub fn new(
        node_id: u32,
        settings: Arc<RaftNodeConfig>,
        hard_state_from_db: Option<HardState>,
        last_applied_index_option: Option<u64>,
    ) -> Self {
        Self {
            shared_state: SharedState::new(node_id, hard_state_from_db, last_applied_index_option),
            timer: ElectionTimer::new((
                settings.raft.election.election_timeout_min,
                settings.raft.election.election_timeout_max,
            )),
            settings,
            _marker: PhantomData,
        }
    }

    /// The fun will retrieve current state snapshot
    #[tracing::instrument]
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
        }
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for FollowerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            timer: ElectionTimer::new((
                candidate_state.settings.raft.election.election_timeout_min,
                candidate_state.settings.raft.election.election_timeout_max,
            )),
            settings: candidate_state.settings.clone(),
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&LeaderState<T>> for FollowerState<T> {
    fn from(leader_state: &LeaderState<T>) -> Self {
        Self {
            shared_state: leader_state.shared_state.clone(),
            timer: ElectionTimer::new((
                leader_state.settings.raft.election.election_timeout_min,
                leader_state.settings.raft.election.election_timeout_max,
            )),
            settings: leader_state.settings.clone(),
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&LearnerState<T>> for FollowerState<T> {
    fn from(learner_state: &LearnerState<T>) -> Self {
        Self {
            //TODO: should we copy or new?
            shared_state: learner_state.shared_state.clone(),
            timer: ElectionTimer::new((
                learner_state.settings.raft.election.election_timeout_min,
                learner_state.settings.raft.election.election_timeout_max,
            )),
            settings: learner_state.settings.clone(),
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> Drop for FollowerState<T> {
    fn drop(&mut self) {}
}

impl<T: TypeConfig> Debug for FollowerState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("FollowerState")
            .field("shared_state", &self.shared_state)
            .finish()
    }
}
