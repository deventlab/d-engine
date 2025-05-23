use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::mpsc::{self};
use tokio::time::Instant;
use tonic::async_trait;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::candidate_state::CandidateState;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use crate::alias::POF;
use crate::proto::ClientResponse;
use crate::proto::ErrorCode;
use crate::proto::VoteResponse;
use crate::proto::VotedFor;
use crate::ElectionTimer;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::RoleEvent;
use crate::StateTransitionError;
use crate::TypeConfig;

pub struct LearnerState<T: TypeConfig> {
    pub shared_state: SharedState,
    pub(super) timer: ElectionTimer,
    // Shared global settings
    pub(super) settings: Arc<RaftNodeConfig>,

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

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_candidate(&self) -> Result<RaftRole<T>> {
        warn!("become_candidate Illegal. I am Learner");

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
        warn!("I am Learner already");

        Err(StateTransitionError::InvalidTransition.into())
    }

    /// As Leader should not vote any more
    fn voted_for(&self) -> Result<Option<VotedFor>> {
        warn!("voted_for - As Learner should not vote any more.");

        Err(StateTransitionError::InvalidTransition.into())
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

    #[tracing::instrument]
    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        _peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let state_snapshot = self.state_snapshot();
        let my_term = self.current_term();

        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                info!("handle_raft_event::ReceiveVoteRequest. Learner cannot vote.");
                // 1. Update term FIRST if needed
                if vote_request.term > my_term {
                    self.update_current_term(vote_request.term);
                }

                // 2. Response sender with vote_granted=false
                let (last_log_index, last_log_term) = ctx.raft_log().get_last_entry_metadata();
                let response = VoteResponse {
                    term: my_term,
                    vote_granted: false,
                    last_log_index,
                    last_log_term,
                };
                debug!(
                    "Response candidate_{:?} with response: {:?}",
                    vote_request.candidate_id, response
                );

                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                debug!("Learner receive ClusterConf request...");
                sender
                    .send(Err(Status::permission_denied(
                        "Not able to respond to cluster conf request as node is Learner",
                    )))
                    .map_err(|e| {
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
            RaftEvent::ClientReadRequest(_client_read_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Learner can not process client read request",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }
        }
        return Ok(());
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
    pub fn new(
        node_id: u32,
        settings: Arc<RaftNodeConfig>,
    ) -> Self {
        LearnerState {
            shared_state: SharedState::new(node_id, None, None),
            timer: ElectionTimer::new((
                settings.raft.election.election_timeout_min,
                settings.raft.election.election_timeout_max,
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
                follower_state.settings.raft.election.election_timeout_min,
                follower_state.settings.raft.election.election_timeout_max,
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
                candidate_state.settings.raft.election.election_timeout_min,
                candidate_state.settings.raft.election.election_timeout_max,
            )),
            settings: candidate_state.settings.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: TypeConfig> Debug for LearnerState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LearnerState")
            .field("shared_state", &self.shared_state)
            .finish()
    }
}
