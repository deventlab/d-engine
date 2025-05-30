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
use tracing::trace;
use tracing::warn;

use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use super::RaftRole;
use super::Result;
use super::SharedState;
use super::StateSnapshot;
use super::CANDIDATE;
use crate::alias::POF;
use crate::proto::client::ClientResponse;
use crate::proto::common::LogId;
use crate::proto::election::VoteResponse;
use crate::proto::election::VotedFor;
use crate::proto::error::ErrorCode;
use crate::ConsensusError;
use crate::ElectionCore;
use crate::ElectionError;
use crate::ElectionTimer;
use crate::Error;
use crate::Membership;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::ReplicationCore;
use crate::RoleEvent;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;

/// Candidate node's volatile state during leader election.
///
/// This transient state manages election timers and vote solicitation process.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
#[derive(Clone)]
pub struct CandidateState<T: TypeConfig> {
    // -- Core State --
    /// Shared cluster state with atomic access
    pub shared_state: SharedState,

    // -- Log Compaction --
    /// === Persistent State ===
    /// Last physically purged log index (inclusive)
    ///
    /// Note:
    /// Even though candidates don’t purge logs, they must keep last_purged_index from the follower.
    /// Otherwise, the system may lose purge info and get into an inconsistent state.
    pub(super) last_purged_index: Option<LogId>,

    // -- Election Timing --
    /// Election timeout manager
    ///
    /// Tracks:
    /// - Randomized election timeout duration
    /// - Remaining time before transitioning to new election
    pub(super) timer: ElectionTimer,

    // -- Cluster Configuration --
    /// Immutable node configuration (shared reference)
    pub(super) node_config: Arc<RaftNodeConfig>,

    // -- Type System Marker --
    /// Phantom type for compile-time safety
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
        Ok(RaftRole::Leader(Box::new(self.into())))
    }

    fn become_candidate(&self) -> Result<RaftRole<T>> {
        warn!("I am candidate already");
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
        Ok(RaftRole::Learner(Box::new(self.into())))
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

        debug!("candidate new term: {}", self.current_term());

        self.vote_myself()?;

        match ctx
            .election_handler()
            .broadcast_vote_requests(
                self.current_term(),
                ctx.voting_members(peer_channels),
                ctx.raft_log(),
                ctx.transport(),
                &ctx.node_config(),
            )
            .await
        {
            Ok(_) => {
                debug!("BecomeLeader");
                if let Err(e) = role_tx.send(RoleEvent::BecomeLeader) {
                    error!(
                        "self.my_role_change_event_sender.send(RaftRole::Leader) failed: {:?}",
                        e
                    );
                }
            }
            Err(Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(higher_term)))) => {
                // Immediately update the Term and become a Follower.
                self.update_current_term(higher_term);
                self.send_become_follower_event(role_tx)?;
            }
            Err(e) => {
                warn!("candidate broadcast_vote_requests with error: {:?}", e);
            }
        }
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
        let my_term = self.current_term();
        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                debug!("handle_raft_event::RaftEvent::ReceiveVoteRequest: {:?}", &vote_request);
                let (last_log_index, last_log_term) = ctx.raft_log().get_last_entry_metadata();

                if ctx.election_handler().check_vote_request_is_legal(
                    &vote_request,
                    my_term,
                    last_log_index,
                    last_log_term,
                    self.voted_for().unwrap(),
                ) {
                    self.update_current_term(vote_request.term);
                    // Step down as Follower
                    self.send_become_follower_event(&role_tx)?;

                    info!("Candiate will not process ReceiveVoteRequest, it should let Follower do it.");
                    self.send_replay_raft_event(&role_tx, RaftEvent::ReceiveVoteRequest(vote_request, sender))?;
                } else {
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
                debug!("Candidate receive ClusterConf: {:?}", &cluster_conf);

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
                debug!(
                    "handle_raft_event::RaftEvent::AppendEntries: {:?}",
                    &append_entries_request
                );

                // Important to reset timer immediatelly
                self.reset_timer();

                let my_term = self.current_term();

                let response = ctx.replication_handler().check_append_entries_request_is_legal(
                    my_term,
                    &append_entries_request,
                    ctx.raft_log(),
                );

                // Handle illegal requests (return conflict or higher Term)
                if response.is_conflict() || response.is_higher_term() {
                    debug!("Rejecting AppendEntries: {:?}", &response);

                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                    return Ok(());
                } else {
                    // Keep syncing leader_id
                    ctx.membership_ref().mark_leader_id(append_entries_request.leader_id)?;

                    if append_entries_request.term > my_term {
                        self.update_current_term(append_entries_request.term);
                    }
                    // Step down as Follower
                    self.send_become_follower_event(&role_tx)?;

                    info!("Candiate will not process AppendEntries request, it should let Follower do it.");
                    self.send_replay_raft_event(&role_tx, RaftEvent::AppendEntries(append_entries_request, sender))?;
                }
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
                        .handlers
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

            RaftEvent::InstallSnapshotChunk(_streaming, sender) => {
                sender
                    .send(Err(Status::permission_denied("Not Follower or Learner.")))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }

            RaftEvent::RaftLogCleanUp(purchase_log_request, sender) => {
                debug!(?purchase_log_request, "RaftEvent::RaftLogCleanUp");

                warn!(%self.shared_state.node_id, "Candidate should not receive RaftEvent::RaftLogCleanUp request from Leader");
                sender
                    .send(Err(Status::permission_denied("Not Follower")))
                    .map_err(|e| {
                        let error_str = format!("{:?}", e);
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }

            RaftEvent::CreateSnapshotEvent => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!("Candidate node {} attempted to create snapshot.", ctx.node_id),
                }
                .into())
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
            true
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
            role: CANDIDATE,
        }
    }

    pub(super) fn send_become_follower_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
            let error_str = format!("{:?}", e);
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str).into()
        })
    }

    pub(super) fn send_replay_raft_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        raft_event: RaftEvent,
    ) -> Result<()> {
        debug!("send_replay_raft_event, raft_event:{:?}", &raft_event);
        role_tx
            .send(RoleEvent::ReprocessEvent(Box::new(raft_event)))
            .map_err(|e| {
                let error_str = format!("{:?}", e);
                error!("Failed to send: {}", error_str);
                NetworkError::SingalSendFailed(error_str).into()
            })
    }

    #[cfg(test)]
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        Self {
            shared_state: SharedState::new(node_id, None, None),
            timer: ElectionTimer::new((1, 2)),
            node_config,
            _marker: PhantomData,
            last_purged_index: None, //TODO
        }
    }
}

impl<T: TypeConfig> From<&FollowerState<T>> for CandidateState<T> {
    fn from(follower: &FollowerState<T>) -> Self {
        trace!(%follower.node_config.raft.election.election_timeout_min, "From<&FollowerState<T>> for CandidateState");
        Self {
            shared_state: follower.shared_state.clone(),
            timer: ElectionTimer::new((
                follower.node_config.raft.election.election_timeout_min,
                follower.node_config.raft.election.election_timeout_max,
            )),
            node_config: follower.node_config.clone(),
            last_purged_index: follower.last_purged_index,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> Drop for CandidateState<T> {
    fn drop(&mut self) {
        // self.votes.clear();
    }
}

impl<T: TypeConfig> Debug for CandidateState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("CandidateState")
            .field("shared_state", &self.shared_state)
            .finish()
    }
}
