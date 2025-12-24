use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::RaftRole;
use super::Result;
use super::SharedState;
use super::StateSnapshot;
use super::can_serve_read_locally;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
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
use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Candidate;

use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;

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
    /// Even though candidates don’t purge logs, they must keep last_purged_index from the
    /// follower. Otherwise, the system may lose purge info and get into an inconsistent state.
    pub last_purged_index: Option<LogId>,

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
            "Node {} term {} transitioning to Leader",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Candidate → Leader (term {})",
            self.node_id(),
            self.current_term()
        );
        Ok(RaftRole::Leader(Box::new(self.into())))
    }

    fn become_candidate(&self) -> Result<RaftRole<T>> {
        warn!("I am candidate already");
        Err(StateTransitionError::InvalidTransition.into())
    }

    fn become_follower(&self) -> Result<RaftRole<T>> {
        info!(
            "Node {} term {} transitioning to Follower",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Candidate → Follower (term {})",
            self.node_id(),
            self.current_term()
        );
        Ok(RaftRole::Follower(Box::new(self.into())))
    }

    fn become_learner(&self) -> Result<RaftRole<T>> {
        info!(
            "Node {} term {} transitioning to Learner",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Candidate → Learner (term {})",
            self.node_id(),
            self.current_term()
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
                ctx.membership(),
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
            Err(Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(
                higher_term,
            )))) => {
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

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let my_term = self.current_term();
        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::ReceiveVoteRequest: {:?}",
                    &vote_request
                );

                let LogId {
                    index: last_log_index,
                    term: last_log_term,
                } = ctx.raft_log().last_log_id().unwrap_or(LogId { index: 0, term: 0 });

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

                    info!(
                        "Candidate will not process ReceiveVoteRequest, it should let Follower do it."
                    );
                    self.send_replay_raft_event(
                        &role_tx,
                        RaftEvent::ReceiveVoteRequest(vote_request, sender),
                    )?;
                } else {
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
                let cluster_conf = ctx
                    .membership()
                    .retrieve_cluster_membership_config(self.shared_state().current_leader())
                    .await;
                debug!("Candidate receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
                let current_conf_version = ctx.membership().get_cluster_conf_version().await;

                let current_leader_id = self.shared_state().current_leader();

                debug!(?current_leader_id, %current_conf_version, ?cluste_conf_change_request,
                    "Candidate receive ClusterConfUpdate"
                );

                let my_id = self.node_id();
                let response = match ctx
                    .membership()
                    .update_cluster_conf_from_leader(
                        my_id,
                        my_term,
                        current_conf_version,
                        current_leader_id,
                        &cluste_conf_change_request,
                    )
                    .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        error!(?e, "update_cluster_conf_from_leader");
                        ClusterConfUpdateResponse::internal_error(
                            my_id,
                            my_term,
                            current_conf_version,
                        )
                    }
                };

                debug!(
                    "[peer-{}] update_cluster_conf_from_leader response: {:?}",
                    my_id, &response
                );
                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{e:?}");
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
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                    return Ok(());
                } else {
                    // Keep syncing leader_id (hot-path: ~5ns atomic store)
                    self.shared_state().set_current_leader(append_entries_request.leader_id);

                    if append_entries_request.term > my_term {
                        self.update_current_term(append_entries_request.term);
                    }
                    // Step down as Follower
                    self.send_become_follower_event(&role_tx)?;

                    info!(
                        "Candidate will not process AppendEntries request, it should let Follower do it."
                    );
                    self.send_replay_raft_event(
                        &role_tx,
                        RaftEvent::AppendEntries(append_entries_request, sender),
                    )?;
                }
            }
            RaftEvent::ClientPropose(_client_propose_request, sender) => {
                // Return NOT_LEADER with leader metadata for client redirection
                let response = self.create_not_leader_response(ctx).await;
                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }
            RaftEvent::ClientReadRequest(client_read_request, sender) => {
                match can_serve_read_locally(&client_read_request, ctx) {
                    Some(_policy) => {
                        // Only EventualConsistency will reach here - safe to serve locally
                        let results = ctx
                            .handlers
                            .state_machine_handler
                            .read_from_state_machine(client_read_request.keys)
                            .unwrap_or_default();
                        let response = ClientResponse::read_results(results);
                        debug!("Non-leader serving local read: {:?}", response);
                        sender.send(Ok(response)).map_err(|e| {
                            error!("Failed to send local read response: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{e:?}"))
                        })?;
                    }
                    None => {
                        // Policy requires leader access - reject
                        let error = tonic::Status::permission_denied(
                            "Read consistency policy requires leader access. Current node is candidate.",
                        );
                        sender.send(Err(error)).map_err(|e| {
                            error!("Failed to send policy rejection: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{e:?}"))
                        })?;
                    }
                }
            }

            RaftEvent::InstallSnapshotChunk(_streaming, sender) => {
                sender
                    .send(Err(Status::permission_denied("Not Follower or Learner.")))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Follower or Learner",
                    context: format!(
                        "Candidate node {} receives RaftEvent::InstallSnapshotChunk",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::RaftLogCleanUp(purchase_log_request, sender) => {
                debug!(?purchase_log_request, "RaftEvent::RaftLogCleanUp");

                warn!(%self.shared_state.node_id, "Candidate should not receive RaftEvent::RaftLogCleanUp request from Leader");
                sender.send(Err(Status::permission_denied("Not Follower"))).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::CreateSnapshotEvent => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!(
                        "Candidate node {} attempted to create snapshot.",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::SnapshotCreated(_result) => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!(
                        "Candidate node {} attempted to handle created snapshot.",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::LogPurgeCompleted(_purged_id) => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!(
                        "Learner node {} should not receive LogPurgeCompleted event.",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::JoinCluster(_join_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Candidate should not receive JoinCluster event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!(
                        "Candidate node {} receives RaftEvent::JoinCluster",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Candidate::RaftEvent::DiscoverLeader");
                sender
                    .send(Err(Status::permission_denied(
                        "Candidate should not response DiscoverLeader event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Ok(());
            }

            RaftEvent::StreamSnapshot(request, sender) => {
                debug!(?request, "Candidate::RaftEvent::StreamSnapshot");
                sender
                    .send(Err(Status::permission_denied(
                        "Candidate should not receive StreamSnapshot event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Ok(());
            }

            RaftEvent::TriggerSnapshotPush { peer_id: _ } => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!(
                        "Candidate node {} receives RaftEvent::TriggerSnapshotPush",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::PromoteReadyLearners => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!(
                        "Candidate node {} receives RaftEvent::PromoteReadyLearners",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::MembershipApplied => {
                // Candidates don't maintain cluster metadata cache
                // This event is only relevant for leaders
                debug!("Candidate ignoring MembershipApplied event");
            }

            RaftEvent::StepDownSelfRemoved => {
                // Unreachable: handled at Raft level before reaching RoleState
                unreachable!("StepDownSelfRemoved should be handled in Raft::run()");
            }
        }

        Ok(())
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
        trace!(
            "Vote myself: my_id: {}, my_new_term:{}",
            self.node_id(),
            self.current_term()
        );
        let _ = self.update_voted_for(VotedFor {
            voted_for_id: self.node_id(),
            voted_for_term: self.current_term(),
            committed: false,
        })?;
        Ok(())
    }

    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
            role: Candidate as i32,
        }
    }

    pub fn send_become_follower_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
            let error_str = format!("{e:?}");
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str).into()
        })
    }

    pub fn send_replay_raft_event(
        &self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        raft_event: RaftEvent,
    ) -> Result<()> {
        debug!("send_replay_raft_event, raft_event:{:?}", &raft_event);
        role_tx.send(RoleEvent::ReprocessEvent(Box::new(raft_event))).map_err(|e| {
            let error_str = format!("{e:?}");
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str).into()
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
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
