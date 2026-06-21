use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Candidate;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::RaftRole;
use super::Result;
use super::SharedState;
use super::StateSnapshot;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use crate::ConsensusError;
use crate::ElectionCore;
use crate::ElectionError;
use crate::ElectionTimer;
use crate::Error;
use crate::InboundEvent;
use crate::InternalEvent;
use crate::Membership;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::ReplicationCore;
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
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        _inbound_event_tx: &mpsc::Sender<InboundEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        if Instant::now() < self.timer.next_deadline() {
            return Ok(());
        }
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
                if let Err(e) = internal_event_tx.send(InternalEvent::BecomeLeader) {
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
                self.send_become_follower_event(internal_event_tx)?;
            }
            Err(e) => {
                warn!("candidate broadcast_vote_requests with error: {:?}", e);
            }
        }
        Ok(())
    }

    async fn handle_inbound_event(
        &mut self,
        inbound_event: InboundEvent,
        ctx: &RaftContext<T>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        let my_term = self.current_term();
        match inbound_event {
            InboundEvent::ReceiveVoteRequest(vote_request, sender) => {
                debug!(
                    "handle_inbound_event::InboundEvent::ReceiveVoteRequest: {:?}",
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
                    self.send_become_follower_event(&internal_event_tx)?;

                    info!(
                        "Candidate will not process ReceiveVoteRequest, it should let Follower do it."
                    );
                    self.send_replay_inbound_event(
                        &internal_event_tx,
                        InboundEvent::ReceiveVoteRequest(vote_request, sender),
                    )?;
                } else {
                    let response = VoteResponse {
                        term: my_term,
                        vote_granted: false,
                        last_log_index,
                        last_log_term,
                    };
                    if let Err(e) = sender.send(Ok(response)) {
                        // Receiver timed out and dropped — this is normal, do not crash the node
                        error!("Failed to send VoteResponse (receiver dropped): {:?}", e);
                    }
                }
            }

            InboundEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx
                    .membership()
                    .retrieve_cluster_membership_config(self.shared_state().current_leader())
                    .await;
                debug!("Candidate receive ClusterConf: {:?}", &cluster_conf);

                if let Err(e) = sender.send(Ok(cluster_conf)) {
                    // Receiver timed out and dropped — this is normal, do not crash the node
                    error!(
                        "Failed to send ClusterConf response (receiver dropped): {:?}",
                        e
                    );
                }
            }

            InboundEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
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
                if let Err(e) = sender.send(Ok(response)) {
                    // Receiver timed out and dropped — this is normal, do not crash the node
                    error!(
                        "Failed to send ClusterConfUpdate response (receiver dropped): {:?}",
                        e
                    );
                }
            }

            InboundEvent::AppendEntries(append_entries_request, senders) => {
                debug!(
                    "handle_inbound_event::InboundEvent::AppendEntries: {:?}",
                    &append_entries_request
                );

                // Important to reset timer immediatelly
                self.reset_timer();

                let my_term = self.current_term();

                // Raft §5.2: term check takes priority over log matching.
                // Any AppendEntries from a server with term >= ours means a legitimate
                // leader exists — step down immediately and let the Follower handle
                // log conflict responses. Content (log) checks come after identity (term).
                if append_entries_request.term >= my_term {
                    // Keep syncing leader_id (hot-path: ~5ns atomic store)
                    self.shared_state().set_current_leader(append_entries_request.leader_id);

                    if append_entries_request.term > my_term {
                        self.update_current_term(append_entries_request.term);
                    }
                    // Step down as Follower
                    self.send_become_follower_event(&internal_event_tx)?;

                    info!(
                        "Candidate will not process AppendEntries request, it should let Follower do it."
                    );
                    self.send_replay_inbound_event(
                        &internal_event_tx,
                        InboundEvent::AppendEntries(append_entries_request, senders),
                    )?;
                } else {
                    // request.term < my_term: stale leader, reject.
                    let response = ctx.replication_handler().check_append_entries_request_is_legal(
                        my_term,
                        &append_entries_request,
                        ctx.raft_log(),
                    );
                    debug!("Rejecting AppendEntries from stale leader: {:?}", &response);
                    for sender in senders {
                        if let Err(e) = sender.send(Ok(response)) {
                            // Receiver timed out and dropped — this is normal, do not crash the node
                            error!(
                                "Failed to send AppendEntries rejection (receiver dropped): {:?}",
                                e
                            );
                        }
                    }
                }
            }

            InboundEvent::InstallSnapshotChunk(_streaming, sender) => {
                sender
                    .send(Err(Status::permission_denied("Not Follower or Learner.")))
                    .map_err(|e| {
                        error!("Failed to send: {:?}", e);
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Follower or Learner",
                    context: format!(
                        "Candidate node {} receives InboundEvent::InstallSnapshotChunk",
                        ctx.node_id
                    ),
                }
                .into());
            }

            InboundEvent::JoinCluster(_join_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Candidate should not receive JoinCluster event.",
                    )))
                    .map_err(|e| {
                        error!("Failed to send: {:?}", e);
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Candidate",
                    required_role: "Leader",
                    context: format!(
                        "Candidate node {} receives InboundEvent::JoinCluster",
                        ctx.node_id
                    ),
                }
                .into());
            }

            InboundEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Candidate::InboundEvent::DiscoverLeader");
                sender
                    .send(Err(Status::permission_denied(
                        "Candidate should not response DiscoverLeader event.",
                    )))
                    .map_err(|e| {
                        error!("Failed to send: {:?}", e);
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;

                return Ok(());
            }

            InboundEvent::StreamSnapshot(_ack_rx, _chunk_tx, startup_tx) => {
                debug!("Candidate::InboundEvent::StreamSnapshot");
                warn!("Candidate should not receive StreamSnapshot event.");
                if let Err(e) = startup_tx.send(Err(Status::failed_precondition("Not the leader")))
                {
                    error!(
                        ?e,
                        "StreamSnapshot startup_tx send failed: gRPC receiver already dropped"
                    );
                }
                return Ok(());
            }

            InboundEvent::FatalError { source, error } => {
                error!("[Candidate] Fatal error from {}: {}", source, error);
                return Err(crate::Error::Fatal(format!(
                    "Fatal error from {source}: {error}"
                )));
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
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        internal_event_tx.send(InternalEvent::BecomeFollower(None)).map_err(|e| {
            error!("Failed to send: {:?}", e);
            NetworkError::SingalSendFailed(format!("{:?}", e)).into()
        })
    }

    pub fn send_replay_inbound_event(
        &self,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        inbound_event: InboundEvent,
    ) -> Result<()> {
        debug!(
            "send_replay_inbound_event, inbound_event:{:?}",
            &inbound_event
        );
        internal_event_tx
            .send(InternalEvent::ReprocessEvent(Box::new(inbound_event)))
            .map_err(|e| {
                error!("Failed to send: {:?}", e);
                NetworkError::SingalSendFailed(format!("{:?}", e)).into()
            })
    }

    #[cfg(test)]
    pub(crate) fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        Self {
            shared_state: SharedState::new(node_id, None, None),
            timer: ElectionTimer::new((
                node_config.raft.election.election_timeout_min,
                node_config.raft.election.election_timeout_max,
            )),
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
