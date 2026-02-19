use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::election::VoteResponse;

use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotResponse;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::warn;

use super::HardState;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::candidate_state::CandidateState;
use super::leader_state::LeaderState;
use super::learner_state::LearnerState;
use super::role_state::RaftRoleState;
use super::role_state::check_and_trigger_snapshot;
use crate::ConsensusError;
use crate::ElectionCore;
use crate::ElectionTimer;
use crate::Membership;
use crate::NetworkError;
use crate::PurgeExecutor;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::RoleEvent;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::utils::cluster::error;

/// Follower node's state in Raft consensus.
///
/// Maintains state required for responding to leader heartbeats and log replication.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
pub struct FollowerState<T: TypeConfig> {
    // -- Core State --
    /// Shared cluster state with mutex protection
    pub shared_state: SharedState,

    // -- Log Compaction & Purge --
    /// === Persistent State ===
    /// Last physically purged log index (inclusive)
    pub last_purged_index: Option<LogId>,

    // -- Snapshot Management --
    /// Prevents concurrent snapshot creation
    ///
    /// Per industry best practices:
    /// - Protects against concurrent snapshot requests
    /// - Ensures snapshot consistency at Raft layer
    /// - Reduces unnecessary tokio::spawn overhead
    pub(crate) snapshot_in_progress: AtomicBool,

    // -- Cluster Configuration --
    /// Node configuration (shared immutable reference)
    pub(super) node_config: Arc<RaftNodeConfig>,

    // -- Election Timing --
    /// Leader heartbeat detection timer
    ///
    /// Manages:
    /// - Heartbeat timeout tracking
    /// - Transition to candidate state when timeout occurs
    pub(super) timer: ElectionTimer,

    // -- Type System Marker --
    /// Phantom data for type parameterization
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
            "Node {} term {} transitioning to Candidate",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Follower → Candidate (term {})",
            self.node_id(),
            self.current_term()
        );
        Ok(RaftRole::Candidate(Box::new(self.into())))
    }
    fn become_follower(&self) -> Result<RaftRole<T>> {
        warn!("I am follower already");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_learner(&self) -> Result<RaftRole<T>> {
        info!(
            "Node {} term {} transitioning to Learner",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Follower → Learner (term {})",
            self.node_id(),
            self.current_term()
        );
        Ok(RaftRole::Learner(Box::new(self.into())))
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
        _ctx: &RaftContext<T>,
    ) -> Result<()> {
        if Instant::now() < self.timer.next_deadline() {
            return Ok(());
        }

        debug!("reset timer");
        self.timer.reset();

        debug!("follower::start_election...");

        role_tx.send(RoleEvent::BecomeCandidate).map_err(|e| {
            let error_str = format!("{e:?}");
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str)
        })?;

        Ok(())
    }

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let state_snapshot = self.state_snapshot();
        let my_term = self.current_term();

        match raft_event {
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                let candidate_id = vote_request.candidate_id;

                let LogId {
                    index: last_log_index,
                    term: last_log_term,
                } = ctx.raft_log().last_log_id().unwrap_or(LogId { index: 0, term: 0 });

                match ctx
                    .election_handler()
                    .handle_vote_request(
                        vote_request,
                        my_term,
                        self.voted_for().unwrap(),
                        ctx.raft_log(),
                    )
                    .await
                {
                    Ok(state_update) => {
                        debug!(
                            "handle_vote_request success with state_update: {:?}",
                            &state_update
                        );

                        // 1. Update term FIRST if needed
                        if let Some(new_term) = state_update.term_update {
                            self.update_current_term(new_term);
                        }
                        // 2. If update my voted_for
                        let new_voted_for = state_update.new_voted_for;
                        if let Some(v) = new_voted_for {
                            match self.update_voted_for(v) {
                                Ok(_) => {}
                                Err(e) => {
                                    error("update_voted_for", &e);
                                    return Err(e);
                                }
                            }
                        }

                        let response = VoteResponse {
                            term: my_term,
                            vote_granted: new_voted_for.is_some(),
                            last_log_index,
                            last_log_term,
                        };
                        debug!(
                            "Response candidate_{:?} with response: {:?}",
                            candidate_id, response
                        );

                        sender.send(Ok(response)).map_err(|e| {
                            let error_str = format!("{e:?}");
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
                            let error_str = format!("{e:?}");
                            error!("Failed to send: {}", error_str);
                            NetworkError::SingalSendFailed(error_str)
                        })?;
                        error("handle_raft_event::RaftEvent::ReceiveVoteRequest", &e);
                        return Err(e);
                    }
                }
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx
                    .membership()
                    .retrieve_cluster_membership_config(self.shared_state().current_leader())
                    .await;
                debug!("Follower receive ClusterConf: {:?}", &cluster_conf);

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
                    "Follower receive ClusterConfUpdate"
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
                self.handle_append_entries_request_workflow(
                    append_entries_request,
                    sender,
                    ctx,
                    role_tx,
                    &state_snapshot,
                )
                .await?;
            }

            RaftEvent::FlushReadBuffer => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Follower",
                    required_role: "Leader",
                    context: format!(
                        "Follower node {} attempted to create snapshot.",
                        ctx.node_id
                    ),
                }
                .into());
            }
            RaftEvent::InstallSnapshotChunk(stream, sender) => {
                // Create ACK channel (follower sends ACKs to leader)
                let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(32);

                // Spawn ACK handler to send final response
                tokio::spawn(async move {
                    match ack_rx.recv().await {
                        Some(final_ack) => {
                            let response = SnapshotResponse {
                                term: my_term,
                                success: final_ack.status == (ChunkStatus::Accepted as i32),
                                next_chunk: final_ack.next_requested,
                            };
                            let _ = sender.send(Ok(response));
                        }
                        None => {
                            let _ = sender.send(Err(Status::internal("ACK channel closed")));
                        }
                    }
                });

                if let Err(e) = ctx
                    .handlers
                    .state_machine_handler
                    .apply_snapshot_stream_from_leader(
                        my_term,
                        stream,
                        ack_tx,
                        &ctx.node_config.raft.snapshot,
                    )
                    .await
                {
                    error!(?e, "Follower handle  RaftEvent::InstallSnapshotChunk");
                    return Err(e);
                }
            }

            RaftEvent::LogPurgeCompleted(_purged_id) => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Follower",
                    required_role: "Leader",
                    context: format!(
                        "Follower node {} should not receive LogPurgeCompleted event.",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::CreateSnapshotEvent => {
                // Prevent duplicate snapshot creation
                if self.snapshot_in_progress.load(Ordering::Acquire) {
                    info!(
                        "Follower snapshot creation already in progress. Skipping duplicate request."
                    );
                    return Ok(());
                }

                self.snapshot_in_progress.store(true, Ordering::Release);
                let state_machine_handler = ctx.state_machine_handler().clone();

                // Use spawn to perform snapshot creation in the background
                tokio::spawn(async move {
                    let result = state_machine_handler.create_snapshot().await;
                    info!(
                        "Follower SnapshotCreated event will be processed in another event thread"
                    );
                    if let Err(e) = super::role_state::send_replay_raft_event(
                        &role_tx,
                        RaftEvent::SnapshotCreated(result),
                    ) {
                        error!("Follower failed to send snapshot creation result: {}", e);
                    }
                });
            }

            RaftEvent::SnapshotCreated(result) => {
                // Reset snapshot_in_progress flag
                self.snapshot_in_progress.store(false, Ordering::SeqCst);

                // Per Raft §7: Follower independently purges logs after snapshot generation
                match result {
                    Ok((metadata, _path)) => {
                        if let Some(last_included) = metadata.last_included {
                            info!(?last_included, "Follower snapshot created, purging logs");

                            // Follower independently decides to purge after snapshot
                            if self.can_purge_logs(self.last_purged_index, last_included) {
                                match ctx.purge_executor().execute_purge(last_included).await {
                                    Ok(_) => {
                                        self.last_purged_index = Some(last_included);
                                        info!(?last_included, "Follower logs purged successfully");
                                    }
                                    Err(e) => {
                                        error!(?e, "Failed to purge logs after snapshot");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(?e, "Follower snapshot creation failed");
                    }
                }
            }

            RaftEvent::JoinCluster(_join_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Follower should not receive JoinCluster event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Follower",
                    required_role: "Leader",
                    context: format!(
                        "Follower node {} receives RaftEvent::JoinCluster",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Follower::RaftEvent::DiscoverLeader");
                sender
                    .send(Err(Status::permission_denied(
                        "Follower should not response DiscoverLeader event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Ok(());
            }

            RaftEvent::StreamSnapshot(request, sender) => {
                debug!(?request, "Follower::RaftEvent::StreamSnapshot");
                sender
                    .send(Err(Status::permission_denied(
                        "Follower should not receive StreamSnapshot event.",
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
                    current_role: "Follower",
                    required_role: "Leader",
                    context: format!(
                        "Follower node {} receives RaftEvent::TriggerSnapshotPush",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::PromoteReadyLearners => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Follower",
                    required_role: "Leader",
                    context: format!(
                        "Follower node {} receives RaftEvent::PromoteReadyLearners",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::MembershipApplied => {
                // Followers don't maintain cluster metadata cache
                // This event is only relevant for leaders
                trace!("Follower ignoring MembershipApplied event");
            }

            RaftEvent::ApplyCompleted {
                last_index,
                results: _,
            } => {
                // Per Raft §7: each server takes snapshots independently.
                check_and_trigger_snapshot(
                    last_index,
                    Follower as i32,
                    self.current_term(),
                    ctx,
                    &role_tx,
                )?;
            }

            RaftEvent::FatalError { source, error } => {
                error!("[Follower] Fatal error from {}: {}", source, error);
                return Err(crate::Error::Fatal(format!(
                    "Fatal error from {source}: {error}"
                )));
            }

            RaftEvent::StepDownSelfRemoved => {
                // Unreachable: handled at Raft level before reaching RoleState
                unreachable!("StepDownSelfRemoved should be handled in Raft::run()");
            }
        }

        Ok(())
    }
}

impl<T: TypeConfig> FollowerState<T> {
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
        hard_state_from_db: Option<HardState>,
        last_applied_index_option: Option<u64>,
    ) -> Self {
        trace!(
            node_config.raft.election.election_timeout_min,
            "FollowerState::new"
        );

        Self {
            shared_state: SharedState::new(node_id, hard_state_from_db, last_applied_index_option),
            timer: ElectionTimer::new((
                node_config.raft.election.election_timeout_min,
                node_config.raft.election.election_timeout_max,
            )),
            node_config,
            snapshot_in_progress: AtomicBool::new(false),
            _marker: PhantomData,
            last_purged_index: None, /*TODO
                                      * scheduled_purge_upto: None, */
        }
    }

    /// The fun will retrieve current state snapshot
    #[tracing::instrument]
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            role: Follower as i32,
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
        }
    }

    /// Determines if logs prior to `last_included_in_request` can be safely discarded.
    ///
    /// Implements the critical log compaction safety check from Raft paper §7.2:
    /// > "Raft never commits log entries from previous terms by counting replicas"
    ///
    /// # Invariants (MUST ALL hold)
    /// 1. Leader-guaranteed stability: `last_included_in_request.index` < self.commit_index
    ///    - Ensures we never truncate uncommitted entries (gap prevents Figure 8 bugs)
    ///    - Leader must have replicated this index to a quorum before sending purge
    ///
    /// 2. Monotonic advancement: `last_purge_index` < last_included_in_request.index
    ///    - Prevents out-of-order purge operations
    ///    - Maintains purge sequence strictly increasing
    ///
    /// 3. State machine safety:
    ///    - A valid snapshot covering `last_included_in_request` must exist
    ///    - Verified before entering this function via snapshot integrity checks
    ///
    /// # Gap Design Intent
    /// The `index < commit_index` (not ≤) ensures:
    /// - At least one committed entry remains after purge
    /// - Critical for follower's log matching property during reelections
    /// - Prevents "phantom entries" when combined with §5.4.2 election restriction
    #[instrument(skip(self))]
    pub fn can_purge_logs(
        &self,
        last_purge_index: Option<LogId>,
        last_included_in_request: LogId,
    ) -> bool {
        let commit_check = last_included_in_request.index < self.commit_index();

        let monotonic_check = last_purge_index
            .map(|lid| lid.index < last_included_in_request.index)
            .unwrap_or(true);

        commit_check && monotonic_check
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for FollowerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            timer: ElectionTimer::new((
                candidate_state.node_config.raft.election.election_timeout_min,
                candidate_state.node_config.raft.election.election_timeout_max,
            )),
            node_config: candidate_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            last_purged_index: candidate_state.last_purged_index,
            // scheduled_purge_upto: None,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&LeaderState<T>> for FollowerState<T> {
    fn from(leader_state: &LeaderState<T>) -> Self {
        Self {
            shared_state: leader_state.shared_state.clone(),
            timer: ElectionTimer::new((
                leader_state.node_config.raft.election.election_timeout_min,
                leader_state.node_config.raft.election.election_timeout_max,
            )),
            node_config: leader_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(
                leader_state.snapshot_in_progress.load(Ordering::SeqCst),
            ),
            last_purged_index: leader_state.last_purged_index,
            // scheduled_purge_upto: None,
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
                learner_state.node_config.raft.election.election_timeout_min,
                learner_state.node_config.raft.election.election_timeout_max,
            )),
            node_config: learner_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            last_purged_index: None, //TODO
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
