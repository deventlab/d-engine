use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotResponse;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use tokio::sync::mpsc::{self};
use tokio::time::Instant;
use tonic::Status;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::candidate_state::CandidateState;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use super::role_state::check_and_trigger_snapshot;
use crate::ConsensusError;
use crate::Membership;
use crate::MembershipError;
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
use crate::Transport;
use crate::TypeConfig;
use crate::alias::MOF;

/// Learner node's state in Raft cluster.
///
/// This state contains both:
/// - **Persistent State**: Should be written to stable storage before responding to RPCs
/// - **Volatile State**: Reinitialized after node restarts
///
/// Learners are non-voting members participating in log replication but not in leader election.
/// This state tracks the minimal required information for log synchronization.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
pub struct LearnerState<T: TypeConfig> {
    // -- Core State --
    /// Shared cluster state with concurrency control
    pub shared_state: SharedState,

    /// === Persistent State (MUST be on disk) ===
    /// The last log position that has been **physically removed** from stable storage.
    ///
    /// This value is atomically updated when:
    /// 1. A new snapshot is persisted (marking logs up to `last_included_index` as purgeable)
    /// 2. The background purge task completes successfully
    ///
    /// Raft safety invariant:
    /// Any log entry with index ≤ `last_purged_index` is guaranteed to be
    /// reflected in the latest snapshot.
    pub last_purged_index: Option<LogId>,

    // -- Snapshot Management --
    /// Prevents concurrent snapshot creation
    ///
    /// Per industry best practices :
    /// - Protects against concurrent snapshot requests
    /// - Ensures snapshot consistency at Raft layer
    /// - Reduces unnecessary tokio::spawn overhead
    pub(crate) snapshot_in_progress: AtomicBool,

    // -- Cluster Configuration --
    /// Cached Raft node configuration (immutable shared reference)
    ///
    /// Contains:
    /// - Cluster membership topology
    /// - Timeout parameters
    /// - Performance tuning parameters
    pub(super) node_config: Arc<RaftNodeConfig>,

    // -- Type System Marker --
    /// Phantom type marker for compile-time validation
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
            "Node {} term {} transitioning to Follower",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Learner → Follower (term {})",
            self.node_id(),
            self.current_term()
        );

        // Print promotion message (Plan B)
        crate::utils::cluster_printer::print_learner_promoted_to_voter(self.node_id());

        Ok(RaftRole::Follower(Box::new(self.into())))
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
        false
    }
    fn reset_timer(&mut self) {
        // Nothing to do for Learner
    }

    fn next_deadline(&self) -> Instant {
        Instant::now() + Duration::from_secs(24 * 60 * 60) //1 day
    }

    async fn tick(
        &mut self,
        _role_event_tx: &mpsc::UnboundedSender<RoleEvent>,
        _raft_tx: &mpsc::Sender<RaftEvent>,
        _ctx: &RaftContext<T>,
    ) -> Result<()> {
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
                info!("handle_raft_event::ReceiveVoteRequest. Learner cannot vote.");
                // 1. Update term FIRST if needed
                if vote_request.term > my_term {
                    self.update_current_term(vote_request.term);
                }

                // 2. Response sender with vote_granted=false
                let last_log_id =
                    ctx.raft_log().last_log_id().unwrap_or(LogId { index: 0, term: 0 });
                let response = VoteResponse {
                    term: my_term,
                    vote_granted: false,
                    last_log_index: last_log_id.index,
                    last_log_term: last_log_id.term,
                };
                debug!(
                    "Response candidate_{:?} with response: {:?}",
                    vote_request.candidate_id, response
                );

                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{e:?}");
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
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }

            RaftEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
                let current_conf_version = ctx.membership().get_cluster_conf_version().await;

                let current_leader_id = self.shared_state().current_leader();

                debug!(?current_leader_id, %current_conf_version, ?cluste_conf_change_request,
                    "Learner receive ClusterConfUpdate"
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

            RaftEvent::InstallSnapshotChunk(stream, sender) => {
                // Create ACK channel (follower sends ACKs to leader)
                let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(32);

                // Spawn ACK handler to send final response.
                // Drain ALL ACKs and use the last one — the channel closes only after
                // apply_snapshot_stream_from_leader returns (success or error), so the
                // last ACK reflects the true final outcome of the full snapshot install.
                tokio::spawn(async move {
                    let mut last_ack: Option<SnapshotAck> = None;
                    while let Some(ack) = ack_rx.recv().await {
                        last_ack = Some(ack);
                    }
                    match last_ack {
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
                    error!(?e, "Learner handle  RaftEvent::InstallSnapshotChunk");
                    return Err(e);
                }

                // Advance raft log purge boundary to snapshot's last_included so that
                // last_log_id() returns the correct position after snapshot install.
                if let Some(metadata) = ctx.state_machine_handler().get_latest_snapshot_metadata()
                    && let Some(last_included) = metadata.last_included
                {
                    if let Err(e) = ctx.raft_log().purge_logs_up_to(last_included).await {
                        error!(?e, "Failed to set raft log boundary after snapshot install");
                    } else {
                        info!(
                            ?last_included,
                            "Learner raft log boundary set after InstallSnapshotChunk"
                        );
                    }
                }
            }

            RaftEvent::CreateSnapshotEvent => {
                // Prevent duplicate snapshot creation
                if self.snapshot_in_progress.load(Ordering::Acquire) {
                    info!(
                        "Learner snapshot creation already in progress. Skipping duplicate request."
                    );
                    return Ok(());
                }

                self.snapshot_in_progress.store(true, Ordering::Release);
                let state_machine_handler = ctx.state_machine_handler().clone();

                // Use spawn to perform snapshot creation in the background
                tokio::spawn(async move {
                    let result = state_machine_handler.create_snapshot().await;
                    info!(
                        "Learner SnapshotCreated event will be processed in another event thread"
                    );
                    if let Err(e) = super::role_state::send_replay_raft_event(
                        &role_tx,
                        RaftEvent::SnapshotCreated(result),
                    ) {
                        error!("Learner failed to send snapshot creation result: {}", e);
                    }
                });
            }

            RaftEvent::SnapshotCreated(result) => {
                // Reset snapshot_in_progress flag
                self.snapshot_in_progress.store(false, Ordering::SeqCst);

                // Per Raft §7: Learner independently purges logs after snapshot generation
                match result {
                    Ok((metadata, _path)) => {
                        if let Some(last_included) = metadata.last_included {
                            info!(?last_included, "Learner snapshot created, purging logs");

                            // Learner independently decides to purge after snapshot
                            if self.can_purge_logs(self.last_purged_index, last_included) {
                                match ctx.purge_executor().execute_purge(last_included).await {
                                    Ok(_) => {
                                        self.last_purged_index = Some(last_included);
                                        info!(?last_included, "Learner logs purged successfully");
                                    }
                                    Err(e) => {
                                        error!(?e, "Failed to purge logs after snapshot");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(?e, "Learner snapshot creation failed");
                    }
                }
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
                        "Learner should not receive JoinCluster event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!(
                        "Learner node {} receives RaftEvent::JoinCluster",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Learner::RaftEvent::DiscoverLeader");
                sender
                    .send(Err(Status::permission_denied(
                        "Learner should not response DiscoverLeader event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Ok(());
            }
            RaftEvent::StreamSnapshot(request, sender) => {
                debug!(?request, "Learner::RaftEvent::StreamSnapshot");
                sender
                    .send(Err(Status::permission_denied(
                        "Learner should not receive StreamSnapshot event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Ok(());
            }
            RaftEvent::PromoteReadyLearners => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!(
                        "Learner node {} receives RaftEvent::PromoteReadyLearners",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::MembershipApplied => {
                // Check if this learner has been promoted to Voter
                let my_id = self.node_id();
                let node_meta = ctx.membership().retrieve_node_meta(my_id).await;

                if let Some(meta) = node_meta {
                    // Check if role is Voter (any role except LEARNER)
                    // FOLLOWER=0, CANDIDATE=1, LEADER=2, LEARNER=3
                    let is_voter = meta.role != NodeRole::Learner as i32;

                    if is_voter {
                        info!(
                            "Learner {} detected promotion to Voter (role={}), transitioning to Follower",
                            my_id, meta.role
                        );

                        // Transition to Follower role
                        role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
                            let error_str = format!("{e:?}");
                            error!("Failed to send BecomeFollower event: {}", error_str);
                            NetworkError::SingalSendFailed(error_str)
                        })?;
                    } else {
                        debug!(
                            "Learner {} still in Learner role (role={}) after MembershipApplied",
                            my_id, meta.role
                        );
                    }
                } else {
                    warn!(
                        "Learner {} not found in membership after MembershipApplied",
                        my_id
                    );
                }
            }

            RaftEvent::FatalError { source, error } => {
                error!("[Learner] Fatal error from {}: {}", source, error);
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

    async fn join_cluster(
        &self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // 1. Check if there is a Leader address (as specified in the configuration)
        let membership = ctx.membership();
        let leader_id = match self.shared_state().current_leader() {
            None => {
                // 2. Trigger broadcast discovery
                self.broadcast_discovery(membership.clone(), ctx).await?
            }
            Some(leader_id) => leader_id,
        };

        debug!(%leader_id, "join_cluster, leadder_id");

        // 3. Continue the original Join process
        let node_config = ctx.node_config();

        // Get this node's configured status from initial_cluster
        // Node MUST be defined in initial_cluster with explicit status
        let node_status = node_config
            .cluster
            .initial_cluster
            .iter()
            .find(|n| n.id == node_config.cluster.node_id)
            .map(|n| n.status)
            .ok_or_else(|| MembershipError::JoinClusterFailed(node_config.cluster.node_id))?;

        let response = ctx
            .transport()
            .join_cluster(
                leader_id,
                JoinRequest {
                    node_id: node_config.cluster.node_id,
                    node_role: Learner as i32,
                    address: node_config.cluster.listen_address.to_string(),
                    status: node_status,
                },
                node_config.retry.join_cluster,
                membership.clone(),
            )
            .await?;

        debug!(?response, "transport::join_cluster");
        if !response.success {
            return Err(MembershipError::JoinClusterFailed(self.shared_state.node_id).into());
        }

        // 4. mark leader_id (hot-path: ~5ns atomic store)
        self.shared_state().set_current_leader(leader_id);

        // Print join success message (Plan B)
        crate::utils::cluster_printer::print_learner_join_success(
            self.shared_state.node_id,
            leader_id,
        );

        Ok(())
    }

    // Update the fetch_initial_snapshot method
    async fn fetch_initial_snapshot(
        &self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        let leader_id = self.shared_state().current_leader().ok_or(MembershipError::NoLeader)?;

        // Create ACK channel (learner sends ACKs to leader)
        let (ack_tx, ack_rx) = mpsc::channel(32);

        // Get snapshot stream from leader (with ACK feedback)
        let snapshot_stream = ctx
            .transport()
            .request_snapshot_from_leader(
                leader_id,
                ack_rx,
                &ctx.node_config.retry.install_snapshot,
                ctx.membership().clone(),
            )
            .await?;

        debug!("Install snapshot and handle ACK feedback");
        // Install snapshot and handle ACK feedback
        ctx.handlers
            .state_machine_handler
            .apply_snapshot_stream_from_leader(
                self.current_term(),
                snapshot_stream,
                ack_tx,
                &ctx.node_config.raft.snapshot,
            )
            .await?;

        // After snapshot install the raft log is empty. Advance the purge boundary
        // to last_included so last_log_id() returns the correct position and the
        // leader starts sending entries from last_included.index + 1 instead of 1.
        if let Some(metadata) = ctx.state_machine_handler().get_latest_snapshot_metadata()
            && let Some(last_included) = metadata.last_included
        {
            ctx.raft_log().purge_logs_up_to(last_included).await?;
            info!(
                ?last_included,
                "Learner raft log boundary set after snapshot install"
            );
        }

        info!("Successfully fetched and installed initial snapshot");
        Ok(())
    }

    async fn handle_apply_completed(
        &mut self,
        last_index: u64,
        _results: Vec<crate::ApplyResult>,
        ctx: &crate::RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> crate::Result<()> {
        // Per Raft §7: each server takes snapshots independently.
        check_and_trigger_snapshot(
            last_index,
            Learner as i32,
            self.current_term(),
            ctx,
            role_tx,
        )
    }
}

impl<T: TypeConfig> LearnerState<T> {
    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
            role: Learner as i32,
        }
    }
}

impl<T: TypeConfig> LearnerState<T> {
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        LearnerState {
            shared_state: SharedState::new(node_id, None, None),
            last_purged_index: None,
            snapshot_in_progress: AtomicBool::new(false),
            node_config,
            _marker: PhantomData,
        }
    }

    /// Determines if logs can be safely purged up to the given index
    ///
    /// Per Raft §7: Learner independently purges logs after snapshot generation
    ///
    /// # Safety Checks
    /// 1. Committed Entry Guarantee: last_included < commit_index
    /// 2. Monotonic Purge: last_purged_index < last_included
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

    pub async fn broadcast_discovery(
        &self,
        membership: Arc<MOF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<u32> {
        let retry_policy = ctx.node_config.retry.auto_discovery;
        let mut retry_count = 0;
        let mut current_delay = Duration::from_millis(retry_policy.base_delay_ms);

        let request = LeaderDiscoveryRequest {
            node_id: ctx.node_id,
            requester_address: ctx.node_config.cluster.listen_address.to_string(),
        };

        let rpc_enable_compression = ctx.node_config.raft.auto_join.rpc_enable_compression;
        loop {
            // Execute discovery attempt with timeout
            let discovery_result = tokio::time::timeout(
                Duration::from_millis(retry_policy.timeout_ms),
                ctx.transport.discover_leader(
                    request.clone(),
                    rpc_enable_compression,
                    membership.clone(),
                ),
            )
            .await;

            debug!(?discovery_result);

            match discovery_result {
                Ok(responses) => {
                    if let Some(leader_id) = self.select_valid_leader(responses?).await {
                        debug!(%leader_id, "find valid leader");
                        return Ok(leader_id);
                    }
                }
                Err(_) => {
                    warn!(
                        "Discovery request timed out after {}ms",
                        retry_policy.timeout_ms
                    );
                }
            }

            // Check retry limits
            retry_count += 1;
            if retry_policy.max_retries > 0 && retry_count >= retry_policy.max_retries {
                error!("Discovery failed after {} retries", retry_count);
                return Err(NetworkError::RetryTimeoutError(Duration::from_millis(
                    retry_policy.timeout_ms,
                ))
                .into());
            }

            // Calculate next backoff delay
            current_delay = Duration::from_millis(
                (current_delay.as_millis() as u64)
                    .saturating_mul(2) // Exponential backoff
                    .min(retry_policy.max_delay_ms), // Cap at maximum delay
            );

            debug!(
                "Retrying discovery in {}ms (attempt {}/{})",
                current_delay.as_millis(),
                retry_count,
                retry_policy.max_retries
            );

            tokio::time::sleep(current_delay).await;
        }
    }

    /// @return Option<(leader_id, Channel)>
    pub async fn select_valid_leader(
        &self,
        responses: Vec<LeaderDiscoveryResponse>,
    ) -> Option<u32> {
        // Filter invalid responses
        let mut valid_responses: Vec<_> =
            responses.into_iter().filter(|r| r.leader_id != 0 && r.term > 0).collect();

        if valid_responses.is_empty() {
            return None;
        }

        // Sort by term in descending order, node_id in descending order
        valid_responses
            .sort_by(|a, b| b.term.cmp(&a.term).then_with(|| b.leader_id.cmp(&a.leader_id)));

        trace!(?valid_responses);

        // Select the response with the highest term
        let resp = valid_responses.first().unwrap();

        Some(resp.leader_id)
    }
}
impl<T: TypeConfig> From<&FollowerState<T>> for LearnerState<T> {
    fn from(follower_state: &FollowerState<T>) -> Self {
        Self {
            shared_state: follower_state.shared_state.clone(),
            node_config: follower_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            last_purged_index: None, //TODO
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for LearnerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            node_config: candidate_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            last_purged_index: candidate_state.last_purged_index,
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
