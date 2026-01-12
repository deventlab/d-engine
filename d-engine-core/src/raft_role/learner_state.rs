use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use d_engine_proto::client::ClientResponse;
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
use super::can_serve_read_locally;
use super::candidate_state::CandidateState;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use crate::ConsensusError;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
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
                        debug!("Learner serving local read: {:?}", response);
                        sender.send(Ok(response)).map_err(|e| {
                            error!("Failed to send local read response: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{e:?}"))
                        })?;
                    }
                    None => {
                        // Policy requires leader access - return NOT_LEADER with leader metadata
                        let response = self.create_not_leader_response(ctx).await;
                        sender.send(Ok(response)).map_err(|e| {
                            error!("Failed to send NOT_LEADER response: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{e:?}"))
                        })?;
                    }
                }
            }
            RaftEvent::FlushReadBuffer => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!("Learner node {} attempted to create snapshot.", ctx.node_id),
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
                    error!(?e, "Learner handle  RaftEvent::InstallSnapshotChunk");
                    return Err(e);
                }
            }

            RaftEvent::RaftLogCleanUp(purchase_log_request, sender) => {
                debug!(?purchase_log_request, "RaftEvent::RaftLogCleanUp");

                warn!(%self.shared_state.node_id, "Learner should not receive RaftEvent::RaftLogCleanUp request from Leader");
                sender.send(Err(Status::permission_denied("Not Follower"))).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::CreateSnapshotEvent => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!("Learner node {} attempted to create snapshot.", ctx.node_id),
                }
                .into());
            }

            RaftEvent::SnapshotCreated(_result) => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!(
                        "Learner node {} attempted to handle created snapshot.",
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
            RaftEvent::TriggerSnapshotPush { peer_id: _ } => {
                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!(
                        "Learner node {} receives RaftEvent::TriggerSnapshotPush",
                        ctx.node_id
                    ),
                }
                .into());
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

        info!("Successfully fetched and installed initial snapshot");
        Ok(())
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
            node_config,
            _marker: PhantomData,
        }
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
            // last_purged_index: follower_state.last_purged_index,
            // scheduled_purge_upto: follower_state.scheduled_purge_upto,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for LearnerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            node_config: candidate_state.node_config.clone(),
            // last_purged_index: candidate_state.last_purged_index,
            // scheduled_purge_upto: None,
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
