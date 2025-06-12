use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{self};
use tokio::time::Instant;
use tonic::async_trait;
use tonic::transport::Channel;
use tonic::Status;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing::{debug, trace};

use super::candidate_state::CandidateState;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::LEARNER;
use crate::alias::POF;
use crate::proto::client::ClientResponse;
use crate::proto::cluster::{ClusterConfUpdateResponse, JoinRequest, LeaderDiscoveryRequest, LeaderDiscoveryResponse};
use crate::proto::election::VoteResponse;
use crate::proto::election::VotedFor;
use crate::proto::error::ErrorCode;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftLog;
use crate::RoleEvent;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::{ConsensusError, Membership};
use crate::{MembershipError, Result};
use crate::{PeerChannels, RaftEvent};
use crate::{RaftNodeConfig, Transport};

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
        warn!("Learner should not has timer");

        false
    }
    fn reset_timer(&mut self) {
        warn!("Learner should not be asked to reset timer");
    }
    fn next_deadline(&self) -> Instant {
        warn!("Learner should not be asked for next_deadline");
        Instant::now()
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
        warn!("Learner should not has timer tick");

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
                let current_conf_version = ctx.membership().get_cluster_conf_version();

                let current_leader_id = ctx.membership().current_leader_id();

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
                        ClusterConfUpdateResponse::internal_error(my_id, my_term, current_conf_version)
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
                //TODO: direct to leader
                // self.redirect_to_leader(client_propose_request).await;
                sender
                    .send(Ok(ClientResponse::client_error(ErrorCode::NotLeader)))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
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
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
            }

            RaftEvent::InstallSnapshotChunk(stream, sender) => {
                ctx.handlers
                    .state_machine_handler
                    .install_snapshot_chunk(my_term, stream, sender)
                    .await?;
            }

            RaftEvent::RaftLogCleanUp(purchase_log_request, sender) => {
                debug!(?purchase_log_request, "RaftEvent::RaftLogCleanUp");

                warn!(%self.shared_state.node_id, "Learner should not receive RaftEvent::RaftLogCleanUp request from Leader");
                sender
                    .send(Err(Status::permission_denied("Not Follower")))
                    .map_err(|e| {
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
                .into())
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
                    context: format!("Learner node {} receives RaftEvent::JoinCluster", ctx.node_id),
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

                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!("Learner node {} should not response DiscoverLeader event", ctx.node_id),
                }
                .into());
            }
        }
        return Ok(());
    }

    async fn join_cluster(
        &self,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // 1. Check if there is a Leader address (as specified in the configuration)
        let leader_channel: Channel = match ctx.membership().current_leader_id() {
            None => {
                // 2. Trigger broadcast discovery
                self.broadcast_discovery(peer_channels.clone(), ctx).await?
            }
            Some(leader_id) => match peer_channels.get_peer_channel(leader_id) {
                Some(channel_with_address) => channel_with_address.channel,
                None => {
                    warn!(%leader_id, "peer_channels.get_peer_channel(leader_id) found none");
                    return Err(MembershipError::JoinClusterFailed(self.shared_state.node_id).into());
                }
            },
        };

        // 3. Continue the original Join process
        let node_config = ctx.node_config();
        let response = ctx
            .transport()
            .join_cluster(
                leader_channel,
                JoinRequest {
                    node_id: node_config.cluster.node_id,
                    address: node_config.cluster.listen_address.to_string(),
                },
                node_config.retry.join_cluster,
            )
            .await?;

        debug!(?response, "transport::join_cluster");
        if !response.success {
            return Err(MembershipError::JoinClusterFailed(self.shared_state.node_id).into());
        }

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
            role: LEARNER,
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

    pub(crate) async fn broadcast_discovery(
        &self,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<Channel> {
        let retry_policy = ctx.node_config.retry.auto_discovery;
        let mut retry_count = 0;
        let mut current_delay = Duration::from_millis(retry_policy.base_delay_ms);

        let voting_members = peer_channels.voting_members();
        let request = LeaderDiscoveryRequest {
            node_id: ctx.node_id,
            requester_address: ctx.node_config.cluster.listen_address.to_string(),
        };

        let rpc_enable_compression = ctx.node_config.raft.auto_join.rpc_enable_compression;
        loop {
            // Execute discovery attempt with timeout
            let discovery_result = tokio::time::timeout(
                Duration::from_millis(retry_policy.timeout_ms),
                ctx.transport
                    .discover_leader(voting_members.clone(), request.clone(), rpc_enable_compression),
            )
            .await;

            trace!(?discovery_result);

            match discovery_result {
                Ok(responses) => {
                    if let Some((leader_id, leader_channel)) =
                        self.select_valid_leader(responses?, peer_channels.clone()).await
                    {
                        debug!(%leader_id, "find valid leader");
                        return Ok(leader_channel);
                    }
                }
                Err(_) => {
                    warn!("Discovery request timed out after {}ms", retry_policy.timeout_ms);
                }
            }

            // Check retry limits
            retry_count += 1;
            if retry_policy.max_retries > 0 && retry_count >= retry_policy.max_retries {
                error!("Discovery failed after {} retries", retry_count);
                return Err(NetworkError::RetryTimeoutError(Duration::from_millis(retry_policy.timeout_ms)).into());
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
    pub(super) async fn select_valid_leader(
        &self,
        responses: Vec<LeaderDiscoveryResponse>,
        peer_channels: Arc<POF<T>>,
    ) -> Option<(u32, Channel)> {
        // Filter invalid responses
        let mut valid_responses: Vec<_> = responses
            .into_iter()
            .filter(|r| r.leader_id != 0 && r.term > 0)
            .collect();

        if valid_responses.is_empty() {
            return None;
        }

        // Sort by term in descending order, node_id in descending order
        valid_responses.sort_by(|a, b| b.term.cmp(&a.term).then_with(|| b.leader_id.cmp(&a.leader_id)));

        trace!(?valid_responses);

        // Select the response with the highest term
        valid_responses.first().and_then(|resp| {
            peer_channels
                .get_peer_channel(resp.leader_id)
                .map(|c| (resp.leader_id, c.channel))
        })
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
