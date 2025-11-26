use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use nanoid::nanoid;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use tonic::Status;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::warn;

use super::LeaderStateSnapshot;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::candidate_state::CandidateState;
use super::role_state::RaftRoleState;
use crate::AppendResults;
use crate::BackgroundSnapshotTransfer;
use crate::BatchBuffer;
use crate::ConnectionType;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::PeerUpdate;
use crate::PurgeExecutor;
use crate::QuorumVerificationResult;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::RaftOneshot;
use crate::RaftRequestWithSignal;
use crate::ReadConsistencyPolicy as ServerReadConsistencyPolicy;
use crate::ReplicationConfig;
use crate::ReplicationCore;
use crate::ReplicationError;
use crate::ReplicationTimer;
use crate::Result;
use crate::RoleEvent;
use crate::SnapshotConfig;
use crate::StateMachine;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::Transport;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::client_command_to_entry_payloads;
use crate::cluster::is_majority;
use crate::ensure_safe_join;
use crate::scoped_timer::ScopedTimer;
use crate::stream::create_production_snapshot_stream;
use crate::utils::cluster::error;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ReadConsistencyPolicy as ClientReadConsistencyPolicy;
use d_engine_proto::common::AddNode;
use d_engine_proto::common::BatchPromote;
use d_engine_proto::common::BatchRemove;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::common::membership_change::Change;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::PurgeLogResponse;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;

// Supporting data structures
#[derive(Debug, Clone)]
pub struct PendingPromotion {
    pub node_id: u32,
    pub ready_since: Instant,
}

impl PendingPromotion {
    pub fn new(
        node_id: u32,
        ready_since: Instant,
    ) -> Self {
        PendingPromotion {
            node_id,
            ready_since,
        }
    }
}

/// Leader node's state in Raft consensus algorithm.
///
/// This structure maintains all state that should be persisted on leader crashes,
/// including replication progress tracking and log compaction management.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
pub struct LeaderState<T: TypeConfig> {
    // -- Core Raft Leader State --
    /// Shared cluster state with lock protection
    pub shared_state: SharedState,

    /// === Volatile State ===
    /// For each server (node_id), index of the next log entry to send to that server
    ///
    /// Raft Paper: §5.3 Figure 2 (nextIndex)
    pub next_index: HashMap<u32, u64>,

    /// === Volatile State ===
    /// For each server (node_id), index of highest log entry known to be replicated
    ///
    /// Raft Paper: §5.3 Figure 2 (matchIndex)
    pub(super) match_index: HashMap<u32, u64>,

    /// === Volatile State ===
    /// Temporary storage for no-op entry log ID during leader initialization
    pub(super) noop_log_id: Option<u64>,

    // -- Log Compaction & Purge --
    /// === Volatile State ===
    /// The upper bound (exclusive) of log entries scheduled for asynchronous physical deletion.
    ///
    /// This value is set immediately after a new snapshot is successfully created.
    /// It represents the next log position that will trigger compaction.
    ///
    /// The actual log purge is performed by a background task, which may be delayed
    /// due to resource constraints or retry mechanisms.
    pub scheduled_purge_upto: Option<LogId>,

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

    /// === Volatile State ===
    /// Peer purge progress tracking for flow control
    ///
    /// Key: Peer node ID
    /// Value: Last confirmed purge index from peer
    pub peer_purge_progress: HashMap<u32, u64>,

    /// Record if there is on-going snapshot creation activity
    pub snapshot_in_progress: AtomicBool,

    // -- Request Processing --
    /// Batched proposal buffer for client requests
    ///
    /// Accumulates requests until either:
    /// 1. Batch reaches configured size limit
    /// 2. Explicit flush is triggered
    batch_buffer: Box<BatchBuffer<RaftRequestWithSignal>>,

    // -- Timing & Scheduling --
    /// Replication heartbeat timer manager
    ///
    /// Handles:
    /// - Heartbeat interval tracking
    /// - Election timeout prevention
    /// - Batch proposal flushing
    timer: Box<ReplicationTimer>,

    // -- Cluster Configuration --
    /// Cached Raft node configuration (shared reference)
    ///
    /// This includes:
    /// - Cluster membership
    /// - Timeout parameters
    /// - Performance tuning knobs
    pub(super) node_config: Arc<RaftNodeConfig>,

    /// Last time we checked for learners
    pub(super) last_learner_check: Instant,

    // -- Stale Learner Handling --
    /// The next scheduled time to check for stale learners.
    ///
    /// This is used to implement periodic checking of learners that have been in the promotion
    /// queue for too long without making progress. The check interval is configurable via the
    /// node configuration (`node_config.promotion.stale_check_interval`).
    ///
    /// When the current time reaches or exceeds this instant, the leader will perform a scan
    /// of a subset of the pending promotions to detect stale learners. After the scan, the field
    /// is updated to `current_time + stale_check_interval`.
    ///
    /// Rationale: Avoiding frequent full scans improves batching efficiency and reduces CPU spikes
    /// in high-load environments (particularly crucial for RocketMQ-on-DLedger workflows).
    pub next_membership_maintenance_check: Instant,

    /// Queue of learners that have caught up and are pending promotion to voter.
    pub pending_promotions: VecDeque<PendingPromotion>,

    /// Lease timestamp for LeaseRead policy
    /// Tracks when leadership was last confirmed with quorum
    pub(super) lease_timestamp: AtomicU64,

    // -- Type System Marker --
    /// Phantom data for type parameter anchoring
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for LeaderState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    ///Overwrite default behavior.
    /// As leader, I should not receive commit index,
    ///     which is lower than my current one
    #[tracing::instrument]
    fn update_commit_index(
        &mut self,
        new_commit_index: u64,
    ) -> Result<()> {
        if self.commit_index() < new_commit_index {
            debug!("update_commit_index to: {:?}", new_commit_index);
            self.shared_state.commit_index = new_commit_index;
        } else {
            warn!(
                "Illegal operation, might be a bug! I am Leader old_commit_index({}) >= new_commit_index:({})",
                self.commit_index(),
                new_commit_index
            )
        }
        Ok(())
    }

    /// As Leader should not vote any more
    fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.shared_state().voted_for()
    }

    /// As Leader might also be able to vote ,
    ///     if new legal Leader found
    fn update_voted_for(
        &mut self,
        voted_for: VotedFor,
    ) -> Result<()> {
        self.shared_state_mut().update_voted_for(voted_for)
    }

    fn next_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        Some(if let Some(n) = self.next_index.get(&node_id) {
            *n
        } else {
            1
        })
    }

    fn update_next_index(
        &mut self,
        node_id: u32,
        new_next_id: u64,
    ) -> Result<()> {
        debug!("update_next_index({}) to {}", node_id, new_next_id);
        self.next_index.insert(node_id, new_next_id);
        Ok(())
    }

    fn update_match_index(
        &mut self,
        node_id: u32,
        new_match_id: u64,
    ) -> Result<()> {
        self.match_index.insert(node_id, new_match_id);
        Ok(())
    }

    fn match_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        self.match_index.get(&node_id).copied()
    }

    fn init_peers_next_index_and_match_index(
        &mut self,
        last_entry_id: u64,
        peer_ids: Vec<u32>,
    ) -> Result<()> {
        for peer_id in peer_ids {
            debug!("init leader state for peer_id: {:?}", peer_id);
            let new_next_id = last_entry_id + 1;
            self.update_next_index(peer_id, new_next_id)?;
            self.update_match_index(peer_id, 0)?;
        }
        Ok(())
    }
    fn noop_log_id(&self) -> Result<Option<u64>> {
        Ok(self.noop_log_id)
    }

    /// Verifies leadership status using persistent retry until timeout.
    ///
    /// This function is designed for critical operations like configuration changes
    /// that must eventually succeed. It implements:
    ///   - Infinite retries with exponential backoff
    ///   - Jitter randomization to prevent synchronization
    ///   - Termination only on success, leadership loss, or global timeout
    ///
    /// # Parameters
    /// - `payloads`: Log entries to verify
    /// - `bypass_queue`: Whether to skip request queues for direct transmission
    /// - `ctx`: Raft execution context
    /// - `role_tx`: Channel for role transition events
    ///
    /// # Returns
    /// - `Ok(true)`: Quorum verification succeeded
    /// - `Ok(false)`: Leadership definitively lost during verification
    /// - `Err(_)`: Global timeout exceeded or critical failure occurred
    async fn verify_leadership_persistent(
        &mut self,
        payloads: Vec<EntryPayload>,
        bypass_queue: bool,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        let initial_delay =
            Duration::from_millis(ctx.node_config.retry.internal_quorum.base_delay_ms);
        let max_delay = Duration::from_millis(ctx.node_config.retry.internal_quorum.max_delay_ms);
        let global_timeout = ctx.node_config.raft.membership.verify_leadership_persistent_timeout;

        let mut current_delay = initial_delay;
        let start_time = Instant::now();

        loop {
            match self.verify_internal_quorum(payloads.clone(), bypass_queue, ctx, role_tx).await {
                Ok(QuorumVerificationResult::Success) => return Ok(true),
                Ok(QuorumVerificationResult::LeadershipLost) => return Ok(false),
                Ok(QuorumVerificationResult::RetryRequired) => {
                    // Check global timeout before retrying
                    if start_time.elapsed() > global_timeout {
                        return Err(NetworkError::GlobalTimeout(
                            "Leadership verification timed out".to_string(),
                        )
                        .into());
                    }

                    current_delay =
                        current_delay.checked_mul(2).unwrap_or(max_delay).min(max_delay);
                    let jitter = Duration::from_millis(rand::random::<u64>() % 500);
                    sleep(current_delay + jitter).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Immidiatelly verifies leadership status using a limited retry strategy.
    ///
    /// - Bypasses all queues with direct RPC transmission
    /// - Enforces synchronous quorum validation
    /// - Guarantees real-time network visibility
    ///
    /// This function is designed for latency-sensitive operations like linear reads,
    /// where rapid failure is preferred over prolonged retries. It implements:
    ///   - Exponential backoff with jitter
    ///   - Fixed maximum retry attempts
    ///   - Immediate failure on leadership loss
    ///
    /// # Parameters
    /// - `payloads`: Log entries to verify (typically empty for leadership checks)
    /// - `bypass_queue`: Whether to skip request queues for direct transmission
    /// - `ctx`: Raft execution context
    /// - `role_tx`: Channel for role transition events
    ///
    /// # Returns
    /// - `Ok(true)`: Quorum verification succeeded within retry limits
    /// - `Ok(false)`: Leadership definitively lost during verification
    /// - `Err(_)`: Maximum retries exceeded or critical failure occurred
    async fn verify_leadership_limited_retry(
        &mut self,
        payloads: Vec<EntryPayload>,
        bypass_queue: bool,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        let retry_policy = ctx.node_config.retry.internal_quorum;
        let max_retries = retry_policy.max_retries;
        let initial_delay =
            Duration::from_millis(ctx.node_config.retry.internal_quorum.base_delay_ms);
        let max_delay = Duration::from_millis(ctx.node_config.retry.internal_quorum.max_delay_ms);

        let mut current_delay = initial_delay;
        let mut attempts = 0;

        loop {
            match self.verify_internal_quorum(payloads.clone(), bypass_queue, ctx, role_tx).await {
                Ok(QuorumVerificationResult::Success) => return Ok(true),
                Ok(QuorumVerificationResult::LeadershipLost) => return Ok(false),
                Ok(QuorumVerificationResult::RetryRequired) => {
                    debug!(%attempts, "verify_internal_quorum");
                    if attempts >= max_retries {
                        return Err(NetworkError::TaskBackoffFailed(
                            "Max retries exceeded".to_string(),
                        )
                        .into());
                    }

                    current_delay =
                        current_delay.checked_mul(2).unwrap_or(max_delay).min(max_delay);
                    let jitter = Duration::from_millis(rand::random::<u64>() % 500);
                    sleep(current_delay + jitter).await;

                    attempts += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Note: This method acts as the centerialized internal Raft client
    ///
    /// Check leadership quorum verification Immidiatelly
    ///
    /// - Bypasses all queues with direct RPC transmission
    /// - Enforces synchronous quorum validation
    /// - Guarantees real-time network visibility
    ///
    /// Scenario handling summary:
    ///
    /// 1. Quorum achieved:
    ///    - Return: `Ok(QuorumVerificationResult::Success)`
    ///
    /// 2. Quorum NOT achieved (verifiable):
    ///    - Return: `Ok(QuorumVerificationResult::RetryRequired)`
    ///
    /// 3. Quorum NOT achieved (non-verifiable):
    ///    - Return: `Ok(QuorumVerificationResult::LeadershipLost)`
    ///
    /// 4. Partial timeouts:
    ///    - Return: `Ok(QuorumVerificationResult::RetryRequired)`
    ///
    /// 5. All timeouts:
    ///    - Return: `Ok(QuorumVerificationResult::RetryRequired)`
    ///
    /// 6. Higher term detected:
    ///    - Return: `Err(HigherTerm)`
    ///
    /// 7. Critical failure (e.g., system or logic error):
    ///    - Return: original error
    async fn verify_internal_quorum(
        &mut self,
        payloads: Vec<EntryPayload>,
        bypass_queue: bool,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<QuorumVerificationResult> {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads,
                sender: resp_tx,
            },
            ctx,
            bypass_queue,
            role_tx,
        )
        .await?;

        // Wait for response with timeout
        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        match timeout(timeout_duration, resp_rx).await {
            // Case 1: Response received successfully and verification passed
            Ok(Ok(Ok(response))) => {
                debug!("Leadership check response: {:?}", response);

                // Handle different response cases
                Ok(if response.is_write_success() {
                    QuorumVerificationResult::Success
                } else if response.is_retry_required() {
                    // Verifiable quorum failure
                    QuorumVerificationResult::RetryRequired
                } else {
                    // Non-verifiable failure or explicit rejection
                    QuorumVerificationResult::LeadershipLost
                })
            }

            // Case 2: Received explicit rejection status
            Ok(Ok(Err(status))) => {
                warn!("Leadership rejected by follower: {status:?}");
                Ok(QuorumVerificationResult::LeadershipLost)
            }

            // Case 3: Channel communication failure (unrecoverable error)
            Ok(Err(e)) => {
                error!("Channel error during leadership check: {:?}", e);
                Err(NetworkError::SingalReceiveFailed(e.to_string()).into())
            }

            // Case 4: Waiting for response timeout
            Err(_) => {
                warn!("Leadership check timed out after {:?}", timeout_duration);
                Err(NetworkError::Timeout {
                    node_id: self.node_id(),
                    duration: timeout_duration,
                }
                .into())
            }
        }
    }

    fn is_leader(&self) -> bool {
        true
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        warn!("I am leader already");

        Err(StateTransitionError::InvalidTransition.into())
    }

    fn become_candidate(&self) -> Result<RaftRole<T>> {
        error!("Leader can not become Candidate");

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
        error!("Leader can not become Learner");

        Err(StateTransitionError::InvalidTransition.into())
    }

    fn is_timer_expired(&self) -> bool {
        self.timer.is_expired()
    }

    /// Raft starts, we will check if we need reset all timer
    fn reset_timer(&mut self) {
        self.timer.reset_batch();
        self.timer.reset_replication();
    }

    fn next_deadline(&self) -> Instant {
        self.timer.next_deadline()
    }

    /// Trigger heartbeat now
    async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        _raft_tx: &mpsc::Sender<RaftEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        let now = Instant::now();
        // Keep syncing leader_id
        ctx.membership_ref().mark_leader_id(self.node_id()).await?;

        // 1. Clear expired learners
        if let Err(e) = self.run_periodic_maintenance(role_tx, ctx).await {
            error!("Failed to run periodic maintenance: {}", e);
        }

        // 3. Batch trigger check (should be prioritized before heartbeat check)
        if now >= self.timer.batch_deadline() {
            self.timer.reset_batch();

            if self.batch_buffer.should_flush() {
                debug!(?now, "tick::reset_batch batch timer");
                self.timer.reset_replication();

                // Take out the batched messages and send them immediately
                // Do not move batch out of this block
                let batch = self.batch_buffer.take();
                self.process_batch(batch, role_tx, ctx).await?;
            }
        }

        // 4. Heartbeat trigger check
        // Send heartbeat if the replication timer expires
        if now >= self.timer.replication_deadline() {
            debug!(?now, "tick::reset_replication timer");
            self.timer.reset_replication();

            // Do not move batch out of this block
            let batch = self.batch_buffer.take();
            self.process_batch(batch, role_tx, ctx).await?;
        }

        Ok(())
    }

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let state_machine = ctx.state_machine();
        let last_applied_index = state_machine.last_applied().index;
        let my_id = self.shared_state.node_id;
        let my_term = self.current_term();

        match raft_event {
            // Leader receives RequestVote(term=X, candidate=Y)
            // 1. If X > currentTerm:
            // - Leader → Follower, currentTerm = X
            // - Replay event
            // 2. Else:
            // - Reply with VoteGranted=false, currentTerm=currentTerm
            RaftEvent::ReceiveVoteRequest(vote_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::ReceiveVoteRequest: {:?}",
                    &vote_request
                );

                let my_term = self.current_term();
                if my_term < vote_request.term {
                    self.update_current_term(vote_request.term);
                    // Step down as Follower
                    self.send_become_follower_event(None, &role_tx)?;

                    info!("Leader will not process Vote request, it should let Follower do it.");
                    send_replay_raft_event(
                        &role_tx,
                        RaftEvent::ReceiveVoteRequest(vote_request, sender),
                    )?;
                } else {
                    let last_log_id =
                        ctx.raft_log().last_log_id().unwrap_or(LogId { index: 0, term: 0 });
                    let response = VoteResponse {
                        term: my_term,
                        vote_granted: false,
                        last_log_index: last_log_id.index,
                        last_log_term: last_log_id.term,
                    };
                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }
            }

            RaftEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx.membership().retrieve_cluster_membership_config().await;
                debug!("Leader receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
                let current_conf_version = ctx.membership().get_cluster_conf_version().await;
                debug!(%current_conf_version, ?cluste_conf_change_request,
                    "handle_raft_event::RaftEvent::ClusterConfUpdate",
                );

                // Reject the fake Leader append entries request
                if my_term >= cluste_conf_change_request.term {
                    let response = ClusterConfUpdateResponse::higher_term(
                        my_id,
                        my_term,
                        current_conf_version,
                    );

                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                } else {
                    // Step down as Follower as new Leader found
                    info!(
                        "my({}) term < request one, now I will step down to Follower",
                        my_id
                    );
                    //TODO: if there is a bug?  self.update_current_term(vote_request.term);
                    self.send_become_follower_event(Some(cluste_conf_change_request.id), &role_tx)?;

                    info!(
                        "Leader will not process append_entries_request, it should let Follower do it."
                    );
                    send_replay_raft_event(
                        &role_tx,
                        RaftEvent::ClusterConfUpdate(cluste_conf_change_request, sender),
                    )?;
                }
            }

            RaftEvent::AppendEntries(append_entries_request, sender) => {
                debug!(
                    "handle_raft_event::RaftEvent::AppendEntries: {:?}",
                    &append_entries_request
                );

                // Reject the fake Leader append entries request
                if my_term >= append_entries_request.term {
                    let response = AppendEntriesResponse::higher_term(my_id, my_term);

                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                } else {
                    // Step down as Follower as new Leader found
                    info!(
                        "my({}) term < request one, now I will step down to Follower",
                        my_id
                    );
                    //TODO: if there is a bug?  self.update_current_term(vote_request.term);
                    self.send_become_follower_event(
                        Some(append_entries_request.leader_id),
                        &role_tx,
                    )?;

                    info!(
                        "Leader will not process append_entries_request, it should let Follower do it."
                    );
                    send_replay_raft_event(
                        &role_tx,
                        RaftEvent::AppendEntries(append_entries_request, sender),
                    )?;
                }
            }

            RaftEvent::ClientPropose(client_write_request, sender) => {
                if let Err(e) = self
                    .process_raft_request(
                        RaftRequestWithSignal {
                            id: nanoid!(),
                            payloads: client_command_to_entry_payloads(
                                client_write_request.commands,
                            ),
                            sender,
                        },
                        ctx,
                        false,
                        &role_tx,
                    )
                    .await
                {
                    error("Leader::process_raft_request", &e);
                    return Err(e);
                }
            }

            RaftEvent::ClientReadRequest(client_read_request, sender) => {
                let _timer = ScopedTimer::new("leader_linear_read");
                debug!(
                    "Leader::ClientReadRequest client_read_request:{:?}",
                    &client_read_request
                );

                let keys = client_read_request.keys.clone();
                let response: std::result::Result<ClientResponse, tonic::Status> = {
                    let read_operation =
                        || -> std::result::Result<ClientResponse, tonic::Status> {
                            let results = ctx
                                .handlers
                                .state_machine_handler
                                .read_from_state_machine(keys)
                                .unwrap_or_default();
                            debug!("handle_client_read results: {:?}", results);
                            Ok(ClientResponse::read_results(results))
                        };

                    // Determine effective consistency policy using server configuration
                    let effective_policy = if client_read_request.has_consistency_policy() {
                        // Client explicitly specified policy
                        if ctx.node_config().raft.read_consistency.allow_client_override {
                            match client_read_request.consistency_policy() {
                                ClientReadConsistencyPolicy::LeaseRead => {
                                    ServerReadConsistencyPolicy::LeaseRead
                                }
                                ClientReadConsistencyPolicy::LinearizableRead => {
                                    ServerReadConsistencyPolicy::LinearizableRead
                                }
                                ClientReadConsistencyPolicy::EventualConsistency => {
                                    ServerReadConsistencyPolicy::EventualConsistency
                                }
                            }
                        } else {
                            // Client override not allowed - use server default
                            ctx.node_config().raft.read_consistency.default_policy.clone()
                        }
                    } else {
                        // No client policy specified - use server default
                        ctx.node_config().raft.read_consistency.default_policy.clone()
                    };

                    // Apply consistency policy
                    match effective_policy {
                        ServerReadConsistencyPolicy::LinearizableRead => {
                            if !self
                                .verify_leadership_limited_retry(vec![], true, ctx, &role_tx)
                                .await
                                .unwrap_or(false)
                            {
                                warn!("enforce_quorum_consensus failed for linear read request");

                                Err(tonic::Status::failed_precondition(
                                    "enforce_quorum_consensus failed".to_string(),
                                ))
                            } else if let Err(e) = self.ensure_state_machine_upto_commit_index(
                                &ctx.handlers.state_machine_handler,
                                last_applied_index,
                            ) {
                                warn!(
                                    "ensure_state_machine_upto_commit_index failed for linear read request"
                                );
                                Err(tonic::Status::failed_precondition(format!(
                                    "ensure_state_machine_upto_commit_index failed: {e:?}"
                                )))
                            } else {
                                read_operation()
                            }
                        }
                        ServerReadConsistencyPolicy::LeaseRead => {
                            // New lease-based implementation
                            if self.is_lease_valid(ctx) {
                                // Lease is valid, serve read locally
                                read_operation()
                            } else {
                                // Lease expired, need to refresh with quorum
                                if !self
                                    .verify_leadership_limited_retry(vec![], true, ctx, &role_tx)
                                    .await
                                    .unwrap_or(false)
                                {
                                    warn!("LeaseRead policy: lease renewal failed");
                                    Err(tonic::Status::failed_precondition(
                                        "LeaseRead policy: lease renewal failed".to_string(),
                                    ))
                                } else {
                                    // Update lease timestamp after successful verification
                                    self.update_lease_timestamp();
                                    read_operation()
                                }
                            }
                        }
                        ServerReadConsistencyPolicy::EventualConsistency => {
                            // Eventual consistency: serve immediately without verification
                            // Leader can always serve eventually consistent reads
                            debug!("EventualConsistency: serving local read without verification");
                            read_operation()
                        }
                    }
                };

                debug!(
                    "Leader::ClientReadRequest is going to response: {:?}",
                    &response
                );
                sender.send(response).map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }

            RaftEvent::InstallSnapshotChunk(_streaming, sender) => {
                sender
                    .send(Err(Status::permission_denied("Not Follower or Learner. ")))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Leader",
                    required_role: "Follower or Learner",
                    context: format!(
                        "Leader node {} receives RaftEvent::InstallSnapshotChunk",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::RaftLogCleanUp(_purchase_log_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Leader should not receive RaftLogCleanUp event.",
                    )))
                    .map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Leader",
                    required_role: "None Leader",
                    context: format!(
                        "Leader node {} receives RaftEvent::RaftLogCleanUp",
                        ctx.node_id
                    ),
                }
                .into());
            }

            RaftEvent::CreateSnapshotEvent => {
                // Prevent duplicate snapshot creation
                if self.snapshot_in_progress.load(std::sync::atomic::Ordering::Acquire) {
                    info!("Snapshot creation already in progress. Skipping duplicate request.");
                    return Ok(());
                }

                self.snapshot_in_progress.store(true, std::sync::atomic::Ordering::Release);
                let state_machine_handler = ctx.state_machine_handler().clone();

                // Use spawn to perform snapshot creation in the background
                tokio::spawn(async move {
                    let result = state_machine_handler.create_snapshot().await;
                    info!("SnapshotCreated event will be processed in another event thread");
                    if let Err(e) =
                        send_replay_raft_event(&role_tx, RaftEvent::SnapshotCreated(result))
                    {
                        error!("Failed to send snapshot creation result: {}", e);
                    }
                });
            }

            RaftEvent::SnapshotCreated(result) => {
                self.snapshot_in_progress.store(false, Ordering::SeqCst);
                let my_id = self.shared_state.node_id;
                let my_term = self.current_term();

                match result {
                    Err(e) => {
                        error!(%e, "State machine snapshot creation failed");
                    }
                    Ok((
                        SnapshotMetadata {
                            last_included: last_included_option,
                            checksum,
                        },
                        _final_path,
                    )) => {
                        info!("Initiating log purge after snapshot creation");

                        if let Some(last_included) = last_included_option {
                            // ----------------------
                            // Phase 1: Schedule log purge if possible
                            // ----------------------
                            trace!("Phase 1: Schedule log purge if possible");
                            if self.can_purge_logs(self.last_purged_index, last_included) {
                                trace!(?last_included, "Phase 1: Scheduling log purge");
                                self.scheduled_purge_upto(last_included);
                            }

                            // ----------------------
                            // Phase 2.1: Pre-Checks before sending Purge request
                            // ----------------------
                            trace!("Phase 2.1: Pre-Checks before sending Purge request");
                            let membership = ctx.membership();
                            let members = membership.voters().await;
                            if members.is_empty() {
                                warn!("no peer found for leader({})", my_id);
                                return Err(MembershipError::NoPeersAvailable.into());
                            }

                            // ----------------------
                            // Phase 2.2: Send Purge request to the other nodes
                            // ----------------------
                            trace!("Phase 2.2: Send Purge request to the other nodes");
                            let transport = ctx.transport();
                            match transport
                                .send_purge_requests(
                                    PurgeLogRequest {
                                        term: my_term,
                                        leader_id: my_id,
                                        last_included: Some(last_included),
                                        snapshot_checksum: checksum.clone(),
                                        leader_commit: self.commit_index(),
                                    },
                                    &self.node_config.retry,
                                    membership,
                                )
                                .await
                            {
                                Ok(result) => {
                                    info!(?result, "receive PurgeLogResult");

                                    self.peer_purge_progress(result, &role_tx)?;
                                }
                                Err(e) => {
                                    error!(?e, "RaftEvent::CreateSnapshotEvent");
                                    return Err(e);
                                }
                            }

                            // ----------------------
                            // Phase 3: Execute scheduled purge task
                            // ----------------------
                            trace!("Phase 3: Execute scheduled purge task");
                            debug!(?last_included, "Execute scheduled purge task");
                            if let Some(scheduled) = self.scheduled_purge_upto {
                                let purge_executor = ctx.purge_executor();
                                // //TODO: bug
                                // self.last_purged_index = Some(scheduled);
                                match purge_executor.execute_purge(scheduled).await {
                                    Ok(_) => {
                                        if let Err(e) = send_replay_raft_event(
                                            &role_tx,
                                            RaftEvent::LogPurgeCompleted(scheduled),
                                        ) {
                                            error!(%e, "Failed to notify purge completion");
                                        }
                                    }
                                    Err(e) => {
                                        error!(?e, ?scheduled, "Log purge execution failed");
                                    }
                                }
                            }
                        }
                    }
                }
            }

            RaftEvent::LogPurgeCompleted(purged_id) => {
                // Ensure we don't regress the purge index
                if self.last_purged_index.map_or(true, |current| purged_id.index > current.index) {
                    debug!(
                        ?purged_id,
                        "Updating last purged index after successful execution"
                    );
                    self.last_purged_index = Some(purged_id);
                } else {
                    warn!(
                        ?purged_id,
                        ?self.last_purged_index,
                        "Received outdated purge completion, ignoring"
                    );
                }
            }

            RaftEvent::JoinCluster(join_request, sender) => {
                debug!(?join_request, "Leader::RaftEvent::JoinCluster");
                self.handle_join_cluster(join_request, sender, ctx, &role_tx).await?;
            }

            RaftEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Leader::RaftEvent::DiscoverLeader");

                if let Some(meta) = ctx.membership().retrieve_node_meta(my_id).await {
                    let response = LeaderDiscoveryResponse {
                        leader_id: my_id,
                        leader_address: meta.address,
                        term: my_term,
                    };
                    sender.send(Ok(response)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                    return Ok(());
                } else {
                    let msg = "Leader can not find its address? It must be a bug.";
                    error!("{}", msg);
                    panic!("{}", msg);
                }
            }
            RaftEvent::StreamSnapshot(request, sender) => {
                debug!("Leader::RaftEvent::StreamSnapshot");

                // Get the latest snapshot metadata
                if let Some(metadata) = ctx.state_machine().snapshot_metadata() {
                    // Create response channel
                    let (response_tx, response_rx) =
                        mpsc::channel::<std::result::Result<Arc<SnapshotChunk>, Status>>(32);
                    // Convert to properly encoded tonic stream
                    let size = 1024 * 1024 * 1024; // 1GB max message size
                    let response_stream = create_production_snapshot_stream(response_rx, size);
                    // Immediately respond with the stream
                    sender.send(Ok(response_stream)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Stream response failed: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;

                    // Spawn background transfer task
                    let state_machine_handler = ctx.state_machine_handler().clone();
                    let config = ctx.node_config.raft.snapshot.clone();
                    // Load snapshot data stream
                    let data_stream =
                        state_machine_handler.load_snapshot_data(metadata.clone()).await?;

                    tokio::spawn(async move {
                        if let Err(e) = BackgroundSnapshotTransfer::<T>::run_pull_transfer(
                            request,
                            response_tx,
                            data_stream,
                            config,
                        )
                        .await
                        {
                            error!("StreamSnapshot failed: {:?}", e);
                        }
                    });
                } else {
                    warn!("No snapshot available for streaming");
                    sender.send(Err(Status::not_found("Snapshot not found"))).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Stream response failed: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }
            }
            RaftEvent::TriggerSnapshotPush { peer_id } => {
                if let Some(lastest_snapshot_metadata) = ctx.state_machine().snapshot_metadata() {
                    Self::trigger_background_snapshot(
                        peer_id,
                        lastest_snapshot_metadata,
                        ctx.state_machine_handler().clone(),
                        ctx.membership(),
                        ctx.node_config.raft.snapshot.clone(),
                    )
                    .await?;
                }
            }

            RaftEvent::PromoteReadyLearners => {
                // SAFETY: Called from main event loop, no reentrancy issues
                info!("Promoting ready learners");
                self.process_pending_promotions(ctx, &role_tx).await?;
            }
        }

        return Ok(());
    }
}

impl<T: TypeConfig> LeaderState<T> {
    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
            role: Leader.into(),
        }
    }

    /// The fun will retrieve current Leader state snapshot
    #[tracing::instrument]
    pub fn leader_state_snapshot(&self) -> LeaderStateSnapshot {
        LeaderStateSnapshot {
            next_index: self.next_index.clone(),
            match_index: self.match_index.clone(),
            noop_log_id: self.noop_log_id,
        }
    }

    /// # Params
    /// - `execute_now`: should this propose been executed immediatelly. e.g.
    ///   enforce_quorum_consensus expected to be executed immediatelly
    pub async fn process_raft_request(
        &mut self,
        raft_request_with_signal: RaftRequestWithSignal,
        ctx: &RaftContext<T>,
        execute_now: bool,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!(
            "Leader::process_raft_request, request_id: {}",
            raft_request_with_signal.id
        );

        let push_result = self.batch_buffer.push(raft_request_with_signal);
        // only buffer exceeds the max, the size will return
        if execute_now || push_result.is_some() {
            let batch = self.batch_buffer.take();

            trace!(
                "replication_handler.handle_raft_request_in_batch: batch size:{:?}",
                batch.len()
            );

            self.process_batch(batch, role_tx, ctx).await?;
        }

        Ok(())
    }

    /// Scenario handling summary:
    ///
    /// 1. Quorum achieved:
    ///    - Client response: `write_success()`
    ///    - State update: update peer indexes and commit index
    ///    - Return: `Ok(())`
    ///
    /// 2. Quorum NOT achieved (verifiable):
    ///    - Client response: `RetryRequired`
    ///    - State update: update peer indexes
    ///    - Return: `Ok(())`
    ///
    /// 3. Quorum NOT achieved (non-verifiable):
    ///    - Client response: `ProposeFailed`
    ///    - State update: update peer indexes
    ///    - Return: `Ok(())`
    ///
    /// 4. Partial timeouts:
    ///    - Client response: `ProposeFailed`
    ///    - State update: update only the peer indexes that responded
    ///    - Return: `Ok(())`
    ///
    /// 5. All timeouts:
    ///    - Client response: `ProposeFailed`
    ///    - State update: no update
    ///    - Return: `Ok(())`
    ///
    /// 6. Higher term detected:
    ///    - Client response: `TermOutdated`
    ///    - State update: update term and convert to follower
    ///    - Return: `Err(HigherTerm)`
    ///
    /// 7. Critical failure (e.g., system or logic error):
    ///    - Client response: `ProposeFailed`
    ///    - State update: none
    ///    - Return: original error
    pub async fn process_batch(
        &mut self,
        batch: VecDeque<RaftRequestWithSignal>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // 1. Prepare batch data
        let entry_payloads: Vec<EntryPayload> =
            batch.iter().flat_map(|req| &req.payloads).cloned().collect();
        if !entry_payloads.is_empty() {
            trace!(?entry_payloads, "[Node-{} process_batch..", ctx.node_id);
        }

        // 2. Execute the copy
        let membership = ctx.membership();
        let voters = membership.voters().await;
        let cluster_size = voters.len() + 1;
        trace!(%cluster_size);

        let result = ctx
            .replication_handler()
            .handle_raft_request_in_batch(
                entry_payloads,
                self.state_snapshot(),
                self.leader_state_snapshot(),
                ctx,
            )
            .await;
        debug!(?result, "replication_handler::handle_raft_request_in_batch");

        // 3. Unify the processing results
        match result {
            // Case 1: Successfully reached majority
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates,
                learner_progress,
            }) => {
                // 1. Update all peer index
                self.update_peer_indexes(&peer_updates);

                // 2. Check Learner's catch-up status
                if let Err(e) = self.check_learner_progress(&learner_progress, ctx, role_tx).await {
                    error!(?e, "check_learner_progress failed");
                };

                // 3. Update commit index
                if let Some(new_commit_index) =
                    self.calculate_new_commit_index(ctx.raft_log(), &peer_updates)
                {
                    debug!(
                        "[Leader-{}] New commit been acknowledged: {}",
                        self.node_id(),
                        new_commit_index
                    );
                    self.update_commit_index_with_signal(
                        Leader.into(),
                        self.current_term(),
                        new_commit_index,
                        role_tx,
                    )?;
                }

                // 4. Notify all clients of success
                for request in batch {
                    let _ = request.sender.send(Ok(ClientResponse::write_success()));
                }
            }

            // Case 2: Failed to reach majority
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates,
                learner_progress,
            }) => {
                // 1. Update all peer index
                self.update_peer_indexes(&peer_updates);

                // 2. Check Learner's catch-up status
                if let Err(e) = self.check_learner_progress(&learner_progress, ctx, role_tx).await {
                    error!(?e, "check_learner_progress failed");
                };

                // 3. Determine error code based on verifiability
                let responses_received = peer_updates.len();
                let error_code = if is_majority(responses_received, cluster_size) {
                    ErrorCode::RetryRequired
                } else {
                    ErrorCode::ProposeFailed
                };

                // 4. Notify all clients of failure
                for request in batch {
                    let _ = request.sender.send(Ok(ClientResponse::client_error(error_code)));
                }
            }

            // Case 3: High term found
            Err(Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(
                higher_term,
            )))) => {
                warn!("Higher term detected: {}", higher_term);
                self.update_current_term(higher_term);
                self.send_become_follower_event(None, role_tx)?;

                // Notify client of term expiration
                for request in batch {
                    let _ = request
                        .sender
                        .send(Ok(ClientResponse::client_error(ErrorCode::TermOutdated)));
                }

                return Err(ReplicationError::HigherTerm(higher_term).into());
            }

            // Case 4: Other errors
            Err(e) => {
                error!("Batch processing failed: {:?}", e);

                // Notify all clients of failure
                for request in batch {
                    let _ = request
                        .sender
                        .send(Ok(ClientResponse::client_error(ErrorCode::ProposeFailed)));
                }

                return Err(e);
            }
        }

        Ok(())
    }

    /// Update peer node index
    #[instrument(skip(self))]
    fn update_peer_indexes(
        &mut self,
        peer_updates: &HashMap<u32, PeerUpdate>,
    ) {
        for (peer_id, update) in peer_updates {
            if let Err(e) = self.update_next_index(*peer_id, update.next_index) {
                error!("Failed to update next index: {:?}", e);
            }
            trace!(
                "Updated next index for peer {}-{}",
                peer_id, update.next_index
            );
            if let Some(match_index) = update.match_index {
                if let Err(e) = self.update_match_index(*peer_id, match_index) {
                    error!("Failed to update match index: {:?}", e);
                }
                trace!("Updated match index for peer {}-{}", peer_id, match_index);
            }
        }
    }

    async fn check_learner_progress(
        &mut self,
        learner_progress: &HashMap<u32, Option<u64>>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!(?learner_progress, "check_learner_progress");
        // Throttle checks to once per second
        if self.last_learner_check.elapsed() < Duration::from_secs(1) {
            return Ok(());
        }
        self.last_learner_check = Instant::now();

        if learner_progress.is_empty() {
            return Ok(());
        }

        let leader_commit_index = self.commit_index();
        let config = &ctx.node_config.raft;
        let mut ready_learners = Vec::new();

        for (node_id, match_index) in learner_progress {
            let node_status = match ctx.membership().get_node_status(*node_id).await {
                Some(status) => status,
                None => {
                    warn!("Node {} not in membership", node_id);
                    continue;
                }
            };
            debug!("Learner: {} in status: {:?} ", node_id, &node_status);

            // Skip non-promotable nodes early
            if !node_status.is_promotable() {
                debug!("Node {} is not promotable", node_id);
                continue;
            }

            debug!(
                ?leader_commit_index,
                ?match_index,
                config.learner_catchup_threshold,
                "check_learner_progress"
            );

            let match_index = match_index.unwrap_or(0);
            let caught_up = leader_commit_index
                .checked_sub(match_index)
                .map(|diff| diff <= config.learner_catchup_threshold)
                .unwrap_or(true);

            debug!("Caught up: {}", caught_up);
            if caught_up {
                if ctx.membership().contains_node(*node_id).await {
                    ready_learners.push(*node_id);
                } else {
                    warn!("Node {} is already removed from cluster", node_id);
                }
            }
        }

        if !ready_learners.is_empty() {
            debug!("Ready learners: {:?}", ready_learners);

            // Print promotion messages for each ready learner (Plan B)
            for learner_id in &ready_learners {
                let match_index = learner_progress.get(learner_id).and_then(|mi| *mi).unwrap_or(0);
                crate::utils::cluster_printer::print_leader_promoting_learner(
                    self.node_id(),
                    *learner_id,
                    match_index,
                    leader_commit_index,
                );
            }

            // Add to pending queue
            let promotions = ready_learners
                .into_iter()
                .map(|node_id| PendingPromotion::new(node_id, Instant::now()));
            self.pending_promotions.extend(promotions);

            // Create a PromoteReadyLearners event
            let event = RaftEvent::PromoteReadyLearners;

            // We use the ReprocessEvent mechanism to push the PromoteReadyLearners event back to
            // the main event queue
            if let Err(e) = role_tx.send(RoleEvent::ReprocessEvent(Box::new(event))) {
                error!("Failed to send Promotion event via RoleEvent: {}", e);
            } else {
                trace!("Scheduled Promotion event via ReprocessEvent");
            }

            // self.batch_promote_learners(ready_learners, ctx, role_tx).await?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn batch_promote_learners(
        &mut self,
        ready_learners_ids: Vec<u32>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        // 1. Determine optimal promotion status based on quorum safety
        debug!("1. Determine optimal promotion status based on quorum safety");
        let membership = ctx.membership();
        let current_voters = membership.voters().await.len();
        // let syncings = membership.nodes_with_status(NodeStatus::Syncing).len();
        let new_active_count = current_voters + ready_learners_ids.len();

        // Determine target status based on quorum safety
        trace!(
            ?current_voters,
            ?ready_learners_ids,
            "[Node-{}] new_active_count: {}",
            self.node_id(),
            new_active_count
        );
        let target_status = if ensure_safe_join(self.node_id(), new_active_count).is_ok() {
            trace!(
                "Going to update nodes-{:?} status to Active",
                ready_learners_ids
            );
            NodeStatus::Active
        } else {
            trace!(
                "Not enough quorum to promote learners: {:?}",
                ready_learners_ids
            );
            return Ok(());
        };

        // 2. Create configuration change payload
        debug!("2. Create configuration change payload");
        let config_change = Change::BatchPromote(BatchPromote {
            node_ids: ready_learners_ids.clone(),
            new_status: target_status as i32,
        });

        info!(?config_change, "Replicating cluster config");

        // 3. Submit single config change for all ready learners
        debug!("3. Submit single config change for all ready learners");
        match self
            .verify_leadership_limited_retry(
                vec![EntryPayload::config(config_change)],
                true,
                ctx,
                role_tx,
            )
            .await
        {
            Ok(true) => {
                info!(
                    "Batch promotion committed for nodes: {:?}",
                    ready_learners_ids
                );
            }
            Ok(false) => {
                warn!("Failed to commit batch promotion");
            }
            Err(e) => {
                error!("Batch promotion error: {:?}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Calculate new submission index
    #[instrument(skip(self))]
    fn calculate_new_commit_index(
        &mut self,
        raft_log: &Arc<ROF<T>>,
        peer_updates: &HashMap<u32, PeerUpdate>,
    ) -> Option<u64> {
        let old_commit_index = self.commit_index();
        let current_term = self.current_term();

        let matched_ids: Vec<u64> =
            peer_updates.keys().filter_map(|&id| self.match_index(id)).collect();

        let new_commit_index =
            raft_log.calculate_majority_matched_index(current_term, old_commit_index, matched_ids);

        if new_commit_index.is_some() && new_commit_index.unwrap() > old_commit_index {
            new_commit_index
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn if_update_commit_index(
        &self,
        new_commit_index_option: Option<u64>,
    ) -> (bool, u64) {
        let current_commit_index = self.commit_index();
        if let Some(new_commit_index) = new_commit_index_option {
            debug!("Leader::update_commit_index: {:?}", new_commit_index);
            if current_commit_index < new_commit_index {
                return (true, new_commit_index);
            }
        }
        debug!("Leader::update_commit_index: false");
        (false, current_commit_index)
    }

    pub fn ensure_state_machine_upto_commit_index(
        &self,
        state_machine_handler: &Arc<SMHOF<T>>,
        last_applied: u64,
    ) -> Result<()> {
        let commit_index = self.commit_index();

        debug!(
            "ensure_state_machine_upto_commit_index: last_applied:{} < commit_index:{} ?",
            last_applied, commit_index
        );
        if last_applied < commit_index {
            state_machine_handler.update_pending(commit_index);

            debug!("ensure_state_machine_upto_commit_index success");
        }
        Ok(())
    }

    #[instrument(skip(self))]
    fn scheduled_purge_upto(
        &mut self,
        received_last_included: LogId,
    ) {
        if let Some(existing) = self.scheduled_purge_upto {
            if existing.index >= received_last_included.index {
                warn!(
                    ?received_last_included,
                    ?existing,
                    "Will not update scheduled_purge_upto, received invalid last_included log"
                );
                return;
            }
        }
        info!(?self.scheduled_purge_upto, ?received_last_included, "Updte scheduled_purge_upto.");
        self.scheduled_purge_upto = Some(received_last_included);
    }

    fn peer_purge_progress(
        &mut self,
        responses: Vec<Result<PurgeLogResponse>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        if responses.is_empty() {
            return Ok(());
        }
        for r in responses.iter().flatten() {
            if r.term > self.current_term() {
                self.update_current_term(r.term);
                self.send_become_follower_event(None, role_tx)?;
            }

            if let Some(last_purged) = r.last_purged {
                self.peer_purge_progress
                    .entry(r.node_id)
                    .and_modify(|v| *v = last_purged.index)
                    .or_insert(last_purged.index);
            }
        }

        Ok(())
    }

    fn send_become_follower_event(
        &self,
        new_leader_id: Option<u32>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        info!(
            ?new_leader_id,
            "Leader is going to step down as Follower..."
        );
        role_tx.send(RoleEvent::BecomeFollower(new_leader_id)).map_err(|e| {
            let error_str = format!("{e:?}");
            error!("Failed to send: {}", error_str);
            NetworkError::SingalSendFailed(error_str)
        })?;

        Ok(())
    }

    /// Determines if logs prior to `last_included_in_snapshot` can be permanently discarded.
    ///
    /// Implements Leader-side log compaction safety checks per Raft paper §7.2:
    /// > "The leader uses a new RPC called InstallSnapshot to send snapshots to followers that are
    /// > too far behind"
    ///
    /// # Safety Invariants (ALL must hold)
    ///
    /// 1. **Committed Entry Guarantee**   `last_included_in_snapshot.index < self.commit_index`
    ///    - Ensures we never discard uncommitted entries (Raft §5.4.2)
    ///    - Maintains at least one committed entry after purge for log matching property
    ///
    /// 2. **Monotonic Snapshot Advancement**   `last_purge_index < last_included_in_snapshot.index`
    ///    - Enforces snapshot indices strictly increase (prevents rollback attacks)
    ///    - Maintains sequential purge ordering (FSM safety requirement)
    ///
    /// 3. **Cluster-wide Progress Validation**   `peer_purge_progress.values().all(≥
    ///    snapshot.index)`
    ///    - Ensures ALL followers have confirmed ability to reach this snapshot
    ///    - Prevents leadership changes from causing log inconsistencies
    ///
    /// 4. **Operation Atomicity**   `pending_purge.is_none()`
    ///    - Ensures only one concurrent purge operation
    ///    - Critical for linearizable state machine semantics
    ///
    /// # Implementation Notes
    /// - Leader must maintain `peer_purge_progress` through AppendEntries responses
    /// - Actual log discard should be deferred until storage confirms snapshot persistence
    /// - Design differs from followers by requiring full cluster confirmation (Raft extension for
    ///   enhanced durability)
    #[instrument(skip(self))]
    pub fn can_purge_logs(
        &self,
        last_purge_index: Option<LogId>,
        last_included_in_snapshot: LogId,
    ) -> bool {
        let commit_index = self.commit_index();
        debug!(?self
                    .peer_purge_progress, ?commit_index, ?last_purge_index, ?last_included_in_snapshot, "can_purge_logs");
        let monotonic_check = last_purge_index
            .map(|lid| lid.index < last_included_in_snapshot.index)
            .unwrap_or(true);

        last_included_in_snapshot.index < commit_index
            && monotonic_check
            && self.peer_purge_progress.values().all(|&v| v >= last_included_in_snapshot.index)
    }

    pub async fn handle_join_cluster(
        &mut self,
        join_request: JoinRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let node_id = join_request.node_id;
        let node_role = join_request.node_role;
        let address = join_request.address;
        let membership = ctx.membership();

        // 1. Validate join request
        debug!("1. Validate join request");
        if membership.contains_node(node_id).await {
            let error_msg = format!("Node {node_id} already exists in cluster");
            warn!(%error_msg);
            return self.send_join_error(sender, MembershipError::NodeAlreadyExists(node_id)).await;
        }

        // 2. Create configuration change payload
        debug!("2. Create configuration change payload");
        if let Err(e) = membership.can_rejoin(node_id, node_role).await {
            let error_msg = format!("Node {node_id} cannot rejoin: {e}",);
            warn!(%error_msg);
            return self
                .send_join_error(sender, MembershipError::JoinClusterError(error_msg))
                .await;
        }

        let config_change = Change::AddNode(AddNode {
            node_id,
            address: address.clone(),
        });

        // 3. Submit config change, and wait for quorum confirmation
        debug!("3. Wait for quorum confirmation");
        match self
            .verify_leadership_limited_retry(
                vec![EntryPayload::config(config_change)],
                true,
                ctx,
                role_tx,
            )
            .await
        {
            Ok(true) => {
                // 4. Update node status to Syncing
                debug!("4. Update node status to Syncing");

                debug!(
                    "After updating, the replications peers: {:?}",
                    ctx.membership().replication_peers().await
                );

                // 5. Send successful response
                debug!("5. Send successful response");
                info!("Join config committed for node {}", node_id);
                self.send_join_success(node_id, &address, sender, ctx).await?;
            }
            Ok(false) => {
                warn!("Failed to commit join config for node {}", node_id);
                self.send_join_error(sender, MembershipError::CommitTimeout).await?
            }
            Err(e) => {
                error!("Error waiting for commit: {:?}", e);
                self.send_join_error(sender, e).await?
            }
        }
        Ok(())
    }

    async fn send_join_success(
        &self,
        node_id: u32,
        address: &str,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // Retrieve latest snapshot metadata, if there is
        let snapshot_metadata = ctx.state_machine_handler().get_latest_snapshot_metadata();

        // Prepare response
        let response = JoinResponse {
            success: true,
            error: String::new(),
            config: Some(ctx.membership().retrieve_cluster_membership_config().await),
            config_version: ctx.membership().get_cluster_conf_version().await,
            snapshot_metadata,
            leader_id: self.node_id(),
        };

        sender.send(Ok(response)).map_err(|e| {
            error!("Failed to send join response: {:?}", e);
            NetworkError::SingalSendFailed(format!("{e:?}"))
        })?;

        info!(
            "Node {} ({}) successfully added as learner",
            node_id, address
        );

        // Print leader accepting new node message (Plan B)
        crate::utils::cluster_printer::print_leader_accepting_new_node(
            self.node_id(),
            node_id,
            address,
            d_engine_proto::common::NodeRole::Learner as i32,
        );

        Ok(())
    }

    async fn send_join_error(
        &self,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
        error: impl Into<Error>,
    ) -> Result<()> {
        let error = error.into();
        let status = Status::failed_precondition(error.to_string());

        sender.send(Err(status)).map_err(|e| {
            error!("Failed to send join error: {:?}", e);
            NetworkError::SingalSendFailed(format!("{e:?}"))
        })?;

        Err(error)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        let ReplicationConfig {
            rpc_append_entries_in_batch_threshold,
            rpc_append_entries_batch_process_delay_in_ms,
            rpc_append_entries_clock_in_ms,
            ..
        } = node_config.raft.replication;

        LeaderState {
            shared_state: SharedState::new(node_id, None, None),
            timer: Box::new(ReplicationTimer::new(
                rpc_append_entries_clock_in_ms,
                rpc_append_entries_batch_process_delay_in_ms,
            )),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            batch_buffer: Box::new(BatchBuffer::new(
                rpc_append_entries_in_batch_threshold,
                Duration::from_millis(rpc_append_entries_batch_process_delay_in_ms),
            )),

            node_config,
            scheduled_purge_upto: None,
            last_purged_index: None, //TODO
            last_learner_check: Instant::now(),
            peer_purge_progress: HashMap::new(),
            snapshot_in_progress: AtomicBool::new(false),
            next_membership_maintenance_check: Instant::now(),
            pending_promotions: VecDeque::new(),
            _marker: PhantomData,
            lease_timestamp: AtomicU64::new(0),
        }
    }

    pub async fn trigger_background_snapshot(
        node_id: u32,
        metadata: SnapshotMetadata,
        state_machine_handler: Arc<SMHOF<T>>,
        membership: Arc<MOF<T>>,
        config: SnapshotConfig,
    ) -> Result<()> {
        let (result_tx, result_rx) = oneshot::channel();

        // Delegate the actual transfer to a dedicated thread pool
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let result = rt.block_on(async move {
                let bulk_channel = membership
                    .get_peer_channel(node_id, ConnectionType::Bulk)
                    .await
                    .ok_or(NetworkError::PeerConnectionNotFound(node_id))?;

                let data_stream =
                    state_machine_handler.load_snapshot_data(metadata.clone()).await?;

                BackgroundSnapshotTransfer::<T>::run_push_transfer(
                    node_id,
                    data_stream,
                    bulk_channel,
                    config,
                )
                .await
            });

            // Non-blocking send result
            let _ = result_tx.send(result);
        });

        // Non-blocking check result
        tokio::spawn(async move {
            match result_rx.await {
                Ok(Ok(_)) => info!("Snapshot to {} completed", node_id),
                Ok(Err(e)) => error!("Snapshot to {} failed: {:?}", node_id, e),
                Err(_) => warn!("Snapshot result channel closed unexpectedly"),
            }
        });

        Ok(())
    }

    /// Processes all pending promotions while respecting the cluster's odd-node constraint
    pub async fn process_pending_promotions(
        &mut self,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        // Get promotion configuration from the node config
        let config = &ctx.node_config().raft.membership.promotion;

        // Step 1: Remove stale entries (older than configured threshold)
        let now = Instant::now();
        self.pending_promotions.retain(|entry| {
            now.duration_since(entry.ready_since) <= config.stale_learner_threshold
        });

        if self.pending_promotions.is_empty() {
            return Ok(());
        }

        // Step 2: Get current voter count
        let membership = ctx.membership();
        let current_voters = membership.voters().await.len();

        // Step 3: Calculate the maximum batch size that preserves an odd total
        let max_batch_size =
            calculate_safe_batch_size(current_voters, self.pending_promotions.len());

        if max_batch_size == 0 {
            // Nothing we can safely promote now
            return Ok(());
        }

        // Step 4: Extract the batch from the queue (FIFO order)
        let promotion_entries = self.drain_batch(max_batch_size);
        let promotion_node_ids = promotion_entries.iter().map(|e| e.node_id).collect::<Vec<_>>();

        // Step 5: Execute batch promotion
        if !promotion_node_ids.is_empty() {
            // Log the batch promotion
            info!(
                "Promoting learner batch of {} nodes: {:?} (total voters: {} -> {})",
                promotion_node_ids.len(),
                promotion_node_ids,
                current_voters,
                current_voters + promotion_node_ids.len()
            );

            // Attempt promotion and restore batch on failure
            let result = self.safe_batch_promote(promotion_node_ids.clone(), ctx, role_tx).await;

            if let Err(e) = result {
                // Restore entries to the front of the queue in reverse order
                for entry in promotion_entries.into_iter().rev() {
                    self.pending_promotions.push_front(entry);
                }
                return Err(e);
            }

            println!("============== Promotion successful ================");
            println!("Now cluster members: {:?}", membership.voters().await);
        }

        trace!(
            ?self.pending_promotions,
            "Step 6: Reschedule if any pending promotions remain"
        );
        // Step 6: Reschedule if any pending promotions remain
        if !self.pending_promotions.is_empty() {
            // Important: Re-send the event to trigger next cycle
            role_tx
                .send(RoleEvent::ReprocessEvent(Box::new(
                    RaftEvent::PromoteReadyLearners,
                )))
                .map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Send PromoteReadyLearners event failed: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
        }

        Ok(())
    }

    /// Removes the first `count` nodes from the pending queue and returns them
    pub(super) fn drain_batch(
        &mut self,
        count: usize,
    ) -> Vec<PendingPromotion> {
        let mut batch = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(entry) = self.pending_promotions.pop_front() {
                batch.push(entry);
            } else {
                break;
            }
        }
        batch
    }

    async fn safe_batch_promote(
        &mut self,
        batch: Vec<u32>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let change = Change::BatchPromote(BatchPromote {
            node_ids: batch.clone(),
            new_status: NodeStatus::Active as i32,
        });

        // Submit batch activation
        self.verify_leadership_limited_retry(
            vec![EntryPayload::config(change)],
            true,
            ctx,
            role_tx,
        )
        .await?;

        Ok(())
    }

    async fn run_periodic_maintenance(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        if let Err(e) = self.conditionally_purge_stale_learners(ctx).await {
            error!("Stale learner purge failed: {}", e);
        }

        if let Err(e) = self.conditionally_purge_zombie_nodes(role_tx, ctx).await {
            error!("Zombie node purge failed: {}", e);
        }

        // Regardless of whether a stale node is found, we set the next check according to a fixed
        // period
        self.reset_next_membership_maintenance_check(
            ctx.node_config().raft.membership.membership_maintenance_interval,
        );
        Ok(())
    }

    /// Periodic check triggered every ~30s in the worst-case scenario
    /// using priority-based lazy scheduling. Actual average frequency
    /// is inversely proportional to system load.
    pub async fn conditionally_purge_stale_learners(
        &mut self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        let config = &ctx.node_config.raft.membership.promotion;

        // Optimization: Skip check 99.9% of the time using scheduled trial method
        if self.pending_promotions.is_empty()
            || self.next_membership_maintenance_check > Instant::now()
        {
            trace!("Skipping stale learner check");
            return Ok(());
        }

        let now = Instant::now();
        let queue_len = self.pending_promotions.len();

        // Inspect only oldest 1% of items or max 100 entries per rules
        let inspect_count = queue_len.min(100).min(1.max(queue_len / 100));
        let mut stale_entries = Vec::new();

        trace!("Inspecting {} entries", inspect_count);
        for _ in 0..inspect_count {
            if let Some(entry) = self.pending_promotions.pop_front() {
                trace!(
                    "Inspecting entry: {:?} - {:?} - {:?}",
                    entry,
                    now.duration_since(entry.ready_since),
                    &config.stale_learner_threshold
                );
                if now.duration_since(entry.ready_since) > config.stale_learner_threshold {
                    stale_entries.push(entry);
                } else {
                    // Return non-stale entry and stop
                    self.pending_promotions.push_front(entry);
                    break;
                }
            } else {
                break;
            }
        }

        trace!("Stale learner check completed: {:?}", stale_entries);

        // Process collected stale entries
        for entry in stale_entries {
            if let Err(e) = self.handle_stale_learner(entry.node_id, ctx).await {
                error!("Failed to handle stale learner: {}", e);
            }
        }

        Ok(())
    }

    /// Remove non-Active zombie nodes that exceed failure threshold
    async fn conditionally_purge_zombie_nodes(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        let zombie_candidates = ctx.membership().get_zombie_candidates().await;
        let mut nodes_to_remove = Vec::new();

        for node_id in zombie_candidates {
            if let Some(status) = ctx.membership().get_node_status(node_id).await {
                if status != NodeStatus::Active {
                    nodes_to_remove.push(node_id);
                }
            }
        }
        // Batch removal if we have candidates
        if !nodes_to_remove.is_empty() {
            let change = Change::BatchRemove(BatchRemove {
                node_ids: nodes_to_remove.clone(),
            });

            info!(
                "Proposing batch removal of zombie nodes: {:?}",
                nodes_to_remove
            );

            // Submit single config change for all nodes
            match self
                .verify_leadership_limited_retry(
                    vec![EntryPayload::config(change)],
                    false,
                    ctx,
                    role_tx,
                )
                .await
            {
                Ok(true) => {
                    info!("Batch removal committed for nodes: {:?}", nodes_to_remove);
                }
                Ok(false) => {
                    warn!("Failed to commit batch removal");
                }
                Err(e) => {
                    error!("Batch removal error: {:?}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    pub fn reset_next_membership_maintenance_check(
        &mut self,
        membership_maintenance_interval: Duration,
    ) {
        self.next_membership_maintenance_check = Instant::now() + membership_maintenance_interval;
    }

    /// FINRA Rule 4370-approved remediation
    pub async fn handle_stale_learner(
        &mut self,
        node_id: u32,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // Step 1: Automated downgrade
        ctx.membership().update_node_status(node_id, NodeStatus::StandBy).await?;

        // Step 2: Trigger operator notification
        // ctx.ops_tx().send(OpsEvent::LearnerStalled(node_id));
        println!(
            "
            =====================
            Learner {node_id} is stalled
            =====================
            ",
        );
        info!("Learner {} is stalled", node_id);

        Ok(())
    }

    /// Check if current lease is still valid for LeaseRead policy
    pub fn is_lease_valid(
        &self,
        ctx: &RaftContext<T>,
    ) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let last_confirmed = self.lease_timestamp.load(std::sync::atomic::Ordering::Acquire);
        let lease_duration = ctx.node_config().raft.read_consistency.lease_duration_ms;

        if now <= last_confirmed {
            // Clock moved backwards or equal: conservatively treat lease as invalid
            error!("Clock moved backwards or equal: Now {now}, Last Confirmed {last_confirmed}");
            return false;
        }
        (now - last_confirmed) < lease_duration
    }

    /// Update lease timestamp after successful leadership verification
    fn update_lease_timestamp(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.lease_timestamp.store(now, std::sync::atomic::Ordering::Release);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_update_lease_timestamp(&self) {
        self.update_lease_timestamp();
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_set_lease_timestamp(
        &self,
        timestamp: u64,
    ) {
        self.lease_timestamp.store(timestamp, std::sync::atomic::Ordering::Release);
    }
}

impl<T: TypeConfig> From<&CandidateState<T>> for LeaderState<T> {
    fn from(candidate: &CandidateState<T>) -> Self {
        let ReplicationConfig {
            rpc_append_entries_in_batch_threshold,
            rpc_append_entries_batch_process_delay_in_ms,
            rpc_append_entries_clock_in_ms,
            ..
        } = candidate.node_config.raft.replication;

        Self {
            shared_state: candidate.shared_state.clone(),
            timer: Box::new(ReplicationTimer::new(
                rpc_append_entries_clock_in_ms,
                rpc_append_entries_batch_process_delay_in_ms,
            )),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            batch_buffer: Box::new(BatchBuffer::new(
                rpc_append_entries_in_batch_threshold,
                Duration::from_millis(rpc_append_entries_batch_process_delay_in_ms),
            )),

            node_config: candidate.node_config.clone(),

            scheduled_purge_upto: None,
            last_purged_index: candidate.last_purged_index,
            last_learner_check: Instant::now(),
            snapshot_in_progress: AtomicBool::new(false),
            peer_purge_progress: HashMap::new(),
            next_membership_maintenance_check: Instant::now(),
            pending_promotions: VecDeque::new(),

            _marker: PhantomData,
            lease_timestamp: AtomicU64::new(0),
        }
    }
}

impl<T: TypeConfig> Debug for LeaderState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LeaderState")
            .field("shared_state", &self.shared_state)
            .field("next_index", &self.next_index)
            .field("match_index", &self.match_index)
            .field("noop_log_id", &self.noop_log_id)
            .finish()
    }
}

/// Calculates the maximum number of nodes we can promote while keeping the total voter count
/// odd
///
/// - `current`: current number of voting nodes
/// - `available`: number of ready learners pending promotion
///
/// Returns the maximum number of nodes to promote (0 if no safe promotion exists)
pub fn calculate_safe_batch_size(
    current: usize,
    available: usize,
) -> usize {
    if (current + available) % 2 == 1 {
        // Promoting all is safe
        available
    } else {
        // We can only promote (available - 1) to keep the invariant
        // But if available - 1 == 0, then we cannot promote any?
        available.saturating_sub(1)
    }
}

pub(super) fn send_replay_raft_event(
    role_tx: &mpsc::UnboundedSender<RoleEvent>,
    raft_event: RaftEvent,
) -> Result<()> {
    role_tx.send(RoleEvent::ReprocessEvent(Box::new(raft_event))).map_err(|e| {
        let error_str = format!("{e:?}");
        error!("Failed to send: {}", error_str);
        NetworkError::SingalSendFailed(error_str).into()
    })
}
