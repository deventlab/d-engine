use super::LeaderStateSnapshot;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::buffers::BatchBuffer;
use super::buffers::ProposeBatchBuffer;
use super::candidate_state::CandidateState;
use super::role_state::RaftRoleState;
use super::role_state::check_and_trigger_snapshot;
use super::role_state::send_replay_raft_event;
use crate::AppendResults;
use crate::BackgroundSnapshotTransfer;
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
use crate::SystemError;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::cluster::is_majority;
use crate::ensure_safe_join;
use crate::event::ClientCmd;
use crate::stream::create_production_snapshot_stream;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
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
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use nanoid::nanoid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
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

// Type aliases for improved readability
type LinearizableReadRequest = (
    ClientReadRequest,
    MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
);

/// Write batch metadata: (start_log_index, response_senders, wait_for_apply)
type WriteMetadata = (
    u64,
    Vec<MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>>,
    bool,
);

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

/// Cached cluster topology metadata for hot path optimization.
///
/// This metadata is immutable during leadership and only updated on membership changes.
/// Caching avoids repeated async calls to membership queries in hot paths like append_entries.
#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    /// Single-voter cluster (quorum = 1, used for election and commit optimization)
    pub single_voter: bool,
    /// Total number of voters including self (updated on membership changes)
    pub total_voters: usize,
    /// Cached replication targets (voters + learners, excluding self)
    pub replication_targets: Vec<NodeMeta>,
}

/// Metrics context for backpressure monitoring (zero-allocation hot path)
///
/// Encapsulates all metrics-related state to avoid polluting the core LeaderState structure.
/// Pre-allocated labels ensure zero allocations in the request hot path.
pub struct BackpressureMetrics {
    /// Pre-allocated labels for write rejections: [("node_id", "\<id>"), ("type", "write")]
    labels_write: Arc<[(String, String)]>,
    /// Pre-allocated labels for read rejections: [("node_id", "\<id>"), ("type", "read")]
    labels_read: Arc<[(String, String)]>,
    /// Runtime switch (branch prediction optimized, ~0ns overhead when false)
    enabled: bool,
    /// Sample counter for gauge metrics (reduces overhead at high QPS)
    sample_counter: AtomicU32,
    /// Sampling rate (1 = no sampling, 10 = sample 1 in 10)
    sample_rate: u32,
}

impl BackpressureMetrics {
    /// Create new metrics context with pre-allocated labels
    pub fn new(
        node_id: u32,
        enabled: bool,
        sample_rate: u32,
    ) -> Self {
        let sample_rate = if sample_rate == 0 { 1 } else { sample_rate };
        let node_id_str = node_id.to_string();
        let labels_write = Arc::new([
            ("node_id".to_string(), node_id_str.clone()),
            ("type".to_string(), "write".to_string()),
        ]);
        let labels_read = Arc::new([
            ("node_id".to_string(), node_id_str),
            ("type".to_string(), "read".to_string()),
        ]);

        Self {
            labels_write,
            labels_read,
            enabled,
            sample_counter: AtomicU32::new(0),
            sample_rate,
        }
    }

    /// Record rejection metric (always counted, not sampled)
    #[inline]
    pub fn record_rejection(
        &self,
        is_write: bool,
    ) {
        if self.enabled {
            let labels = if is_write {
                &self.labels_write
            } else {
                &self.labels_read
            };
            metrics::counter!("backpressure.rejections", labels.as_ref()).increment(1);
        }
    }

    /// Record buffer utilization gauge (with sampling)
    #[inline]
    pub fn record_buffer_utilization(
        &self,
        utilization: f64,
        is_write: bool,
    ) {
        if self.enabled {
            let counter = self.sample_counter.fetch_add(1, Ordering::Relaxed);
            if counter % self.sample_rate == 0 {
                let labels = if is_write {
                    &self.labels_write
                } else {
                    &self.labels_read
                };
                metrics::gauge!("backpressure.buffer_utilization", labels.as_ref())
                    .set(utilization);
            }
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
    #[doc(hidden)]
    pub noop_log_id: Option<u64>,

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

    /// Record if there is on-going snapshot creation activity
    pub snapshot_in_progress: AtomicBool,

    // -- Request Processing --
    /// Batched proposal buffer for client requests
    ///
    /// Accumulates requests until either:
    /// 1. Batch reaches configured size limit
    /// 2. Explicit flush is triggered
    ///    SoA layout: payloads and senders stored in separate contiguous Vecs.
    ///    flush() builds one RaftRequestWithSignal via mem::take — true O(1), no unzip scan.
    pub(super) propose_buffer: Box<ProposeBatchBuffer>,

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
    pub last_learner_check: Instant,

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

    // -- Cluster Topology Cache --
    /// Cached cluster metadata (updated on membership changes)
    /// Avoids repeated async calls in hot paths
    pub(super) cluster_metadata: ClusterMetadata,

    /// Buffered linearizable read requests awaiting batch processing
    /// Only LinearizableRead policy uses batching with size/timeout thresholds
    pub(super) linearizable_read_buffer: Box<BatchBuffer<LinearizableReadRequest>>,

    /// Lease-based read requests awaiting processing
    /// Processed immediately in flush_cmd_buffers if lease is valid
    pub(super) lease_read_queue: VecDeque<(
        ClientReadRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    )>,

    /// Eventual consistency read requests awaiting processing
    /// Processed immediately in flush_cmd_buffers without any verification
    pub(super) eventual_read_queue: VecDeque<(
        ClientReadRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    )>,

    // -- Client Request Tracking --
    /// Maps log index to client response sender
    /// Used to return apply results (e.g., CAS success/failure) to clients
    /// Entries are removed after response is sent
    pub(super) pending_requests:
        HashMap<u64, MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>>,

    /// Pending linearizable reads waiting for SM to apply up to read_index.
    /// Key: read_index (SM must reach this index before reads can be served)
    /// Value: batch of (request, sender) pairs registered at that read_index
    /// Drained on role change; processed in ApplyCompleted handler.
    pub(super) pending_reads: BTreeMap<u64, VecDeque<LinearizableReadRequest>>,

    // -- Metrics (optional, encapsulated) --
    /// Backpressure metrics context (None when metrics disabled)
    backpressure_metrics: Option<Arc<BackpressureMetrics>>,

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
    ) -> Result<bool> {
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

    /// Track no-op entry index for linearizable read optimization.
    /// Called after no-op entry is committed on leadership transition.
    fn on_noop_committed(
        &mut self,
        ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        let noop_index = ctx.raft_log().last_entry_id();
        self.noop_log_id = Some(noop_index);
        debug!("Tracked noop_log_id: {}", noop_index);
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
            match self.verify_internal_quorum(payloads.clone(), ctx, role_tx).await {
                Ok(QuorumVerificationResult::Success) => {
                    return Ok(true);
                }
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
    /// Override: Initialize cluster metadata after becoming leader.
    /// Must be called once after leader election succeeds.
    async fn init_cluster_metadata(
        &mut self,
        membership: &Arc<T::M>,
    ) -> Result<()> {
        // Calculate total voter count including self as leader
        let voters = membership.voters().await;
        let total_voters = voters.len() + 1; // +1 for leader (self)

        // Get all replication targets (voters + learners, excluding self)
        let replication_targets = membership.replication_peers().await;

        // Single-voter cluster: only this node is a voter (quorum = 1)
        let single_voter = total_voters == 1;

        self.cluster_metadata = ClusterMetadata {
            single_voter,
            total_voters,
            replication_targets: replication_targets.clone(),
        };

        debug!(
            "Initialized cluster metadata: single_voter={}, total_voters={}, replication_targets={}",
            single_voter,
            total_voters,
            replication_targets.len()
        );
        Ok(())
    }

    async fn verify_internal_quorum(
        &mut self,
        payloads: Vec<EntryPayload>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<QuorumVerificationResult> {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.execute_request_immediately(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads,
                senders: vec![resp_tx],
                wait_for_apply_event: false,
            },
            ctx,
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
            "Node {} term {} transitioning to Follower",
            self.node_id(),
            self.current_term(),
        );
        println!(
            "[Node {}] Leader → Follower (term {})",
            self.node_id(),
            self.current_term()
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
        // Keep syncing leader_id (hot-path: ~5ns atomic store)
        self.shared_state().set_current_leader(self.node_id());

        // 1. Clear expired learners
        if let Err(e) = self.run_periodic_maintenance(role_tx, ctx).await {
            error!("Failed to run periodic maintenance: {}", e);
        }

        // 2. Heartbeat trigger check
        // Send heartbeat if the replication timer expires
        if now >= self.timer.replication_deadline() {
            debug!(?now, "tick::reset_replication timer");

            // Piggyback pending writes onto heartbeat; if nothing to write,
            // send an empty AppendEntries to maintain leadership and reset timer.
            let request = self.propose_buffer.flush();
            self.send_heartbeat_or_batch(request, role_tx, ctx).await?;
        }

        Ok(())
    }

    fn drain_read_buffer(&mut self) -> Result<()> {
        // Drain linearizable read buffer
        let batch = self.linearizable_read_buffer.take_all();
        if !batch.is_empty() {
            warn!(
                "Read batch: draining {} linearizable read requests due to role change",
                batch.len()
            );
            for (_, sender) in batch {
                let _ = sender.send(Err(tonic::Status::unavailable("Leader stepped down")));
            }
        }

        // Drain lease read queue
        if !self.lease_read_queue.is_empty() {
            warn!(
                "Read batch: draining {} lease read requests due to role change",
                self.lease_read_queue.len()
            );
            for (_, sender) in self.lease_read_queue.drain(..) {
                let _ = sender.send(Err(tonic::Status::unavailable("Leader stepped down")));
            }
        }

        // Drain eventual read queue
        if !self.eventual_read_queue.is_empty() {
            warn!(
                "Read batch: draining {} eventual read requests due to role change",
                self.eventual_read_queue.len()
            );
            for (_, sender) in self.eventual_read_queue.drain(..) {
                let _ = sender.send(Err(tonic::Status::unavailable("Leader stepped down")));
            }
        }

        // Drain pending linearizable reads awaiting ApplyCompleted
        if !self.pending_reads.is_empty() {
            let count: usize = self.pending_reads.values().map(|b| b.len()).sum();
            warn!(
                "Read batch: draining {} pending linearizable reads due to role change",
                count
            );
            for (_, batch) in std::mem::take(&mut self.pending_reads) {
                for (_, sender) in batch {
                    let _ = sender.send(Err(tonic::Status::unavailable("Leader stepped down")));
                }
            }
        }

        // Drain write buffer (propose_buffer)
        if !self.propose_buffer.is_empty() {
            warn!(
                "Write batch: draining {} pending write requests due to role change",
                self.propose_buffer.len()
            );

            if let Some(batch) = self.propose_buffer.flush() {
                for sender in batch.senders {
                    let _ = sender.send(Err(tonic::Status::failed_precondition("Not leader")));
                }
            }
        }

        Ok(())
    }

    fn push_client_cmd(
        &mut self,
        cmd: ClientCmd,
        ctx: &RaftContext<Self::T>,
    ) {
        use crate::client_command_to_entry_payloads;

        let backpressure = &ctx.node_config.raft.backpressure;

        match cmd {
            ClientCmd::Propose(req, sender) => {
                let current_pending = self.propose_buffer.len();

                // Record buffer utilization metric (with sampling)
                if let Some(ref metrics) = self.backpressure_metrics
                    && backpressure.max_pending_writes > 0
                {
                    let utilization =
                        (current_pending as f64 / backpressure.max_pending_writes as f64) * 100.0;
                    metrics.record_buffer_utilization(utilization, true);
                }

                // Check write backpressure limit
                if backpressure.should_reject_write(current_pending) {
                    // Record rejection metric
                    if let Some(ref metrics) = self.backpressure_metrics {
                        metrics.record_rejection(true);
                    }

                    let _ = sender.send(Err(Status::resource_exhausted(
                        "Too many pending write requests",
                    )));
                    return;
                }

                // Convert command to payload (will be merged in drain_batch)
                if let Some(cmd) = req.command {
                    let payload = client_command_to_entry_payloads(vec![cmd])
                        .into_iter()
                        .next()
                        .expect("client_command_to_entry_payloads should return 1 element");

                    // Push lightweight tuple directly (zero-copy, no Vec allocation)
                    self.propose_buffer.push(payload, sender);
                } else {
                    // Empty command: reject
                    let _ = sender.send(Err(Status::invalid_argument("Command is empty")));
                }
            }
            ClientCmd::Read(req, sender) => {
                // Determine effective read consistency policy
                let effective_policy = self.determine_read_policy(&req);

                // Route to appropriate buffer based on policy
                match effective_policy {
                    ServerReadConsistencyPolicy::LinearizableRead => {
                        let current_pending = self.linearizable_read_buffer.len();

                        // Record buffer utilization metric (with sampling)
                        if let Some(ref metrics) = self.backpressure_metrics
                            && backpressure.max_pending_reads > 0
                        {
                            let utilization = (current_pending as f64
                                / backpressure.max_pending_reads as f64)
                                * 100.0;
                            metrics.record_buffer_utilization(utilization, false);
                        }

                        // Check read backpressure limit
                        if backpressure.should_reject_read(current_pending) {
                            // Record rejection metric
                            if let Some(ref metrics) = self.backpressure_metrics {
                                metrics.record_rejection(false);
                            }

                            let _ = sender.send(Err(Status::resource_exhausted(
                                "Too many pending read requests",
                            )));
                            return;
                        }

                        self.linearizable_read_buffer.push((req, sender));
                    }
                    ServerReadConsistencyPolicy::LeaseRead => {
                        let current_pending = self.lease_read_queue.len();

                        // Record buffer utilization metric (with sampling)
                        if let Some(ref metrics) = self.backpressure_metrics
                            && backpressure.max_pending_reads > 0
                        {
                            let utilization = (current_pending as f64
                                / backpressure.max_pending_reads as f64)
                                * 100.0;
                            metrics.record_buffer_utilization(utilization, false);
                        }

                        // Check read backpressure limit
                        if backpressure.should_reject_read(current_pending) {
                            // Record rejection metric
                            if let Some(ref metrics) = self.backpressure_metrics {
                                metrics.record_rejection(false);
                            }

                            let _ = sender.send(Err(Status::resource_exhausted(
                                "Too many pending read requests",
                            )));
                            return;
                        }

                        self.lease_read_queue.push_back((req, sender));
                    }
                    ServerReadConsistencyPolicy::EventualConsistency => {
                        let current_pending = self.eventual_read_queue.len();

                        // Record buffer utilization metric (with sampling)
                        if let Some(ref metrics) = self.backpressure_metrics
                            && backpressure.max_pending_reads > 0
                        {
                            let utilization = (current_pending as f64
                                / backpressure.max_pending_reads as f64)
                                * 100.0;
                            metrics.record_buffer_utilization(utilization, false);
                        }

                        // Check read backpressure limit
                        if backpressure.should_reject_read(current_pending) {
                            // Record rejection metric
                            if let Some(ref metrics) = self.backpressure_metrics {
                                metrics.record_rejection(false);
                            }

                            let _ = sender.send(Err(Status::resource_exhausted(
                                "Too many pending read requests",
                            )));
                            return;
                        }

                        self.eventual_read_queue.push_back((req, sender));
                    }
                }
            }
        }
    }

    async fn flush_cmd_buffers(
        &mut self,
        ctx: &RaftContext<Self::T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        // Drain-based: unconditionally flush all buffered commands
        // No timeout/size checks - drain from channel already collected the batch

        let has_writes = !self.propose_buffer.is_empty();
        let has_reads = !self.linearizable_read_buffer.is_empty();

        // Fast path: pure write workload (most common case, ~95%)
        // Preserves original high-performance code path for write-heavy workloads
        if has_writes && !has_reads {
            if let Some(request) = self.propose_buffer.flush() {
                self.process_batch(std::iter::once(request), role_tx, ctx).await?;
            }
        }
        // Unified path: mixed write+read or pure read workloads
        // Merges RPC for 2*RTT → 1*RTT optimization in mixed scenarios
        else if has_writes || has_reads {
            self.unified_write_and_linear_read(ctx, role_tx).await?;
        }

        // Process lease reads (immediate, no batching)
        while let Some((req, sender)) = self.lease_read_queue.pop_front() {
            if let Err(e) = self.process_lease_read(req, sender, ctx, role_tx).await {
                error!("process_lease_read failed: {:?}", e);
            }
        }

        // Process eventual consistency reads (immediate, no batching)
        while let Some((req, sender)) = self.eventual_read_queue.pop_front() {
            self.process_eventual_read(req, sender, ctx);
        }

        Ok(())
    }

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
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
                let cluster_conf = ctx
                    .membership()
                    .retrieve_cluster_membership_config(self.shared_state().current_leader())
                    .await;
                debug!("Leader receive ClusterConf: {:?}", &cluster_conf);
                if let Err(e) = sender.send(Ok(cluster_conf)) {
                    // Receiver timed out and dropped — this is normal, do not crash the node
                    error!(
                        "Failed to send ClusterConf response (receiver dropped): {:?}",
                        e
                    );
                }
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

                match result {
                    Err(e) => {
                        error!(%e, "State machine snapshot creation failed");
                    }
                    Ok((
                        SnapshotMetadata {
                            last_included: last_included_option,
                            checksum: _,
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
                            // Phase 2: Execute local purge
                            // ----------------------
                            // Per Raft §7: Leader purges independently without peer coordination
                            trace!("Phase 2: Execute scheduled purge task");
                            debug!(?last_included, "Execute scheduled purge task");
                            if let Some(scheduled) = self.scheduled_purge_upto {
                                let purge_executor = ctx.purge_executor();
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
                info!(
                    "[Leader {}] ⚡ PromoteReadyLearners event received, pending_promotions: {:?}",
                    self.node_id(),
                    self.pending_promotions.iter().map(|p| p.node_id).collect::<Vec<_>>()
                );
                self.process_pending_promotions(ctx, &role_tx).await?;
            }

            RaftEvent::MembershipApplied => {
                // Save old membership for comparison
                let old_replication_targets = self.cluster_metadata.replication_targets.clone();

                // Refresh cluster metadata cache after membership change is applied
                debug!("Refreshing cluster metadata cache after membership change");
                if let Err(e) = self.update_cluster_metadata(&ctx.membership()).await {
                    warn!("Failed to update cluster metadata: {:?}", e);
                }

                // CRITICAL FIX #218: Initialize replication state for newly added peers
                // Per Raft protocol: When new members join, Leader must initialize their next_index
                let newly_added: Vec<u32> = self
                    .cluster_metadata
                    .replication_targets
                    .iter()
                    .filter(|new_peer| {
                        !old_replication_targets.iter().any(|old_peer| old_peer.id == new_peer.id)
                    })
                    .map(|peer| peer.id)
                    .collect();

                if !newly_added.is_empty() {
                    debug!(
                        "Initializing replication state for {} new peer(s): {:?}",
                        newly_added.len(),
                        newly_added
                    );
                    let last_entry_id = ctx.raft_log().last_entry_id();
                    if let Err(e) =
                        self.init_peers_next_index_and_match_index(last_entry_id, newly_added)
                    {
                        warn!("Failed to initialize next_index for new peers: {:?}", e);
                        // Non-fatal: next_index will use default value of 1,
                        // replication will still work but may be less efficient
                    }
                }
            }

            RaftEvent::ApplyCompleted {
                last_index,
                results,
            } => {
                let num_results = results.len();

                // Match apply results to pending client requests and send responses
                let responses: Vec<_> = results
                    .into_iter()
                    .filter_map(|r| {
                        self.pending_requests.remove(&r.index).map(|sender| (r, sender))
                    })
                    .collect();

                for (result, sender) in responses {
                    let response = if result.succeeded {
                        ClientResponse::write_success()
                    } else {
                        ClientResponse::cas_failure()
                    };

                    trace!(
                        "[Leader-{}] Sending response to client for index {}: succeeded={}",
                        self.node_id(),
                        result.index,
                        result.succeeded
                    );

                    if sender.send(Ok(response)).is_err() {
                        trace!(
                            "[Leader-{}] Client receiver dropped for index {}",
                            self.node_id(),
                            result.index
                        );
                    }
                }

                // Serve pending linearizable reads whose read_index <= last_index.
                let reads_to_serve: Vec<_> =
                    self.pending_reads.range(..=last_index).map(|(k, _)| *k).collect();

                for read_index in reads_to_serve {
                    if let Some(batch) = self.pending_reads.remove(&read_index) {
                        self.execute_pending_reads(batch, ctx);
                    }
                }

                // Check snapshot after SM apply — last_applied is now accurate.
                check_and_trigger_snapshot(
                    last_index,
                    Leader as i32,
                    self.current_term(),
                    ctx,
                    &role_tx,
                )?;

                trace!(
                    "[Leader-{}] TIMING: process_apply_completed({} results)",
                    self.node_id(),
                    num_results,
                );
            }

            RaftEvent::FatalError { source, error } => {
                error!("[Leader] Fatal error from {}: {}", source, error);
                let fatal_status = || tonic::Status::internal(format!("Node fatal error: {error}"));
                // Notify all pending write requests
                let pending: Vec<_> = self.pending_requests.drain().collect();
                if !pending.is_empty() {
                    warn!(
                        "[Leader] FatalError: notifying {} pending write requests",
                        pending.len()
                    );
                    for (_index, sender) in pending {
                        let _ = sender.send(Err(fatal_status()));
                    }
                }
                // Notify buffered linearizable reads (pre-flush)
                let lin_buf = self.linearizable_read_buffer.take_all();
                if !lin_buf.is_empty() {
                    warn!(
                        "[Leader] FatalError: notifying {} buffered linearizable reads",
                        lin_buf.len()
                    );
                    for (_, sender) in lin_buf {
                        let _ = sender.send(Err(fatal_status()));
                    }
                }
                // Notify pending linearizable reads awaiting ApplyCompleted
                if !self.pending_reads.is_empty() {
                    let count: usize = self.pending_reads.values().map(|b| b.len()).sum();
                    warn!(
                        "[Leader] FatalError: notifying {} pending linearizable reads",
                        count
                    );
                    for (_, batch) in std::mem::take(&mut self.pending_reads) {
                        for (_, sender) in batch {
                            let _ = sender.send(Err(fatal_status()));
                        }
                    }
                }
                // Notify queued lease reads
                if !self.lease_read_queue.is_empty() {
                    warn!(
                        "[Leader] FatalError: notifying {} queued lease reads",
                        self.lease_read_queue.len()
                    );
                    for (_, sender) in self.lease_read_queue.drain(..) {
                        let _ = sender.send(Err(fatal_status()));
                    }
                }
                // Notify queued eventual reads
                if !self.eventual_read_queue.is_empty() {
                    warn!(
                        "[Leader] FatalError: notifying {} queued eventual reads",
                        self.eventual_read_queue.len()
                    );
                    for (_, sender) in self.eventual_read_queue.drain(..) {
                        let _ = sender.send(Err(fatal_status()));
                    }
                }
                return Err(crate::Error::Fatal(format!(
                    "Fatal error from {source}: {error}"
                )));
            }

            RaftEvent::StepDownSelfRemoved => {
                // Only Leader can propose configuration changes and remove itself
                // Per Raft protocol: Leader steps down immediately after self-removal
                warn!(
                    "[Leader-{}] Removed from cluster membership, stepping down to Follower",
                    self.node_id()
                );
                role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
                    error!(
                        "[Leader-{}] Failed to send BecomeFollower after self-removal: {:?}",
                        self.node_id(),
                        e
                    );
                    NetworkError::SingalSendFailed(format!(
                        "BecomeFollower after self-removal: {e:?}"
                    ))
                })?;
                return Ok(());
            }
        }

        Ok(())
    }
}

impl<T: TypeConfig> LeaderState<T> {
    /// Initialize cluster metadata after becoming leader.
    /// Update cluster metadata when membership changes.
    pub async fn update_cluster_metadata(
        &mut self,
        membership: &Arc<T::M>,
    ) -> Result<()> {
        // Calculate total voter count including self as leader
        let voters = membership.voters().await;
        let total_voters = voters.len() + 1; // +1 for leader (self)

        // Get all replication targets (voters + learners, excluding self)
        let replication_targets = membership.replication_peers().await;

        // Single-voter cluster: only this node is a voter (quorum = 1)
        let single_voter = total_voters == 1;

        self.cluster_metadata = ClusterMetadata {
            single_voter,
            total_voters,
            replication_targets: replication_targets.clone(),
        };

        debug!(
            "Updated cluster metadata: single_voter={}, total_voters={}, replication_targets={}",
            single_voter,
            total_voters,
            replication_targets.len()
        );
        Ok(())
    }

    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
            role: Leader as i32,
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
    /// Execute a Raft request immediately, bypassing the normal batching mechanism.
    ///
    /// This is used for quorum verification requests that must be sent immediately
    /// without waiting for batching (e.g., config changes, leadership verification).
    pub async fn execute_request_immediately(
        &mut self,
        raft_request_with_signal: RaftRequestWithSignal,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!(
            "Leader::execute_request_immediately, request_id: {}",
            raft_request_with_signal.id
        );

        // Always bypass buffer for immediate execution (quorum verification)
        let batch = VecDeque::from([raft_request_with_signal]);
        self.process_batch(batch, role_tx, ctx).await?;

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
        batch: impl IntoIterator<Item = RaftRequestWithSignal>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        self.timer.reset_replication();

        let start_index = ctx.raft_log().last_entry_id() + 1;
        let (payloads, write_metadata) = Self::merge_batch_to_write_metadata(batch, start_index);

        if !payloads.is_empty() {
            trace!(?payloads, "[Node-{}] process_batch", ctx.node_id);
        }

        self.execute_and_process_raft_rpc(payloads, write_metadata, None, ctx, role_tx)
            .await
    }

    /// Heartbeat dispatch: piggyback a write batch if available, otherwise send
    /// an empty AppendEntries to maintain leadership. Always resets the replication
    /// timer — the intent is explicit here rather than hidden inside `process_batch`.
    async fn send_heartbeat_or_batch(
        &mut self,
        request: Option<RaftRequestWithSignal>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        self.timer.reset_replication();
        match request {
            Some(req) => {
                // Has pending writes — process as a real batch.
                let start_index = ctx.raft_log().last_entry_id() + 1;
                let (payloads, write_metadata) =
                    Self::merge_batch_to_write_metadata(std::iter::once(req), start_index);
                self.execute_and_process_raft_rpc(payloads, write_metadata, None, ctx, role_tx)
                    .await
            }
            None => {
                // No pending writes — send empty AppendEntries as heartbeat.
                self.execute_and_process_raft_rpc(vec![], None, None, ctx, role_tx).await
            }
        }
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

    pub async fn check_learner_progress(
        &mut self,
        learner_progress: &HashMap<u32, Option<u64>>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        debug!(?learner_progress, "check_learner_progress");

        if !self.should_check_learner_progress(ctx) {
            return Ok(());
        }

        if learner_progress.is_empty() {
            return Ok(());
        }

        let ready_learners = self.find_promotable_learners(learner_progress, ctx).await;
        let new_promotions = self.deduplicate_promotions(ready_learners);

        if !new_promotions.is_empty() {
            self.enqueue_and_notify_promotions(new_promotions, role_tx)?;
        }

        Ok(())
    }

    /// Check if enough time has elapsed since last learner progress check
    fn should_check_learner_progress(
        &mut self,
        ctx: &RaftContext<T>,
    ) -> bool {
        let throttle_interval =
            Duration::from_millis(ctx.node_config().raft.learner_check_throttle_ms);
        if self.last_learner_check.elapsed() < throttle_interval {
            return false;
        }
        self.last_learner_check = Instant::now();
        true
    }

    /// Find learners that are caught up and eligible for promotion
    async fn find_promotable_learners(
        &self,
        learner_progress: &HashMap<u32, Option<u64>>,
        ctx: &RaftContext<T>,
    ) -> Vec<u32> {
        let leader_commit = self.commit_index();
        let threshold = ctx.node_config().raft.learner_catchup_threshold;
        let membership = ctx.membership();

        let mut ready_learners = Vec::new();

        for (&node_id, &match_index_opt) in learner_progress.iter() {
            if !membership.contains_node(node_id).await {
                continue;
            }

            if !self.is_learner_caught_up(match_index_opt, leader_commit, threshold) {
                continue;
            }

            let node_status =
                membership.get_node_status(node_id).await.unwrap_or(NodeStatus::ReadOnly);
            if !node_status.is_promotable() {
                debug!(
                    ?node_id,
                    ?node_status,
                    "Learner caught up but status is not Promotable, skipping"
                );
                continue;
            }

            debug!(
                ?node_id,
                match_index = ?match_index_opt.unwrap_or(0),
                ?leader_commit,
                gap = leader_commit.saturating_sub(match_index_opt.unwrap_or(0)),
                "Learner caught up"
            );
            ready_learners.push(node_id);
        }

        ready_learners
    }

    /// Check if learner has caught up with leader based on log gap
    fn is_learner_caught_up(
        &self,
        match_index: Option<u64>,
        leader_commit: u64,
        threshold: u64,
    ) -> bool {
        let match_index = match_index.unwrap_or(0);
        let gap = leader_commit.saturating_sub(match_index);
        gap <= threshold
    }

    /// Remove learners already in pending promotions queue
    fn deduplicate_promotions(
        &self,
        ready_learners: Vec<u32>,
    ) -> Vec<u32> {
        let already_pending: std::collections::HashSet<_> =
            self.pending_promotions.iter().map(|p| p.node_id).collect();

        ready_learners.into_iter().filter(|id| !already_pending.contains(id)).collect()
    }

    /// Add promotions to queue and send notification event
    fn enqueue_and_notify_promotions(
        &mut self,
        new_promotions: Vec<u32>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        info!(
            ?new_promotions,
            "Learners caught up, adding to pending promotions"
        );

        for node_id in new_promotions {
            self.pending_promotions
                .push_back(PendingPromotion::new(node_id, Instant::now()));
        }

        role_tx
            .send(RoleEvent::ReprocessEvent(Box::new(
                RaftEvent::PromoteReadyLearners,
            )))
            .map_err(|e| {
                let error_str = format!("{e:?}");
                error!("Failed to send PromoteReadyLearners: {}", error_str);
                Error::System(SystemError::Network(NetworkError::SingalSendFailed(
                    error_str,
                )))
            })?;

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
        // Use verify_leadership_persistent for config changes (must eventually succeed)
        match self
            .verify_leadership_persistent(vec![EntryPayload::config(config_change)], ctx, role_tx)
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

    /// Calculate safe read index for linearizable reads.
    ///
    /// Returns max(commitIndex, noopIndex) to ensure:
    /// 1. All committed data is visible
    /// 2. Current term's no-op entry is accounted for
    /// 3. Read index is fixed at request arrival time (avoids waiting for concurrent writes)
    ///
    /// This optimization prevents reads from waiting for writes that arrived
    /// after the read request started processing.
    #[doc(hidden)]
    pub fn calculate_read_index(&self) -> u64 {
        let commit_index = self.commit_index();
        let noop_index = self.noop_log_id.unwrap_or(0);
        std::cmp::max(commit_index, noop_index)
    }

    /// Wait for state machine to apply up to target index.
    ///
    /// This is used by linearizable reads to ensure they see all committed data
    /// up to the read_index calculated at request arrival time.
    #[doc(hidden)]
    pub async fn wait_until_applied(
        &self,
        target_index: u64,
        state_machine_handler: &Arc<SMHOF<T>>,
        last_applied: u64,
    ) -> Result<()> {
        if last_applied < target_index {
            state_machine_handler.update_pending(target_index);

            let timeout_ms = self.node_config.raft.read_consistency.state_machine_sync_timeout_ms;
            state_machine_handler
                .wait_applied(target_index, std::time::Duration::from_millis(timeout_ms))
                .await?;

            debug!("wait_until_applied: target_index={} success", target_index);
        }
        Ok(())
    }

    #[instrument(skip(self))]
    fn scheduled_purge_upto(
        &mut self,
        received_last_included: LogId,
    ) {
        if let Some(existing) = self.scheduled_purge_upto
            && existing.index >= received_last_included.index
        {
            warn!(
                ?received_last_included,
                ?existing,
                "Will not update scheduled_purge_upto, received invalid last_included log"
            );
            return;
        }
        info!(?self.scheduled_purge_upto, ?received_last_included, "Updte scheduled_purge_upto.");
        self.scheduled_purge_upto = Some(received_last_included);
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
    /// 3. **Operation Atomicity**   `pending_purge.is_none()`
    ///    - Ensures only one concurrent purge operation
    ///    - Critical for linearizable state machine semantics
    ///
    /// # Implementation Notes
    /// - Per Raft §7: "Each server compacts its log independently"
    /// - Leader purges immediately after snapshot without waiting for followers
    /// - Lagging followers recover via InstallSnapshot RPC
    /// - Actual log discard should be deferred until storage confirms snapshot persistence
    #[instrument(skip(self))]
    pub fn can_purge_logs(
        &self,
        last_purge_index: Option<LogId>,
        last_included_in_snapshot: LogId,
    ) -> bool {
        let commit_index = self.commit_index();
        debug!(
            ?commit_index,
            ?last_purge_index,
            ?last_included_in_snapshot,
            "can_purge_logs"
        );

        let monotonic_check = last_purge_index
            .map(|lid| lid.index < last_included_in_snapshot.index)
            .unwrap_or(true);

        // Per Raft §7: Leader purges independently after snapshot
        // No peer coordination required - lagging followers get InstallSnapshot
        last_included_in_snapshot.index < commit_index && monotonic_check
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
        let status = join_request.status;
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
            status,
        });

        // 3. Submit config change, and wait for quorum confirmation
        debug!("3. Wait for quorum confirmation");
        // Use verify_leadership_persistent for config changes (must eventually succeed)
        match self
            .verify_leadership_persistent(vec![EntryPayload::config(config_change)], ctx, role_tx)
            .await
        {
            Ok(true) => {
                // 4. Update node status to Syncing
                debug!("4. Update node status to Syncing");

                debug!(
                    "After updating, the replications peers: {:?}",
                    ctx.membership().replication_peers().await
                );

                // Note: Cluster metadata cache will be updated via MembershipApplied event
                // after CommitHandler applies the config change to membership state

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
            config: Some(
                ctx.membership()
                    .retrieve_cluster_membership_config(self.shared_state().current_leader())
                    .await,
            ),
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

    #[cfg(any(test, feature = "__test_support"))]
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        let ReplicationConfig {
            rpc_append_entries_clock_in_ms,
            ..
        } = node_config.raft.replication;

        let batch_size = node_config.raft.batching.max_batch_size;

        let enable_batch = node_config.raft.metrics.enable_batch;
        // Initialize backpressure metrics (None if disabled)
        let backpressure_metrics = if node_config.raft.metrics.enable_backpressure {
            Some(Arc::new(BackpressureMetrics::new(
                node_id,
                true,
                node_config.raft.metrics.sample_rate,
            )))
        } else {
            None
        };

        LeaderState {
            cluster_metadata: ClusterMetadata {
                single_voter: false,
                total_voters: 0,
                replication_targets: vec![],
            },
            shared_state: SharedState::new(node_id, None, None),
            timer: Box::new(ReplicationTimer::new(rpc_append_entries_clock_in_ms)),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            propose_buffer: Box::new(ProposeBatchBuffer::new(batch_size).with_length_gauge(
                node_id,
                "propose",
                enable_batch,
            )),

            node_config,
            scheduled_purge_upto: None,
            last_purged_index: None, //TODO
            last_learner_check: Instant::now(),
            snapshot_in_progress: AtomicBool::new(false),
            next_membership_maintenance_check: Instant::now(),
            pending_promotions: VecDeque::new(),
            lease_timestamp: AtomicU64::new(0),
            linearizable_read_buffer: Box::new(BatchBuffer::new(batch_size).with_length_gauge(
                node_id,
                "linearizable",
                enable_batch,
            )),
            lease_read_queue: VecDeque::new(),
            eventual_read_queue: VecDeque::new(),
            pending_requests: HashMap::new(),
            pending_reads: BTreeMap::new(),
            backpressure_metrics,
            _marker: PhantomData,
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
        debug!(
            "[Leader {}] 🔄 process_pending_promotions called, pending: {:?}",
            self.node_id(),
            self.pending_promotions.iter().map(|p| p.node_id).collect::<Vec<_>>()
        );

        // Get promotion configuration from the node config
        let config = &ctx.node_config().raft.membership.promotion;

        // Step 1: Remove stale entries (older than configured threshold)
        let now = Instant::now();
        self.pending_promotions.retain(|entry| {
            now.duration_since(entry.ready_since) <= config.stale_learner_threshold
        });

        if self.pending_promotions.is_empty() {
            debug!(
                "[Leader {}] ❌ pending_promotions is empty after stale cleanup",
                self.node_id()
            );
            return Ok(());
        }

        // Step 2: Get current voter count (including self as leader)
        let membership = ctx.membership();
        let current_voters = membership.voters().await.len() + 1; // +1 for self (leader)
        debug!(
            "[Leader {}] 📊 current_voters: {}, pending: {}",
            self.node_id(),
            current_voters,
            self.pending_promotions.len()
        );

        // Step 3: Calculate the maximum batch size that preserves an odd total
        let max_batch_size =
            calculate_safe_batch_size(current_voters, self.pending_promotions.len());
        debug!(
            "[Leader {}] 🎯 max_batch_size: {}",
            self.node_id(),
            max_batch_size
        );

        if max_batch_size == 0 {
            // Nothing we can safely promote now
            debug!(
                "[Leader {}] ⚠️ max_batch_size is 0, cannot promote now",
                self.node_id()
            );
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

            info!(
                "Promotion successful. Cluster members: {:?}",
                membership.voters().await
            );
        }

        trace!(
            ?self.pending_promotions,
            "Step 6: Reschedule if any pending promotions remain"
        );
        // Step 6: Reschedule if any pending promotions remain
        if !self.pending_promotions.is_empty() {
            debug!(
                "[Leader {}] 🔁 Re-sending PromoteReadyLearners for remaining pending: {:?}",
                self.node_id(),
                self.pending_promotions.iter().map(|p| p.node_id).collect::<Vec<_>>()
            );
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
    /// Merge a batch of requests into unified write metadata for RPC processing.
    ///
    /// OR-combines wait_for_apply flags: if any request needs apply confirmation, all wait.
    /// This is safe because in practice all requests in a batch share the same flag
    /// (writes always true, noop/config always false — never mixed).
    pub(super) fn merge_batch_to_write_metadata(
        batch: impl IntoIterator<Item = RaftRequestWithSignal>,
        start_idx: u64,
    ) -> (Vec<EntryPayload>, Option<WriteMetadata>) {
        let mut all_payloads = Vec::new();
        let mut all_senders = Vec::new();
        let mut any_wait_for_apply = false;

        for mut req in batch {
            all_payloads.extend(std::mem::take(&mut req.payloads));
            all_senders.extend(req.senders);
            any_wait_for_apply |= req.wait_for_apply_event;
        }

        if all_payloads.is_empty() && all_senders.is_empty() {
            return (all_payloads, None);
        }

        (
            all_payloads,
            Some((start_idx, all_senders, any_wait_for_apply)),
        )
    }

    /// Core RPC execution and unified result processing.
    ///
    /// Shared by `process_batch` (write-only) and `unified_write_and_linear_read` (write+read).
    /// Handles all 4 result cases: quorum success, quorum failure, higher term, other errors.
    async fn execute_and_process_raft_rpc(
        &mut self,
        payloads: Vec<EntryPayload>,
        write_metadata: Option<WriteMetadata>,
        read_batch: Option<Vec<LinearizableReadRequest>>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        trace!(
            cluster_size = self.cluster_metadata.total_voters,
            payload_count = payloads.len(),
        );

        let result = ctx
            .replication_handler()
            .handle_raft_request_in_batch(
                payloads,
                self.state_snapshot(),
                self.leader_state_snapshot(),
                &self.cluster_metadata,
                ctx,
            )
            .await;

        debug!(?result, "execute_and_process_raft_rpc");

        match result {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates,
                learner_progress,
            }) => {
                self.update_lease_timestamp();
                self.update_peer_indexes(&peer_updates);
                if let Err(e) = self.check_learner_progress(&learner_progress, ctx, role_tx).await {
                    error!(?e, "check_learner_progress failed");
                }

                let new_commit_index = if self.cluster_metadata.single_voter {
                    let last_log_index = ctx.raft_log().last_entry_id();
                    if last_log_index > self.commit_index() {
                        Some(last_log_index)
                    } else {
                        None
                    }
                } else {
                    self.calculate_new_commit_index(ctx.raft_log(), &peer_updates)
                };

                if let Some(new_commit_index) = new_commit_index {
                    debug!(
                        "[Leader-{}] New commit acknowledged: {}",
                        self.node_id(),
                        new_commit_index
                    );
                    self.update_commit_index_with_signal(
                        Leader as i32,
                        self.current_term(),
                        new_commit_index,
                        role_tx,
                    )?;
                }

                if let Some((start_idx, senders, wait_for_apply)) = write_metadata {
                    if wait_for_apply {
                        for (i, sender) in senders.into_iter().enumerate() {
                            self.pending_requests.insert(start_idx + i as u64, sender);
                        }
                    } else {
                        for sender in senders {
                            let _ = sender.send(Ok(ClientResponse::write_success()));
                        }
                    }
                }

                if let Some(read_batch) = read_batch {
                    let read_index = self.calculate_read_index();
                    let last_applied = ctx.state_machine().last_applied().index;
                    if last_applied >= read_index {
                        // Fast path: SM already caught up, serve immediately
                        self.execute_pending_reads(read_batch, ctx);
                    } else {
                        // Slow path: register for ApplyCompleted callback
                        self.pending_reads.entry(read_index).or_default().extend(read_batch);
                    }
                }

                Ok(())
            }

            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates,
                learner_progress,
            }) => {
                self.update_peer_indexes(&peer_updates);
                if let Err(e) = self.check_learner_progress(&learner_progress, ctx, role_tx).await {
                    error!(?e, "check_learner_progress failed");
                }

                let responses_received = peer_updates.len();
                let cluster_size = self.cluster_metadata.total_voters;
                let error_code = if is_majority(responses_received, cluster_size) {
                    ErrorCode::RetryRequired
                } else {
                    ErrorCode::ProposeFailed
                };

                if let Some((_, senders, _)) = write_metadata {
                    for sender in senders {
                        let _ = sender.send(Ok(ClientResponse::client_error(error_code)));
                    }
                }
                if let Some(read_batch) = read_batch {
                    let status = tonic::Status::failed_precondition("Quorum not reached");
                    for (_, sender) in read_batch {
                        let _ = sender.send(Err(status.clone()));
                    }
                }

                Ok(())
            }

            Err(Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(
                higher_term,
            )))) => {
                warn!("Higher term detected: {}", higher_term);
                self.update_current_term(higher_term);
                self.send_become_follower_event(None, role_tx)?;

                if let Some((_, senders, _)) = write_metadata {
                    for sender in senders {
                        let _ =
                            sender.send(Ok(ClientResponse::client_error(ErrorCode::TermOutdated)));
                    }
                }
                if let Some(read_batch) = read_batch {
                    let status = tonic::Status::failed_precondition("Term outdated");
                    for (_, sender) in read_batch {
                        let _ = sender.send(Err(status.clone()));
                    }
                }

                Err(ReplicationError::HigherTerm(higher_term).into())
            }

            Err(e) => {
                error!("RPC failed: {:?}", e);

                if let Some((_, senders, _)) = write_metadata {
                    for sender in senders {
                        let _ =
                            sender.send(Ok(ClientResponse::client_error(ErrorCode::ProposeFailed)));
                    }
                }
                if let Some(read_batch) = read_batch {
                    let error_msg = format!("RPC failed: {e}");
                    let status = tonic::Status::failed_precondition(error_msg);
                    for (_, sender) in read_batch {
                        let _ = sender.send(Err(status.clone()));
                    }
                }

                Err(e)
            }
        }
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
        // Use verify_leadership_persistent for config changes (must eventually succeed)
        self.verify_leadership_persistent(vec![EntryPayload::config(change)], ctx, role_tx)
            .await?;

        Ok(())
    }

    async fn run_periodic_maintenance(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        if let Err(e) = self.conditionally_purge_stale_learners(role_tx, ctx).await {
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
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
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
            if let Err(e) = self.handle_stale_learner(entry.node_id, role_tx, ctx).await {
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
        // Optimize: get membership reference once to reduce lock contention
        let membership = ctx.membership();
        let zombie_candidates = membership.get_zombie_candidates().await;
        let mut nodes_to_remove = Vec::new();

        for node_id in zombie_candidates {
            if let Some(status) = membership.get_node_status(node_id).await
                && status != NodeStatus::Active
            {
                nodes_to_remove.push(node_id);
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
            // Use verify_leadership_persistent for config changes (must eventually succeed)
            match self
                .verify_leadership_persistent(vec![EntryPayload::config(change)], ctx, role_tx)
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

    /// Remove stalled learner via membership change consensus
    pub async fn handle_stale_learner(
        &mut self,
        node_id: u32,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // Stalled learner detected - remove via membership change (requires consensus)
        warn!(
            "Learner {} is stalled, removing from cluster via consensus",
            node_id
        );

        let change = Change::BatchRemove(BatchRemove {
            node_ids: vec![node_id],
        });

        // Submit removal through membership change entry (requires quorum consensus)
        // Use verify_leadership_persistent for config changes (must eventually succeed)
        match self
            .verify_leadership_persistent(vec![EntryPayload::config(change)], ctx, role_tx)
            .await
        {
            Ok(true) => {
                info!(
                    "Stalled learner {} successfully removed from cluster",
                    node_id
                );
            }
            Ok(false) => {
                warn!("Failed to commit removal of stalled learner {}", node_id);
            }
            Err(e) => {
                error!("Error removing stalled learner {}: {:?}", node_id, e);
                return Err(e);
            }
        }

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

        if now < last_confirmed {
            // Clock moved backwards: system clock issue, treat lease as invalid
            error!("Clock moved backwards: Now {now}, Last Confirmed {last_confirmed}");
            return false;
        }

        // Allow multiple requests within the same millisecond (now == last_confirmed)
        if now == last_confirmed {
            return true;
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

    /// Verify leadership with quorum and refresh lease timestamp on success.
    /// This is a single-shot verification (fast-fail) optimized for read operations.
    ///
    /// Returns Ok(()) if verification succeeds and lease is refreshed.
    /// Returns Err on any verification failure.
    async fn verify_leadership_and_refresh_lease(
        &mut self,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        match self.verify_internal_quorum(vec![], ctx, role_tx).await {
            Ok(QuorumVerificationResult::Success) => Ok(()),
            Ok(QuorumVerificationResult::LeadershipLost) => Err(ConsensusError::Replication(
                ReplicationError::NotLeader { leader_id: None },
            )
            .into()),
            Ok(QuorumVerificationResult::RetryRequired) => {
                Err(ConsensusError::Replication(ReplicationError::QuorumNotReached).into())
            }
            Err(e) => Err(e),
        }
    }

    /// Unified write + linearizable read: single RPC for both (2*RTT → 1*RTT).
    ///
    /// Safety: write quorum verification also confirms leadership for reads (Raft §6.4).
    pub(super) async fn unified_write_and_linear_read(
        &mut self,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        // Extract write metadata
        let (payloads, write_metadata) = if !self.propose_buffer.is_empty() {
            if let Some(req) = self.propose_buffer.flush() {
                let start_idx = ctx.raft_log().last_entry_id() + 1;
                (
                    req.payloads,
                    Some((start_idx, req.senders, req.wait_for_apply_event)),
                )
            } else {
                (Vec::new(), None)
            }
        } else {
            (Vec::new(), None)
        };

        // Extract read batch
        let read_batch = if !self.linearizable_read_buffer.is_empty() {
            Some(self.linearizable_read_buffer.take_all())
        } else {
            None
        };

        self.execute_and_process_raft_rpc(payloads, write_metadata, read_batch, ctx, role_tx)
            .await
    }

    /// Execute a batch of reads against the state machine and respond to clients.
    /// Caller must ensure SM has already applied up to the required read_index.
    fn execute_pending_reads(
        &self,
        read_batch: impl IntoIterator<Item = LinearizableReadRequest>,
        ctx: &RaftContext<T>,
    ) {
        for (req, sender) in read_batch {
            let results = ctx
                .handlers
                .state_machine_handler
                .read_from_state_machine(req.keys)
                .unwrap_or_default();
            let _ = sender.send(Ok(ClientResponse::read_results(results)));
        }
    }

    #[cfg(test)]
    pub(crate) fn test_update_lease_timestamp(&mut self) {
        self.update_lease_timestamp();
    }

    /// Determine effective read consistency policy based on client request and server config
    pub(super) fn determine_read_policy(
        &self,
        req: &ClientReadRequest,
    ) -> ServerReadConsistencyPolicy {
        use d_engine_proto::client::ReadConsistencyPolicy as ClientReadConsistencyPolicy;

        if req.has_consistency_policy() {
            // Client explicitly specified policy
            if self.node_config.raft.read_consistency.allow_client_override {
                match req.consistency_policy() {
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
                self.node_config.raft.read_consistency.default_policy.clone()
            }
        } else {
            // No client policy specified - use server default
            self.node_config.raft.read_consistency.default_policy.clone()
        }
    }

    /// Process single lease-based read request
    pub(super) async fn process_lease_read(
        &mut self,
        req: ClientReadRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        if self.is_lease_valid(ctx) {
            // Lease valid - serve immediately
            let results = ctx
                .handlers
                .state_machine_handler
                .read_from_state_machine(req.keys)
                .unwrap_or_default();
            let response = ClientResponse::read_results(results);
            let _ = sender.send(Ok(response));
        } else {
            // Lease expired - refresh and then serve
            if let Err(e) = self.verify_leadership_and_refresh_lease(ctx, role_tx).await {
                warn!("[Leader] Lease read: leadership verification failed: {e}");
                if sender
                    .send(Err(tonic::Status::unavailable(format!(
                        "Leadership verification failed: {e}"
                    ))))
                    .is_err()
                {
                    warn!(
                        "[Leader] Lease read: client already disconnected before error response could be sent"
                    );
                }
                return Err(e);
            }
            let results = ctx
                .handlers
                .state_machine_handler
                .read_from_state_machine(req.keys)
                .unwrap_or_default();
            let response = ClientResponse::read_results(results);
            let _ = sender.send(Ok(response));
        }
        Ok(())
    }

    /// Process single eventual consistency read request
    fn process_eventual_read(
        &mut self,
        req: ClientReadRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
        ctx: &RaftContext<T>,
    ) {
        // Eventual consistency: serve immediately without any verification
        let results = ctx
            .handlers
            .state_machine_handler
            .read_from_state_machine(req.keys)
            .unwrap_or_default();
        let response = ClientResponse::read_results(results);
        let _ = sender.send(Ok(response));
    }
}

impl<T: TypeConfig> From<&CandidateState<T>> for LeaderState<T> {
    fn from(candidate: &CandidateState<T>) -> Self {
        let ReplicationConfig {
            rpc_append_entries_clock_in_ms,
            ..
        } = candidate.node_config.raft.replication;

        // Clone shared_state and set self as leader immediately
        let shared_state = candidate.shared_state.clone();
        shared_state.set_current_leader(candidate.node_id());

        // Initialize backpressure metrics (None if disabled)
        let backpressure_metrics = if candidate.node_config.raft.metrics.enable_backpressure {
            Some(Arc::new(BackpressureMetrics::new(
                candidate.node_id(),
                true,
                candidate.node_config.raft.metrics.sample_rate,
            )))
        } else {
            None
        };

        Self {
            shared_state,
            timer: Box::new(ReplicationTimer::new(rpc_append_entries_clock_in_ms)),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,

            propose_buffer: Box::new(
                ProposeBatchBuffer::new(candidate.node_config.raft.batching.max_batch_size)
                    .with_length_gauge(
                        candidate.node_id(),
                        "propose",
                        candidate.node_config.raft.metrics.enable_batch,
                    ),
            ),

            node_config: candidate.node_config.clone(),

            scheduled_purge_upto: None,
            last_purged_index: candidate.last_purged_index,
            last_learner_check: Instant::now(),
            snapshot_in_progress: AtomicBool::new(false),
            next_membership_maintenance_check: Instant::now(),
            pending_promotions: VecDeque::new(),
            cluster_metadata: ClusterMetadata {
                single_voter: false,
                total_voters: 0,
                replication_targets: vec![],
            },
            lease_timestamp: AtomicU64::new(0),
            linearizable_read_buffer: Box::new(
                BatchBuffer::new(candidate.node_config.raft.batching.max_batch_size)
                    .with_length_gauge(
                        candidate.node_id(),
                        "linearizable",
                        candidate.node_config.raft.metrics.enable_batch,
                    ),
            ),
            lease_read_queue: VecDeque::new(),
            eventual_read_queue: VecDeque::new(),
            pending_requests: HashMap::new(),
            pending_reads: BTreeMap::new(),
            backpressure_metrics,
            _marker: PhantomData,
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
