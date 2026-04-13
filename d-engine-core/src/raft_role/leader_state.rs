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
use crate::BackgroundSnapshotTransfer;
use crate::ConnectionType;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::PeerUpdate;
use crate::PurgeExecutor;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::RaftRequestWithSignal;
use crate::ReadConsistencyPolicy as ServerReadConsistencyPolicy;
use crate::ReplicationConfig;
use crate::ReplicationCore;
use crate::ReplicationError;
use crate::ReplicationTimer;
use crate::Result;
use crate::RetryPolicies;
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
use crate::alias::TROF;
use crate::event::ClientCmd;
use crate::network::Transport;
use crate::stream::create_production_snapshot_stream;
use async_trait::async_trait;
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
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::append_entries_response;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use rand::distr::SampleString;
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
use tonic::Status;
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

/// Metadata for an in-flight write batch awaiting quorum commit.
/// Stored in `pending_client_writes` keyed by `end_log_index`.
pub(super) struct WriteMetadata {
    pub(super) start_idx: u64,
    pub(super) senders: Vec<MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>>,
    pub(super) wait_for_apply: bool,
    /// Wall-clock deadline; batch is failed with deadline_exceeded if commit has not
    /// arrived by this time.  Set in execute_and_process_raft_rpc Phase 2.
    // NOTE: expiry is checked on each tick; actual timeout may exceed deadline by up to
    // one tick interval (~150-300ms). Acceptable for Raft workloads.
    pub(super) deadline: Instant,
}

/// A lease read request waiting for quorum ACK to refresh the lease.
/// Stored in `pending_lease_reads` (VecDeque); drained after `update_lease_timestamp()`.
pub(super) struct PendingLeaseRead {
    pub(super) request: ClientReadRequest,
    pub(super) sender: MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    pub(super) deadline: Instant,
}

/// A batch of linearizable read requests waiting for the state machine to reach `read_index`.
/// Stored in `pending_reads` keyed by `read_index`.
pub(super) struct PendingReadBatch {
    /// Deadline inherited from the first flush that created this batch.
    /// Subsequent flushes at the same read_index share this deadline (not overwritten).
    // NOTE: same tick-granularity caveat as WriteMetadata::deadline.
    pub(super) deadline: Instant,
    pub(super) requests: VecDeque<LinearizableReadRequest>,
}

/// Action to execute once a specific log index is committed.
/// Stored in `pending_commit_actions` keyed by the log index that must commit first.
pub(super) struct PostCommitEntry {
    /// Deadline for the commit to arrive; action fails with timeout on expiry.
    // NOTE: checked at tick granularity, same caveat as WriteMetadata::deadline.
    pub(super) deadline: Instant,
    pub(super) action: PostCommitAction,
}

/// Post-commit actions for Raft protocol internal control operations.
///
/// These actions fire immediately when their log entry commits (reaches majority),
/// WITHOUT waiting for state machine apply. This is correct because they represent
/// Raft protocol metadata (quorum confirmation, membership changes), not user data.
///
/// Contrast with `pending_client_writes`, which may require state machine apply
/// (e.g., CAS operations need read-modify-write through the SM).
///
/// # Lifecycle
/// 1. Action registered when entry appended (e.g., `handle_join_cluster`, `initiate_noop_commit`)
/// 2. Action triggered when `commit_index` advances past the entry's index
/// 3. Fired via `drain_commit_actions` → `RoleEvent` → `raft.rs` handler
///
/// # Invariants
/// - Actions only move forward with commit_index advancement
/// - Each action fires exactly once (ensured by `split_off` pattern)
/// - Expired actions (deadline passed) trigger error responses in `tick()`
pub(super) enum PostCommitAction {
    /// Leader noop entry committed: confirms quorum and enables lease-based reads.
    /// Fired once the noop entry reaches majority, proving leader has quorum.
    LeaderNoop { term: u64 },

    /// Membership change (AddNode) entry committed: responds to join request.
    /// Fired when the AddNode config entry commits (majority replication).
    /// The joining node becomes a cluster member at this point (Raft protocol level).
    NodeJoin {
        node_id: u32,
        addr: String,
        sender: MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
    },
}

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

/// Task routed to a per-follower replication worker.
enum ReplicationTask {
    /// Normal AppendEntries replication.
    Append(AppendEntriesRequest),
    /// Peer's next_index fell below the purge boundary; transfer the latest snapshot.
    Snapshot(SnapshotMetadata),
}

/// Immutable configuration passed to every per-follower replication worker.
///
/// Bundles the seven items that are always passed together to `spawn_worker`,
/// `run_replication_worker`, and `send_to_worker_or_spawn`, keeping those
/// function signatures within the clippy `too_many_arguments` limit.
struct ReplicationWorkerConfig<T: TypeConfig> {
    transport: Arc<TROF<T>>,
    membership: Arc<MOF<T>>,
    retry_policies: RetryPolicies,
    response_compress_enabled: bool,
    role_event_tx: mpsc::UnboundedSender<RoleEvent>,
    state_machine_handler: Arc<SMHOF<T>>,
    snapshot_config: SnapshotConfig,
}

/// Handle for a per-follower replication worker task.
///
/// Dropping this handle (via `LeaderState::replication_workers`) signals the worker to exit:
/// when `task_tx` is dropped, `task_rx.recv()` returns `None` and the worker exits cleanly.
struct ReplicationWorkerHandle {
    task_tx: mpsc::UnboundedSender<ReplicationTask>,
    /// Consecutive snapshot push failures for this peer since the last success.
    snapshot_failure_count: u32,
    /// Earliest time the next snapshot push attempt is allowed (exponential backoff window).
    /// `None` means no active backoff — the next heartbeat may attempt immediately.
    snapshot_next_retry_at: Option<Instant>,
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
    /// Writes committed but awaiting state machine apply before responding.
    /// Key: log index. Value: response sender.
    /// Populated when `wait_for_apply=true` (e.g. CAS, or any write needing SM result).
    /// Drained by `handle_apply_completed`; cleared with error on step-down / fatal error.
    pub(super) pending_write_apply:
        HashMap<u64, MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>>,

    /// Pending linearizable reads waiting for SM to apply up to read_index.
    /// Key: read_index (SM must reach this index before reads can be served)
    /// Value: PendingReadBatch (deadline + requests registered at that read_index)
    /// Drained on role change; processed in ApplyCompleted handler.
    pub(super) pending_reads: BTreeMap<u64, PendingReadBatch>,

    /// Pending lease reads waiting for quorum ACK to refresh the lease.
    /// Pushed when lease is expired; drained after update_lease_timestamp().
    /// Drained with error on step-down or deadline exceeded (tick).
    pub(super) pending_lease_reads: VecDeque<PendingLeaseRead>,

    /// Pending client write batches indexed by end_log_index.
    /// Key: end_log_index of this write batch. Value: WriteMetadata (senders + deadline).
    /// Drained when commit_index advances past end_log_index (quorum achieved or single-voter flush).
    /// Drained with error on step-down / HigherTerm / deadline exceeded.
    pub(super) pending_client_writes: BTreeMap<u64, WriteMetadata>,

    /// Post-commit action queue for Raft protocol internal operations (noop, join).
    ///
    /// **Key semantics**: Log index → Action to fire when that index commits.
    ///
    /// **Trigger condition**: commit_index advances (NOT apply_index).
    /// These are protocol-level operations that respond immediately upon majority replication,
    /// without waiting for state machine apply.
    ///
    /// **Drain pattern**: `split_off(&(new_commit + 1))` ensures each action fires exactly once.
    ///
    /// **Lifecycle**:
    /// 1. Insert: When entry appended (e.g., noop at index 5 → insert(5, LeaderNoop))
    /// 2. Drain: When commit_index ≥ 5 → `drain_commit_actions` → send RoleEvent
    /// 3. Timeout: `tick()` scans deadlines → expired entries get error responses
    ///
    /// **Contrast with `pending_client_writes`**:
    /// - `pending_commit_actions`: Protocol metadata, respond on commit
    /// - `pending_client_writes`: User data, may require SM apply (CAS, etc.)
    pub(super) pending_commit_actions: BTreeMap<u64, PostCommitEntry>,

    /// DIAG: wall-clock timestamp when each write entry was registered (propose time).
    /// Key: log entry index. Cleared on apply or error drain.
    write_propose_times: HashMap<u64, std::time::Instant>,

    /// Per-follower replication worker handles. Key = follower node_id.
    /// Workers send AppendEntries via transport and relay results back as RoleEvent::AppendResult.
    /// Dropped on role change (LeaderState drop) → workers exit via channel close.
    replication_workers: HashMap<u32, ReplicationWorkerHandle>,

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
        // Pipeline ACKs can arrive out of order: a stale conflict response must never
        // retreat next_index below what match_index already confirms.
        // Floor = match_index + 1 (the minimum safe starting point for the next send).
        let floor = self.match_index.get(&node_id).copied().unwrap_or(0) + 1;
        let safe = new_next_id.max(floor);
        debug!(
            "update_next_index({}) to {} (floor={})",
            node_id, safe, floor
        );
        self.next_index.insert(node_id, safe);
        Ok(())
    }

    fn update_match_index(
        &mut self,
        node_id: u32,
        new_match_id: u64,
    ) -> Result<()> {
        // Pipeline responses can arrive out of order; only advance, never retreat.
        let current = self.match_index.get(&node_id).copied().unwrap_or(0);
        if new_match_id > current {
            self.match_index.insert(node_id, new_match_id);
        }
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

    fn handle_snapshot_push_completed(
        &mut self,
        peer_id: u32,
        success: bool,
        policy: &crate::InstallSnapshotBackoffPolicy,
        node_id: u32,
    ) {
        self.handle_snapshot_push_completed(peer_id, success, policy, node_id);
    }

    fn noop_log_id(&self) -> Result<Option<u64>> {
        Ok(self.noop_log_id)
    }

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

        // 3. Drain expired pending client writes.
        // NOTE: expiry checked at tick granularity; actual timeout may be up to one tick late.
        self.pending_client_writes.retain(|_, meta| {
            if now >= meta.deadline {
                for sender in meta.senders.drain(..) {
                    let _ = sender.send(Err(Status::deadline_exceeded("write request timeout")));
                }
                false
            } else {
                true
            }
        });

        // 4. Drain expired pending linearizable reads.
        self.pending_reads.retain(|_, batch| {
            if now >= batch.deadline {
                for (_, sender) in batch.requests.drain(..) {
                    let _ = sender.send(Err(Status::deadline_exceeded("read request timeout")));
                }
                false
            } else {
                true
            }
        });

        // 4b. Drain expired pending lease reads.
        let mut unexpired_reads = VecDeque::new();
        for entry in self.pending_lease_reads.drain(..) {
            if now >= entry.deadline {
                let _ = entry.sender.send(Err(Status::deadline_exceeded("lease read timeout")));
            } else {
                unexpired_reads.push_back(entry);
            }
        }
        self.pending_lease_reads = unexpired_reads;

        // 5. Drain expired post-commit actions (e.g. noop/join timed out before quorum).
        // A noop timeout means we cannot confirm leadership — step down immediately.
        // A join timeout sends a deadline_exceeded error to the waiting client.
        let expired_keys: Vec<u64> = self
            .pending_commit_actions
            .iter()
            .filter(|(_, e)| now >= e.deadline)
            .map(|(&k, _)| k)
            .collect();

        let mut noop_timed_out = false;
        for key in expired_keys {
            if let Some(entry) = self.pending_commit_actions.remove(&key) {
                match entry.action {
                    PostCommitAction::LeaderNoop { .. } => {
                        noop_timed_out = true;
                    }
                    PostCommitAction::NodeJoin { sender, .. } => {
                        let _ = sender.send(Err(Status::deadline_exceeded("join commit timeout")));
                    }
                }
            }
        }
        if noop_timed_out {
            warn!("LeaderNoop commit timed out — stepping down");
            let _ = role_tx.send(RoleEvent::BecomeFollower(None));
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
            let count: usize = self.pending_reads.values().map(|b| b.requests.len()).sum();
            warn!(
                "Read batch: draining {} pending linearizable reads due to role change",
                count
            );
            for (_, batch) in std::mem::take(&mut self.pending_reads) {
                for (_, sender) in batch.requests {
                    let _ = sender.send(Err(tonic::Status::unavailable("Leader stepped down")));
                }
            }
        }

        // Drain pending lease reads awaiting quorum ACK
        if !self.pending_lease_reads.is_empty() {
            warn!(
                "Read batch: draining {} pending lease reads due to role change",
                self.pending_lease_reads.len()
            );
            for entry in self.pending_lease_reads.drain(..) {
                let _ = entry.sender.send(Err(tonic::Status::unavailable("Leader stepped down")));
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

        // Drain pending client writes (already in log, quorum not yet achieved).
        if !self.pending_client_writes.is_empty() {
            let count: usize = self.pending_client_writes.values().map(|m| m.senders.len()).sum();
            warn!(
                "Draining {} pending write responses due to role change",
                count
            );
            self.drain_pending_writes_with_error(ErrorCode::ProposeFailed);
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
                    if let Err(e) = self
                        .init_peers_next_index_and_match_index(last_entry_id, newly_added.clone())
                    {
                        warn!("Failed to initialize next_index for new peers: {:?}", e);
                        // Non-fatal: next_index will use default value of 1,
                        // replication will still work but may be less efficient
                    } else {
                        trace!(
                            "[MEMBERSHIP-APPLIED-LEADER] Successfully initialized next_index/match_index for peers: {:?}",
                            newly_added
                        );
                    }
                } else {
                    trace!("[MEMBERSHIP-APPLIED-LEADER] No newly added peers detected");
                }

                // Drop workers for peers that were removed from membership.
                // Dropping ReplicationWorkerHandle closes task_tx → worker exits naturally.
                let removed: Vec<u32> = old_replication_targets
                    .iter()
                    .filter(|old_peer| {
                        !self
                            .cluster_metadata
                            .replication_targets
                            .iter()
                            .any(|new_peer| new_peer.id == old_peer.id)
                    })
                    .map(|peer| peer.id)
                    .collect();
                for removed_id in &removed {
                    if self.replication_workers.remove(removed_id).is_some() {
                        debug!("Dropped replication worker for removed peer {}", removed_id);
                    }
                }
            }

            RaftEvent::FatalError { source, error } => {
                error!("[Leader] Fatal error from {}: {}", source, error);
                let fatal_status = || tonic::Status::internal(format!("Node fatal error: {error}"));
                // Notify all pending write requests
                let pending: Vec<_> = self.pending_write_apply.drain().collect();
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
                    let count: usize = self.pending_reads.values().map(|b| b.requests.len()).sum();
                    warn!(
                        "[Leader] FatalError: notifying {} pending linearizable reads",
                        count
                    );
                    for (_, batch) in std::mem::take(&mut self.pending_reads) {
                        for (_, sender) in batch.requests {
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

    async fn handle_apply_completed(
        &mut self,
        last_index: u64,
        results: Vec<crate::ApplyResult>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let num_results = results.len();

        // Match apply results to pending client requests and send responses.
        let responses: Vec<_> = results
            .into_iter()
            .filter_map(|r| self.pending_write_apply.remove(&r.index).map(|sender| (r, sender)))
            .collect();

        for (result, sender) in responses {
            let e2e_ms = self
                .write_propose_times
                .remove(&result.index)
                .map(|t| t.elapsed().as_millis())
                .unwrap_or(0);
            if e2e_ms > 0 {
                metrics::histogram!("raft.write.propose_to_apply_ms").record(e2e_ms as f64);
            }

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
                self.execute_pending_reads(batch.requests, ctx);
            }
        }

        // Check snapshot after SM apply — last_applied is now accurate.
        check_and_trigger_snapshot(last_index, Leader as i32, self.current_term(), ctx, role_tx)?;

        trace!(
            "[Leader-{}] TIMING: process_apply_completed({} results)",
            self.node_id(),
            num_results,
        );

        Ok(())
    }

    /// Handle LogFlushed(durable): re-calculate commit_index after local log entries are crash-safe.
    ///
    /// Leader also writes to the local raft log. process_batch reads durable_index() synchronously,
    /// which may be 0 (flush not yet complete) — so commit never advances without this handler.
    /// Follower ACK: handled via pending_flush. Leader commit: handled here.
    async fn handle_log_flushed(
        &mut self,
        durable: u64,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) {
        let new_commit_index = if self.cluster_metadata.single_voter {
            // MemFirst single-voter: LogFlushed(durable) is the IO checkpoint.
            // Commit to last_entry_id() — not just durable — to allow pipelining
            // across IO batch boundaries. Matches multi-voter MemFirst where leader
            // contributes last_entry_id() to quorum (not durable_index).
            let last_log_index = ctx.raft_log().last_entry_id();
            debug_assert!(
                last_log_index >= durable,
                "last_entry_id ({last_log_index}) must be >= durable ({durable})"
            );
            if last_log_index > self.commit_index() {
                Some(last_log_index)
            } else {
                None
            }
        } else {
            // Multi-voter: quorum of match_index determines commit.
            // Only voter peers (non-Learner role) may contribute to majority.
            // Learners replicate entries but must never count toward commit quorum.
            self.calculate_new_commit_index(ctx.raft_log())
        };

        if let Some(new_commit) = new_commit_index {
            if let Err(e) = self.update_commit_index_with_signal(
                Leader as i32,
                self.current_term(),
                new_commit,
                role_tx,
            ) {
                error!(
                    ?e,
                    "handle_log_flushed: update_commit_index_with_signal failed"
                );
            } else {
                // Drain pending writes committed via local flush (single-voter or flush path).
                self.drain_pending_client_writes(new_commit);
                // Fire post-commit actions (noop confirmation, join responses) for this commit.
                self.drain_commit_actions(new_commit, ctx, role_tx).await;
                // Single-voter: log flush confirms leadership (leader is the entire quorum).
                // Refresh lease timestamp after all post-commit work to eliminate the yield-point
                // race window introduced when drain_commit_actions became async.
                if self.cluster_metadata.single_voter {
                    self.update_lease_timestamp();
                    self.drain_pending_lease_reads(ctx);
                }
            }
        }
    }

    async fn handle_append_result(
        &mut self,
        follower_id: u32,
        result: Result<AppendEntriesResponse>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let leader_term = self.current_term();

        let response = match result {
            Err(e) => {
                // ResponseChannelClosed means the follower's pending_response_tx was replaced
                // by a newer AppendEntries (pipeline replication). The old sender was dropped,
                // but the follower still has the entry in memory and will flush it.
                // Safety: by Raft Log Matching Property, if follower ACKs index=N,
                // it implicitly confirms all entries up to N including this one.
                // Do NOT retry — the higher-index ACK will update match_index correctly.
                if matches!(
                    e,
                    Error::System(SystemError::Network(NetworkError::ResponseChannelClosed))
                ) {
                    debug!(
                        "follower {} sender superseded (ResponseChannelClosed) — \
                         waiting for higher-index ACK",
                        follower_id
                    );
                    return Ok(());
                }
                // Real network error — log and continue; worker handles retries per BackoffPolicy.
                warn!(
                    "AppendEntries to peer {} network error: {:?}",
                    follower_id, e
                );
                return Ok(());
            }
            Ok(resp) => resp,
        };

        // Skip stale responses from earlier terms.
        if response.term < leader_term {
            debug!(
                "Ignoring stale AppendResult from peer {} (term {} < {})",
                follower_id, response.term, leader_term
            );
            return Ok(());
        }

        // Higher term: step down immediately.
        if response.term > leader_term {
            warn!(
                "HigherTerm {} from peer {} — stepping down",
                response.term, follower_id
            );
            self.update_current_term(response.term);
            self.drain_pending_writes_with_error(ErrorCode::TermOutdated);
            self.send_become_follower_event(None, role_tx)?;
            return Err(ReplicationError::HigherTerm(response.term).into());
        }

        // Determine if this peer is a voter (affects quorum calculation and learner checks).
        let is_voter = self.cluster_metadata.replication_targets.iter().any(|n| {
            n.id == follower_id && n.role != d_engine_proto::common::NodeRole::Learner as i32
        });

        // Process success / conflict / embedded higher-term.
        let peer_update = match response.result {
            Some(append_entries_response::Result::Success(success)) => ctx
                .replication_handler()
                .handle_success_response(follower_id, response.term, success, leader_term)?,
            Some(append_entries_response::Result::Conflict(conflict)) => {
                let current_next_index = self.next_index.get(&follower_id).copied().unwrap_or(1);
                ctx.replication_handler().handle_conflict_response(
                    follower_id,
                    conflict,
                    ctx.raft_log(),
                    current_next_index,
                )?
            }
            Some(append_entries_response::Result::HigherTerm(term)) => {
                if term > leader_term {
                    self.update_current_term(term);
                    self.drain_pending_writes_with_error(ErrorCode::TermOutdated);
                    self.send_become_follower_event(None, role_tx)?;
                    return Err(ReplicationError::HigherTerm(term).into());
                }
                return Ok(());
            }
            None => {
                error!(
                    "AppendResult from peer {} has no result variant",
                    follower_id
                );
                return Ok(());
            }
        };

        // Update next_index and match_index for this peer.
        let peer_updates = HashMap::from([(follower_id, peer_update.clone())]);
        self.update_peer_indexes(&peer_updates);

        // Check learner promotion progress.
        if !is_voter {
            // Collect the current progress of all learners
            let learner_progress: HashMap<u32, Option<u64>> = self
                .match_index
                .iter()
                .filter_map(|(&peer_id, &match_idx)| {
                    // Check whether this peer is a learner
                    let is_learner = self.cluster_metadata.replication_targets.iter().any(|n| {
                        n.id == peer_id
                            && n.role == d_engine_proto::common::NodeRole::Learner as i32
                    });

                    if is_learner {
                        Some((peer_id, Some(match_idx)))
                    } else {
                        None
                    }
                })
                .collect();
            if !learner_progress.is_empty()
                && let Err(e) = self.check_learner_progress(&learner_progress, ctx, role_tx).await
            {
                error!(?e, "check_learner_progress failed");
            }
        } else {
            trace!(
                "[APPEND-RESULT] Node {} is a voter, skipping learner check",
                follower_id
            );
        }

        // Re-calculate commit index after updating this voter's match_index.
        if peer_update.success && is_voter {
            if let Some(new_commit) = self.calculate_new_commit_index(ctx.raft_log()) {
                self.update_commit_index_with_signal(
                    Leader as i32,
                    self.current_term(),
                    new_commit,
                    role_tx,
                )?;
                self.drain_pending_client_writes(new_commit);
                self.drain_commit_actions(new_commit, ctx, role_tx).await;
            }

            // Lease refresh and pending_lease_reads drain are triggered by quorum ACK,
            // independent of whether commit_index advanced. When an expired lease fires an
            // empty AppendEntries heartbeat, commit_index does not change (nothing new to
            // commit), so calculate_new_commit_index returns None and the block above is
            // skipped. We must check quorum confirmation separately here.
            let quorum_confirmed = ctx
                .raft_log()
                .calculate_majority_matched_index(
                    self.current_term(),
                    self.commit_index(),
                    self.match_index
                        .iter()
                        .filter(|(id, _)| {
                            self.cluster_metadata.replication_targets.iter().any(|n| {
                                n.id == **id
                                    && n.role != d_engine_proto::common::NodeRole::Learner as i32
                            })
                        })
                        .map(|(_, idx)| *idx)
                        .collect(),
                )
                .is_some();
            if quorum_confirmed {
                // Refresh after all post-commit work to eliminate the yield-point race window
                // introduced when drain_commit_actions became async.
                self.update_lease_timestamp();
                self.drain_pending_lease_reads(ctx);
            }
        }

        Ok(())
    }
}

/// Computes the exponential backoff delay for a snapshot push failure.
///
/// Doubles `base_delay_ms` on each consecutive failure, capped at
/// `push_backoff_max_delay_ms`. Saturating arithmetic prevents overflow for
/// large failure counts.
///
/// Leader protection is the highest priority: the cap ensures the leader never
/// stalls healthy-follower replication waiting for an unreachable peer.
fn snapshot_push_backoff_duration(
    failure_count: u32,
    policy: &crate::InstallSnapshotBackoffPolicy,
) -> std::time::Duration {
    let millis = policy
        .base_delay_ms
        .saturating_mul(1u64 << failure_count.min(20))
        .min(policy.push_backoff_max_delay_ms);
    std::time::Duration::from_millis(millis)
}

impl<T: TypeConfig> LeaderState<T> {
    // ---- Per-follower ReplicationWorker management ----------------------------------------

    /// Spawn a long-running replication worker task for the given peer.
    ///
    /// The worker runs a FIFO loop: receives AppendEntriesRequests from `task_tx`,
    /// calls `transport.send_append_request`, and sends results back via `role_event_tx`.
    /// It exits cleanly when `task_tx` is dropped (LeaderState step-down).
    fn spawn_worker(
        peer_id: u32,
        cfg: ReplicationWorkerConfig<T>,
    ) -> ReplicationWorkerHandle {
        let (task_tx, task_rx) = mpsc::unbounded_channel::<ReplicationTask>();
        tokio::spawn(async move {
            Self::run_replication_worker(peer_id, task_rx, cfg).await;
        });
        ReplicationWorkerHandle {
            task_tx,
            snapshot_failure_count: 0,
            snapshot_next_retry_at: None,
        }
    }

    /// Worker loop: pipeline replication for one peer.
    ///
    /// Handles two task types:
    ///   - `Append`: send AppendEntries RPC (skipped while snapshot is in progress)
    ///   - `Snapshot`: push latest snapshot to a lagging peer.
    ///
    /// Snapshot dedup: `snapshot_in_progress` is set when a snapshot is spawned and
    /// cleared when it completes.  While true, duplicate `Snapshot` tasks and stale
    /// `Append` tasks (whose prev_log indices are meaningless before the snapshot
    /// base is installed) are discarded.
    async fn run_replication_worker(
        peer_id: u32,
        mut task_rx: mpsc::UnboundedReceiver<ReplicationTask>,
        cfg: ReplicationWorkerConfig<T>,
    ) {
        let ReplicationWorkerConfig {
            transport,
            membership,
            retry_policies,
            response_compress_enabled,
            role_event_tx,
            state_machine_handler,
            snapshot_config,
        } = cfg;

        // NOTE: snapshot_in_progress is intentionally outside the reconnect loop.
        // A snapshot transfer must complete (or fail) regardless of stream reconnects;
        // the bidi stream is only used for AppendEntries, not snapshot chunks.
        let snapshot_in_progress = Arc::new(AtomicBool::new(false));
        let base_delay_ms = retry_policies.append_entries.base_delay_ms;
        let max_delay_ms = retry_policies.append_entries.max_delay_ms;

        // Open persistent bidi stream with backoff reconnection
        loop {
            let mut backoff_ms = base_delay_ms;
            let stream = loop {
                match transport
                    .open_replication_stream(peer_id, membership.clone(), response_compress_enabled)
                    .await
                {
                    Ok(s) => {
                        info!(peer_id, "Bidi replication stream established");
                        break s;
                    }
                    Err(e) => {
                        info!(
                            peer_id,
                            "Bidi stream reconnecting (backoff={}ms): {:?}", backoff_ms, e
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(max_delay_ms);
                    }
                }
            };

            let stream_sender = stream.sender;
            let mut stream_receiver = stream.receiver;

            // Shared flag: both recv task and main loop can detect stream breakage
            let stream_broken = Arc::new(AtomicBool::new(false));
            let recv_broken = stream_broken.clone();

            // Spawn recv task to monitor ACKs and detect stream disconnection
            let recv_role_event_tx = role_event_tx.clone();
            let recv_handle = tokio::spawn(async move {
                use futures::StreamExt;
                while let Some(result) = stream_receiver.next().await {
                    match result {
                        Ok(response) => {
                            let _ = recv_role_event_tx.send(RoleEvent::AppendResult {
                                follower_id: peer_id,
                                result: Ok(response),
                            });
                        }
                        Err(status) => {
                            warn!(peer_id, "Bidi stream recv error: {:?}", status);
                            recv_broken.store(true, Ordering::Release);
                            let _ = recv_role_event_tx.send(RoleEvent::PeerStreamError { peer_id });
                            break;
                        }
                    }
                }
                debug!(peer_id, "Recv task exiting (stream closed)");
            });

            // Main worker loop: process replication tasks
            while let Some(task) = task_rx.recv().await {
                // Check stream health before processing task
                if stream_broken.load(Ordering::Acquire) {
                    debug!(peer_id, "Stream broken detected, reconnecting...");
                    break;
                }

                match task {
                    ReplicationTask::Append(request) => {
                        if snapshot_in_progress.load(Ordering::Acquire) {
                            debug!(
                                peer_id,
                                "Skipping AppendEntries while snapshot is in progress"
                            );
                            continue;
                        }
                        // Push batch directly into the persistent bidi stream (non-blocking)
                        if stream_sender.send(request).await.is_err() {
                            warn!(peer_id, "Bidi stream sender closed, reconnecting");
                            stream_broken.store(true, Ordering::Release);
                            let _ = role_event_tx.send(RoleEvent::PeerStreamError { peer_id });
                            break;
                        }
                    }
                    ReplicationTask::Snapshot(metadata) => {
                        if snapshot_in_progress.load(Ordering::Acquire) {
                            debug!(
                                peer_id,
                                "Skipping duplicate Snapshot task (already in progress)"
                            );
                            continue;
                        }
                        snapshot_in_progress.store(true, Ordering::Release);
                        // Spawn snapshot transfer; bidi stream stays open
                        let flag = snapshot_in_progress.clone();
                        let t = transport.clone();
                        let smh = state_machine_handler.clone();
                        let m = membership.clone();
                        let c = snapshot_config.clone();
                        let tx = role_event_tx.clone();
                        tokio::spawn(async move {
                            let result = t.send_snapshot(peer_id, metadata, smh, m, c).await;
                            let success = result.is_ok();
                            if !success {
                                warn!(peer_id, "Snapshot push failed: {:?}", result);
                            }
                            flag.store(false, Ordering::Release);
                            let _ = tx.send(RoleEvent::SnapshotPushCompleted { peer_id, success });
                        });
                    }
                }
            }

            // task_rx closed (leader stepped down) or stream broken
            recv_handle.abort();
            if task_rx.is_closed() {
                debug!(peer_id, "Replication worker exiting (leader stepped down)");
                break;
            }
            // Otherwise reconnect (flag persists across reconnects)
        }
    }

    /// Send a task to the given peer's worker, spawning it first if needed.
    /// If the existing worker has died (task_tx.send returns Err), rebuilds it transparently.
    fn send_to_worker_or_spawn(
        &mut self,
        peer_id: u32,
        mut task: ReplicationTask,
        cfg: ReplicationWorkerConfig<T>,
    ) {
        // Try existing worker; if dead, recover task and fall through to respawn.
        if let Some(handle) = self.replication_workers.get(&peer_id) {
            match handle.task_tx.send(task) {
                Ok(()) => return,
                Err(mpsc::error::SendError(t)) => {
                    warn!("Replication worker for peer {} died, rebuilding", peer_id);
                    task = t;
                }
            }
        }
        // Worker absent or just died — spawn and send.
        let handle = Self::spawn_worker(peer_id, cfg);
        let _ = handle.task_tx.send(task);
        self.replication_workers.insert(peer_id, handle);
    }

    // ---- Snapshot push backoff + alerting -------------------------------------------------

    /// Update per-peer snapshot push state after a completed (success or failure) attempt.
    ///
    /// On success: resets the failure counter and clears any active backoff window.
    /// On failure: increments the counter, schedules an exponential backoff window, and
    /// emits an error-level alert once `push_failure_alert_threshold` is reached.
    ///
    /// Leader protection is the highest priority: the backoff window prevents a permanently
    /// unreachable peer from consuming the leader's I/O bandwidth on every heartbeat cycle.
    pub(crate) fn handle_snapshot_push_completed(
        &mut self,
        peer_id: u32,
        success: bool,
        policy: &crate::InstallSnapshotBackoffPolicy,
        node_id: u32,
    ) {
        let Some(handle) = self.replication_workers.get_mut(&peer_id) else {
            return;
        };

        if success {
            handle.snapshot_failure_count = 0;
            handle.snapshot_next_retry_at = None;
            return;
        }

        handle.snapshot_failure_count += 1;
        let count = handle.snapshot_failure_count;
        let delay = snapshot_push_backoff_duration(count, policy);
        handle.snapshot_next_retry_at = Some(Instant::now() + delay);

        if count >= policy.push_failure_alert_threshold {
            error!(
                peer_id,
                failure_count = count,
                next_retry_secs = delay.as_secs(),
                "Snapshot push to peer has failed {} consecutive times; \
                 peer may be unreachable or out of disk space. \
                 Next attempt in {}s.",
                count,
                delay.as_secs()
            );
            metrics::counter!(
                "raft.snapshot.push_consecutive_failures",
                "peer_id" => peer_id.to_string(),
                "node_id" => node_id.to_string(),
            )
            .increment(1);
        }
    }

    // ---- Pending client write drain -------------------------------------------------------

    /// Drain pending client write batches whose end_log_index <= new_commit.
    ///
    /// For `wait_for_apply=false` batches: send write_success immediately.
    /// For `wait_for_apply=true` batches (e.g. CAS): move senders to `pending_write_apply`
    /// so they are resolved by the ApplyCompleted handler.
    fn drain_pending_client_writes(
        &mut self,
        new_commit: u64,
    ) {
        // split_off returns the sub-map with keys > new_commit; we keep that for later.
        let remaining = if new_commit < u64::MAX {
            self.pending_client_writes.split_off(&(new_commit + 1))
        } else {
            BTreeMap::new()
        };
        let committed = std::mem::replace(&mut self.pending_client_writes, remaining);
        for (_, meta) in committed {
            let (start_idx, senders, wait_for_apply) =
                (meta.start_idx, meta.senders, meta.wait_for_apply);
            if wait_for_apply {
                for (i, sender) in senders.into_iter().enumerate() {
                    let idx = start_idx + i as u64;
                    if let Some(t) = self.write_propose_times.get(&idx) {
                        let ms = t.elapsed().as_millis();
                        metrics::histogram!("raft.write.propose_to_commit_ms").record(ms as f64);
                    }
                    self.pending_write_apply.insert(idx, sender);
                }
            } else {
                for sender in senders {
                    let _ = sender.send(Ok(ClientResponse::write_success()));
                }
            }
        }
    }

    /// Fire-and-forget noop entry to confirm quorum after election.
    ///
    /// Writes a noop `EntryPayload::noop()` to the local log (no sender → no inline wait),
    /// then records the expected commit index in `pending_commit_actions` keyed by the
    /// noop log index.  When that index commits, `drain_commit_actions` fires
    /// `RoleEvent::NoopCommitted { term }` which the Raft loop handles by calling
    /// `on_noop_committed()` and notifying watch listeners.
    ///
    /// Returns `Err` only on storage failure (write to raft log).  All other paths are
    /// handled asynchronously via the post-commit action queue.
    pub(super) async fn initiate_noop_commit(
        &mut self,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        let term = self.current_term();
        let deadline = Instant::now()
            + Duration::from_millis(
                ctx.node_config.raft.membership.verify_leadership_persistent_timeout.as_millis()
                    as u64,
            );

        // Predict the noop log index BEFORE writing: last_entry_id + 1.
        // Must insert BEFORE execute_request_immediately so that Phase 4's
        // synchronous drain_commit_actions call (single-voter path) can find the entry.
        // No concurrent writes: Raft loop is single-threaded.
        let noop_index = ctx.raft_log().last_entry_id() + 1;
        self.pending_commit_actions.insert(
            noop_index,
            PostCommitEntry {
                deadline,
                action: PostCommitAction::LeaderNoop { term },
            },
        );

        // Write noop with no sender — purely for commit confirmation.
        if let Err(e) = self
            .execute_request_immediately(
                RaftRequestWithSignal {
                    id: { rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21) },
                    payloads: vec![EntryPayload::noop()],
                    senders: vec![],
                    wait_for_apply_event: false,
                },
                ctx,
                role_tx,
            )
            .await
        {
            // Storage failure: clean up the pre-inserted entry and propagate.
            self.pending_commit_actions.remove(&noop_index);
            return Err(e);
        }

        debug!(
            "initiate_noop_commit: noop_index={} term={}",
            noop_index, term
        );
        Ok(())
    }

    /// Track no-op entry index for linearizable read optimization.
    /// Called directly by drain_commit_actions when the noop entry commits.
    fn on_noop_committed(
        &mut self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        let noop_index = ctx.raft_log().last_entry_id();
        self.noop_log_id = Some(noop_index);
        debug!("Tracked noop_log_id: {}", noop_index);
        Ok(())
    }

    /// Drain and fire Raft protocol internal actions whose log entries have committed.
    ///
    /// **Purpose**: Respond to protocol-level operations (noop, join) when their entries
    /// reach majority replication. These actions DO NOT wait for state machine apply,
    /// because they represent Raft metadata, not user data.
    ///
    /// **Call sites**:
    /// 1. `handle_log_flushed`: Single-voter or multi-voter durability path
    /// 2. `handle_append_result`: Multi-voter quorum path (when peer ACK satisfies majority)
    ///
    /// Both call sites are necessary due to commit timing uncertainty:
    /// - Multi-voter: Quorum may arrive before or after leader's local flush
    /// - Single-voter: Only LogFlushed triggers commit (no peers)
    ///
    /// **Idempotency**: `split_off` pattern ensures each action fires exactly once,
    /// even if commit_index advances multiple times to the same value.
    ///
    /// # Example
    /// ```ignore
    /// // T1: Insert action when noop appended
    /// pending_commit_actions.insert(5, PostCommitEntry::LeaderNoop{term: 2});
    ///
    /// // T2: Commit advances to 5
    /// drain_commit_actions(5, role_tx);
    /// // → Sends RoleEvent::NoopCommitted{term: 2}
    /// // → pending_commit_actions now empty (or has entries > 5)
    ///
    /// // T3: Commit stays at 5 (redundant call)
    /// drain_commit_actions(5, role_tx);
    /// // → No action fired (already drained)
    /// ```
    ///
    /// Uses the same `split_off` pattern as `drain_pending_client_writes`:
    /// entries with index ≤ new_commit are drained; entries with index > new_commit
    /// remain in the map for future commits.
    pub(super) async fn drain_commit_actions(
        &mut self,
        new_commit: u64,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) {
        let remaining = if new_commit < u64::MAX {
            self.pending_commit_actions.split_off(&(new_commit + 1))
        } else {
            BTreeMap::new()
        };
        let committed = std::mem::replace(&mut self.pending_commit_actions, remaining);
        for (_, entry) in committed {
            match entry.action {
                PostCommitAction::LeaderNoop { term } => {
                    // Call on_noop_committed directly — establishes noop_log_id without
                    // waiting for a second loop iteration through role_rx (P2).
                    // Only send the event for notify_leader_change, which lives on Raft<T>.
                    if let Err(e) = self.on_noop_committed(ctx) {
                        warn!(
                            ?e,
                            "on_noop_committed failed — skipping notify_leader_change"
                        );
                    } else {
                        let _ = role_tx.send(RoleEvent::NoopCommitted { term });
                    }
                }
                PostCommitAction::NodeJoin {
                    node_id,
                    addr,
                    sender,
                } => {
                    // Call send_join_success directly — no channel roundtrip needed.
                    // drain_commit_actions is async, so we can await the membership queries inline.
                    if let Err(e) = self.send_join_success(node_id, &addr, sender, ctx).await {
                        error!(
                            ?e,
                            node_id, "send_join_success failed after NodeJoin commit"
                        );
                    }
                }
            }
        }
    }

    /// Drain all pending client writes with an error response.
    /// Called on step-down or HigherTerm detection.
    fn drain_pending_writes_with_error(
        &mut self,
        error_code: ErrorCode,
    ) {
        self.write_propose_times.clear();
        for (_, meta) in std::mem::take(&mut self.pending_client_writes) {
            for sender in meta.senders {
                let _ = sender.send(Ok(ClientResponse::client_error(error_code)));
            }
        }
    }

    // ---- Cluster metadata management -------------------------------------------------------

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

        trace!(
            "[CHECK-LEARNER-PROGRESS] Leader {} checking progress for learners: {:?}",
            self.node_id(),
            learner_progress
        );

        if !self.should_check_learner_progress(ctx) {
            return Ok(());
        }

        if learner_progress.is_empty() {
            return Ok(());
        }

        let _tlp = std::time::Instant::now();
        let ready_learners = self.find_promotable_learners(learner_progress, ctx).await;
        trace!(
            "[FIND-PROMOTABLE] Leader {} found {} promotable learners: {:?} (took {:?})",
            self.node_id(),
            ready_learners.len(),
            ready_learners,
            _tlp.elapsed()
        );

        let new_promotions = self.deduplicate_promotions(ready_learners);
        trace!(
            "[DEDUPLICATE] Leader {} has {} new promotions after dedup: {:?}",
            self.node_id(),
            new_promotions.len(),
            new_promotions
        );

        if !new_promotions.is_empty() {
            trace!(
                "[ENQUEUE-PROMOTIONS] Leader {} enqueueing {} promotions",
                self.node_id(),
                new_promotions.len()
            );
            self.enqueue_and_notify_promotions(new_promotions, role_tx)?;
        } else {
            trace!(
                "[NO-PROMOTIONS] Leader {} has no new promotions to enqueue",
                self.node_id()
            );
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

        trace!(
            "[FIND-PROMOTABLE-START] Leader {} checking {} learners, leader_commit={}, threshold={}",
            self.node_id(),
            learner_progress.len(),
            leader_commit,
            threshold
        );

        let mut ready_learners = Vec::new();

        for (&node_id, &match_index_opt) in learner_progress.iter() {
            trace!(
                "[FIND-PROMOTABLE-LOOP] Checking learner {}, match_index={:?}",
                node_id, match_index_opt
            );

            if !membership.contains_node(node_id).await {
                trace!(
                    "[FIND-PROMOTABLE-LOOP] ❌ Learner {} NOT in membership, skipping",
                    node_id
                );
                continue;
            }

            trace!(
                "[FIND-PROMOTABLE-LOOP] ✅ Learner {} is in membership",
                node_id
            );

            let match_index = match_index_opt.unwrap_or(0);
            let gap = leader_commit.saturating_sub(match_index);
            let is_caught_up = self.is_learner_caught_up(match_index_opt, leader_commit, threshold);

            trace!(
                "[FIND-PROMOTABLE-LOOP] Learner {} catchup check: match_index={}, leader_commit={}, gap={}, threshold={}, caught_up={}",
                node_id, match_index, leader_commit, gap, threshold, is_caught_up
            );

            if !is_caught_up {
                trace!(
                    "[FIND-PROMOTABLE-LOOP] ❌ Learner {} NOT caught up (gap {} > threshold {})",
                    node_id, gap, threshold
                );
                continue;
            }

            trace!("[FIND-PROMOTABLE-LOOP] ✅ Learner {} IS caught up", node_id);

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

    /// Calculate new submission index
    #[instrument(skip(self))]
    fn calculate_new_commit_index(
        &self,
        raft_log: &Arc<ROF<T>>,
    ) -> Option<u64> {
        let old_commit_index = self.commit_index();
        let current_term = self.current_term();
        let replication_targets = &self.cluster_metadata.replication_targets;
        let learner_role = d_engine_proto::common::NodeRole::Learner as i32;

        // Only voter peers (non-Learner) contribute to the commit quorum.
        let matched_ids: Vec<u64> = self
            .match_index
            .iter()
            .filter(|(id, _)| {
                replication_targets.iter().any(|n| n.id == **id && n.role != learner_role)
            })
            .map(|(_, idx)| *idx)
            .collect();

        let new_commit_index =
            raft_log.calculate_majority_matched_index(current_term, old_commit_index, matched_ids);

        if new_commit_index.is_some() && new_commit_index.unwrap() > old_commit_index {
            new_commit_index
        } else {
            None
        }
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

        // 3. Submit config change fire-and-forget; JoinCommitted event delivers the response.
        debug!("3. Submit config change (fire-and-forget)");
        let deadline = Instant::now()
            + Duration::from_millis(
                ctx.node_config.raft.membership.verify_leadership_persistent_timeout.as_millis()
                    as u64,
            );
        if let Err(e) = self
            .execute_request_immediately(
                RaftRequestWithSignal {
                    id: { rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21) },
                    payloads: vec![EntryPayload::config(config_change)],
                    senders: vec![],
                    wait_for_apply_event: false,
                },
                ctx,
                role_tx,
            )
            .await
        {
            let _ = sender.send(Err(Status::failed_precondition(format!(
                "Join request submission failed: {e}"
            ))));
            return Err(e);
        }

        // Record post-commit action: when the config index commits, send JoinResponse.
        let join_index = ctx.raft_log().last_entry_id();
        self.pending_commit_actions.insert(
            join_index,
            PostCommitEntry {
                deadline,
                action: PostCommitAction::NodeJoin {
                    node_id,
                    addr: address,
                    sender,
                },
            },
        );

        debug!("JoinCluster: awaiting commit at index {}", join_index);
        Ok(())
    }

    pub(super) async fn send_join_success(
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
            pending_write_apply: HashMap::new(),
            pending_reads: BTreeMap::new(),
            pending_lease_reads: VecDeque::new(),
            replication_workers: HashMap::new(),
            pending_client_writes: BTreeMap::new(),
            pending_commit_actions: BTreeMap::new(),
            write_propose_times: HashMap::new(),
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
            now.saturating_duration_since(entry.ready_since) <= config.stale_learner_threshold
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
            Some(WriteMetadata {
                start_idx,
                senders: all_senders,
                wait_for_apply: any_wait_for_apply,
                deadline: Instant::now(), // placeholder; overwritten in Phase 2
            }),
        )
    }

    /// Core RPC execution and unified result processing.
    ///
    /// Shared by `process_batch` (write-only) and `unified_write_and_linear_read` (write+read).
    /// Handles all 4 result cases: quorum success, quorum failure, higher term, other errors.
    /// Core RPC execution: non-blocking replication via per-follower workers.
    ///
    /// Phase 1 (serial, in Raft loop): write entries to local log + build per-peer requests.
    /// Phase 2: store pending client writes indexed by end_log_index.
    /// Phase 3: fire requests to per-follower workers (fire-and-forget).
    ///
    /// Commit and client responses are deferred:
    /// - Multi-voter: driven by handle_append_result (RoleEvent::AppendResult from workers)
    /// - Single-voter: driven by handle_log_flushed (RoleEvent::LogFlushed from batch_processor)
    async fn execute_and_process_raft_rpc(
        &mut self,
        payloads: Vec<EntryPayload>,
        write_metadata: Option<WriteMetadata>,
        mut read_batch: Option<Vec<LinearizableReadRequest>>,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        trace!(
            cluster_size = self.cluster_metadata.total_voters,
            payload_count = payloads.len(),
        );

        // Phase 1: write entries to local log + prepare per-peer requests (serial in Raft loop).
        let requests = match ctx
            .replication_handler()
            .prepare_batch_requests(
                payloads,
                self.state_snapshot(),
                self.leader_state_snapshot(),
                &self.cluster_metadata,
                ctx,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("prepare_batch_requests failed: {:?}", e);
                if let Some(meta) = write_metadata {
                    for sender in meta.senders {
                        let _ =
                            sender.send(Ok(ClientResponse::client_error(ErrorCode::ProposeFailed)));
                    }
                }
                if let Some(read_batch) = read_batch {
                    let status = tonic::Status::failed_precondition(format!("Prepare failed: {e}"));
                    for (_, sender) in read_batch {
                        let _ = sender.send(Err(status.clone()));
                    }
                }
                return Err(e);
            }
        };

        debug!(
            "execute_and_process_raft_rpc: {} append requests, {} snapshot targets",
            requests.append_requests.len(),
            requests.snapshot_targets.len(),
        );

        // Phase 2: store pending client writes, keyed by end_log_index.
        // Responses are sent when commit_index advances past end_log_index.
        if let Some(mut meta) = write_metadata
            && !meta.senders.is_empty()
        {
            let end_log_index = meta.start_idx + meta.senders.len() as u64 - 1;
            let propose_ts = std::time::Instant::now();
            meta.deadline = Instant::now()
                + Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
            for i in 0..meta.senders.len() as u64 {
                self.write_propose_times.insert(meta.start_idx + i, propose_ts);
            }
            self.pending_client_writes.insert(end_log_index, meta);
        }

        // Guard: reject LinearizableRead before noop commits (Raft §8 linearizability).
        // noop_log_id = None means this leader has not yet confirmed quorum.
        // calculate_read_index() would fall back to prev-term commit_index with no quorum check,
        // allowing stale reads if the leader is in a minority partition.
        // Writes continue normally — they will commit alongside or after noop.
        if self.noop_log_id.is_none()
            && let Some(reads) = read_batch.take()
        {
            let status = tonic::Status::unavailable("LeaderNotReady: noop not committed");
            for (_, sender) in reads {
                let _ = sender.send(Err(status.clone()));
            }
        }

        // Phase 3: handle reads (same logic as previous slow path — defer to SM apply callback).
        if let Some(read_batch) = read_batch {
            let read_index = self.calculate_read_index();
            let last_applied = ctx.state_machine().last_applied().index;
            if last_applied >= read_index {
                self.execute_pending_reads(read_batch, ctx);
            } else {
                let deadline = Instant::now()
                    + Duration::from_millis(
                        self.node_config.raft.general_raft_timeout_duration_in_ms,
                    );
                self.pending_reads
                    .entry(read_index)
                    .or_insert_with(|| PendingReadBatch {
                        deadline,
                        requests: VecDeque::new(),
                    })
                    .requests
                    .extend(read_batch);
            }
        }

        // Phase 4: no peer work at all — nothing to send (single-voter or empty heartbeat).
        // Single-voter commit is driven asynchronously by LogFlushed events.
        if requests.append_requests.is_empty() && requests.snapshot_targets.is_empty() {
            return Ok(());
        }

        let transport = ctx.transport.clone();
        let membership = ctx.membership.clone();
        let retry_policies = ctx.node_config.retry.clone();
        let response_compress_enabled = ctx.node_config.raft.rpc_compression.replication_response;
        let state_machine_handler = ctx.state_machine_handler().clone();
        let snapshot_config = ctx.node_config.raft.snapshot.clone();

        // Phase 5: fire AppendEntries requests to per-follower workers (non-blocking).
        for (peer_id, request) in requests.append_requests {
            self.send_to_worker_or_spawn(
                peer_id,
                ReplicationTask::Append(request),
                ReplicationWorkerConfig {
                    transport: transport.clone(),
                    membership: membership.clone(),
                    retry_policies: retry_policies.clone(),
                    response_compress_enabled,
                    role_event_tx: role_tx.clone(),
                    state_machine_handler: state_machine_handler.clone(),
                    snapshot_config: snapshot_config.clone(),
                },
            );
        }

        // Phase 6: trigger snapshot push for peers behind the purge boundary.
        // Each peer has a dedicated worker with a `snapshot_in_progress` flag that
        // prevents duplicate transfers when multiple heartbeats arrive before completion.
        // Peers within an active backoff window (after consecutive failures) are skipped
        // to protect the leader from repeatedly pushing to unreachable peers.
        if !requests.snapshot_targets.is_empty() {
            let snapshot_metadata = ctx.state_machine().snapshot_metadata();
            if let Some(metadata) = snapshot_metadata {
                for peer_id in requests.snapshot_targets {
                    // Respect per-peer backoff window: if recent push attempts failed,
                    // skip until the backoff expires rather than retrying every heartbeat.
                    if let Some(handle) = self.replication_workers.get(&peer_id)
                        && let Some(retry_at) = handle.snapshot_next_retry_at
                        && Instant::now() < retry_at
                    {
                        debug!(
                            peer_id,
                            "Snapshot push backoff active, skipping this heartbeat"
                        );
                        continue;
                    }
                    self.send_to_worker_or_spawn(
                        peer_id,
                        ReplicationTask::Snapshot(metadata.clone()),
                        ReplicationWorkerConfig {
                            transport: transport.clone(),
                            membership: membership.clone(),
                            retry_policies: retry_policies.clone(),
                            response_compress_enabled,
                            role_event_tx: role_tx.clone(),
                            state_machine_handler: state_machine_handler.clone(),
                            snapshot_config: snapshot_config.clone(),
                        },
                    );
                }
            } else {
                debug!(
                    "Snapshot targets present but no snapshot available yet; will retry next heartbeat"
                );
            }
        }

        Ok(())
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

        // Submit batch activation (fire-and-forget; next tick re-tries if not committed).
        self.execute_request_immediately(
            RaftRequestWithSignal {
                id: { rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21) },
                payloads: vec![EntryPayload::config(change)],
                senders: vec![],
                wait_for_apply_event: false,
            },
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
                    now.saturating_duration_since(entry.ready_since),
                    &config.stale_learner_threshold
                );
                if now.saturating_duration_since(entry.ready_since) > config.stale_learner_threshold
                {
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
    pub(super) async fn conditionally_purge_zombie_nodes(
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

            // Submit single config change for all nodes (fire-and-forget).
            self.execute_request_immediately(
                RaftRequestWithSignal {
                    id: { rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21) },
                    payloads: vec![EntryPayload::config(change)],
                    senders: vec![],
                    wait_for_apply_event: false,
                },
                ctx,
                role_tx,
            )
            .await?;
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

        // Submit removal (fire-and-forget; maintenance re-tries next tick if not committed).
        self.execute_request_immediately(
            RaftRequestWithSignal {
                id: { rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21) },
                payloads: vec![EntryPayload::config(change)],
                senders: vec![],
                wait_for_apply_event: false,
            },
            ctx,
            role_tx,
        )
        .await?;

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
                    Some(WriteMetadata {
                        start_idx,
                        senders: req.senders,
                        wait_for_apply: req.wait_for_apply_event,
                        deadline: Instant::now(), // placeholder; overwritten in Phase 2
                    }),
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

    /// Drain all pending lease reads: read from SM and send success response to each sender.
    /// Called after update_lease_timestamp() confirms quorum (multi-voter: handle_append_result,
    /// single-voter: handle_log_flushed).
    fn drain_pending_lease_reads(
        &mut self,
        ctx: &RaftContext<T>,
    ) {
        while let Some(entry) = self.pending_lease_reads.pop_front() {
            let results = ctx
                .handlers
                .state_machine_handler
                .read_from_state_machine(entry.request.keys)
                .unwrap_or_default();
            let _ = entry.sender.send(Ok(ClientResponse::read_results(results)));
        }
    }

    #[cfg(test)]
    pub(crate) fn test_update_lease_timestamp(&mut self) {
        self.update_lease_timestamp();
    }

    /// Returns the current match_index for `peer_id`, or 0 if not yet tracked.
    /// Used by tests to verify pipeline out-of-order response handling.
    #[cfg(test)]
    pub(crate) fn match_index_for_test(
        &self,
        peer_id: u32,
    ) -> u64 {
        self.match_index.get(&peer_id).copied().unwrap_or(0)
    }

    /// Returns the number of live per-follower worker handles.
    /// Used by tests to assert that workers are spawned / reused / rebuilt correctly.
    #[cfg(test)]
    pub(crate) fn worker_count_for_test(&self) -> usize {
        self.replication_workers.len()
    }

    /// Injects a dead worker handle for `peer_id` so that the next `send_to_worker_or_spawn`
    /// call for that peer observes a closed channel and must rebuild the worker.
    ///
    /// The handle is created by dropping the receiver side immediately after creating the
    /// channel, so any subsequent `task_tx.send()` returns `Err(SendError(_))`.
    #[cfg(test)]
    pub(crate) fn inject_dead_worker_for_test(
        &mut self,
        peer_id: u32,
    ) {
        let (dead_tx, dead_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationTask>();
        drop(dead_rx); // receiver gone → sends will fail
        self.replication_workers.insert(
            peer_id,
            ReplicationWorkerHandle {
                task_tx: dead_tx,
                snapshot_failure_count: 0,
                snapshot_next_retry_at: None,
            },
        );
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
            // Lease expired - need to confirm leadership before serving.
            if self.cluster_metadata.single_voter {
                // Single-voter: self is the entire quorum, refresh immediately and serve.
                self.update_lease_timestamp();
                let results = ctx
                    .handlers
                    .state_machine_handler
                    .read_from_state_machine(req.keys)
                    .unwrap_or_default();
                let _ = sender.send(Ok(ClientResponse::read_results(results)));
            } else {
                // Multi-voter: queue the request and trigger a heartbeat.
                // drain_pending_lease_reads() is called from handle_append_result after
                // update_lease_timestamp() confirms quorum ACK from majority.
                let deadline = Instant::now()
                    + Duration::from_millis(
                        self.node_config.raft.general_raft_timeout_duration_in_ms,
                    );
                self.pending_lease_reads.push_back(PendingLeaseRead {
                    request: req,
                    sender,
                    deadline,
                });
                // Fire an empty AppendEntries to trigger lease refresh (fire-and-forget).
                self.execute_and_process_raft_rpc(vec![], None, None, ctx, role_tx).await?;
            }
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
            pending_write_apply: HashMap::new(),
            pending_reads: BTreeMap::new(),
            pending_lease_reads: VecDeque::new(),
            replication_workers: HashMap::new(),
            pending_client_writes: BTreeMap::new(),
            pending_commit_actions: BTreeMap::new(),
            write_propose_times: HashMap::new(),
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
