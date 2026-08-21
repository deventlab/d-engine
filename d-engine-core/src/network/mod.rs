//! This module is the network abstraction layer with timeout-aware gRPC
//! implementation
//!
//! This module provides network communication facilities with configurable
//! timeout policies for distributed system operations. All network operations
//! are governed by timeout parameters defined in [`RaftConfig`] to ensure
//! system responsiveness.

mod background_snapshot_transfer;

#[cfg(test)]
mod background_snapshot_transfer_test;

#[cfg(any(test, feature = "__test_support"))]
mod snapshot_transfer_gate;
use async_trait::async_trait;
pub use background_snapshot_transfer::*;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::SnapshotMetadata;
use futures::stream::BoxStream;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;
#[cfg(any(test, feature = "__test_support"))]
pub use snapshot_transfer_gate::SnapshotTransferGate;
#[cfg(any(test, feature = "__test_support"))]
pub use snapshot_transfer_gate::SnapshotTransferGateGuard;
#[cfg(any(test, feature = "__test_support"))]
pub use snapshot_transfer_gate::install_snapshot_transfer_gate;
use tokio::sync::mpsc;
use tracing::info;

use crate::BackoffPolicy;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;
use crate::TypeConfig;

/// Per-peer persistent bidirectional replication stream.
///
/// Opened once per follower at leader startup (or on reconnect). The sender
/// pushes `AppendEntriesRequest` batches; the receiver yields
/// `AppendEntriesResponse` ACKs. Dropping the sender closes the stream.
pub struct ReplicationStream {
    /// Push AppendEntries batches directly into the open h2 stream (capacity = 128).
    pub sender: mpsc::Sender<AppendEntriesRequest>,
    /// Receive ACKs from the follower; items are `Result<_, tonic::Status>`.
    pub receiver: BoxStream<'static, std::result::Result<AppendEntriesResponse, tonic::Status>>,
}

// Define a structured return value
#[derive(Debug, Clone)]
pub struct AppendResults {
    /// Whether a majority quorum is achieved (can be directly determined via
    /// peer_updates)
    pub commit_quorum_achieved: bool,
    /// Updates to each peer's match_index and next_index
    pub peer_updates: HashMap<u32, PeerUpdate>,
    /// Learner log catch-up progress
    pub learner_progress: HashMap<u32, Option<u64>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeerUpdate {
    pub match_index: Option<u64>,
    pub next_index: u64,
    /// if peer response success
    pub success: bool,
}

impl PeerUpdate {
    #[allow(unused)]
    pub fn success(
        match_index: u64,
        next_index: u64,
    ) -> Self {
        PeerUpdate {
            match_index: Some(match_index),
            next_index,
            success: true,
        }
    }

    #[allow(unused)]
    pub fn failed() -> Self {
        Self {
            match_index: None,
            next_index: 1,
            success: false,
        }
    }
}

#[derive(Debug)]
pub struct ClusterUpdateResult {
    pub peer_ids: HashSet<u32>,
    pub responses: Vec<Result<ClusterConfUpdateResponse>>,
}

#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait Transport<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    /// Propagates cluster configuration changes to voting members using Raft's joint consensus.
    ///
    /// # Protocol
    /// - Implements membership change protocol from Raft §6
    /// - Leader-exclusive operation
    /// - Automatically filters self-references and duplicates
    ///
    /// # Parameters
    /// - `req`: Configuration change details with transition state
    /// - `retry`: Network retry policy with exponential backoff
    /// - `membership`: Cluster membership for channel resolution
    ///
    /// # Errors
    /// - `NetworkError::EmptyPeerList` if no peers provided
    /// - `NetworkError::TaskFailed` for background execution failures
    /// - `ConsensusError::NotLeader` if executed by non-leader
    ///
    /// # Implementation
    /// - Uses compressed gRPC streams for efficiency
    /// - Maintains response order matching input peers
    /// - Concurrent request processing with ordered aggregation
    async fn send_cluster_update(
        &self,
        req: ClusterConfChangeRequest,
        retry: &RetryPolicies,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<ClusterUpdateResult>;

    /// Sends a vote request to a single peer as part of leader election (Raft §5.2).
    /// Candidate-exclusive; caller (core layer) fans this out per-peer and tallies results.
    async fn send_vote_request(
        &self,
        peer_id: u32,
        request: VoteRequest,
        retry: &RetryPolicies,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<VoteResponse>;

    /// Orchestrates log compaction across cluster peers after snapshot creation.
    ///
    /// # Protocol
    /// - Implements log truncation from Raft §7
    /// Initiates cluster join process for a learner node
    ///
    /// # Protocol
    /// - Implements cluster join protocol from Raft §6
    /// - Learner-exclusive operation
    /// - Requires leader connection
    ///
    /// # Parameters
    /// - `leader_channel`: Pre-established gRPC channel to cluster leader
    /// - `request`: Join request with node metadata
    /// - `retry`: Join-specific retry configuration
    /// - `membership`: Cluster membership for channel resolution
    ///
    /// # Errors
    /// - NetworkError::JoinFailed: On unrecoverable join failure
    /// - NetworkError::NotLeader: If contacted node isn't leader
    ///
    /// # Guarantees
    /// - At-least-once delivery
    /// - Automatic leader discovery
    /// - Idempotent operation
    async fn join_cluster(
        &self,
        leader_id: u32,
        request: d_engine_proto::server::cluster::JoinRequest,
        retry: BackoffPolicy,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<d_engine_proto::server::cluster::JoinResponse>;

    /// Discovers current cluster leader
    ///
    /// # Protocol
    /// - Broadcast-based leader discovery
    /// - Handles redirection to current leader
    ///
    /// # Parameters
    /// - `bootstrap_endpoints`: Initial cluster endpoints
    /// - `request`: Discovery request with node metadata
    ///
    /// # Errors
    /// - `NetworkError::DiscoveryTimeout`: When no response received
    async fn discover_leader(
        &self,
        request: d_engine_proto::server::cluster::LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<Vec<d_engine_proto::server::cluster::LeaderDiscoveryResponse>>;

    /// Pushes a snapshot to a lagging peer (called by per-follower ReplicationWorker).
    ///
    /// Used when a peer's `next_index` falls below the leader's purge boundary and
    /// AppendEntries would carry a stale `prev_log_term = 0`, causing a perpetual
    /// conflict loop.  The worker calls this and awaits completion before emitting
    /// `InternalEvent::SnapshotPushCompleted`.
    ///
    /// # Parameters
    /// - `peer_id`: Target follower node ID
    /// - `metadata`: Snapshot metadata (term, index, size)
    /// - `leader_term`: Sender's own current term, not `metadata`'s (the snapshotted
    ///   data's boundary term can be stale after a leader change — #436)
    /// - `state_machine_handler`: Used to load the snapshot data stream
    /// - `membership`: Cluster membership for bulk-channel resolution
    /// - `config`: Snapshot transfer configuration (chunk size, timeout, etc.)
    ///
    /// # Returns
    /// `Ok(())` on successful transfer, `Err` on network or serialization failure.
    async fn send_snapshot(
        &self,
        peer_id: u32,
        metadata: SnapshotMetadata,
        leader_term: u64,
        state_machine_handler: std::sync::Arc<crate::alias::SMHOF<T>>,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
        config: crate::SnapshotConfig,
    ) -> Result<()>;

    /// Opens a persistent bidirectional AppendEntries stream to the given peer.
    ///
    /// Called once per follower when the leader becomes active (or on reconnect).
    /// The returned [`ReplicationStream`] contains:
    /// - `sender`: push batches into the open h2 stream (non-blocking, capacity 128)
    /// - `receiver`: stream of ACKs from the follower
    ///
    /// When the stream breaks (network error, peer restart), the receiver yields
    /// an `Err(tonic::Status)` and the caller should emit
    /// `InternalEvent::PeerStreamError { peer_id }` so the Raft loop can reset
    /// `next_index` and schedule reconnection.
    ///
    /// # Errors
    /// Returns `NetworkError` if the initial stream handshake fails.
    async fn open_replication_stream(
        &self,
        peer_id: u32,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
        compress: bool,
    ) -> Result<ReplicationStream>;
}

// Module level utils
// -----------------------------------------------------------------------------
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use tokio::time::sleep;
use tokio::time::timeout;
use tonic::Code;
use tracing::debug;

use crate::Error;

/// As soon as task has return we should return from this function
pub async fn grpc_task_with_timeout_and_exponential_backoff<F, T, U>(
    task_name: &'static str,
    mut task: F,
    policy: BackoffPolicy,
) -> std::result::Result<tonic::Response<U>, Error>
where
    F: FnMut() -> T,
    T: std::future::Future<Output = std::result::Result<tonic::Response<U>, tonic::Status>>
        + Send
        + 'static,
{
    // let max_retries = 5;
    let mut retries = 0;
    let mut current_delay = Duration::from_millis(policy.base_delay_ms);
    let timeout_duration = Duration::from_millis(policy.timeout_ms);
    let max_delay = Duration::from_millis(policy.max_delay_ms);
    let max_retries = policy.max_retries;

    let mut last_error =
        NetworkError::TaskBackoffFailed("Task failed after max retries".to_string());
    while retries < max_retries {
        debug!("[{task_name}] Attempt {} of {}", retries + 1, max_retries);
        let tr = timeout(timeout_duration, task()).await;
        match tr {
            Ok(Ok(r)) => {
                return Ok(r); // Exit on success
            }
            Ok(Err(status)) => {
                // A single attempt failing is expected — e.g. the peer's own raft
                // component not ready yet during cluster bootstrap. Only
                // exhausting all retries (below) is worth surfacing at warn (#428).
                last_error = match status.code() {
                    Code::Unavailable => {
                        debug!("[{task_name}] Service unavailable: {}", status.message());
                        NetworkError::ServiceUnavailable(format!(
                            "Service unavailable: {}",
                            status.message()
                        ))
                    }
                    _ => {
                        debug!("[{task_name}] RPC error: {}", status);
                        NetworkError::TonicStatusError(Box::new(status))
                    }
                };
            }
            Err(_e) => {
                debug!("[{task_name}] Task timed out after {:?}", timeout_duration);
                last_error = NetworkError::RetryTimeoutError(timeout_duration);
            }
        };

        if retries < max_retries - 1 {
            debug!("[{task_name}] Retrying in {:?}...", current_delay);
            sleep(current_delay).await;

            // Exponential backoff (double the delay each time)
            current_delay = (current_delay * 2).min(max_delay);
        }
        retries += 1;
    }
    info!("[{task_name}] Task failed after {} retries", max_retries);
    Err(last_error.into()) // Fallback error message if no task returns Ok
}
