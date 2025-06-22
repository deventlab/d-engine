//! This module is the network abstraction layer with timeout-aware gRPC
//! implementation
//!
//! This module provides network communication facilities with configurable
//! timeout policies for distributed system operations. All network operations
//! are governed by timeout parameters defined in [`RaftConfig`] to ensure
//! system responsiveness.
pub mod grpc;

// Trait definition of the current module
// -----------------------------------------------------------------------------
// Core model in Raft: Transport Definition
//
#[cfg(test)]
mod network_test;

#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use crate::cluster::majority_count;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;
use crate::BackoffPolicy;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;
use crate::TypeConfig;

// Define a structured return value
#[derive(Debug, Clone)]
pub(crate) struct AppendResults {
    /// Whether a majority quorum is achieved (can be directly determined via
    /// peer_updates)
    pub commit_quorum_achieved: bool,
    /// Updates to each peer's match_index and next_index
    pub peer_updates: HashMap<u32, PeerUpdate>,
    /// Learner log catch-up progress
    pub learner_progress: HashMap<u32, u64>,
}

impl AppendResults {
    pub fn if_leadership_maintained(
        &self,
        total_nodes: usize,
    ) -> bool {
        self.peer_updates.len() + 1 >= majority_count(total_nodes)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PeerUpdate {
    pub match_index: u64,
    pub next_index: u64,
    /// if peer response success
    pub success: bool,
}

impl PeerUpdate {
    pub fn success(
        match_index: u64,
        next_index: u64,
    ) -> Self {
        PeerUpdate {
            match_index,
            next_index,
            success: true,
        }
    }

    pub fn failed() -> Self {
        Self {
            match_index: 0,
            next_index: 1,
            success: false,
        }
    }
}
#[derive(Debug)]
pub struct AppendResult {
    pub peer_ids: HashSet<u32>,
    pub responses: Vec<Result<AppendEntriesResponse>>,
}
#[derive(Debug)]
pub struct VoteResult {
    pub peer_ids: HashSet<u32>,
    pub responses: Vec<Result<VoteResponse>>,
}
#[allow(dead_code)]
#[derive(Debug)]
pub struct ClusterUpdateResult {
    pub peer_ids: HashSet<u32>,
    pub responses: Vec<Result<ClusterConfUpdateResponse>>,
}

#[cfg_attr(test, automock)]
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
    #[allow(dead_code)]
    async fn send_cluster_update(
        &self,
        req: ClusterConfChangeRequest,
        retry: &RetryPolicies,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<ClusterUpdateResult>;

    /// Replicates log entries to followers and learners.
    ///
    /// # Protocol
    /// - Implements log replication from Raft §5.3
    /// - Leader-exclusive operation
    /// - Handles log consistency checks automatically
    ///
    /// # Parameters
    /// - `requests`: Vector of (peer_id, AppendEntriesRequest)
    /// - `retry`: Network retry configuration
    /// - `membership`: Cluster membership for channel resolution
    ///
    /// # Returns
    /// - On success: `Ok(AppendResult)` containing aggregated responses
    /// - On failure: `Err(NetworkError)` for unrecoverable errors
    ///
    /// ## **Error Conditions**: Top-level `Err` is returned ONLY when:
    /// - Input `requests_with_peer_address` is empty (`NetworkError::EmptyPeerList`)
    /// - Critical failures prevent spawning async tasks (not shown in current impl)
    ///
    /// # Errors
    /// - `NetworkError::EmptyPeerList` for empty input
    /// - `NetworkError::TaskFailed` for partial execution failures
    ///
    /// # Guarantees
    /// - At-least-once delivery semantics
    /// - Automatic deduplication of peer entries
    /// - Non-blocking error handling
    async fn send_append_requests(
        &self,
        requests: Vec<(u32, AppendEntriesRequest)>,
        retry: &RetryPolicies,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<AppendResult>;

    /// Initiates leader election by requesting votes from cluster peers.
    ///
    /// # Protocol
    /// - Implements leader election from Raft §5.2
    /// - Candidate-exclusive operation
    /// - Validates log completeness requirements
    ///
    /// # Parameters
    /// - `req`: Election metadata with candidate's term and log state
    /// - `retry`: Election-specific retry strategy
    /// - `membership`: Cluster membership for channel resolution
    ///
    /// # Errors
    /// - `NetworkError::EmptyPeerList` for empty peer list
    /// - `NetworkError::TaskFailed` for RPC execution failures
    ///
    /// # Safety
    /// - Automatic term validation in responses
    /// - Strict candidate state enforcement
    /// - Non-blocking partial failure handling
    async fn send_vote_requests(
        &self,
        req: VoteRequest,
        retry: &RetryPolicies,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<VoteResult>;

    /// Orchestrates log compaction across cluster peers after snapshot creation.
    ///
    /// # Protocol
    /// - Implements log truncation from Raft §7
    /// - Leader-exclusive operation
    /// - Requires valid snapshot checksum
    ///
    /// # Parameters
    /// - `req`: Snapshot metadata with truncation index
    /// - `retry`: Purge-specific retry configuration
    /// - `membership`: Cluster membership for channel resolution
    ///
    /// # Errors
    /// - `NetworkError::EmptyPeerList` for empty peer list
    /// - `NetworkError::TaskFailed` for background execution errors
    ///
    /// # Guarantees
    /// - At-least-once delivery
    /// - Automatic progress tracking
    /// - Crash-safe persistence requirements
    async fn send_purge_requests(
        &self,
        req: PurgeLogRequest,
        retry: &RetryPolicies,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<Vec<Result<PurgeLogResponse>>>;

    /// Transfers snapshot to a follower node
    ///
    /// # Parameters
    /// - `metadata`: Snapshot metadata
    /// - `data_stream`: Stream of snapshot chunks
    /// - `retry`: Snapshot-specific retry configuration
    /// - `membership`: Cluster membership for channel resolution
    ///
    /// # Errors
    /// - `SnapshotError::TransferFailed`: On unrecoverable transfer failure
    async fn install_snapshot(
        &self,
        node_id: u32,
        metadata: SnapshotMetadata,
        data_stream: futures::stream::BoxStream<'static, Result<SnapshotChunk>>,
        retry: &crate::InstallSnapshotBackoffPolicy,
        config: &crate::SnapshotConfig,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<()>;

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
        request: crate::proto::cluster::JoinRequest,
        retry: BackoffPolicy,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<crate::proto::cluster::JoinResponse>;

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
        request: crate::proto::cluster::LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
        membership: std::sync::Arc<crate::alias::MOF<T>>,
    ) -> Result<Vec<crate::proto::cluster::LeaderDiscoveryResponse>>;
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
use tracing::warn;

use crate::Error;

/// As soon as task has return we should return from this function
pub(crate) async fn grpc_task_with_timeout_and_exponential_backoff<F, T, U>(
    task_name: &'static str,
    mut task: F,
    policy: BackoffPolicy,
) -> std::result::Result<tonic::Response<U>, Error>
where
    F: FnMut() -> T,
    T: std::future::Future<Output = std::result::Result<tonic::Response<U>, tonic::Status>> + Send + 'static,
{
    // let max_retries = 5;
    let mut retries = 0;
    let mut current_delay = Duration::from_millis(policy.base_delay_ms);
    let timeout_duration = Duration::from_millis(policy.timeout_ms);
    let max_delay = Duration::from_millis(policy.max_delay_ms);
    let max_retries = policy.max_retries;

    let mut last_error = NetworkError::TaskBackoffFailed("Task failed after max retries".to_string());
    while retries < max_retries {
        debug!("[{task_name}] Attempt {} of {}", retries + 1, max_retries);
        match timeout(timeout_duration, task()).await {
            Ok(Ok(r)) => {
                return Ok(r); // Exit on success
            }
            Ok(Err(status)) => {
                last_error = match status.code() {
                    Code::Unavailable => {
                        warn!("[{task_name}] Service unavailable: {}", status.message());
                        NetworkError::ServiceUnavailable(format!("Service unavailable: {}", status.message()))
                    }
                    _ => {
                        warn!("[{task_name}] RPC error: {}", status);
                        NetworkError::TonicStatusError(Box::new(status))
                    }
                };
            }
            Err(_e) => {
                warn!("[{task_name}] Task timed out after {:?}", timeout_duration);
                last_error = NetworkError::RetryTimeoutError(timeout_duration);
            }
        };

        if retries < max_retries - 1 {
            debug!("[{task_name}] Retrying in {:?}...", current_delay);
            sleep(current_delay).await;

            // Exponential backoff (double the delay each time)
            current_delay = (current_delay * 2).min(max_delay);
        } else {
            warn!("[{task_name}] Task failed after {} retries", retries);
            //bug: no need to return if the it is not a business logic error
            // return Err(Error::RetryTaskFailed("Task failed after max
            // retries".to_string())); // Return the last error after max
            // retries
        }
        retries += 1;
    }
    warn!("[{task_name}] Task failed after {} retries", max_retries);
    Err(last_error.into()) // Fallback error message if no task returns Ok
}
