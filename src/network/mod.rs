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
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;
use tonic::transport::Channel;

// Define a structured return value
#[derive(Debug, Clone)]
pub(crate) struct AppendResults {
    /// Whether a majority quorum is achieved (can be directly determined via
    /// peer_updates)
    pub commit_quorum_achieved: bool,
    /// Updates to each peer's match_index and next_index
    pub peer_updates: HashMap<u32, PeerUpdate>,
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
pub trait Transport: Send + Sync + 'static {
    /// Propagates cluster configuration changes to voting members using Raft's joint consensus.
    ///
    /// # Protocol
    /// - Implements membership change protocol from Raft §6
    /// - Leader-exclusive operation
    /// - Automatically filters self-references and duplicates
    ///
    /// # Parameters
    /// - `peers`: Target voting members (excluding learners)
    /// - `req`: Configuration change details with transition state
    /// - `retry`: Network retry policy with exponential backoff
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
        peers: Vec<ChannelWithAddressAndRole>,
        req: ClusterConfChangeRequest,
        retry: &RetryPolicies,
    ) -> Result<ClusterUpdateResult>;

    /// Replicates log entries to followers and learners.
    ///
    /// # Protocol
    /// - Implements log replication from Raft §5.3
    /// - Leader-exclusive operation
    /// - Handles log consistency checks automatically
    ///
    /// # Parameters
    /// - `requests_with_peer_address`: Target nodes with customized entries
    /// - `retry`: Network retry configuration
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
        requests_with_peer_address: Vec<(u32, ChannelWithAddress, AppendEntriesRequest)>,
        retry: &RetryPolicies,
    ) -> Result<AppendResult>;

    /// Initiates leader election by requesting votes from cluster peers.
    ///
    /// # Protocol
    /// - Implements leader election from Raft §5.2
    /// - Candidate-exclusive operation
    /// - Validates log completeness requirements
    ///
    /// # Parameters
    /// - `peers`: Voting-eligible cluster members
    /// - `req`: Election metadata with candidate's term and log state
    /// - `retry`: Election-specific retry strategy
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
        peers: Vec<ChannelWithAddressAndRole>,
        req: VoteRequest,
        retry: &RetryPolicies,
    ) -> Result<VoteResult>;

    /// Orchestrates log compaction across cluster peers after snapshot creation.
    ///
    /// # Protocol
    /// - Implements log truncation from Raft §7
    /// - Leader-exclusive operation
    /// - Requires valid snapshot checksum
    ///
    /// # Parameters
    /// - `peers`: Target followers and learners
    /// - `req`: Snapshot metadata with truncation index
    /// - `retry`: Purge-specific retry configuration
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
        peers: Vec<ChannelWithAddressAndRole>,
        req: PurgeLogRequest,
        retry: &RetryPolicies,
    ) -> Result<Vec<Result<PurgeLogResponse>>>;

    /// Transfers snapshot to a follower node
    ///
    /// # Parameters
    /// - `channel`: Pre-established gRPC channel
    /// - `metadata`: Snapshot metadata
    /// - `data_stream`: Stream of snapshot chunks
    /// - `retry`: Snapshot-specific retry configuration
    ///
    /// # Errors
    /// - `NetworkError::SnapshotTransferFailed`: On unrecoverable transfer failure
    async fn install_snapshot(
        &self,
        channel: Channel,
        metadata: SnapshotMetadata,
        data_stream: futures::stream::BoxStream<'static, Result<SnapshotChunk>>,
        retry: &BackoffPolicy,
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
    /// - [request](http://_vscodecontentref_/2): Join request with node metadata
    /// - [retry](http://_vscodecontentref_/3): Join-specific retry configuration
    ///
    /// # Errors
    /// - [NetworkError::JoinFailed](http://_vscodecontentref_/4): On unrecoverable join failure
    /// - [NetworkError::NotLeader](http://_vscodecontentref_/5): If contacted node isn't leader
    ///
    /// # Guarantees
    /// - At-least-once delivery
    /// - Automatic leader discovery
    /// - Idempotent operation
    async fn join_cluster(
        &self,
        leader_channel: Channel,
        request: crate::proto::cluster::JoinRequest,
        retry: BackoffPolicy,
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
        voting_members: dashmap::DashMap<u32, ChannelWithAddress>,
        request: crate::proto::cluster::LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
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
pub(crate) async fn task_with_timeout_and_exponential_backoff<F, T, U>(
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
        debug!("Attempt {} of {}", retries + 1, max_retries);
        match timeout(timeout_duration, task()).await {
            Ok(Ok(r)) => {
                return Ok(r); // Exit on success
            }
            Ok(Err(status)) => {
                last_error = match status.code() {
                    Code::Unavailable => {
                        warn!("Service unavailable: {}", status.message());
                        NetworkError::ServiceUnavailable(format!("Service unavailable: {}", status.message()))
                    }
                    _ => {
                        warn!("RPC error: {}", status);
                        NetworkError::TonicStatusError(Box::new(status))
                    }
                };
            }
            Err(_e) => {
                warn!("Task timed out after {:?}", timeout_duration);
                last_error = NetworkError::RetryTimeoutError(timeout_duration);
            }
        };

        if retries < max_retries - 1 {
            debug!("Retrying in {:?}...", current_delay);
            sleep(current_delay).await;

            // Exponential backoff (double the delay each time)
            current_delay = (current_delay * 2).min(max_delay);
        } else {
            warn!("Task failed after {} retries", retries);
            //bug: no need to return if the it is not a business logic error
            // return Err(Error::RetryTaskFailed("Task failed after max
            // retries".to_string())); // Return the last error after max
            // retries
        }
        retries += 1;
    }
    warn!("Task failed after {} retries", max_retries);
    Err(last_error.into()) // Fallback error message if no task returns Ok
}
