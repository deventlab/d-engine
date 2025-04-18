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
use mockall::automock;
use tonic::async_trait;

use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClusteMembershipChangeRequest;
use crate::proto::ClusterConfUpdateResponse;
use crate::proto::VoteRequest;
use crate::proto::VoteResponse;
use crate::BackoffPolicy;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;

// Define a structured return value
#[derive(Debug, Clone)]
pub(crate) struct AppendResults {
    /// Whether a majority quorum is achieved (can be directly determined via
    /// peer_updates)
    pub commit_quorum_achieved: bool,
    /// Updates to each peer's match_index and next_index
    pub peer_updates: HashMap<u32, PeerUpdate>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PeerUpdate {
    pub match_index: u64,
    pub next_index: u64,
    /// if peer response success
    pub success: bool,
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
#[derive(Debug)]
pub struct ClusterUpdateResult {
    pub peer_ids: HashSet<u32>,
    pub responses: Vec<Result<ClusterConfUpdateResponse>>,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Propagates cluster membership changes to all voting peers using Raft's joint consensus
    /// mechanism.
    ///
    /// Implements the membership change protocol from Raft §6. This must only be called by the
    /// leader.
    ///
    /// # Arguments
    /// * `peers` - List of cluster peers (excluding self) to receive configuration updates
    /// * `req` - Configuration change request with joint consensus details
    /// * `retry` - Backoff policy for transient network failures
    ///
    /// # Errors
    /// - Returns [`Error::System(SystemError::Network(NetworkError::EmptyPeerList))`] if `peers` is
    ///   empty
    /// - Returns [`Error::System(SystemError::Network(NetworkError::TaskFailed))`] for background
    ///   task failures
    /// - Returns [`Error::System(SystemError::Network(NetworkError::TonicError))`] for gRPC
    ///   transport errors
    /// - Returns [`Error::Consensus(ConsensusError::NotLeader)`] if local node isn't leader
    ///
    /// # Behavior
    /// - Skips self-references (peer ID matching local node ID)
    /// - Deduplicates peer list entries
    /// - Uses compressed gRPC streams for efficient large config updates
    /// - Maintains response order matching input peer list
    async fn send_cluster_update(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: ClusteMembershipChangeRequest,
        retry: &RetryPolicies,
    ) -> Result<ClusterUpdateResult>;

    /// Sends AppendEntries RPCs to multiple peers concurrently with retry mechanisms.
    ///
    /// This transport-layer function handles:
    /// 1. Request distribution to multiple cluster peers
    /// 2. Network retries with exponential backoff
    /// 3. Timeout handling for individual requests
    /// 4. Response collection and aggregation
    ///
    /// # Parameters
    /// - `requests_with_peer_address`: A list of tuples containing the peer ID, communication
    ///   channel, and the specific AppendEntries request
    /// to be sent to each peer. Each tuple consists of:
    ///     - `peer_id`: The unique identifier of the peer.
    ///     - `channel`: The communication channel (e.g., network connection) to the peer.
    ///     - `request`: The AppendEntries request tailored for the specific peer, as the log
    ///       entries to be synced
    ///   may differ for each peer based on their current log state.
    ///
    /// - `retry`: Retry configuration parameters
    ///
    /// # Returns
    /// - `Result<AppendResult>`: Aggregated responses from peers
    async fn send_append_requests(
        &self,
        requests_with_peer_address: Vec<(u32, ChannelWithAddress, AppendEntriesRequest)>,
        retry: &RetryPolicies,
    ) -> Result<AppendResult>;

    /// Conducts leader election by sending VoteRequest RPCs to candidate/follower nodes.
    ///
    /// Implements the leader election mechanism from Raft §5.2. Must be called by candidates.
    ///
    /// # Arguments
    /// * `peers` - Voting-eligible cluster members (excluding learners and self)
    /// * `req` - Vote request containing candidate's term and log metadata
    /// * `retry` - Election-specific retry strategy
    ///
    /// # Errors
    /// - Returns [`Error::System(SystemError::Network(NetworkError::EmptyPeerList))`] if `peers` is
    ///   empty
    /// - Returns [`Error::System(SystemError::Network(NetworkError::TaskFailed))`] for background
    ///   task failures
    /// - Returns [`Error::System(SystemError::Network(NetworkError::TonicError))`] for gRPC
    ///   transport errors
    /// - Returns [`Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm))`] via
    ///   response parsing
    ///
    /// # Protocol Behavior
    /// - Filters out self-references and duplicates
    /// - Maintains compressed gRPC channel connections
    /// - Aggregates responses while preserving peer order
    /// - Continues processing even with partial failures
    async fn send_vote_requests(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: VoteRequest,
        retry: &RetryPolicies,
    ) -> Result<VoteResult>;
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
                        NetworkError::TonicStatusError(status)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ClusterConfUpdateResponse;

    async fn async_ok(number: u64) -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
        sleep(Duration::from_millis(number)).await;
        let c = ClusterConfUpdateResponse {
            id: 1,
            term: 1,
            version: 1,
            success: true,
        };
        let response: tonic::Response<ClusterConfUpdateResponse> = tonic::Response::new(c);
        Ok(response)
    }

    async fn async_err() -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
        sleep(Duration::from_millis(100)).await;
        Err(tonic::Status::aborted("message"))
    }

    #[tokio::test]
    async fn test_rpc_task_with_exponential_backoff() {
        tokio::time::pause();

        // Case 1: when ok task return ok
        let policy = BackoffPolicy {
            max_retries: 3,
            timeout_ms: 100,
            base_delay_ms: 1000,
            max_delay_ms: 3000,
        };

        assert!(
            task_with_timeout_and_exponential_backoff(|| async { async_ok(3).await }, policy,)
                .await
                .is_ok()
        );

        // Case 2: when err task return error
        assert!(task_with_timeout_and_exponential_backoff(async_err, policy)
            .await
            .is_err());

        // Case 3: when ok task always failed on timeout error
        let policy = BackoffPolicy {
            max_retries: 3,
            timeout_ms: 1,
            base_delay_ms: 1,
            max_delay_ms: 3,
        };
        assert!(
            task_with_timeout_and_exponential_backoff(|| async { async_ok(3).await }, policy)
                .await
                .is_err()
        );
    }
}
