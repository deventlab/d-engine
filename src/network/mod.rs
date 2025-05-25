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
use crate::proto::PurgeLogRequest;
use crate::proto::PurgeLogResponse;
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
        req: ClusteMembershipChangeRequest,
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
