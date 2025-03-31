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

use crate::grpc::rpc_service::AppendEntriesRequest;
use crate::grpc::rpc_service::ClusteMembershipChangeRequest;
use crate::grpc::rpc_service::VoteRequest;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::RaftConfig;
use crate::Result;
use crate::RetryPolicies;

// Define a structured return value
#[derive(Debug, Clone)]
pub struct AppendResults {
    /// Whether a majority quorum is achieved (can be directly determined via
    /// peer_updates)
    pub commit_quorum_achieved: bool,
    /// Updates to each peer's match_index and next_index
    pub peer_updates: HashMap<u32, PeerUpdate>,
}

#[derive(Debug, Clone)]
pub struct PeerUpdate {
    pub match_index: u64,
    pub next_index: u64,
    /// if peer response success
    pub success: bool,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Sync new cluster membership config to all connected peers,
    ///     no matter it is Learner or other roles
    /// pre-validation:
    /// - the sender is Leader
    /// - if there is no peer passed, then return Ok(false)
    async fn send_cluster_membership_requests(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: ClusteMembershipChangeRequest,
        retry: &RetryPolicies,
    ) -> Result<bool>;

    /// Only when Majority receives, true will be returned.
    /// This function is special comapring with other rpc functions in this
    /// file.
    ///
    /// It will also take responbility to maintain peer's next_index and
    /// match_index
    ///
    /// @requests_with_peer_address:
    /// - only inlclude Follower and Candidates, no Learner will be considered.
    /// - (peer_id,  peer_address, peer_request)
    /// - the request send to each peer is unique.
    async fn send_append_requests(
        &self,
        // role_tx: mpsc::UnboundedSender<RoleEvent>,
        leader_term: u64,
        requests_with_peer_address: Vec<(u32, ChannelWithAddress, AppendEntriesRequest)>,
        retry: &RetryPolicies,
    ) -> Result<AppendResults>;

    /// Send vote request to either Candidate or Followers,
    ///     which means, learner will not receive the vote request
    ///
    /// The candidate then requests votes from other nodes.
    /// parameters:
    /// * callback function
    /// -- If votes received from majority of servers: become leader
    /// -- If AppendEntries RPC received from new leader: convert to follower
    /// -- If election timeout elapses: start new election
    ///
    /// `requests` - means send to peers;
    async fn send_vote_requests(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: VoteRequest,
        retry: &RetryPolicies,
    ) -> Result<bool>;
}

// Module level utils
// -----------------------------------------------------------------------------
use std::collections::HashMap;
use std::time::Duration;

use log::debug;
use log::error;
use log::warn;
use tokio::time::sleep;
use tokio::time::timeout;
use tonic::Code;

use crate::Error;
/// As soon as task has return we should return from this function
pub(crate) async fn task_with_timeout_and_exponential_backoff<F, T, U>(
    mut task: F,
    max_retries: usize,
    delay_duration: Duration,
    timeout_duration: Duration,
) -> std::result::Result<tonic::Response<U>, Error>
where
    F: FnMut() -> T,
    T: std::future::Future<Output = std::result::Result<tonic::Response<U>, tonic::Status>> + Send + 'static,
{
    // let max_retries = 5;
    let mut retries = 0;
    let mut delay = delay_duration; // Initial delay

    let mut e = Error::RetryTaskFailed("Task failed after max retries".to_string());
    while retries < max_retries {
        debug!("task_with_timeout_and_exponential_backoff, time: {}", retries + 1);
        match timeout(timeout_duration, task()).await {
            Ok(Ok(r)) => {
                return Ok(r); // Exit on success
            }
            Ok(Err(status)) => {
                if status.code() == Code::Unavailable {
                    warn!("Service is unavailable.");
                    e = Error::ServerIsNotReadyError;
                } else {
                    warn!("Received different status: {}", status);
                    e = Error::RPCServerStatusError(format!("status: {:?}", status));
                }
            }
            Err(e) => {
                error!("task_with_timeout_and_exponential_backoff timeout: {:?}", e);
            }
        };

        retries += 1;
        if retries < max_retries {
            sleep(delay).await;
            delay = delay * 2; // Exponential backoff (double the delay each
                               // time)
        } else {
            warn!("Task failed after {} retries", retries);
            //bug: no need to return if the it is not a business logic error
            // return Err(Error::RetryTaskFailed("Task failed after max
            // retries".to_string())); // Return the last error after max
            // retries
        }
    }
    warn!("Task failed after {} retries", max_retries);
    Err(e) // Fallback error message if no task returns Ok
}

#[cfg(test)]
mod tests {
    use grpc::rpc_service::ClusterConfUpdateResponse;

    use super::*;

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
        // Case 1: when ok task return ok
        if let Ok(_) = task_with_timeout_and_exponential_backoff(
            || async { async_ok(3).await },
            3,
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
        .await
        {
            assert!(true);
        } else {
            assert!(false);
        }
        // Case 2: when err task return error
        if let Ok(_) =
            task_with_timeout_and_exponential_backoff(async_err, 3, Duration::from_millis(100), Duration::from_secs(1))
                .await
        {
            assert!(false);
        } else {
            assert!(true);
        }

        // Case 3: when ok task always failed on timeout error
        if let Ok(_) = task_with_timeout_and_exponential_backoff(
            || async { async_ok(3).await },
            3,
            Duration::from_millis(100),
            Duration::from_millis(1),
        )
        .await
        {
            assert!(false);
        } else {
            assert!(true);
        }
    }
}
