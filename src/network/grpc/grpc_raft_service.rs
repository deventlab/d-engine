//! Raft gRPC service implementation handling RPC communication between cluster nodes
//! and client requests. Implements core Raft protocol logic for leader election,
//! log replication, and cluster configuration management.

use std::future::Future;
use std::time::Duration;

use autometrics::autometrics;
use log::debug;
use log::error;
use log::warn;
use tokio::select;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::proto::rpc_service_server::RpcService;
use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientProposeRequest;
use crate::proto::ClientReadRequest;
use crate::proto::ClientResponse;
use crate::proto::ClusteMembershipChangeRequest;
use crate::proto::ClusterConfUpdateResponse;
use crate::proto::ClusterMembership;
use crate::proto::MetadataRequest;
use crate::proto::VoteRequest;
use crate::proto::VoteResponse;
use crate::MaybeCloneOneshot;
use crate::Node;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::TypeConfig;
use crate::API_SLO;

#[tonic::async_trait]
impl<T> RpcService for Node<T>
where T: TypeConfig
{
    /// Handles RequestVote RPC calls from candidate nodes during leader elections
    /// # Raft Protocol Logic
    /// - Part of leader election mechanism (Section 5.2)
    /// - Validates candidate's term and log completeness
    /// - Grants vote if candidate's log is at least as up-to-date as local log
    #[cfg_attr(not(doc), autometrics(objective = API_SLO))]
    #[tracing::instrument]
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> std::result::Result<Response<VoteResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[rpc|request_vote] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ReceiveVoteRequest(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;
        let timeout_duration = Duration::from_millis(self.settings.raft.election.election_timeout_min);
        handle_rpc_timeout(resp_rx, timeout_duration, "request_vote").await
    }

    /// Processes AppendEntries RPC calls from cluster leader
    /// # Raft Protocol Logic
    /// - Heartbeat mechanism (Section 5.2)
    /// - Log replication entry point (Section 5.3)
    /// - Term comparison logic:
    ///   - If incoming term > current term: revert to follower state
    ///   - Reset election timeout on valid leader communication
    #[cfg_attr(not(doc), autometrics(objective = API_SLO))]
    #[tracing::instrument]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> std::result::Result<Response<AppendEntriesResponse>, tonic::Status> {
        if !self.server_is_ready() {
            warn!("[rpc|append_entries] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::AppendEntries(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.settings.retry.election.timeout_ms);

        handle_rpc_timeout(resp_rx, timeout_duration, "append_entries").await
    }

    /// Handles cluster membership changes (joint consensus)
    /// # Raft Protocol Logic
    /// - Implements cluster configuration changes (Section 6)
    /// - Validates new configuration against current cluster state
    /// - Ensures safety during membership transitions
    #[cfg_attr(not(doc), autometrics(objective = API_SLO))]
    #[tracing::instrument]
    async fn update_cluster_conf(
        &self,
        request: tonic::Request<ClusteMembershipChangeRequest>,
    ) -> std::result::Result<Response<ClusterConfUpdateResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[rpc|update_cluster_conf] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClusterConfUpdate(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.settings.retry.membership.timeout_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "update_cluster_conf").await
    }

    /// Processes client write requests requiring consensus
    /// # Raft Protocol Logic
    /// - Entry point for client proposals (Section 7)
    /// - Validates requests before appending to leader's log
    /// - Ensures linearizable writes through log replication
    #[cfg_attr(not(doc), autometrics(objective = API_SLO))]
    #[tracing::instrument]
    async fn handle_client_propose(
        &self,
        request: tonic::Request<ClientProposeRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[handle_client_propose] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let remote_addr = request.remote_addr();
        let event_tx = self.event_tx.clone();
        let timeout_duration = Duration::from_millis(self.settings.raft.general_raft_timeout_duration_in_ms);

        let request_future = async move {
            let req: ClientProposeRequest = request.into_inner();
            // Extract request and validate
            if req.commands.is_empty() {
                return Err(Status::invalid_argument("Commands cannot be empty"));
            }

            let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
            event_tx
                .send(RaftEvent::ClientPropose(req, resp_tx))
                .await
                .map_err(|_| Status::internal("Event channel closed"))?;

            handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_propose").await
        };

        let cancellation_future = async move {
            warn!("Request from {:?} cancelled by client", remote_addr);
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err::<Response<ClientResponse>, Status>(Status::cancelled("Request cancelled by client"))
        };

        with_cancellation_handler(request_future, cancellation_future).await
    }

    /// Returns current cluster membership and state metadata
    /// # Usage
    /// - Administrative API for cluster inspection
    /// - Provides snapshot of current configuration
    #[cfg_attr(not(doc), autometrics(objective = API_SLO))]
    #[tracing::instrument]
    async fn get_cluster_metadata(
        &self,
        request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        debug!("receive get_cluster_metadata");
        if !self.server_is_ready() {
            warn!("[rpc|get_cluster_metadata] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClusterConf(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.settings.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "get_cluster_metadata").await
    }

    /// Handles client read requests with linearizability guarantees
    /// # Raft Protocol Logic
    /// - Implements lease-based leader reads (Section 6.4)
    /// - Verifies leadership before serving reads
    /// - Ensures read-after-write consistency
    #[cfg_attr(not(doc), autometrics(objective = API_SLO))]
    #[tracing::instrument]
    async fn handle_client_read(
        &self,
        request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        if !self.server_is_ready() {
            warn!("handle_client_read: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClientReadRequest(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.settings.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_read").await
    }
}

/// Gracefully handles client request cancellations
/// # Functionality
/// - Manages cleanup of abandoned requests
/// - Tracks request cancellation metrics
/// - Prevents resource leaks from dropped requests
pub(crate) async fn with_cancellation_handler<FRequest, FCancellation>(
    request_future: FRequest,
    cancellation_future: FCancellation,
) -> std::result::Result<Response<ClientResponse>, Status>
where
    FRequest: Future<Output = std::result::Result<Response<ClientResponse>, Status>> + Send + 'static,
    FCancellation: Future<Output = std::result::Result<Response<ClientResponse>, Status>> + Send + 'static,
{
    let token = CancellationToken::new();
    // Will call token.cancel() when the future is dropped, such as when the client
    // cancels the request
    let _drop_guard = token.clone().drop_guard();
    let select_task = tokio::spawn(async move {
        // Can select on token cancellation on any cancellable future while handling the
        // request, allowing for custom cleanup code or monitoring
        select! {
            res = request_future => res,
            _ = token.cancelled() => cancellation_future.await,
        }
    });

    select_task.await.unwrap()
}

/// Centralized timeout handler for all RPC operations
/// # Features
/// - Uniform timeout enforcement across RPC types
/// - Detailed error categorization:
///   - Channel errors
///   - Application-level errors
///   - Deadline exceeded
/// - Logging and metrics integration
async fn handle_rpc_timeout<T, E>(
    resp_rx: impl Future<Output = Result<Result<T, Status>, E>>,
    timeout_duration: Duration,
    rpc_name: &'static str,
) -> Result<Response<T>, Status>
where
    T: std::fmt::Debug,
    E: std::fmt::Debug,
{
    debug!("grpc_raft_serice::handle_rpc_timeout::{}", rpc_name);

    match timeout(timeout_duration, resp_rx).await {
        Ok(Ok(Ok(response))) => {
            debug!("[{}] Success response: {:?}", rpc_name, &response);
            Ok(Response::new(response))
        }
        Ok(Ok(Err(status))) => {
            error!("[{}] Error status: {:?}", rpc_name, &status);
            Err(status)
        }
        Ok(Err(e)) => {
            error!("[{}] Channel error: {:?}", rpc_name, e);
            Err(Status::deadline_exceeded("RPC channel closed"))
        }
        Err(_) => {
            warn!("[{}] Response timeout after {:?}", rpc_name, timeout_duration);
            Err(Status::deadline_exceeded("RPC timeout exceeded"))
        }
    }
}
