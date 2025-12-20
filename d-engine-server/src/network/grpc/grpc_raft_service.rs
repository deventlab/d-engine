//! Raft gRPC service implementation handling RPC communication between cluster nodes
//! and client requests. Implements core Raft protocol logic for leader election,
//! log replication, and cluster configuration management.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use tokio::select;
use tokio::time::timeout;

use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::Node;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::RaftEvent;
use d_engine_core::RaftOneshot;
use d_engine_core::StreamResponseSender;
use d_engine_core::TypeConfig;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::WatchRequest;

use d_engine_proto::client::raft_client_service_server::RaftClientService;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementService;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionService;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationService;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::PurgeLogResponse;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotResponse;
use d_engine_proto::server::storage::snapshot_service_server::SnapshotService;

#[tonic::async_trait]
impl<T> RaftElectionService for Node<T>
where
    T: TypeConfig,
{
    /// Handles RequestVote RPC calls from candidate nodes during leader elections
    /// # Raft Protocol Logic
    /// - Part of leader election mechanism (Section 5.2)
    /// - Validates candidate's term and log completeness
    /// - Grants vote if candidate's log is at least as up-to-date as local log
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> std::result::Result<Response<VoteResponse>, Status> {
        if !self.is_rpc_ready() {
            warn!(
                "[rpc|request_vote] My raft setup(Node:{}) is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ReceiveVoteRequest(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;
        let timeout_duration =
            Duration::from_millis(self.node_config.raft.election.election_timeout_min);
        handle_rpc_timeout(resp_rx, timeout_duration, "request_vote").await
    }
}
#[tonic::async_trait]
impl<T> RaftReplicationService for Node<T>
where
    T: TypeConfig,
{
    /// Processes AppendEntries RPC calls from cluster leader
    /// # Raft Protocol Logic
    /// - Heartbeat mechanism (Section 5.2)
    /// - Log replication entry point (Section 5.3)
    /// - Term comparison logic:
    ///   - If incoming term > current term: revert to follower state
    ///   - Reset election timeout on valid leader communication
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> std::result::Result<Response<AppendEntriesResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            warn!("[rpc|append_entries] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::AppendEntries(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.retry.append_entries.timeout_ms);

        handle_rpc_timeout(resp_rx, timeout_duration, "append_entries").await
    }
}

#[tonic::async_trait]
impl<T> SnapshotService for Node<T>
where
    T: TypeConfig,
{
    type StreamSnapshotStream = tonic::Streaming<SnapshotChunk>;

    async fn stream_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<SnapshotAck>>,
    ) -> std::result::Result<tonic::Response<Self::StreamSnapshotStream>, tonic::Status> {
        if !self.is_rpc_ready() {
            warn!("stream_snapshot: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = StreamResponseSender::new();

        self.event_tx
            .send(RaftEvent::StreamSnapshot(
                Box::new(request.into_inner()),
                resp_tx,
            ))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.node_config.raft.snapshot_rpc_timeout_ms);

        handle_rpc_timeout(
            async { resp_rx.await.map_err(|_| Status::internal("Response channel closed")) },
            timeout_duration,
            "stream_snapshot",
        )
        .await
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<Streaming<SnapshotChunk>>,
    ) -> std::result::Result<tonic::Response<SnapshotResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            warn!("install_snapshot: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::InstallSnapshotChunk(
                Box::new(request.into_inner()),
                resp_tx,
            ))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.node_config.raft.snapshot_rpc_timeout_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "install_snapshot").await
    }

    async fn purge_log(
        &self,
        request: tonic::Request<PurgeLogRequest>,
    ) -> std::result::Result<tonic::Response<PurgeLogResponse>, Status> {
        if !self.is_rpc_ready() {
            warn!("purge_log: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::RaftLogCleanUp(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "purge_log").await
    }
}

#[tonic::async_trait]
impl<T> ClusterManagementService for Node<T>
where
    T: TypeConfig,
{
    /// Handles cluster membership changes (joint consensus)
    /// # Raft Protocol Logic
    /// - Implements cluster configuration changes (Section 6)
    /// - Validates new configuration against current cluster state
    /// - Ensures safety during membership transitions
    async fn update_cluster_conf(
        &self,
        request: tonic::Request<ClusterConfChangeRequest>,
    ) -> std::result::Result<Response<ClusterConfUpdateResponse>, Status> {
        if !self.is_rpc_ready() {
            warn!(
                "[rpc|update_cluster_conf_from_leader] Node-{} is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClusterConfUpdate(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.node_config.retry.membership.timeout_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "update_cluster_conf_from_leader").await
    }

    /// Returns current cluster membership and state metadata
    /// # Usage
    /// - Administrative API for cluster inspection
    /// - Provides snapshot of current configuration
    async fn get_cluster_metadata(
        &self,
        request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        debug!("receive get_cluster_metadata");
        if !self.is_rpc_ready() {
            warn!(
                "[rpc|get_cluster_metadata] Node-{} is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClusterConf(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "get_cluster_metadata").await
    }

    // Request to join the cluster as a new learner node
    async fn join_cluster(
        &self,
        request: tonic::Request<JoinRequest>,
    ) -> std::result::Result<tonic::Response<JoinResponse>, tonic::Status> {
        debug!("receive join_cluster");
        if !self.is_rpc_ready() {
            warn!("[rpc|join_cluster] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::JoinCluster(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "join_cluster").await
    }

    async fn discover_leader(
        &self,
        request: tonic::Request<LeaderDiscoveryRequest>,
    ) -> std::result::Result<tonic::Response<LeaderDiscoveryResponse>, tonic::Status> {
        debug!("receive discover_leader");
        if !self.is_rpc_ready() {
            warn!("[rpc|discover_leader] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::DiscoverLeader(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "discover_leader").await
    }
}
#[tonic::async_trait]
impl<T> RaftClientService for Node<T>
where
    T: TypeConfig,
{
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<d_engine_proto::client::WatchResponse, Status>> + Send>>;

    /// Processes client write requests requiring consensus
    /// # Raft Protocol Logic
    /// - Entry point for client proposals (Section 7)
    /// - Validates requests before appending to leader's log
    /// - Ensures linearizable writes through log replication
    async fn handle_client_write(
        &self,
        request: tonic::Request<ClientWriteRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, Status> {
        if !self.is_rpc_ready() {
            warn!("[handle_client_write] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let remote_addr = request.remote_addr();
        let event_tx = self.event_tx.clone();
        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);

        let request_future = async move {
            let req: ClientWriteRequest = request.into_inner();
            // Extract request and validate
            if req.commands.is_empty() {
                return Err(Status::invalid_argument("Commands cannot be empty"));
            }

            let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
            event_tx
                .send(RaftEvent::ClientPropose(req, resp_tx))
                .await
                .map_err(|_| Status::internal("Event channel closed"))?;

            handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_write").await
        };

        let cancellation_future = async move {
            warn!("Request from {:?} cancelled by client", remote_addr);
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err::<Response<ClientResponse>, Status>(Status::cancelled(
                "Request cancelled by client",
            ))
        };

        with_cancellation_handler(request_future, cancellation_future).await
    }

    /// Handles client read requests with linearizability guarantees
    /// # Raft Protocol Logic
    /// - Implements lease-based leader reads (Section 6.4)
    /// - Verifies leadership before serving reads
    /// - Ensures read-after-write consistency
    async fn handle_client_read(
        &self,
        request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            warn!("handle_client_read: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClientReadRequest(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_read").await
    }

    /// Watch for changes on a specific key
    ///
    /// Returns a stream of WatchResponse events whenever the watched key changes.
    /// Watch for changes to a specific key
    ///
    /// Returns a stream of events (PUT/DELETE) for the specified key.
    /// The stream will continue until the client disconnects or the server shuts down.
    ///
    /// # Arguments
    /// * `request` - Contains the key to watch
    ///
    /// # Returns
    /// A stream of WatchResponse messages containing PUT/DELETE events
    ///
    /// # Errors
    /// Returns Status::UNAVAILABLE if Watch is disabled in configuration
    #[cfg(feature = "watch")]
    async fn watch(
        &self,
        request: tonic::Request<WatchRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let watch_request = request.into_inner();
        let key = watch_request.key;

        // Check if watch registry is available
        let registry = self.watch_registry.as_ref().ok_or_else(|| {
            Status::unavailable("Watch feature is disabled in server configuration")
        })?;

        info!(
            node_id = self.node_id,
            key = ?key,
            "Registering watch for key"
        );

        // Register watcher and get receiver
        let handle = registry.register(key.into());
        let (_watcher_id, _key, receiver) = handle.into_receiver();

        // Convert mpsc::Receiver -> Boxed Stream with sentinel error on close
        // When the stream ends (receiver closed), send an UNAVAILABLE error to help clients
        // detect server shutdown/restart and implement reconnection logic
        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
            .map(Ok)
            .chain(futures::stream::once(async {
                Err(Status::unavailable(
                    "Watch stream closed: server may have shut down or restarted. Please reconnect and re-register the watcher."
                ))
            }));

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    #[cfg(not(feature = "watch"))]
    async fn watch(
        &self,
        _request: tonic::Request<WatchRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(Status::unimplemented(
            "Watch feature is not compiled in this build",
        ))
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
    FRequest:
        Future<Output = std::result::Result<Response<ClientResponse>, Status>> + Send + 'static,
    FCancellation:
        Future<Output = std::result::Result<Response<ClientResponse>, Status>> + Send + 'static,
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
            warn!(
                "[{}] Response timeout after {:?}",
                rpc_name, timeout_duration
            );
            Err(Status::deadline_exceeded("RPC timeout exceeded"))
        }
    }
}
