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

use super::rpc_service::rpc_service_server::RpcService;
use super::rpc_service::AppendEntriesRequest;
use super::rpc_service::AppendEntriesResponse;
use super::rpc_service::ClientProposeRequest;
use super::rpc_service::ClientReadRequest;
use super::rpc_service::ClientResponse;
use super::rpc_service::ClusteMembershipChangeRequest;
use super::rpc_service::ClusterConfUpdateResponse;
use super::rpc_service::ClusterMembership;
use super::rpc_service::MetadataRequest;
use super::rpc_service::VoteRequest;
use super::rpc_service::VoteResponse;
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
    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> std::result::Result<Response<VoteResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[rpc|request_vote] Node is not ready!");
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

    // 1: compare request.term and current_term
    // 1.1: if request.term <= current_term:
    // 1.2: if request.term > current_term:
    //      1.2.1 we should swith node state to Follower if it is in Leader or
    // Candidate state      1.2.2 we should turn off election timeout and
    // heartbeat timeout
    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> std::result::Result<Response<AppendEntriesResponse>, tonic::Status> {
        if !self.server_is_ready() {
            warn!("[rpc|append_entries] Node is not ready!");
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

    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    async fn update_cluster_conf(
        &self,
        request: tonic::Request<ClusteMembershipChangeRequest>,
    ) -> std::result::Result<Response<ClusterConfUpdateResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[rpc|update_cluster_conf] Node is not ready!");
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

    //----------------- External request handler---------------------
    ///Only `Propose` command need to be synced.
    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    async fn handle_client_propose(
        &self,
        request: tonic::Request<ClientProposeRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[handle_client_propose] Node is not ready!");
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

    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    async fn get_cluster_metadata(
        &self,
        request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        debug!("receive get_cluster_metadata");
        if !self.server_is_ready() {
            warn!("[rpc|get_cluster_metadata] Node is not ready!");
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

    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    async fn handle_client_read(
        &self,
        request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        if !self.server_is_ready() {
            warn!("handle_client_read: Node is not ready!");
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

/// Generic timeout handler for RPC response channels
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
