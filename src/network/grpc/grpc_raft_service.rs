use super::rpc_service::{
    rpc_service_server::RpcService, AppendEntriesRequest, AppendEntriesResponse,
    ClientProposeRequest, ClientReadRequest, ClientResponse, ClusteMembershipChangeRequest,
    ClusterConfUpdateResponse, ClusterMembership, MetadataRequest, VoteRequest, VoteResponse,
};
use crate::{MaybeCloneOneshot, Node, RaftEvent, RaftOneshot, TypeConfig, API_SLO};
use autometrics::autometrics;
use log::{debug, error, warn};
use std::{future::Future, time::Duration};
use tokio::{select, time::timeout};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl<T> RpcService for Node<T>
where
    T: TypeConfig,
{
    #[autometrics(objective = API_SLO)]
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> std::result::Result<Response<VoteResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[rpc|request_vote] Node is not ready!");
            return Err(Status::unavailable("Service is not ready"));
        }

        debug!("request_vote::Received: {:?}", request);
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ReceiveVoteRequest(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let response = resp_rx
            .await
            .map_err(|e| {
                debug!("request_vote::Channel error: {:?}", e);
                Status::deadline_exceeded("Response timeout")
            })?
            .map(Response::new);
        debug!("request_vote::Response: {:?}", &response);

        response
    }

    // 1: compare request.term and current_term
    // 1.1: if request.term <= current_term:
    // 1.2: if request.term > current_term:
    //      1.2.1 we should swith node state to Follower if it is in Leader or Candidate state
    //      1.2.2 we should turn off election timeout and heartbeat timeout
    #[autometrics(objective = API_SLO)]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> std::result::Result<Response<AppendEntriesResponse>, tonic::Status> {
        if !self.server_is_ready() {
            warn!("[rpc|append_entries] Node is not ready!");
            return Err(Status::unavailable("Service is not ready"));
        }

        debug!("request_append_entries::Received: {:?}", request);
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::AppendEntries(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let response = resp_rx
            .await
            .map_err(|e| {
                debug!("append_entries::Channel error: {:?}", e);
                Status::deadline_exceeded("Response timeout")
            })?
            .map(Response::new);
        debug!("append_entries::Response: {:?}", &response);

        response
    }

    #[autometrics(objective = API_SLO)]
    async fn update_cluster_conf(
        &self,
        request: tonic::Request<ClusteMembershipChangeRequest>,
    ) -> std::result::Result<Response<ClusterConfUpdateResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[rpc|update_cluster_conf] Node is not ready!");
            return Err(Status::unavailable("Service is not ready"));
        }

        debug!("update_cluster_conf::Received req: {:?}", request);

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClusterConfUpdate(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let response = resp_rx
            .await
            .map_err(|e| {
                debug!("update_cluster_conf::Channel error: {:?}", e);
                Status::deadline_exceeded("Response timeout")
            })?
            .map(Response::new);
        debug!("update_cluster_conf::Response: {:?}", &response);

        response
    }

    //----------------- External request handler---------------------
    ///Only `Propose` command need to be synced.
    #[autometrics(objective = API_SLO)]
    async fn handle_client_propose(
        &self,
        request: tonic::Request<ClientProposeRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, Status> {
        if !self.server_is_ready() {
            warn!("[handle_client_propose] Node is not ready!");
            return Err(Status::unavailable("Service is not ready"));
        }

        //--------------------

        // let state = self.state();
        // let is_leader = state.is_leader();
        //Bugfix:
        // The client could cancel the RPC, so we need keep the variable with static lifetime
        // https://github.com/hyperium/tonic/blob/eeb3268f71ae5d1107c937392389db63d8f721fb/examples/src/cancellation/server.rs#L58
        // let handler = self.get_client_request_handler();
        // let cluster_membership_controller = self.cluster_membership_controller();
        // let append_entries_controller = self.append_entries_controller();
        // Deduplicate request using request_id
        // if self.has_seen_request(&request.id).await {
        //     return Ok(tonic::Response::new(ClientProposeResponse {
        //         request_id: request.id.clone(),
        //         status: "duplicate".into(),
        //         timestamp: chrono::Utc::now().to_rfc3339(),
        //     }));
        // }

        let remote_addr = request.remote_addr();
        let event_tx = self.event_tx.clone();
        let leader_propose_timeout_duration_in_ms = self
            .settings
            .raft_settings
            .leader_propose_timeout_duration_in_ms;

        let request_future = async move {
            debug!(
                "[handle_client_propose] handle_client_propose::Received: {:?}",
                request
            );
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

            match timeout(
                Duration::from_millis(leader_propose_timeout_duration_in_ms),
                resp_rx,
            )
            .await
            {
                Ok(Ok(Ok(client_response))) => {
                    debug!(
                        "handle_client_propose: client_response: {:?}",
                        &client_response
                    );
                    Ok(Response::new(client_response))
                }
                Ok(Ok(Err(status))) => {
                    error!("handle_client_propose, status: {:?}", &status);
                    Err(status)
                }
                Ok(Err(_oneshot_error)) => {
                    Err(Status::deadline_exceeded("channel has been closed"))
                }
                Err(_timeout_error) => Err(Status::deadline_exceeded("Response timeout")),
            }
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

    #[autometrics(objective = API_SLO)]
    async fn get_cluster_metadata(
        &self,
        request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        debug!("receive get_cluster_metadata");
        if !self.server_is_ready() {
            warn!("[rpc|get_cluster_metadata] Node is not ready!");
            return Err(Status::unavailable("Service is not ready"));
        }
        debug!(
            "[get_cluster_metadata] get_cluster_metadata::Received: {:?}",
            request
        );
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClusterConf(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let response = resp_rx
            .await
            .map_err(|e| {
                debug!("get_cluster_metadata::Channel error: {:?}", e);
                Status::deadline_exceeded("Response timeout")
            })?
            .map(Response::new);
        debug!("get_cluster_metadata::Response: {:?}", &response);

        response
    }

    #[autometrics(objective = API_SLO)]
    async fn handle_client_read(
        &self,
        request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        if !self.server_is_ready() {
            warn!("handle_client_read: Node is not ready!");
            return Err(Status::unavailable("Service is not ready"));
        }

        debug!("[handle_client_read] req received: {:?}", request);
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(RaftEvent::ClientReadRequest(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let response = resp_rx
            .await
            .map_err(|e| {
                debug!("handle_client_read::Channel error: {:?}", e);
                Status::deadline_exceeded("Response timeout")
            })?
            .map(Response::new);
        debug!("handle_client_read::Response: {:?}", &response);

        response
    }
}

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
    // Will call token.cancel() when the future is dropped, such as when the client cancels the request
    let _drop_guard = token.clone().drop_guard();
    let select_task = tokio::spawn(async move {
        // Can select on token cancellation on any cancellable future while handling the request,
        // allowing for custom cleanup code or monitoring
        select! {
            res = request_future => res,
            _ = token.cancelled() => cancellation_future.await,
        }
    });

    select_task.await.unwrap()
}
