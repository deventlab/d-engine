use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::WatchRequest;
use d_engine_proto::client::WatchResponse;
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
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct MockRpcService {
    pub server_port: Option<u16>,
    // Expected responses for each method
    pub expected_update_cluster_conf_response:
        Option<Result<ClusterConfUpdateResponse, tonic::Status>>,
    pub expected_client_propose_response: Option<Result<ClientResponse, tonic::Status>>,
    pub expected_client_read_response: Option<Result<ClientResponse, tonic::Status>>,

    #[allow(clippy::type_complexity)]
    pub expected_metadata_response:
        Option<Arc<dyn Fn(u16) -> Result<ClusterMembership, tonic::Status> + Send + Sync>>,

    pub expected_join_cluster_response: Option<Result<JoinResponse, tonic::Status>>,
    pub expected_discover_leader_response: Option<Result<LeaderDiscoveryResponse, tonic::Status>>,
}
impl MockRpcService {
    pub fn with_metadata_response(
        mut self,
        f: impl Fn(u16) -> Result<ClusterMembership, tonic::Status> + Send + Sync + 'static,
    ) -> Self {
        self.expected_metadata_response = Some(Arc::new(f));
        self
    }

    pub fn set_port(
        &mut self,
        port: u16,
    ) {
        self.server_port = Some(port);
    }
}

#[tonic::async_trait]
impl ClusterManagementService for MockRpcService {
    async fn update_cluster_conf(
        &self,
        _request: tonic::Request<ClusterConfChangeRequest>,
    ) -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
        match &self.expected_update_cluster_conf_response {
            Some(Ok(response)) => Ok(tonic::Response::new(*response)),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown(
                "No mock update_cluster_conf response set",
            )),
        }
    }
    async fn get_cluster_metadata(
        &self,
        _request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        match (&self.expected_metadata_response, self.server_port) {
            (Some(f), Some(port)) => f(port).map(tonic::Response::new).map_err(|e| e.clone()),
            _ => Err(tonic::Status::unimplemented(
                "Metadata response not configured",
            )),
        }
    }

    async fn join_cluster(
        &self,
        _request: tonic::Request<JoinRequest>,
    ) -> std::result::Result<tonic::Response<JoinResponse>, tonic::Status> {
        match &self.expected_join_cluster_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock join_cluster response set")),
        }
    }

    async fn discover_leader(
        &self,
        _request: tonic::Request<LeaderDiscoveryRequest>,
    ) -> std::result::Result<tonic::Response<LeaderDiscoveryResponse>, tonic::Status> {
        match &self.expected_discover_leader_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown(
                "No mock discover_leader response set",
            )),
        }
    }
}

#[tonic::async_trait]
impl RaftClientService for MockRpcService {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, tonic::Status>> + Send>>;

    async fn handle_client_write(
        &self,
        _request: tonic::Request<ClientWriteRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_propose_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown(
                "No mock handle_client_write response set",
            )),
        }
    }

    async fn handle_client_read(
        &self,
        _request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_read_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown(
                "No mock handle_client_read response set",
            )),
        }
    }

    async fn watch(
        &self,
        _request: tonic::Request<WatchRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        // Mock implementation - return empty stream
        Err(tonic::Status::unimplemented(
            "Watch not implemented in mock",
        ))
    }
}
