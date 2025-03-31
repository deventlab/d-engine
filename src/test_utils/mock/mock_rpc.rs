use crate::grpc::rpc_service::rpc_service_server::RpcService;
use crate::grpc::rpc_service::ClientResponse;
use crate::network::grpc::rpc_service::AppendEntriesRequest;
use crate::network::grpc::rpc_service::AppendEntriesResponse;
use crate::network::grpc::rpc_service::ClientProposeRequest;
use crate::network::grpc::rpc_service::ClientReadRequest;
use crate::network::grpc::rpc_service::ClusteMembershipChangeRequest;
use crate::network::grpc::rpc_service::ClusterConfUpdateResponse;
use crate::network::grpc::rpc_service::ClusterMembership;
use crate::network::grpc::rpc_service::MetadataRequest;
use crate::network::grpc::rpc_service::VoteRequest;
use crate::network::grpc::rpc_service::VoteResponse;

#[derive(Debug, Clone, Default)]
pub struct MockRpcService {
    // Expected responses for each method
    pub expected_vote_response: Option<Result<VoteResponse, tonic::Status>>,
    pub expected_append_entries_response: Option<Result<AppendEntriesResponse, tonic::Status>>,
    pub expected_update_cluster_conf_response: Option<Result<ClusterConfUpdateResponse, tonic::Status>>,
    pub expected_client_propose_response: Option<Result<ClientResponse, tonic::Status>>,
    pub expected_client_read_response: Option<Result<ClientResponse, tonic::Status>>,
    pub expected_metadata_response: Option<Result<ClusterMembership, tonic::Status>>,
}

#[tonic::async_trait]
impl RpcService for MockRpcService {
    async fn request_vote(
        &self,
        _request: tonic::Request<VoteRequest>,
    ) -> std::result::Result<tonic::Response<VoteResponse>, tonic::Status> {
        match &self.expected_vote_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock vote response set")),
        }
    }

    async fn append_entries(
        &self,
        _request: tonic::Request<AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        match &self.expected_append_entries_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }

    async fn update_cluster_conf(
        &self,
        _request: tonic::Request<ClusteMembershipChangeRequest>,
    ) -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
        match &self.expected_update_cluster_conf_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }

    async fn handle_client_propose(
        &self,
        _request: tonic::Request<ClientProposeRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_propose_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }

    async fn handle_client_read(
        &self,
        request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_read_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }

    async fn get_cluster_metadata(
        &self,
        request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        match &self.expected_metadata_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }
}
