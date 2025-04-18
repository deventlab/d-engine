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
            Some(Ok(response)) => Ok(tonic::Response::new(*response)),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock vote response set")),
        }
    }

    async fn append_entries(
        &self,
        _request: tonic::Request<AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        match &self.expected_append_entries_response {
            Some(Ok(response)) => Ok(tonic::Response::new(*response)),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }

    async fn update_cluster_conf(
        &self,
        _request: tonic::Request<ClusteMembershipChangeRequest>,
    ) -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
        match &self.expected_update_cluster_conf_response {
            Some(Ok(response)) => Ok(tonic::Response::new(*response)),
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
        _request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_read_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }

    async fn get_cluster_metadata(
        &self,
        _request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        match &self.expected_metadata_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock append entries response set")),
        }
    }
}
