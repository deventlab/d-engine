use tonic::Streaming;

use crate::proto::client::raft_client_service_server::RaftClientService;
use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientResponse;
use crate::proto::client::ClientWriteRequest;
use crate::proto::cluster::cluster_management_service_server::ClusterManagementService;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::JoinRequest;
use crate::proto::cluster::JoinResponse;
use crate::proto::cluster::LeaderDiscoveryRequest;
use crate::proto::cluster::LeaderDiscoveryResponse;
use crate::proto::cluster::MetadataRequest;
use crate::proto::election::raft_election_service_server::RaftElectionService;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::replication::raft_replication_service_server::RaftReplicationService;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::snapshot_service_server::SnapshotService;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotResponse;

#[derive(Debug, Clone, Default)]
pub struct MockRpcService {
    // Expected responses for each method
    pub expected_vote_response: Option<Result<VoteResponse, tonic::Status>>,
    pub expected_append_entries_response: Option<Result<AppendEntriesResponse, tonic::Status>>,
    pub expected_update_cluster_conf_response: Option<Result<ClusterConfUpdateResponse, tonic::Status>>,
    pub expected_client_propose_response: Option<Result<ClientResponse, tonic::Status>>,
    pub expected_client_read_response: Option<Result<ClientResponse, tonic::Status>>,
    pub expected_metadata_response: Option<Result<ClusterMembership, tonic::Status>>,
    pub expected_snapshot_response: Option<Result<SnapshotResponse, tonic::Status>>,
    pub expected_purge_log_response: Option<Result<PurgeLogResponse, tonic::Status>>,
    pub expected_join_cluster_response: Option<Result<JoinResponse, tonic::Status>>,
    pub expected_discover_leader_response: Option<Result<LeaderDiscoveryResponse, tonic::Status>>,
}
#[tonic::async_trait]
impl RaftElectionService for MockRpcService {
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
}

#[tonic::async_trait]
impl RaftReplicationService for MockRpcService {
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
            None => Err(tonic::Status::unknown("No mock update_cluster_conf response set")),
        }
    }

    async fn get_cluster_metadata(
        &self,
        _request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        match &self.expected_metadata_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock get_cluster_metadata response set")),
        }
    }

    async fn join_cluster(
        &self,
        _request: tonic::Request<JoinRequest>,
    ) -> std::result::Result<tonic::Response<JoinResponse>, tonic::Status> {
        match &self.expected_join_cluster_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock get_cluster_metadata response set")),
        }
    }

    async fn discover_leader(
        &self,
        request: tonic::Request<LeaderDiscoveryRequest>,
    ) -> std::result::Result<tonic::Response<LeaderDiscoveryResponse>, tonic::Status> {
        match &self.expected_discover_leader_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock get_cluster_metadata response set")),
        }
    }
}

#[tonic::async_trait]
impl RaftClientService for MockRpcService {
    async fn handle_client_write(
        &self,
        _request: tonic::Request<ClientWriteRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_propose_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock handle_client_write response set")),
        }
    }

    async fn handle_client_read(
        &self,
        _request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        match &self.expected_client_read_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock handle_client_read response set")),
        }
    }
}

#[tonic::async_trait]
impl SnapshotService for MockRpcService {
    async fn install_snapshot(
        &self,
        _request: tonic::Request<Streaming<SnapshotChunk>>,
    ) -> std::result::Result<tonic::Response<SnapshotResponse>, tonic::Status> {
        match &self.expected_snapshot_response {
            Some(Ok(response)) => Ok(tonic::Response::new(*response)),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock install_snapshot response set")),
        }
    }

    async fn purge_log(
        &self,
        _request: tonic::Request<PurgeLogRequest>,
    ) -> std::result::Result<tonic::Response<PurgeLogResponse>, tonic::Status> {
        match &self.expected_purge_log_response {
            Some(Ok(response)) => Ok(tonic::Response::new(*response)),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock purge_log response set")),
        }
    }
}
