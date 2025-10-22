use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::trace;

use crate::test_utils::crate_test_snapshot_stream;
use d_engine_proto::client::raft_client_service_server::RaftClientService;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementService;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionService;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationService;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::snapshot_service_server::SnapshotService;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::PurgeLogResponse;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotResponse;

#[derive(Clone, Default)]
pub struct MockRpcService {
    pub server_port: Option<u16>,
    // Expected responses for each method
    pub expected_vote_response: Option<Result<VoteResponse, tonic::Status>>,
    pub expected_append_entries_response: Option<Result<AppendEntriesResponse, tonic::Status>>,
    pub expected_update_cluster_conf_response:
        Option<Result<ClusterConfUpdateResponse, tonic::Status>>,
    pub expected_client_propose_response: Option<Result<ClientResponse, tonic::Status>>,
    pub expected_client_read_response: Option<Result<ClientResponse, tonic::Status>>,

    #[allow(clippy::type_complexity)]
    pub expected_metadata_response:
        Option<Arc<dyn Fn(u16) -> Result<ClusterMembership, tonic::Status> + Send + Sync>>,

    pub expected_snapshot_response: Option<Result<SnapshotResponse, tonic::Status>>,
    pub expected_stream_snapshot_response: Option<Result<SnapshotChunk, tonic::Status>>,
    pub expected_purge_log_response: Option<Result<PurgeLogResponse, tonic::Status>>,
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
            None => Err(tonic::Status::unknown(
                "No mock append entries response set",
            )),
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
            None => Err(tonic::Status::unknown(
                "No mock get_cluster_metadata response set",
            )),
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
                "No mock get_cluster_metadata response set",
            )),
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
}

#[tonic::async_trait]
impl SnapshotService for MockRpcService {
    type StreamSnapshotStream = tonic::Streaming<SnapshotChunk>;

    async fn stream_snapshot(
        &self,
        _request: tonic::Request<tonic::Streaming<SnapshotAck>>,
    ) -> std::result::Result<tonic::Response<Self::StreamSnapshotStream>, tonic::Status> {
        match &self.expected_stream_snapshot_response {
            Some(Ok(response)) => {
                let streaming: Self::StreamSnapshotStream =
                    crate_test_snapshot_stream(vec![response.clone()]);
                Ok(tonic::Response::new(streaming))
            }
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown(
                "No mock install_snapshot response set",
            )),
        }
    }
    async fn install_snapshot(
        &self,
        request: tonic::Request<Streaming<SnapshotChunk>>,
    ) -> std::result::Result<tonic::Response<SnapshotResponse>, tonic::Status> {
        let mut stream = request.into_inner();

        while let Some(_chunk) = stream.next().await {
            trace!("install_snapshot receive chunk - ");
        }
        trace!("install_snapshot no more to receive!");

        match &self.expected_snapshot_response {
            Some(Ok(response)) => return Ok(tonic::Response::new(*response)),
            Some(Err(status)) => return Err(status.clone()),
            None => {
                return Err(tonic::Status::unknown(
                    "No mock install_snapshot response set",
                ))
            }
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
