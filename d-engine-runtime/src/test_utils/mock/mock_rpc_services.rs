use d_engine_core::Result;
use d_engine_core::StorageError;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::raft_client_service_server::RaftClientService;
use d_engine_proto::client::raft_client_service_server::RaftClientServiceServer;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementService;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementServiceServer;
use d_engine_proto::server::cluster::{
    ClusterConfUpdateResponse, ClusterMembership, JoinResponse, LeaderDiscoveryResponse,
};
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionService;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionServiceServer;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationService;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationServiceServer;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::snapshot_service_server::SnapshotService;
use d_engine_proto::server::storage::snapshot_service_server::SnapshotServiceServer;
use d_engine_proto::server::storage::{PurgeLogResponse, SnapshotChunk, SnapshotResponse};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::Streaming;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic_health::server::health_reporter;
use tracing::trace;
use tracing::{debug, info};

use d_engine_core::crate_test_snapshot_stream;

#[derive(Clone, Default)]
pub struct MockRpcService {
    pub server_port: Option<u16>,
    // Expected responses for each method
    pub expected_vote_response: Option<std::result::Result<VoteResponse, tonic::Status>>,
    pub expected_append_entries_response:
        Option<std::result::Result<AppendEntriesResponse, tonic::Status>>,
    pub expected_update_cluster_conf_response:
        Option<std::result::Result<ClusterConfUpdateResponse, tonic::Status>>,
    pub expected_client_propose_response:
        Option<std::result::Result<ClientResponse, tonic::Status>>,
    pub expected_client_read_response: Option<std::result::Result<ClientResponse, tonic::Status>>,

    #[allow(clippy::type_complexity)]
    pub expected_metadata_response: Option<
        Arc<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
    >,

    pub expected_snapshot_response: Option<std::result::Result<SnapshotResponse, tonic::Status>>,
    pub expected_stream_snapshot_response:
        Option<std::result::Result<SnapshotChunk, tonic::Status>>,
    pub expected_purge_log_response: Option<std::result::Result<PurgeLogResponse, tonic::Status>>,
    pub expected_join_cluster_response: Option<std::result::Result<JoinResponse, tonic::Status>>,
    pub expected_discover_leader_response:
        Option<std::result::Result<LeaderDiscoveryResponse, tonic::Status>>,
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
                ));
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

impl MockRpcService {
    pub fn with_metadata_response(
        mut self,
        f: impl Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync + 'static,
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

pub struct MockNode;

impl MockNode {
    pub async fn mock_listener(
        mut mock_service: MockRpcService,
        rx: oneshot::Receiver<()>,
        is_ready: bool,
    ) -> Result<(u16, SocketAddr)> {
        // Return port + address
        let (mut health_reporter, health_service) = health_reporter();
        if is_ready {
            health_reporter.set_serving::<RaftClientServiceServer<MockNode>>().await;
            health_reporter.set_serving::<RaftElectionServiceServer<MockNode>>().await;
            health_reporter.set_serving::<RaftReplicationServiceServer<MockNode>>().await;
            health_reporter.set_serving::<ClusterManagementServiceServer<MockNode>>().await;
            health_reporter.set_serving::<SnapshotServiceServer<MockNode>>().await;
            info!("set service is serving");
        } else {
            health_reporter.set_not_serving::<RaftClientServiceServer<MockNode>>().await;
            health_reporter.set_not_serving::<RaftElectionServiceServer<MockNode>>().await;
            health_reporter
                .set_not_serving::<RaftReplicationServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_not_serving::<ClusterManagementServiceServer<MockNode>>()
                .await;
            health_reporter.set_not_serving::<SnapshotServiceServer<MockNode>>().await;
            info!("set service is not serving");
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().map_err(StorageError::IoError)?;
        let port = addr.port();
        debug!("starting mock rpc service:port={port}",);

        // Set the port in the service
        mock_service.set_port(port);

        let mock_service = Arc::new(mock_service);

        let _r = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(health_service)
                .add_service(
                    RaftClientServiceServer::from_arc(mock_service.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    RaftElectionServiceServer::from_arc(mock_service.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    RaftReplicationServiceServer::from_arc(mock_service.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    ClusterManagementServiceServer::from_arc(mock_service.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    SnapshotServiceServer::from_arc(mock_service)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                // add as a dev-dependency the crate `tokio-stream` with feature `net` enabled
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        rx.await.ok();
                    },
                )
                // .serve_with_shutdown("127.0.0.1:50051".parse().unwrap(), )
                .await
                .unwrap();
        });

        Ok((port, addr)) // Return both port and address
    }

    // Update helper functions to handle dynamic ports
    pub(crate) async fn mock_channel_with_port(port: u16) -> Channel {
        Channel::from_shared(format!("http://127.0.0.1:{port}"))
            .expect("valid address")
            .connect()
            .await
            .expect("connection failed")
    }

    pub(crate) fn tcp_addr_to_http_addr(addr: String) -> String {
        format!("http://{addr}")
    }

    pub(crate) async fn simulate_send_votes_mock_server(
        response: VoteResponse,
        rx: oneshot::Receiver<()>,
    ) -> Result<(Channel, u16)> {
        // Return channel + port
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_vote_response: Some(Ok(response)),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    pub(crate) async fn simulate_purge_mock_server(
        response: PurgeLogResponse,
        rx: oneshot::Receiver<()>,
    ) -> Result<(Channel, u16)> {
        // Return channel + port
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_purge_log_response: Some(Ok(response)),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    // Return channel + port
    pub(crate) async fn simulate_snapshot_mock_server(
        response: std::result::Result<SnapshotResponse, tonic::Status>,
        rx: oneshot::Receiver<()>,
    ) -> Result<(Channel, u16)> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_snapshot_response: Some(response),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn simulate_mock_service_with_cluster_conf_reps(
        rx: oneshot::Receiver<()>,
        response_builder: Option<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
    ) -> Result<(Channel, u16)> {
        let builder = response_builder.unwrap_or_else(|| {
            Box::new(|port: u16| {
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: Leader.into(),
                        address: format!("127.0.0.1:{port}",),
                        status: NodeStatus::Active.into(),
                    }],
                })
            })
        });

        let mock_service = MockRpcService::default().with_metadata_response(builder);

        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    pub(crate) async fn simulate_append_entries_mock_server(
        response: std::result::Result<AppendEntriesResponse, Status>,
        rx: oneshot::Receiver<()>,
    ) -> Result<(Channel, u16)> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_append_entries_response: Some(response),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn simulate_mock_service_with_join_cluster_reps(
        rx: oneshot::Receiver<()>,
        metadata_response_builder: Option<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        join_response: std::result::Result<JoinResponse, Status>,
    ) -> Result<(Channel, u16)> {
        let builder = metadata_response_builder.unwrap_or_else(|| {
            Box::new(|port: u16| {
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: Leader.into(),
                        address: format!("127.0.0.1:{port}",),
                        status: NodeStatus::Active.into(),
                    }],
                })
            })
        });
        let mock_service = MockRpcService {
            expected_metadata_response: Some(Arc::new(builder)),
            expected_join_cluster_response: Some(join_response),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn simulate_client_read_mock_server(
        rx: oneshot::Receiver<()>,
        metadata_response_builder: Option<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        response: ClientResponse,
    ) -> Result<(Channel, u16)> {
        let builder = metadata_response_builder.unwrap_or_else(|| {
            Box::new(|port: u16| {
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: Leader.into(),
                        address: format!("127.0.0.1:{port}",),
                        status: NodeStatus::Active.into(),
                    }],
                })
            })
        });
        let mock_service = MockRpcService {
            expected_metadata_response: Some(Arc::new(builder)),
            expected_client_read_response: Some(Ok(response)),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn simulate_client_write_mock_server(
        rx: oneshot::Receiver<()>,
        metadata_response_builder: Option<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        response: ClientResponse,
    ) -> Result<(Channel, u16)> {
        let builder = metadata_response_builder.unwrap_or_else(|| {
            Box::new(|port: u16| {
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: Leader.into(),
                        address: format!("127.0.0.1:{port}",),
                        status: NodeStatus::Active.into(),
                    }],
                })
            })
        });
        let mock_service = MockRpcService {
            expected_metadata_response: Some(Arc::new(builder)),
            expected_client_propose_response: Some(Ok(response)),
            ..Default::default()
        };
        let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
        let channel = Self::mock_channel_with_port(port).await;
        Ok((channel, port))
    }
}
