use d_engine_core::StorageError;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::raft_client_service_server::RaftClientServiceServer;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementServiceServer;
use d_engine_proto::server::cluster::{
    ClusterConfUpdateResponse, ClusterMembership, JoinResponse, LeaderDiscoveryResponse,
};
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionServiceServer;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationServiceServer;
use d_engine_proto::server::storage::snapshot_service_server::SnapshotServiceServer;
use d_engine_proto::server::storage::{PurgeLogResponse, SnapshotChunk, SnapshotResponse};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic_health::server::health_reporter;
use tracing::{debug, info};

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
}
