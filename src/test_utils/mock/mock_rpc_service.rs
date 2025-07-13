use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Status;
use tonic_health::server::health_reporter;
use tracing::debug;
use tracing::info;

use super::MockRpcService;
use crate::proto::client::raft_client_service_server::RaftClientServiceServer;
use crate::proto::cluster::cluster_management_service_server::ClusterManagementServiceServer;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::NodeStatus;
use crate::proto::election::raft_election_service_server::RaftElectionServiceServer;
use crate::proto::election::VoteResponse;
use crate::proto::replication::raft_replication_service_server::RaftReplicationServiceServer;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::snapshot_service_server::SnapshotServiceServer;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotResponse;
use crate::Result;
use crate::StorageError;
use crate::LEADER;

// ports for unit tests
pub(crate) const MOCK_STATE_MACHINE_HANDLER_PORT_BASE: u32 = 60400;

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
            health_reporter
                .set_serving::<RaftElectionServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_serving::<RaftReplicationServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_serving::<ClusterManagementServiceServer<MockNode>>()
                .await;
            health_reporter.set_serving::<SnapshotServiceServer<MockNode>>().await;
            info!("set service is serving");
        } else {
            health_reporter
                .set_not_serving::<RaftClientServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_not_serving::<RaftElectionServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_not_serving::<RaftReplicationServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_not_serving::<ClusterManagementServiceServer<MockNode>>()
                .await;
            health_reporter
                .set_not_serving::<SnapshotServiceServer<MockNode>>()
                .await;
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
                .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                    rx.await.ok();
                })
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

    // pub(crate) async fn simulate_mock_service_with_cluster_conf_reps(
    //     response: std::result::Result<ClusterMembership, tonic::Status>,
    //     rx: oneshot::Receiver<()>,
    // ) -> Result<(Channel, u16)> {
    //     // Return channel + port
    //     //prepare learner's channel address inside membership config
    //     let mock_service = MockRpcService {
    //         expected_metadata_response: Some(response),
    //         ..Default::default()
    //     };
    //     let (port, _addr) = Self::mock_listener(mock_service, rx, true).await?;
    //     let channel = Self::mock_channel_with_port(port).await;
    //     Ok((channel, port))
    // }

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
                        role: LEADER,
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
}
