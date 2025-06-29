use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic_health::server::health_reporter;
use tracing::info;

use super::MockRpcService;
use crate::proto::client::raft_client_service_server::RaftClientServiceServer;
use crate::proto::cluster::cluster_management_service_server::ClusterManagementServiceServer;
use crate::proto::cluster::ClusterMembership;
use crate::proto::election::raft_election_service_server::RaftElectionServiceServer;
use crate::proto::election::VoteResponse;
use crate::proto::replication::raft_replication_service_server::RaftReplicationServiceServer;
use crate::proto::storage::snapshot_service_server::SnapshotServiceServer;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotResponse;
use crate::ChannelWithAddress;
use crate::Result;

pub(crate) const MOCK_RAFT_PORT_BASE: u64 = 60100;
pub(crate) const MOCK_HEALTHCHECK_PORT_BASE: u64 = 60200;
pub(crate) const MOCK_STATE_MACHINE_HANDLER_PORT_BASE: u64 = 60300;
pub(crate) const MOCK_CLIENT_PORT_BASE: u64 = 60400;
pub(crate) const MOCK_RPC_CLIENT_PORT_BASE: u64 = 60500;
pub(crate) const MOCK_REPLICATION_HANDLER_PORT_BASE: u64 = 60600;
pub(crate) const MOCK_PURGE_PORT_BASE: u64 = 60900;
pub(crate) const MOCK_ROLE_STATE_PORT_BASE: u64 = 61100;
pub(crate) const MOCK_LEADER_STATE_PORT_BASE: u64 = 61200;
pub(crate) const MOCK_SNAPSHOT_PORT_BASE: u64 = 61300;

pub struct MockNode;

impl MockNode {
    pub async fn mock_listener(
        mock_service: MockRpcService,
        port: u64,
        rx: oneshot::Receiver<()>,
        is_ready: bool,
    ) -> io::Result<SocketAddr> {
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

        let listener = TcpListener::bind(&format!("127.0.0.1:{port}")).await.unwrap();
        let addr = listener.local_addr();
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
        addr
    }

    pub(crate) async fn mock_channel_with_address(
        address: String,
        port: u64,
    ) -> ChannelWithAddress {
        let channel = match Channel::from_shared(format!("http://127.0.0.1:{port}")) {
            Ok(c) => match c.connect().await {
                Ok(c) => c,
                Err(e) => {
                    panic!("error: {e:?}");
                }
            },
            Err(e) => {
                panic!("error: {e:?}");
            }
        };

        ChannelWithAddress { address, channel }
    }

    pub(crate) fn tcp_addr_to_http_addr(addr: String) -> String {
        format!("http://{addr}")
    }

    pub(crate) async fn simulate_mock_service_without_reps(
        port: u64,
        rx: oneshot::Receiver<()>,
        server_is_ready: bool,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService::default();
        let addr = match Self::mock_listener(mock_service, port, rx, server_is_ready).await {
            Ok(a) => a,
            Err(e) => {
                panic!("error: {e:?}");
            }
        };
        Ok(Self::mock_channel_with_address(Self::tcp_addr_to_http_addr(addr.to_string()), port).await)
    }

    pub(crate) async fn simulate_send_votes_mock_server(
        port: u64,
        response: VoteResponse,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_vote_response: Some(Ok(response)),
            ..Default::default()
        };
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                panic!("error: {e:?}");
            }
        };
        Ok(Self::mock_channel_with_address(Self::tcp_addr_to_http_addr(addr.to_string()), port).await)
    }

    pub(crate) async fn simulate_purge_mock_server(
        port: u64,
        response: PurgeLogResponse,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_purge_log_response: Some(Ok(response)),
            ..Default::default()
        };
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                panic!("error: {e:?}");
            }
        };
        Ok(Self::mock_channel_with_address(Self::tcp_addr_to_http_addr(addr.to_string()), port).await)
    }

    pub(crate) async fn simulate_mock_service_with_cluster_conf_reps(
        port: u64,
        response: std::result::Result<ClusterMembership, tonic::Status>,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_metadata_response: Some(response),
            ..Default::default()
        };
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                panic!("error: {e:?}");
            }
        };
        Ok(Self::mock_channel_with_address(Self::tcp_addr_to_http_addr(addr.to_string()), port).await)
    }

    pub(crate) async fn simulate_snapshot_mock_server(
        port: u64,
        response: std::result::Result<SnapshotResponse, tonic::Status>,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService {
            expected_snapshot_response: Some(response),
            ..Default::default()
        };
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                panic!("error: {e:?}");
            }
        };
        Ok(Self::mock_channel_with_address(Self::tcp_addr_to_http_addr(addr.to_string()), port).await)
    }
}
