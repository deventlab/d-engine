use crate::{
    grpc::rpc_service::{
        rpc_service_server::RpcServiceServer, AppendEntriesResponse,  VoteResponse,
    },
};
use crate::{ChannelWithAddress, Error, Node,  Result};
use log::info;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic_health::server::health_reporter;


use super::{MockRpcService, MockTypeConfig};

pub(crate) const MOCK_RAFT_PORT_BASE: u64 = 60100;
pub(crate) const MOCK_HEALTHCHECK_PORT_BASE: u64 = 60200;
pub(crate) const MOCK_SERVER_PORT_BASE: u64 = 60300;
pub(crate) const MOCK_CLIENT_PORT_BASE: u64 = 60400;
pub(crate) const MOCK_RPC_CLIENT_PORT_BASE: u64 = 60500;
pub(crate) const MOCK_REPLICATION_HANDLER_PORT_BASE: u64 = 60600;
pub(crate) const MOCK_CLUSTER_MEMBERSHIP_CONTROLLER_PORT_BASE: u64 = 60700;
pub(crate) const MOCK_ELECTION_CONTROLLER_PORT_BASE: u64 = 60800;
pub(crate) const MOCK_EVENT_LISTENER_PORT_BASE: u64 = 60900;
pub(crate) const MOCK_PEER_CHANNEL_PORT_BASE: u64 = 62000;


pub struct MockNode {
    pub(crate) node: Arc<Node<MockTypeConfig>>,
}

impl MockNode {
    // pub async fn new(
    //     id: u32,
    //     db_root_dir: String,
    //     peers_meta: Vec<NodeMeta>,
    //     address: String,
    //     mut settings: Settings,
    //     shutdown_signal: watch::Receiver<()>,
    // ) -> Self {
    //     settings.server_settings.id = id;
    //     settings.server_settings.db_root_dir = db_root_dir;
    //     settings.server_settings.initial_cluster = peers_meta;

    //     let node = NodeBuilder::new(settings.clone(), shutdown_signal)
    //         .build()
    //         .ready()
    //         .expect("should succeed");

    //     MockNode { node }
    // }

    pub async fn mock_listener(
        mock_service: MockRpcService,
        port: u64,
        rx: oneshot::Receiver<()>,
        is_ready: bool,
    ) -> io::Result<SocketAddr> {
        let (mut health_reporter, health_service) = health_reporter();
        if is_ready {
            health_reporter
                .set_serving::<RpcServiceServer<MockNode>>()
                .await;
            info!("set service is serving");
        } else {
            health_reporter
                .set_not_serving::<RpcServiceServer<MockNode>>()
                .await;
            info!("set service is not serving");
        }

        let listener = TcpListener::bind(&format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        let addr = listener.local_addr();
        let _r = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(health_service)
                .add_service(
                    RpcServiceServer::new(mock_service)
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
        addr
    }

    pub(crate) async fn mock_channel_with_address(
        address: String,
        port: u64,
    ) -> ChannelWithAddress {
        let channel = match Channel::from_shared(format!("http://127.0.0.1:{}", port)) {
            Ok(c) => match c.connect().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: {:?}", e);
                    assert!(false);
                    panic!("failed");
                }
            },
            Err(e) => {
                eprintln!("error: {:?}", e);
                assert!(false);
                panic!("failed");
            }
        };

        ChannelWithAddress { address, channel }
    }

    pub(crate) fn tcp_addr_to_http_addr(addr: String) -> String {
        format!("http://{}", addr)
    }

    pub(crate) async fn simulate_mock_service_without_reps(
        port: u64,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mock_service = MockRpcService::default();
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                assert!(false);
                return Err(Error::GeneralServerError(format!(
                    "test_utils::MockNode::mock_listener failed: {:?}",
                    e
                )));
            }
        };
        Ok(Self::mock_channel_with_address(addr.to_string(), port).await)
    }

    pub(crate) async fn simulate_mock_service_with_append_reps(
        port: u64,
        response: AppendEntriesResponse,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mut mock_service = MockRpcService::default();
        mock_service.expected_append_entries_response = Some(Ok(response));
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                assert!(false);
                return Err(Error::GeneralServerError(format!(
                    "test_utils::MockNode::mock_listener failed: {:?}",
                    e
                )));
            }
        };
        Ok(Self::mock_channel_with_address(addr.to_string(), port).await)
    }

    pub(crate) async fn simulate_send_votes_mock_server(
        port: u64,
        response: VoteResponse,
        rx: oneshot::Receiver<()>,
    ) -> Result<ChannelWithAddress> {
        //prepare learner's channel address inside membership config
        let mut mock_service = MockRpcService::default();
        mock_service.expected_vote_response = Some(Ok(response));
        let addr = match Self::mock_listener(mock_service, port, rx, true).await {
            Ok(a) => a,
            Err(e) => {
                assert!(false);
                return Err(Error::GeneralServerError(format!(
                    "Self::mock_listener failed: {:?}",
                    e
                )));
            }
        };
        Ok(Self::mock_channel_with_address(addr.to_string(), port).await)
    }
}
