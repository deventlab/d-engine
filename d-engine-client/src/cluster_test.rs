use std::sync::Arc;
use std::vec;

use arc_swap::ArcSwap;
use d_engine_proto::common::NodeRole;
use d_engine_proto::server::cluster::ClusterMembership;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use crate::ClientConfig;
use crate::ClientInner;
use crate::ClusterClient;
use crate::ConnectionPool;
use crate::mock_rpc_service::MockNode;

#[tokio::test]
#[traced_test]
async fn test_list_members_success() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client = ClusterClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let members = client.list_members().await.expect("Should get members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].role, NodeRole::Leader as i32);
}
