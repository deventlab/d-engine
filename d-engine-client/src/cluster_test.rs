use std::sync::Arc;
use std::vec;

use arc_swap::ArcSwap;
use d_engine_proto::common::NodeRole;
use tokio::sync::oneshot;
use tonic::Status;
use tracing_test::traced_test;

use crate::ClientConfig;
use crate::ClientInner;
use crate::ClusterClient;
use crate::ConnectionPool;
use crate::mock_rpc_service::MockNode;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::NodeMeta;

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

#[tokio::test]
#[traced_test]
async fn test_join_cluster_success() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_join_cluster_reps(
        rx,
        Some(Box::new(move |port| {
            Ok(ClusterMembership {
                version: 1,
                nodes: vec![NodeMeta {
                    id: 1,
                    role: NodeRole::Leader as i32,
                    address: format!("127.0.0.1:{port}",),
                    status: NodeStatus::Active.into(),
                }],
                current_leader_id: Some(1),
            })
        })),
        Ok(JoinResponse {
            success: true,
            ..Default::default()
        }),
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

    let request = NodeMeta {
        id: 2,
        role: NodeRole::Follower as i32,
        address: "127.0.0.1:50052".to_string(),
        status: NodeStatus::Active.into(),
    };

    let response = client.join_cluster(request).await.expect("Should join cluster");
    assert!(response.success);
}

#[tokio::test]
#[traced_test]
async fn test_join_cluster_failure() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_join_cluster_reps(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        Err(Status::failed_precondition("Can not join".to_string())),
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

    let request = NodeMeta {
        id: 2,
        role: NodeRole::Leader as i32,
        address: "127.0.0.1:50052".to_string(),
        status: NodeStatus::Active.into(),
    };

    // Simulate leader rejection
    let mut leader_node = NodeMeta {
        id: 1,
        role: NodeRole::Leader as i32,
        address: "127.0.0.1:50051".to_string(),
        status: NodeStatus::Active.into(),
    };
    leader_node.role = NodeRole::Leader as i32;

    let result = client.join_cluster(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::StaleOperation);
}
