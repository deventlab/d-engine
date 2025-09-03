use std::sync::Arc;
use std::vec;

use arc_swap::ArcSwap;
use tokio::sync::oneshot;
use tonic::Status;
use tracing_test::traced_test;

use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::JoinResponse;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::NodeStatus;
use crate::proto::error::ErrorCode;
use crate::test_utils::MockNode;
use crate::ClientConfig;
use crate::ClientInner;
use crate::ClusterClient;
use crate::ConnectionPool;
use crate::FOLLOWER;
use crate::LEADER;

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
    assert_eq!(members[0].role, LEADER);
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
                    role: LEADER,
                    address: format!("127.0.0.1:{port}",),
                    status: NodeStatus::Active.into(),
                }],
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
        role: FOLLOWER,
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
        role: LEADER,
        address: "127.0.0.1:50052".to_string(),
        status: NodeStatus::Active.into(),
    };

    // Simulate leader rejection
    let mut leader_node = NodeMeta {
        id: 1,
        role: LEADER,
        address: "127.0.0.1:50051".to_string(),
        status: NodeStatus::Active.into(),
    };
    leader_node.role = LEADER;

    let result = client.join_cluster(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::StaleOperation as u32);
}
