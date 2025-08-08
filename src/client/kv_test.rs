use crate::proto::client::ClientResponse;
use crate::proto::client::ClientResult;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::NodeStatus;
use crate::proto::error::ErrorCode;
use crate::test_utils::enable_logger;
use crate::test_utils::MockNode;
use crate::ClientConfig;
use crate::ClientInner;
use crate::ConnectionPool;
use crate::KvClient;
use crate::LEADER;
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::vec;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_put_success() {
    enable_logger();
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_write_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::write_success(),
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client = KvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "test_key".to_string().into_bytes();
    let value = "test_value".to_string().into_bytes();

    let result = client.put(key, value).await;
    println!("Result: {:?}", result);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_put_failure() {
    enable_logger();
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_write_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::client_error(ErrorCode::ConnectionTimeout),
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client = KvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    // Simulate network error
    let result = client
        .put(
            "test_key".to_string().into_bytes(),
            "test_value".to_string().into_bytes(),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        ErrorCode::ConnectionTimeout as u32
    );
}

#[tokio::test]
async fn test_get_success() {
    enable_logger();
    let key = "test_key".to_string().into_bytes();
    let value = "test_value".to_string().into_bytes();
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: key.clone(),
            value: value.clone(),
        }]),
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client = KvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    // Set up mock response
    let mut leader_node = NodeMeta {
        id: 1,
        role: LEADER,
        address: "127.0.0.1:50051".to_string(),
        status: NodeStatus::Active.into(),
    };
    leader_node.role = LEADER;

    let result = client.get(key, true).await;
    println!("{:?}", &result);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().as_ref().map(|r| &r.value), Some(&value));
}

#[tokio::test]
async fn test_get_not_found() {
    enable_logger();
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![]),
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client = KvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "nonexistent_key".to_string().into_bytes();
    let result = client.get(key, true).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}
