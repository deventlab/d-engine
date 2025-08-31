use std::sync::Arc;
use std::vec;

use arc_swap::ArcSwap;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use crate::proto::client::ClientResponse;
use crate::proto::client::ClientResult;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::NodeStatus;
use crate::proto::error::ErrorCode;
use crate::test_utils::MockNode;
use crate::ClientConfig;
use crate::ClientInner;
use crate::ConnectionPool;
use crate::KvClient;
use crate::LEADER;

#[tokio::test]
#[traced_test]
async fn test_put_success() {
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
#[traced_test]
async fn test_put_failure() {
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
#[traced_test]
async fn test_get_success() {
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
#[traced_test]
async fn test_get_not_found() {
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

#[tokio::test]
#[traced_test]
async fn test_delete_success() {
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

    let result = client.delete(key).await;
    assert!(result.is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_delete_failure() {
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

    let key = "test_key".to_string().into_bytes();

    let result = client.delete(key).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        ErrorCode::ConnectionTimeout as u32
    );
}
#[tokio::test]
#[traced_test]
async fn test_get_multi_success_linear() {
    // Set up mock data for multiple keys
    let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    let values = [
        "value1".to_string(),
        "value2".to_string(),
        "value3".to_string(),
    ];

    // Create a single response containing all key-value pairs
    let mut client_results = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        client_results.push(ClientResult {
            key: key.to_string().into_bytes(),
            value: values[i].clone().into_bytes(),
        });
    }
    let response = ClientResponse::read_results(client_results);

    // Set up mock server to handle the request and return the response
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        response,
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

    // Perform multi-get with linear consistency
    let result = client.get_multi(keys.clone().into_iter().map(|k| k.to_string()), true).await;

    assert!(result.is_ok());
    let results = result.unwrap();

    // Verify results match expectations
    assert_eq!(results.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            results[i].as_ref().map(|r| &r.key),
            Some(&key.to_string().into_bytes())
        );
        assert_eq!(
            results[i].as_ref().map(|r| &r.value),
            Some(&values[i].clone().into_bytes())
        );
    }
}
#[tokio::test]
#[traced_test]
async fn test_get_multi_success_non_linear() {
    // Set up mock data for multiple keys with some missing
    let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    let values = [
        Some("value1".to_string()),
        Some("value2".to_string()),
        None, // key3 not found
    ];

    // Create a single response containing all key-value pairs
    let mut client_results = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        client_results.push(ClientResult {
            key: key.to_string().into_bytes(),
            value: match &values[i] {
                Some(value) => value.clone().into_bytes(),
                None => vec![], // empty value for not found
            },
        });
    }
    let response = ClientResponse::read_results(client_results);

    // Set up mock server to handle multiple requests
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        response,
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

    // Perform multi-get with non-linear consistency
    let result = client.get_multi(keys.clone().into_iter().map(|k| k.to_string()), false).await;

    assert!(result.is_ok());
    let results = result.unwrap();

    // Verify results match expectations
    assert_eq!(results.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            results[i].as_ref().map(|r| &r.key),
            Some(&key.to_string().into_bytes())
        );
        assert_eq!(
            results[i].as_ref().map(|r| r.value.as_slice()),
            values[i].as_ref().map(|v| v.as_bytes()).or(Some(&[]))
        );
    }
}

#[tokio::test]
#[traced_test]
async fn test_get_multi_failure() {
    let keys = vec!["key1".to_string()];

    // Set up mock server to return an error
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
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

    // Test both linear and non-linear consistency levels
    for linear in [true, false] {
        let result =
            client.get_multi(keys.clone().into_iter().map(|k| k.to_string()), linear).await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            ErrorCode::ConnectionTimeout as u32
        );
    }
}

#[tokio::test]
#[traced_test]
async fn test_get_multi_empty_keys() {
    let response = ClientResponse::read_results(vec![]);
    // Set up mock server to handle the request and return the response
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        response,
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");
    // Test with empty keys vector
    let result = KvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config: ClientConfig::default(),
        endpoints: vec![],
    })))
    .get_multi(std::iter::empty::<String>(), false)
    .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidRequest as u32);
}
