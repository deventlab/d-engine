use std::sync::Arc;
use std::vec;

use arc_swap::ArcSwap;
use bytes::Bytes;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientResult;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterMembership;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use crate::Client;
use crate::ClientConfig;
use crate::ClientInner;
use crate::ClusterClient;
use crate::ConnectionPool;
use crate::GrpcKvClient;
use crate::mock_rpc_service::MockNode;

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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "test_key".to_string().into_bytes();
    let value = "test_value".to_string().into_bytes();

    let result = client.put(key, value).await;
    println!("Result: {result:?}",);
    assert!(result.is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_put_with_ttl_success() {
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "ttl_key".to_string().into_bytes();
    let value = "ttl_value".to_string().into_bytes();
    let ttl_secs = 3600; // 1 hour

    let result = client.put_with_ttl(key, value, ttl_secs).await;
    println!("Result: {result:?}");
    assert!(result.is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_put_with_ttl_failure() {
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "ttl_key".to_string().into_bytes();
    let value = "ttl_value".to_string().into_bytes();
    let ttl_secs = 3600;

    let result = client.put_with_ttl(key, value, ttl_secs).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ConnectionTimeout);
}

#[tokio::test]
#[traced_test]
async fn test_put_with_zero_ttl() {
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "no_ttl_key".to_string().into_bytes();
    let value = "no_ttl_value".to_string().into_bytes();
    let ttl_secs = 0; // Zero TTL should still succeed but key won't expire

    let result = client.put_with_ttl(key, value, ttl_secs).await;
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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
    assert_eq!(result.unwrap_err().code(), ErrorCode::ConnectionTimeout);
}

#[tokio::test]
#[traced_test]
async fn test_get_success() {
    let key = Bytes::from("test_key".to_string());
    let value = Bytes::from("test_value".to_string());
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    // Set up mock response

    let result = client.get_with_policy(key, Some(ReadConsistencyPolicy::LinearizableRead)).await;
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "nonexistent_key".to_string().into_bytes();
    let result = client.get_with_policy(key, Some(ReadConsistencyPolicy::LinearizableRead)).await;
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "test_key".to_string().into_bytes();

    let result = client.delete(key).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ConnectionTimeout);
}
#[tokio::test]
#[traced_test]
async fn test_get_multi_success_linear() {
    // Set up mock data for multiple keys
    let keys = vec![
        Bytes::from("key1".to_string()),
        Bytes::from("key2".to_string()),
        Bytes::from("key3".to_string()),
    ];
    let values = [
        Bytes::from("value1".to_string()),
        Bytes::from("value2".to_string()),
        Bytes::from("value3".to_string()),
    ];

    // Create a single response containing all key-value pairs
    let mut client_results = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        client_results.push(ClientResult {
            key: key.clone(),
            value: values[i].clone(),
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    // Perform multi-get with linear consistency
    let result = client
        .get_multi_with_policy(
            keys.clone().into_iter(),
            Some(ReadConsistencyPolicy::LinearizableRead),
        )
        .await;

    assert!(result.is_ok());
    let results = result.unwrap();

    // Verify results match expectations
    assert_eq!(results.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(results[i].as_ref().map(|r| &r.key), Some(&key.clone()));
        assert_eq!(results[i].as_ref().map(|r| &r.value), Some(&values[i]));
    }
}
#[tokio::test]
#[traced_test]
async fn test_get_multi_success_non_linear() {
    // Set up mock data for multiple keys with some missing
    let keys = vec![
        Bytes::from("key1"),
        Bytes::from("key2"),
        Bytes::from("key3"),
    ];
    let values = [
        Some(Bytes::from_owner("value1".to_string())),
        Some(Bytes::from_owner("value2".to_string())),
        None, // key3 not found
    ];

    // Create a single response containing all key-value pairs
    let mut client_results = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        client_results.push(ClientResult {
            key: key.clone(),
            value: match &values[i] {
                Some(value) => value.clone(),
                None => Bytes::copy_from_slice(&[]), // empty value for not found
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    // Perform multi-get with non-linear consistency
    let result = client.get_multi_with_policy(keys.clone().into_iter(), None).await;

    assert!(result.is_ok());
    let results = result.unwrap();

    // Verify results match expectations
    assert_eq!(results.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(results[i].as_ref().map(|r| &r.key), Some(&key.clone()));
        assert_eq!(
            results[i].as_ref().map(|r| r.value.clone()),
            values[i].clone().or(Some(Bytes::new()))
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    // Test both linear and non-linear consistency levels
    for linear in [Some(ReadConsistencyPolicy::LinearizableRead), None] {
        let result = client
            .get_multi_with_policy(keys.clone().into_iter().map(|k| k.to_string()), linear)
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::ConnectionTimeout);
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
    let result = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config: ClientConfig::default(),
        endpoints: vec![],
    })))
    .get_multi_with_policy(std::iter::empty::<String>(), None)
    .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
#[traced_test]
async fn test_get_linearizable_success() {
    let key = Bytes::from("test_key".to_string());
    let value = Bytes::from("test_value".to_string());
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_linearizable(key).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().as_ref().map(|r| &r.value), Some(&value));
}

#[tokio::test]
#[traced_test]
async fn test_get_lease_success() {
    let key = Bytes::from("test_key".to_string());
    let value = Bytes::from("test_value".to_string());
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_lease(key).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().as_ref().map(|r| &r.value), Some(&value));
}

#[tokio::test]
#[traced_test]
async fn test_get_eventual_success() {
    let key = Bytes::from("test_key".to_string());
    let value = Bytes::from("test_value".to_string());
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_eventual(key).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().as_ref().map(|r| &r.value), Some(&value));
}

#[tokio::test]
#[traced_test]
async fn test_get_multi_success() {
    let keys = vec![
        Bytes::from("key1".to_string()),
        Bytes::from("key2".to_string()),
        Bytes::from("key3".to_string()),
    ];
    let values = [
        Bytes::from("value1".to_string()),
        Bytes::from("value2".to_string()),
        Bytes::from("value3".to_string()),
    ];

    let mut client_results = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        client_results.push(ClientResult {
            key: key.clone(),
            value: values[i].clone(),
        });
    }
    let response = ClientResponse::read_results(client_results);

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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_multi(keys.clone()).await;
    assert!(result.is_ok());
    let results = result.unwrap();

    assert_eq!(results.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(results[i].as_ref().map(|r| &r.key), Some(&key.clone()));
        assert_eq!(results[i].as_ref().map(|r| &r.value), Some(&values[i]));
    }
}

#[tokio::test]
#[traced_test]
async fn test_get_multi_with_mixed_results() {
    let keys = vec![
        Bytes::from("key1".to_string()),
        Bytes::from("key2".to_string()),
        Bytes::from("key3".to_string()),
    ];

    // Only key1 and key3 exist, key2 is missing
    let client_results = vec![
        ClientResult {
            key: keys[0].clone(),
            value: Bytes::from("value1".to_string()),
        },
        ClientResult {
            key: keys[2].clone(),
            value: Bytes::from("value3".to_string()),
        },
    ];
    let response = ClientResponse::read_results(client_results);

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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_multi(keys.clone()).await;
    assert!(result.is_ok());
    let results = result.unwrap();

    assert_eq!(results.len(), 2);
    let found_keys: Vec<&Bytes> =
        results.iter().filter_map(|r| r.as_ref().map(|r| &r.key)).collect();

    assert!(found_keys.contains(&&keys[0]));
    assert!(found_keys.contains(&&keys[2]));
}

#[tokio::test]
#[traced_test]
async fn test_get_consistency_methods_failure() {
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

    let client = GrpcKvClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "test_key".to_string().into_bytes();

    // Test all convenience methods with network failure
    let methods = [
        (
            "get_linearizable",
            client.get_linearizable(key.clone()).await,
        ),
        ("get_lease", client.get_lease(key.clone()).await),
        ("get_eventual", client.get_eventual(key.clone()).await),
        ("get", client.get(key.clone()).await),
    ];

    for (method_name, result) in methods {
        assert!(result.is_err(), "{method_name} should fail",);
        assert_eq!(
            result.unwrap_err().code(),
            ErrorCode::ConnectionTimeout,
            "{method_name} should return ConnectionTimeout",
        );
    }
}

#[tokio::test]
#[traced_test]
async fn test_client_refresh_with_new_endpoints() {
    // Create initial client with first mock server
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_channel1, port1) = MockNode::simulate_client_read_mock_server(
        rx1,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }]),
    )
    .await
    .unwrap();

    let initial_endpoints = vec![format!("http://localhost:{}", port1)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(initial_endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client_inner = Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config: config.clone(),
        endpoints: initial_endpoints.clone(),
    }));

    let mut client = Client {
        kv: GrpcKvClient::new(client_inner.clone()),
        cluster: ClusterClient::new(client_inner.clone()),
        inner: client_inner,
    };

    // Verify initial state
    let initial_inner = client.inner.load();
    assert_eq!(initial_inner.endpoints, initial_endpoints);

    // Create second mock server for refresh
    let (_tx2, rx2) = oneshot::channel::<()>();
    let (_channel2, port2) = MockNode::simulate_client_read_mock_server(
        rx2,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key2"),
            value: Bytes::from("value2"),
        }]),
    )
    .await
    .unwrap();

    let new_endpoints = vec![format!("http://localhost:{}", port2)];

    // Refresh client with new endpoints
    let result = client.refresh(Some(new_endpoints.clone())).await;
    assert!(result.is_ok(), "Refresh should succeed");

    // Verify client was updated with new endpoints
    let refreshed_inner = client.inner.load();
    assert_eq!(refreshed_inner.endpoints, new_endpoints);
    assert_eq!(refreshed_inner.client_id, 123); // Client ID should be preserved
    assert!(client.kv().get("test_key").await.is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_client_refresh_with_none_endpoints() {
    // Create initial client with mock server
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }]),
    )
    .await
    .unwrap();

    let initial_endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(initial_endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client_inner = Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 456,
        config: config.clone(),
        endpoints: initial_endpoints.clone(),
    }));

    let mut client = Client {
        kv: GrpcKvClient::new(client_inner.clone()),
        cluster: ClusterClient::new(client_inner.clone()),
        inner: client_inner,
    };

    // Verify initial state
    let initial_inner = client.inner.load();
    assert_eq!(initial_inner.endpoints, initial_endpoints);

    // Refresh client with None (should reuse existing endpoints)
    let result = client.refresh(None).await;
    assert!(result.is_ok(), "Refresh with None should succeed");

    // Verify client still has original endpoints
    let refreshed_inner = client.inner.load();
    assert_eq!(refreshed_inner.endpoints, initial_endpoints);
    assert_eq!(refreshed_inner.client_id, 456); // Client ID should be preserved
    assert!(client.kv().get("test_key").await.is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_client_refresh_with_multiple_endpoints() {
    // Create multiple mock servers
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_channel1, port1) = MockNode::simulate_client_read_mock_server(
        rx1,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }]),
    )
    .await
    .unwrap();

    let (_tx2, rx2) = oneshot::channel::<()>();
    let (_channel2, port2) = MockNode::simulate_client_read_mock_server(
        rx2,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key2"),
            value: Bytes::from("value2"),
        }]),
    )
    .await
    .unwrap();

    let initial_endpoints = vec![format!("http://localhost:{}", port1)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(initial_endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client_inner = Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 789,
        config: config.clone(),
        endpoints: initial_endpoints.clone(),
    }));

    let mut client = Client {
        kv: GrpcKvClient::new(client_inner.clone()),
        cluster: ClusterClient::new(client_inner.clone()),
        inner: client_inner,
    };

    // Verify initial state
    let initial_inner = client.inner.load();
    assert_eq!(initial_inner.endpoints.len(), 1);

    // Refresh with multiple endpoints
    let multiple_endpoints = vec![
        format!("http://localhost:{}", port1),
        format!("http://localhost:{}", port2),
    ];

    let result = client.refresh(Some(multiple_endpoints.clone())).await;
    assert!(
        result.is_ok(),
        "Refresh with multiple endpoints should succeed"
    );

    // Verify client was updated with multiple endpoints
    let refreshed_inner = client.inner.load();
    assert_eq!(refreshed_inner.endpoints, multiple_endpoints);
    assert_eq!(refreshed_inner.endpoints.len(), 2);
    assert_eq!(refreshed_inner.client_id, 789); // Client ID should be preserved
}

#[tokio::test]
#[traced_test]
async fn test_client_refresh_failure_invalid_endpoints() {
    // Create initial client with valid mock server
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }]),
    )
    .await
    .unwrap();

    let initial_endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(initial_endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client_inner = Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 999,
        config: config.clone(),
        endpoints: initial_endpoints.clone(),
    }));

    let mut client = Client {
        kv: GrpcKvClient::new(client_inner.clone()),
        cluster: ClusterClient::new(client_inner.clone()),
        inner: client_inner,
    };

    // Verify initial state
    let initial_inner = client.inner.load();
    assert_eq!(initial_inner.endpoints, initial_endpoints);

    // Try to refresh with invalid endpoints (should fail)
    let invalid_endpoints = vec!["http://invalid-host:9999".to_string()];
    let result = client.refresh(Some(invalid_endpoints)).await;

    // Should fail due to connection timeout or similar error
    assert!(
        result.is_err(),
        "Refresh with invalid endpoints should fail"
    );

    // Verify client state remains unchanged after failed refresh
    let unchanged_inner = client.inner.load();
    assert_eq!(unchanged_inner.endpoints, initial_endpoints);
    assert_eq!(unchanged_inner.client_id, 999);
}

#[tokio::test]
#[traced_test]
async fn test_client_refresh_preserves_kv_and_cluster_clients() {
    // Create initial client
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_client_read_mock_server(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }]),
    )
    .await
    .unwrap();

    let initial_endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(initial_endpoints.clone(), config.clone())
        .await
        .expect("Should create connection pool");

    let client_inner = Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 111,
        config: config.clone(),
        endpoints: initial_endpoints.clone(),
    }));

    let mut client = Client {
        kv: GrpcKvClient::new(client_inner.clone()),
        cluster: ClusterClient::new(client_inner.clone()),
        inner: client_inner,
    };

    // Create new mock server for refresh
    let (_tx2, rx2) = oneshot::channel::<()>();
    let (_channel2, port2) = MockNode::simulate_client_read_mock_server(
        rx2,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
        ClientResponse::read_results(vec![ClientResult {
            key: Bytes::from("key2"),
            value: Bytes::from("value2"),
        }]),
    )
    .await
    .unwrap();

    let new_endpoints = vec![format!("http://localhost:{}", port2)];

    // Refresh client
    let result = client.refresh(Some(new_endpoints)).await;
    assert!(result.is_ok(), "Refresh should succeed");

    // Verify kv and cluster clients are still accessible and functional
    assert!(client.kv().get("test_key").await.is_ok());
    assert!(client.cluster().list_members().await.is_ok());

    let kv_result = client.kv().get("test_key").await;
    assert!(kv_result.is_ok() || kv_result.is_err()); // Either result is fine as long as it doesn't panic

    let cluster_result = client.cluster().list_members().await;
    assert!(cluster_result.is_ok() || cluster_result.is_err()); // Either result is fine as long as
    // it doesn't panic
}
