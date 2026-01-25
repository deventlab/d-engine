use std::sync::Arc;
use std::vec;

use arc_swap::ArcSwap;
use bytes::Bytes;
use d_engine_core::ClientApi;
use d_engine_core::client::ClientApiResult;
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

use crate::ConnectionPool;
use crate::GrpcClient;
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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
    let result = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_multi(&keys).await;
    assert!(result.is_ok());
    let results = result.unwrap();

    assert_eq!(results.len(), keys.len());
    for (i, _key) in keys.iter().enumerate() {
        assert_eq!(results[i].as_ref(), Some(&values[i]));
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let result = client.get_multi(&keys).await;
    assert!(result.is_ok());
    let results = result.unwrap();

    // Server returns only the keys that exist (sparse results)
    // In this case, only key1 and key3 exist (not key2)
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].as_ref(),
        Some(&Bytes::from("value1".to_string()))
    );
    assert_eq!(
        results[1].as_ref(),
        Some(&Bytes::from("value3".to_string()))
    );
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

    let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
        pool,
        client_id: 123,
        config,
        endpoints,
    })));

    let key = "test_key".to_string().into_bytes();

    // Test all convenience methods with network failure
    let methods: [(&str, ClientApiResult<Option<ClientResult>>); 3] = [
        (
            "get_linearizable",
            client.get_linearizable(key.clone()).await,
        ),
        ("get_lease", client.get_lease(key.clone()).await),
        ("get_eventual", client.get_eventual(key.clone()).await),
    ];

    let get_result: ClientApiResult<Option<Bytes>> = client.get(key.clone()).await;

    for (method_name, result) in methods {
        assert!(result.is_err(), "{method_name} should fail",);
        assert_eq!(
            result.unwrap_err().code(),
            ErrorCode::ConnectionTimeout,
            "{method_name} should return ConnectionTimeout",
        );
    }

    // Also check the get() method
    assert!(get_result.is_err(), "get should fail");
    assert_eq!(
        get_result.unwrap_err().code(),
        ErrorCode::ConnectionTimeout,
        "get should return ConnectionTimeout",
    );
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

    let grpc_client = GrpcClient::new(client_inner.clone());
    let mut client = Client {
        inner: Arc::new(grpc_client),
    };

    // Verify initial state
    let initial_inner = client.inner.client_inner.load();
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
    let refreshed_inner = client.inner.client_inner.load();
    assert_eq!(refreshed_inner.endpoints, new_endpoints);
    assert_eq!(refreshed_inner.client_id, 123); // Client ID should be preserved
    assert!(client.get("test_key").await.is_ok());
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

    let grpc_client = GrpcClient::new(client_inner.clone());
    let mut client = Client {
        inner: Arc::new(grpc_client),
    };

    // Verify initial state
    let initial_inner = client.inner.client_inner.load();
    assert_eq!(initial_inner.endpoints, initial_endpoints);

    // Refresh client with None (should reuse existing endpoints)
    let result = client.refresh(None).await;
    assert!(result.is_ok(), "Refresh with None should succeed");

    // Verify client still has original endpoints
    let refreshed_inner = client.inner.client_inner.load();
    assert_eq!(refreshed_inner.endpoints, initial_endpoints);
    assert_eq!(refreshed_inner.client_id, 456); // Client ID should be preserved
    assert!(client.get("test_key").await.is_ok());
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

    let grpc_client = GrpcClient::new(client_inner.clone());
    let mut client = Client {
        inner: Arc::new(grpc_client),
    };

    // Verify initial state
    let initial_inner = client.inner.client_inner.load();
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
    let refreshed_inner = client.inner.client_inner.load();
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

    let grpc_client = GrpcClient::new(client_inner.clone());
    let mut client = Client {
        inner: Arc::new(grpc_client),
    };

    // Verify initial state
    let initial_inner = client.inner.client_inner.load();
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
    let unchanged_inner = client.inner.client_inner.load();
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

    let grpc_client = GrpcClient::new(client_inner.clone());
    let mut client = Client {
        inner: Arc::new(grpc_client),
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

    // Verify client methods are still accessible and functional (via Deref)
    assert!(client.get("test_key").await.is_ok());
    assert!(client.list_members().await.is_ok());

    let kv_result = client.get("test_key").await;
    assert!(kv_result.is_ok() || kv_result.is_err()); // Either result is fine as long as it doesn't panic

    let cluster_result = client.list_members().await;
    assert!(cluster_result.is_ok() || cluster_result.is_err()); // Either result is fine as long as
    // it doesn't panic
}

// =============================================================================
// CAS (Compare-And-Swap) Operations Tests
// =============================================================================

mod cas_operations {
    use super::*;

    /// Test CAS success scenario - acquiring a distributed lock
    #[tokio::test]
    #[traced_test]
    async fn test_cas_acquire_lock_success() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, true).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        let lock_key = b"distributed_lock";
        let owner = b"client_a";

        // CAS: None -> "client_a" (acquire lock)
        let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner).await;
        println!("CAS acquire lock result: {result:?}");
        assert!(result.is_ok());
        assert!(result.unwrap(), "CAS should succeed");
    }

    /// Test CAS conflict scenario - lock already held by another client
    #[tokio::test]
    #[traced_test]
    async fn test_cas_lock_conflict() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, false).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        let lock_key = b"distributed_lock";
        let owner_b = b"client_b";

        // Assume lock is already held by client_a
        // CAS: None -> "client_b" should fail because lock exists
        let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner_b).await;
        println!("CAS conflict result: {result:?}");
        assert!(result.is_ok());
        assert!(!result.unwrap(), "CAS should fail due to conflict");
    }

    /// Test CAS release lock - correct owner releases the lock
    #[tokio::test]
    #[traced_test]
    async fn test_cas_release_lock() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, true).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        let lock_key = b"distributed_lock";
        let owner = b"client_a";

        // CAS: "client_a" -> delete (release lock by setting to empty or using delete)
        // For this test, we simulate releasing by setting to a tombstone value
        let result = client.compare_and_swap(lock_key, Some(owner), b"").await;
        println!("CAS release lock result: {result:?}");
        assert!(result.is_ok());
        assert!(result.unwrap(), "CAS should succeed - correct owner");
    }

    /// Test CAS prevent wrong release - only correct owner can release
    #[tokio::test]
    #[traced_test]
    async fn test_cas_prevent_wrong_release() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, false).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        let lock_key = b"distributed_lock";
        let wrong_owner = b"client_b";

        // Assume lock is held by client_a
        // CAS: "client_b" -> "" should fail because expected value doesn't match
        let result = client.compare_and_swap(lock_key, Some(wrong_owner), b"").await;
        println!("CAS wrong release result: {result:?}");
        assert!(result.is_ok());
        assert!(!result.unwrap(), "CAS should fail - wrong owner");
    }

    /// Test CAS edge cases - empty values and large values
    #[tokio::test]
    #[traced_test]
    async fn test_cas_edge_cases() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, true).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        // Test 1: Empty value CAS
        let result = client.compare_and_swap(b"empty_key", None::<&[u8]>, b"").await;
        println!("CAS empty value result: {result:?}");
        assert!(result.is_ok());
        assert!(result.unwrap(), "CAS with empty value should succeed");

        // Test 2: Large value CAS
        let large_value = vec![b'x'; 1024 * 1024]; // 1MB
        let result = client.compare_and_swap(b"large_key", None::<&[u8]>, &large_value).await;
        println!("CAS large value result: {result:?}");
        assert!(result.is_ok());
        assert!(result.unwrap(), "CAS with large value should succeed");
    }

    /// Test CAS on non-existent key
    #[tokio::test]
    #[traced_test]
    async fn test_cas_nonexistent_key_success() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, true).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        let nonexistent_key = b"does_not_exist";

        // CAS: None -> "new_value" on non-existent key (should succeed)
        let result = client.compare_and_swap(nonexistent_key, None::<&[u8]>, b"new_value").await;
        println!("CAS nonexistent key result: {result:?}");
        assert!(result.is_ok());
        assert!(
            result.unwrap(),
            "CAS on non-existent key with None should succeed"
        );
    }

    /// Test CAS on non-existent key with wrong expected value
    #[tokio::test]
    #[traced_test]
    async fn test_cas_nonexistent_key_failure() {
        let (_tx, rx) = oneshot::channel::<()>();
        let (_channel, port) = MockNode::simulate_cas_mock_server(rx, false).await.unwrap();

        let endpoints = vec![format!("http://localhost:{}", port)];
        let config = ClientConfig::default();

        let pool = ConnectionPool::create(endpoints.clone(), config.clone())
            .await
            .expect("Should create connection pool");

        let client = GrpcClient::new(Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: 123,
            config,
            endpoints,
        })));

        let nonexistent_key = b"does_not_exist";

        // CAS: Some(wrong) -> "value" on non-existent key (should fail)
        let result = client.compare_and_swap(nonexistent_key, Some(b"wrong"), b"value").await;
        println!("CAS nonexistent with wrong expected result: {result:?}");
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "CAS on non-existent key with Some(wrong) should fail"
        );
    }
}
