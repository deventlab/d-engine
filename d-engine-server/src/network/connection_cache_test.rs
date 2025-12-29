use std::time::Duration;
use std::time::Instant;

use d_engine_core::ConnectionParams;
use d_engine_core::ConnectionType;
use d_engine_core::NetworkConfig;
use d_engine_core::test_utils::MockRpcService;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use super::*;
use crate::utils::net::address_str;

// Helper to create test config
fn test_config() -> NetworkConfig {
    NetworkConfig {
        control: ConnectionParams::default(),
        data: ConnectionParams::default(),
        bulk: ConnectionParams::default(),
        ..Default::default()
    }
}

async fn mock_address() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let is_ready = true;
    let mock_service = MockRpcService::default();
    let (_port, addr) =
        d_engine_core::test_utils::MockNode::mock_listener(mock_service, rx, is_ready)
            .await
            .unwrap();

    (address_str(&addr.to_string()), tx)
}

#[tokio::test]
#[traced_test]
async fn test_cache_hit_same_address() {
    let cache = ConnectionCache::new(test_config());
    let node_id = 1;
    let conn_type = ConnectionType::Data;
    let (address, _tx) = mock_address().await;

    // First call - cache miss
    let _channel1 = cache.get_channel(node_id, conn_type.clone(), address.clone()).await.unwrap();

    // Second call - cache hit
    let _channel2 = cache.get_channel(node_id, conn_type.clone(), address.clone()).await.unwrap();

    // Should reuse the same channel
    assert_eq!(cache.cache.len(), 1);

    // Verify same endpoint configuration
    let entry = cache.cache.get(&(node_id, conn_type)).unwrap();
    assert_eq!(entry.address, address);
}

#[tokio::test]
#[traced_test]
async fn test_cache_miss_address_change() {
    let cache = ConnectionCache::new(test_config());
    let node_id = 1;
    let conn_type = ConnectionType::Data;

    // First address
    let (address1, _tx) = mock_address().await;
    cache.get_channel(node_id, conn_type.clone(), address1.clone()).await.unwrap();

    // Different address
    let (address2, _tx) = mock_address().await;
    cache.get_channel(node_id, conn_type.clone(), address2.clone()).await.unwrap();

    // Should create new channel
    let entry = cache.cache.get(&(node_id, conn_type)).unwrap();
    assert!(address1 != address2);
    assert_eq!(entry.address, address2);
}

#[tokio::test]
#[traced_test]
async fn test_cache_miss_connection_type() {
    let cache = ConnectionCache::new(test_config());
    let node_id = 1;
    let (address, _tx) = mock_address().await;

    // Data connection
    cache.get_channel(node_id, ConnectionType::Data, address.clone()).await.unwrap();

    // Control connection
    cache
        .get_channel(node_id, ConnectionType::Control, address.clone())
        .await
        .unwrap();

    // Bulk connection
    cache.get_channel(node_id, ConnectionType::Bulk, address).await.unwrap();

    // Should create separate channels
    assert_eq!(cache.cache.len(), 3);
    assert!(cache.cache.contains_key(&(node_id, ConnectionType::Data)));
    assert!(cache.cache.contains_key(&(node_id, ConnectionType::Control)));
    assert!(cache.cache.contains_key(&(node_id, ConnectionType::Bulk)));
}

#[tokio::test]
#[traced_test]
async fn test_remove_node() {
    let cache = ConnectionCache::new(test_config());
    let node1 = 1;
    let node2 = 2;
    let (address, _tx) = mock_address().await;

    // Create connections for two nodes
    cache.get_channel(node1, ConnectionType::Data, address.clone()).await.unwrap();
    cache
        .get_channel(node1, ConnectionType::Control, address.clone())
        .await
        .unwrap();
    cache.get_channel(node2, ConnectionType::Data, address.clone()).await.unwrap();

    assert_eq!(cache.cache.len(), 3);

    // Remove node1
    cache.remove_node(node1);
    assert_eq!(cache.cache.len(), 1);

    // Verify remaining is node2
    let keys: Vec<_> = cache.cache.iter().map(|e| e.key().clone()).collect();
    assert_eq!(keys, vec![(node2, ConnectionType::Data)]);
}

#[tokio::test]
#[traced_test]
async fn test_last_used_update() {
    let cache = ConnectionCache::new(test_config());
    let node_id = 1;
    let conn_type = ConnectionType::Data;
    let (address, _tx) = mock_address().await;

    let start = Instant::now();

    // First call
    cache.get_channel(node_id, conn_type.clone(), address.clone()).await.unwrap();

    // Get initial timestamp
    let initial_ts = {
        let entry = cache.cache.get(&(node_id, conn_type.clone())).unwrap();
        entry.last_used
    };

    // Wait briefly
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Second call
    cache.get_channel(node_id, conn_type.clone(), address).await.unwrap();

    // Get updated timestamp
    let updated_ts = {
        let entry = cache.cache.get(&(node_id, conn_type)).unwrap();
        entry.last_used
    };

    // Verify timestamp was updated
    assert!(updated_ts > initial_ts);
    assert!(updated_ts.duration_since(start) > Duration::from_millis(10));
}

#[tokio::test]
#[traced_test]
async fn test_error_handling() {
    let cache = ConnectionCache::new(test_config());
    let node_id = 1;
    let conn_type = ConnectionType::Data;

    // Invalid address (missing scheme)
    let result = cache.get_channel(node_id, conn_type, "invalid-address".to_string()).await;

    assert!(result.is_err());
    assert!(cache.cache.is_empty());
}
