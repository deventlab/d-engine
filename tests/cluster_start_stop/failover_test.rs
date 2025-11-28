use std::time::Duration;

use d_engine::ClientApiError;
use tracing::info;
use tracing_test::traced_test;

use crate::client_manager::ClientManager;
use crate::common::{
    check_cluster_is_ready, create_bootstrap_urls, create_node_config, get_available_ports,
    node_config, reset, start_node, TestContext, WAIT_FOR_NODE_READY_IN_SEC,
};

const TEST_DIR: &str = "cluster_start_stop/failover";
const DB_ROOT_DIR: &str = "./db/cluster_start_stop/failover";
const LOG_DIR: &str = "./logs/cluster_start_stop/failover";

/// Test 3-node cluster failover: kill leader, verify re-election and data consistency
#[tokio::test]
#[traced_test]
async fn test_3_node_failover() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

    let ports = get_available_ports(3).await;
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // Start 3-node cluster
    info!("Starting 3-node cluster");
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config((i + 1) as u64, *port, &ports, DB_ROOT_DIR, LOG_DIR).await,
            ),
            None,
            None,
        )
        .await?;
        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster ready
    for port in &ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    info!("Cluster ready. Writing initial data");
    let mut client = ClientManager::new(&create_bootstrap_urls(&ports)).await?;

    // Write test data before failover
    client.put(b"before-failover".to_vec(), b"initial-value".to_vec()).await?;
    let val = client.get(b"before-failover".to_vec()).await?.unwrap();
    assert_eq!(val, b"initial-value".as_slice());

    info!("Initial data written. Killing node 1 (likely leader)");

    // Kill node 1 (typically the leader in 3-node bootstrap)
    ctx.graceful_txs[0].send(()).await.map_err(|_| ClientApiError::ChannelClosed)?;
    ctx.node_handles[0]
        .await
        .map_err(|e| ClientApiError::ServerError(format!("Node shutdown failed: {e}")))??;

    info!("Node 1 killed. Waiting for re-election");

    // Wait for leader re-election (typically 1-2s)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Refresh client to discover new leader
    client.refresh_client().await?;

    info!("Re-election complete. Verifying cluster still operational");

    // Verify cluster still works with 2 nodes (majority)
    client.put(b"after-failover".to_vec(), b"still-works".to_vec()).await?;

    // Verify old data still readable
    let old_val = client.get(b"before-failover".to_vec()).await?.unwrap();
    assert_eq!(old_val, b"initial-value".as_slice());

    // Verify new data written successfully
    let new_val = client.get(b"after-failover".to_vec()).await?.unwrap();
    assert_eq!(new_val, b"still-works".as_slice());

    info!("Failover test passed. Cluster operational with 2/3 nodes");

    // Restart node 1 and verify it rejoins cluster
    info!("Restarting node 1");
    let (graceful_tx, node_handle) = start_node(
        node_config(&create_node_config(1, ports[0], &ports, DB_ROOT_DIR, LOG_DIR).await),
        None,
        None,
    )
    .await?;
    ctx.graceful_txs[0] = graceful_tx;
    ctx.node_handles[0] = node_handle;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    info!("Node 1 restarted. Verifying data sync");

    // Verify node 1 synced data from cluster
    client.refresh_client().await?;
    let synced_val = client.get(b"after-failover".to_vec()).await?.unwrap();
    assert_eq!(synced_val, b"still-works".as_slice());

    info!("Node 1 synced successfully. Test complete");

    // Cleanup
    ctx.shutdown().await
}

/// Test minority failure: kill 2 nodes, verify cluster cannot serve writes
#[tokio::test]
#[traced_test]
async fn test_minority_failure() -> Result<(), ClientApiError> {
    reset(&format!("{}_minority", TEST_DIR)).await?;

    let ports = get_available_ports(3).await;
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // Start 3-node cluster
    info!("Starting 3-node cluster for minority failure test");
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config(
                    (i + 1) as u64,
                    *port,
                    &ports,
                    &format!("{}_minority", DB_ROOT_DIR),
                    &format!("{}_minority", LOG_DIR),
                )
                .await,
            ),
            None,
            None,
        )
        .await?;
        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in &ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    let mut client = ClientManager::new(&create_bootstrap_urls(&ports)).await?;

    // Write initial data
    client.put(b"test-key".to_vec(), b"test-value".to_vec()).await?;

    info!("Killing 2 nodes to lose majority");

    // Kill node 1 and node 2 (lose majority)
    for i in 0..2 {
        ctx.graceful_txs[i].send(()).await.map_err(|_| ClientApiError::ChannelClosed)?;
        ctx.node_handles[i]
            .await
            .map_err(|e| ClientApiError::ServerError(format!("Node shutdown failed: {e}")))??;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("2 nodes killed. Verifying cluster cannot serve writes");

    // Attempt write should fail (no majority)
    client.refresh_client().await?;
    let write_result = tokio::time::timeout(
        Duration::from_secs(5),
        client.put(b"should-fail".to_vec(), b"no-majority".to_vec()),
    )
    .await;

    // Expect timeout or error (cluster has no leader)
    assert!(
        write_result.is_err() || write_result.unwrap().is_err(),
        "Write should fail without majority"
    );

    info!("Minority failure test passed. Cluster correctly refused writes");

    // Cleanup remaining node
    ctx.graceful_txs[2].send(()).await.map_err(|_| ClientApiError::ChannelClosed)?;
    ctx.node_handles[2]
        .await
        .map_err(|e| ClientApiError::ServerError(format!("Node shutdown failed: {e}")))??;

    Ok(())
}
