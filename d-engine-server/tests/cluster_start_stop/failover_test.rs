use std::time::Duration;

use d_engine_client::{Client, ClientApiError};
use tracing::info;
use tracing_test::traced_test;

use crate::common::{
    LATENCY_IN_MS, TestContext, WAIT_FOR_NODE_READY_IN_SEC, check_cluster_is_ready,
    create_bootstrap_urls, create_node_config, get_available_ports, node_config, reset, start_node,
};

const TEST_DIR: &str = "cluster_start_stop/failover";
const DB_ROOT_DIR: &str = "./db/cluster_start_stop/failover";
const LOG_DIR: &str = "./logs/cluster_start_stop/failover";

/// Test 3-node cluster failover: kill leader, verify re-election and data consistency
#[tokio::test]
#[traced_test]
async fn test_3_node_failover() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

    let _port_guard = get_available_ports(3).await;
    let ports = _port_guard.as_slice();
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // Start 3-node cluster
    info!("Starting 3-node cluster");
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config((i + 1) as u64, *port, ports, DB_ROOT_DIR, LOG_DIR).await,
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
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    info!("Cluster ready. Writing initial data");
    let mut client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Write test data before failover
    client.kv().put("before-failover", "initial-value").await?;
    // Wait for commit to propagate before reading
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    let result = client.kv().get_eventual("before-failover").await?;
    assert_eq!(
        result.expect("Key should exist after write").value.as_ref(),
        b"initial-value"
    );

    info!("Initial data written. Killing node 1 (likely leader)");

    // Kill node 1 (typically the leader in 3-node bootstrap)
    // Remove from ctx to allow restart later
    let node1_tx = ctx.graceful_txs.remove(0);
    let node1_handle = ctx.node_handles.remove(0);
    let _ = node1_tx.send(());
    node1_handle.await??;

    info!("Node 1 killed. Waiting for re-election");

    // Wait for leader re-election (typically 1-2s)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Refresh client to discover new leader
    client.refresh(None).await?;

    info!("Re-election complete. Verifying cluster still operational");

    // Verify cluster still works with 2 nodes (majority)
    // Retry put with multiple attempts in case new leader is still stabilizing
    let mut put_attempts = 0;
    loop {
        match client.kv().put("after-failover", "still-works").await {
            Ok(_) => break,
            Err(_e) if put_attempts < 3 => {
                put_attempts += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                client.refresh(None).await?;
            }
            Err(e) => return Err(e),
        }
    }
    // Wait for commit to propagate after successful put
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS * 2)).await;

    // Verify old data still readable
    let old_val = client.kv().get_eventual("before-failover").await?.unwrap();
    assert_eq!(old_val.value.as_ref(), b"initial-value");

    // Verify new data written successfully
    let new_val = client.kv().get_eventual("after-failover").await?.unwrap();
    assert_eq!(new_val.value.as_ref(), b"still-works");

    info!("Failover test passed. Cluster operational with 2/3 nodes");

    // Restart node 1 and verify it rejoins cluster
    info!("Restarting node 1");
    let (graceful_tx, node_handle) = start_node(
        node_config(&create_node_config(1, ports[0], ports, DB_ROOT_DIR, LOG_DIR).await),
        None,
        None,
    )
    .await?;
    ctx.graceful_txs.insert(0, graceful_tx);
    ctx.node_handles.insert(0, node_handle);

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    info!("Node 1 restarted. Verifying data sync");

    // Verify node 1 synced data from cluster
    client.refresh(None).await?;
    let synced_val = client.kv().get_eventual("after-failover").await?.unwrap();
    assert_eq!(synced_val.value.as_ref(), b"still-works");

    info!("Node 1 synced successfully. Test complete");

    // Cleanup
    ctx.shutdown().await
}

/// Test minority failure: kill 2 nodes, verify cluster cannot serve writes
#[tokio::test]
#[traced_test]
async fn test_minority_failure() -> Result<(), ClientApiError> {
    reset(&format!("{TEST_DIR}_minority")).await?;

    let _port_guard = get_available_ports(3).await;
    let ports = _port_guard.as_slice();
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
                    ports,
                    &format!("{DB_ROOT_DIR}_minority"),
                    &format!("{LOG_DIR}_minority"),
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

    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    let mut client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Write initial data
    client.kv().put("test-key", "test-value").await?;

    info!("Killing 2 nodes to lose majority");

    // Kill node 1 and node 2 (lose majority)
    for _ in 0..2 {
        let tx = ctx.graceful_txs.remove(0);
        let handle = ctx.node_handles.remove(0);
        let _ = tx.send(());
        handle.await??;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("2 nodes killed. Verifying cluster cannot serve writes");

    // Attempt write should fail (no majority)
    client.refresh(None).await?;
    let write_result = tokio::time::timeout(
        Duration::from_secs(5),
        client.kv().put("should-fail", "no-majority"),
    )
    .await;

    // Expect timeout or error (cluster has no leader)
    assert!(
        write_result.is_err() || write_result.unwrap().is_err(),
        "Write should fail without majority"
    );

    info!("Minority failure test passed. Cluster correctly refused writes");

    // Cleanup remaining node
    let tx = ctx.graceful_txs.remove(0);
    let handle = ctx.node_handles.remove(0);
    let _ = tx.send(());
    handle.await??;

    Ok(())
}
