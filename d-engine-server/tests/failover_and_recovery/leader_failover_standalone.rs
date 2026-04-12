use std::time::Duration;

use d_engine_client::Client;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use serial_test::serial;
use tracing::info;
use tracing_test::traced_test;

use crate::common::LATENCY_IN_MS;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::start_node;

/// Test 3-node cluster failover: kill leader, verify re-election and data consistency
#[tokio::test]
#[traced_test]
async fn test_3_node_failover() -> Result<(), ClientApiError> {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_root_dir = temp_dir.path().join("db").to_string_lossy().to_string();
    let log_dir = temp_dir.path().join("logs").to_string_lossy().to_string();

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // Start 3-node cluster
    info!("Starting 3-node cluster");
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config((i + 1) as u64, *port, ports, &db_root_dir, &log_dir).await,
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
    let client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Write test data before failover
    client.put("before-failover", "initial-value").await?;
    // Retry read until replicated (fixed sleep is unreliable under parallel test load)
    let result = {
        let mut val = None;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
            if let Ok(Some(v)) = client.get_eventual("before-failover").await {
                val = Some(v);
                break;
            }
        }
        val
    };
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
        match client.put("after-failover", "still-works").await {
            Ok(_) => break,
            Err(_e) if put_attempts < 3 => {
                put_attempts += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                client.refresh(None).await?;
            }
            Err(e) => return Err(e),
        }
    }
    // Retry reads until replicated after failover and re-election
    let old_val = {
        let mut val = None;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
            if let Ok(Some(v)) = client.get_eventual("before-failover").await {
                val = Some(v);
                break;
            }
        }
        val
    };
    assert_eq!(old_val.unwrap().value.as_ref(), b"initial-value");

    let new_val = {
        let mut val = None;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
            if let Ok(Some(v)) = client.get_eventual("after-failover").await {
                val = Some(v);
                break;
            }
        }
        val
    };
    assert_eq!(new_val.unwrap().value.as_ref(), b"still-works");

    info!("Failover test passed. Cluster operational with 2/3 nodes");

    // Restart node 1 and verify it rejoins cluster
    info!("Restarting node 1");
    let (graceful_tx, node_handle) = start_node(
        node_config(&create_node_config(1, ports[0], ports, &db_root_dir, &log_dir).await),
        None,
        None,
    )
    .await?;
    ctx.graceful_txs.insert(0, graceful_tx);
    ctx.node_handles.insert(0, node_handle);

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Wait for node to be actually ready
    check_cluster_is_ready(&format!("127.0.0.1:{}", ports[0]), 10).await?;

    info!("Node 1 restarted. Verifying data sync");

    // Retry read until node 1 synced data from cluster
    client.refresh(None).await?;
    let synced_val = {
        let mut val = None;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
            if let Ok(Some(v)) = client.get_eventual("after-failover").await {
                val = Some(v);
                break;
            }
        }
        val
    };
    assert_eq!(synced_val.unwrap().value.as_ref(), b"still-works");

    info!("Node 1 synced successfully. Test complete");

    // Cleanup
    ctx.shutdown().await
}

/// Test minority failure: kill 2 nodes, verify cluster cannot serve writes
#[tokio::test]
#[traced_test]
#[serial]
async fn test_minority_failure() -> Result<(), ClientApiError> {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_root_dir = temp_dir.path().join("db").to_string_lossy().to_string();
    let log_dir = temp_dir.path().join("logs").to_string_lossy().to_string();

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // Start 3-node cluster
    info!("Starting 3-node cluster for minority failure test");
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config((i + 1) as u64, *port, ports, &db_root_dir, &log_dir).await,
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

    let client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Write initial data with retry: leader may still be stabilizing under load
    let mut attempts = 0;
    loop {
        match client.put("test-key", "test-value").await {
            Ok(_) => break,
            Err(_) if attempts < 5 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                client.refresh(None).await?;
            }
            Err(e) => return Err(e),
        }
    }

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
    // refresh may fail when no leader is available — that is expected, ignore the error.
    let _ = client.refresh(None).await;
    let write_result = tokio::time::timeout(
        Duration::from_secs(5),
        client.put("should-fail", "no-majority"),
    )
    .await;

    // Expect timeout or error (cluster has no leader)
    // Both timeout and client errors indicate cluster is unavailable (correct behavior)
    match write_result {
        Err(_timeout) => {
            // Timeout - cluster cannot respond (expected)
            info!("Write timed out as expected (no majority)");
        }
        Ok(Err(e)) => {
            // Client returned error - cluster rejected request (also expected)
            info!("Write failed as expected: {:?}", e);
        }
        Ok(Ok(_)) => {
            panic!("Write should fail without majority, but succeeded!");
        }
    }

    info!("Minority failure test passed. Cluster correctly refused writes");

    // Cleanup remaining node
    let tx = ctx.graceful_txs.remove(0);
    let handle = ctx.node_handles.remove(0);
    let _ = tx.send(());
    handle.await??;

    Ok(())
}
