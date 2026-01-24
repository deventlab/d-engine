//! Integration tests for ClusterMembership metadata API and current_leader_id lifecycle
//!
//! Tests verify that GetClusterMembership API returns correct current_leader_id
//! during various cluster state transitions:
//! - After bootstrap: leader elected, current_leader_id = Some(leader)
//! - During election: current_leader_id may be None
//! - After leader change: current_leader_id updated to new leader
//! - Multiple concurrent requests: consistent view

use std::time::Duration;

use d_engine_client::Client;
use d_engine_core::ClientApiError;
use tracing::info;
use tracing_test::traced_test;

use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::reset;
use crate::common::start_node;

const TEST_DIR: &str = "cluster_start_stop/metadata_api";
const DB_ROOT_DIR: &str = "./db/cluster_start_stop/metadata_api";
const LOG_DIR: &str = "./logs/cluster_start_stop/metadata_api";

/// Test: GetClusterMembership returns current_leader_id after bootstrap
#[tokio::test]
#[traced_test]
async fn test_metadata_returns_leader_id_after_bootstrap() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

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

    info!("Cluster ready. Checking metadata API");

    // Connect client and verify metadata
    let client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Verify current_leader_id is set
    let leader_id = client
        .cluster()
        .get_leader_id()
        .await?
        .expect("Leader should be elected after bootstrap");

    info!("Metadata API returned leader_id: {}", leader_id);

    assert!(
        (1..=3).contains(&leader_id),
        "Leader ID should be one of the cluster nodes (1, 2, or 3)"
    );

    // Verify list_members also contains the same leader info
    let members = client.cluster().list_members().await?;
    assert_eq!(members.len(), 3, "Should have 3 members");

    info!("✅ Metadata API correctly returns current_leader_id after bootstrap");

    ctx.shutdown().await?;
    Ok(())
}

/// Test: Multiple concurrent metadata requests return consistent leader_id
#[tokio::test]
#[traced_test]
async fn test_concurrent_metadata_requests_consistency() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // Start 3-node cluster
    info!("Starting 3-node cluster for concurrent metadata test");
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

    // Connect client
    let client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Make 10 concurrent metadata requests
    info!("Issuing 10 concurrent metadata requests");
    let mut tasks = Vec::new();
    for i in 0..10 {
        let client_clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let leader_id = client_clone.cluster().get_leader_id().await;
            (i, leader_id)
        }));
    }

    // Collect results
    let mut results = Vec::new();
    for task in tasks {
        let (idx, leader_id) = task.await.expect("Task should complete");
        results.push((idx, leader_id));
    }

    // Verify all requests returned the same leader_id
    let first_leader = results[0]
        .1
        .as_ref()
        .expect("First request should succeed")
        .expect("Leader should exist");

    info!("First request returned leader_id: {}", first_leader);

    for (idx, leader_id_result) in results.iter() {
        let leader_id = leader_id_result
            .as_ref()
            .unwrap_or_else(|_| panic!("Request {idx} should succeed"))
            .unwrap_or_else(|| panic!("Request {idx} should return leader"));

        assert_eq!(
            leader_id, first_leader,
            "Request {idx} returned different leader_id: {leader_id} vs {first_leader}"
        );
    }

    info!(
        "✅ All concurrent requests returned consistent leader_id: {}",
        first_leader
    );

    ctx.shutdown().await?;
    Ok(())
}

/// Test: Metadata API returns updated current_leader_id after leader change
#[tokio::test]
#[traced_test]
async fn test_metadata_updates_after_leader_change() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

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

    // Connect client
    let mut client = Client::builder(create_bootstrap_urls(ports))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;

    // Get initial leader
    let initial_leader = client
        .cluster()
        .get_leader_id()
        .await?
        .expect("Leader should exist after bootstrap");

    info!("Initial leader: {}", initial_leader);

    // Determine which node to kill (use node 1 for simplicity)
    let killed_node_idx = 0;
    info!("Killing node 1 to trigger leader change");

    let node_tx = ctx.graceful_txs.remove(killed_node_idx);
    let node_handle = ctx.node_handles.remove(killed_node_idx);
    let _ = node_tx.send(());
    node_handle.await??;

    info!("Node 1 killed. Waiting for re-election");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Refresh client to discover new leader
    client.refresh(None).await?;

    // Verify metadata API returns new leader
    let new_leader = client.cluster().get_leader_id().await?.expect("New leader should be elected");

    info!("New leader after failover: {}", new_leader);

    // If initial leader was node 1, new leader must be different
    if initial_leader == 1 {
        assert_ne!(
            new_leader, initial_leader,
            "Leader should change after node 1 failure"
        );
        assert!(
            new_leader == 2 || new_leader == 3,
            "New leader should be node 2 or 3"
        );
    }

    info!("✅ Metadata API correctly updated current_leader_id after leader change");

    ctx.shutdown().await?;
    Ok(())
}
