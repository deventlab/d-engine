use std::time::Duration;

use d_engine_client::ClientApiError;
use tracing::error;
use tracing_test::traced_test;

use crate::client_manager::ClientManager;
use crate::common::ITERATIONS;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;

// Constants for test configuration
const TEST_CASE1_DIR: &str = "cluster_start_stop/case1";
const TEST_CASE2_DIR: &str = "cluster_start_stop/case2";
const TEST_CASE1_DB_ROOT_DIR: &str = "./db/cluster_start_stop/case1";
const TEST_CASE1_LOG_DIR: &str = "./logs/cluster_start_stop/case1";
const TEST_CASE2_DB_ROOT_DIR: &str = "./db/cluster_start_stop/case2";
const TEST_CASE2_LOG_DIR: &str = "./logs/cluster_start_stop/case2";

/// Case 1: start 3 node cluster and test simple get/put, and then stop the
/// cluster
#[tokio::test]
#[traced_test]
async fn test_cluster_put_and_lread_case1() -> Result<(), ClientApiError> {
    reset(TEST_CASE1_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Start cluster nodes
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config(
                    (i + 1) as u64,
                    *port,
                    ports,
                    TEST_CASE1_DB_ROOT_DIR,
                    TEST_CASE1_LOG_DIR,
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

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[test_cluster_put_and_lread_case1] Cluster started. Running tests...");

    // Test basic operations
    let mut client_manager = ClientManager::new(&create_bootstrap_urls(ports)).await?;
    test_put_get(&mut client_manager, 2, 202).await?;

    // Clean up
    ctx.shutdown().await
}

/// # Case 2: test one of the node restart, but with linearizable read from Leader only
///
/// In this case, performance is not something we want to test, so we want to
/// test     if client's linearizable read could be achieved by reading from
/// Leader only.
///
/// Give each put call with 10ms latency is enough for single testing on
/// macmini16g/8c.
///
/// ## T1: L1, F2, F31
/// - put 1 1
/// - get 1
///  1
/// - put 1 2
/// - get 1
/// 2
///
/// ## T2: F2, L3
/// - put 1 3
/// - get 1
/// 3
///
/// ## T3: F1, F2, L3
/// - put 1 4
/// - get 1
/// 4
/// - put 2 20
/// - put 2 21
/// - get 2
/// 21
///
/// ## T4: stop cluster
/// ## T5: start cluster
/// - get 1
/// 4
/// - get 2
/// 21
/// - put 1 5
/// - get 1
/// 5
/// - get 2
/// 21
#[tokio::test]
#[traced_test]
async fn test_cluster_put_and_lread_case2() -> Result<(), ClientApiError> {
    reset(TEST_CASE2_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let bootstrap_urls = create_bootstrap_urls(ports);
    let bootstrap_urls_without_n1 = create_bootstrap_urls(&ports[1..]);

    // Phase T1: Initial cluster setup and first operations
    println!("------------------T1-----------------");
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config(
                    (i + 1) as u64,
                    *port,
                    ports,
                    TEST_CASE2_DB_ROOT_DIR,
                    TEST_CASE2_LOG_DIR,
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

    // Initial operations
    let mut client_manager = ClientManager::new(&bootstrap_urls).await?;
    test_put_get(&mut client_manager, 1, 1).await?;
    test_put_get(&mut client_manager, 1, 2).await?;

    // Phase T2: Stop node 1 and test
    println!("------------------T2-----------------");
    let graceful_tx1 = ctx.graceful_txs.remove(0);
    graceful_tx1.send(()).map_err(|e| {
        error!("Failed to send shutdown signal: {}", e);
        ClientApiError::general_client_error("failed to shutdown".to_string())
    })?;
    let node_n1 = ctx.node_handles.remove(0);
    node_n1.await??;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    client_manager = ClientManager::new(&bootstrap_urls_without_n1).await?;
    client_manager.verify_read(1, 2, ITERATIONS).await;
    test_put_get(&mut client_manager, 1, 3).await?;

    // Phase T3: Restart node 1 and test
    println!("------------------T3-----------------");
    let (graceful_tx1, node_n1) = start_node(
        node_config(
            &create_node_config(
                1,
                ports[0],
                ports,
                TEST_CASE2_DB_ROOT_DIR,
                TEST_CASE2_LOG_DIR,
            )
            .await,
        ),
        None,
        None,
    )
    .await?;
    ctx.graceful_txs.insert(0, graceful_tx1);
    ctx.node_handles.insert(0, node_n1);

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    client_manager = ClientManager::new(&bootstrap_urls).await?;
    client_manager.verify_read(1, 3, ITERATIONS).await;

    test_put_get(&mut client_manager, 1, 4).await?;
    test_put_get(&mut client_manager, 2, 20).await?;
    test_put_get(&mut client_manager, 2, 21).await?;

    // Phase T4: Stop entire cluster
    println!("------------------T4-----------------");
    ctx.shutdown().await?;
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Phase T5: Restart cluster and verify state
    println!("------------------T5-----------------");
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config(
                    (i + 1) as u64,
                    *port,
                    ports,
                    TEST_CASE2_DB_ROOT_DIR,
                    TEST_CASE2_LOG_DIR,
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

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC * 2)).await;

    let mut client_manager = ClientManager::new(&bootstrap_urls).await?;
    client_manager.verify_read(1, 4, ITERATIONS).await;
    client_manager.verify_read(2, 21, ITERATIONS).await;
    test_put_get(&mut client_manager, 1, 5).await?;
    client_manager.verify_read(2, 21, ITERATIONS).await;

    // Final cleanup
    ctx.shutdown().await
}

/// Test 5b: Multi-node linearizable read consistency (must match single-node behavior)
///
/// This test verifies that linearizable reads behave identically in single-node
/// and multi-node clusters, as required by Raft protocol.
///
/// Test scenario:
/// 1. Start 3-node cluster
/// 2. Leader: PUT key-value pair (commits with quorum)
/// 3. Leader: Immediately get_linearizable() with NO sleep
/// 4. Expect to read the value (same as single-node test)
///
/// Why this test is critical:
/// - Raft linearizability guarantee must be consistent across cluster sizes
/// - Users should NOT see different behavior when scaling from 1 to 3 nodes
/// - This was a P0 bug: single-node returned None, but multi-node might work
///   (due to network delay giving state machine time to apply)
///
/// Expected behavior (IDENTICAL to single-node):
/// - PUT returns Ok → log entry committed by quorum
/// - get_linearizable() waits for state machine to apply the entry
/// - Returns the value (NOT None)
///
/// Raft protocol guarantee:
/// - Section 8: "Linearizability requires that read operations return results
///   that reflect a state of the system sometime after the operation was invoked"
/// - This must hold regardless of cluster size (1 or 3 nodes)
#[tokio::test]
#[traced_test]
async fn test_multi_node_linearizable_read_consistency() -> Result<(), ClientApiError> {
    const TEST_DIR: &str = "cluster_start_stop/linearizable_read";
    const DB_ROOT: &str = "./db/cluster_start_stop/linearizable_read";
    const LOG_DIR: &str = "./logs/cluster_start_stop/linearizable_read";

    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Start 3-node cluster
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(&create_node_config((i + 1) as u64, *port, ports, DB_ROOT, LOG_DIR).await),
            None,
            None,
        )
        .await?;
        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[Multi-node linearizable test] Cluster ready. Testing...");

    // Test: PUT → get_linearizable with NO sleep (same as single-node test)
    let mut client_manager = ClientManager::new(&create_bootstrap_urls(ports)).await?;

    // Use the existing test_put_get helper which tests linearizable reads
    test_put_get(&mut client_manager, 1, 100).await?;
    println!("✅ Multi-node linearizable read consistency verified (PUT → GET with no sleep)");

    // Additional test: Sequential writes verify latest value is always visible
    test_put_get(&mut client_manager, 2, 200).await?;
    test_put_get(&mut client_manager, 2, 201).await?; // Overwrite - should read 201, not 200

    println!("✅ Multi-node sequential writes verified");

    // Cleanup
    ctx.shutdown().await
}
