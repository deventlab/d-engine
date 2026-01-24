use std::time::Duration;

use d_engine_client::Client;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
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
use crate::common::reset;
use crate::common::start_node;

const TEST_DIR: &str = "cas_operations/snapshot_recovery";
const DB_ROOT_DIR: &str = "./db/cas_operations/snapshot_recovery";
const LOG_DIR: &str = "./logs/cas_operations/snapshot_recovery";

/// Test CAS state survives snapshot and recovery (standalone mode)
///
/// Scenario:
/// 1. Start 3-node cluster via gRPC
/// 2. Perform CAS operations (acquire lock)
/// 3. Write data to trigger snapshot
/// 4. Stop and restart follower node
/// 5. Verify CAS state persists after recovery
#[tokio::test]
#[traced_test]
async fn test_snapshot_recovery_standalone() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut ctx = TestContext {
        graceful_txs: b"",
        node_handles: b"",
    };

    info!("Starting 3-node cluster for CAS snapshot recovery test");
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

    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    info!("Cluster ready. Testing CAS snapshot recovery");

    let urls = create_bootstrap_urls(ports);
    let client = Client::builder(urls).connect_timeout(Duration::from_secs(5)).build().await?;

    let lock_key = b"persistent_lock";

    // Step 1: Acquire lock via CAS
    info!("Step 1: Acquiring lock via CAS");
    let acquired = client.compare_and_swap(lock_key, None, b"owner_before_snapshot").await?;
    assert!(acquired, "Should acquire lock");
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;

    // Verify lock state
    let holder = client.get(lock_key).await?;
    assert_eq!(holder, Some(b"owner_before_snapshot".to_vec()));
    info!("Lock acquired and verified");

    // Step 2: Write data to trigger snapshot threshold
    info!("Step 2: Writing data to trigger snapshot");
    for i in 0..100 {
        client
            .put(
                format!("key_{i}").as_bytes(),
                format!("value_{i}").as_bytes(),
            )
            .await?;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Step 3: Stop follower node (node 2)
    info!("Step 3: Stopping follower node 2");
    ctx.graceful_txs[1].send(()).ok();
    ctx.node_handles[1].await.ok();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 4: Restart node 2
    info!("Step 4: Restarting node 2");
    let (graceful_tx, node_handle) = start_node(
        node_config(&create_node_config(2, ports[1], ports, DB_ROOT_DIR, LOG_DIR).await),
        None,
        None,
    )
    .await?;
    ctx.graceful_txs[1] = graceful_tx;
    ctx.node_handles[1] = node_handle;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    check_cluster_is_ready(&format!("127.0.0.1:{}", ports[1]), 10).await?;

    // Step 5: Verify lock persists after restart
    info!("Step 5: Verifying lock persists after recovery");
    let recovered_holder = client.get(lock_key).await?;
    assert_eq!(
        recovered_holder,
        Some(b"owner_before_snapshot".to_vec()),
        "Lock should persist after snapshot recovery"
    );

    // Step 6: Release and re-acquire lock
    info!("Step 6: Release and re-acquire lock");
    let released = client
        .compare_and_swap(lock_key, Some(b"owner_before_snapshot"), b"")
        .await?;
    assert!(released, "Should release lock");
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;

    let reacquired = client.compare_and_swap(lock_key, Some(b""), b"new_owner").await?;
    assert!(reacquired, "Should re-acquire lock");

    let final_holder = client.get(lock_key).await?;
    assert_eq!(final_holder, Some(b"new_owner".to_vec()));

    info!("CAS snapshot recovery test (standalone) passed");

    // Cleanup
    for tx in ctx.graceful_txs.iter() {
        let _ = tx.send(());
    }
    for handle in ctx.node_handles {
        let _ = handle.await;
    }

    Ok(())
}
