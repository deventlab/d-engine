use std::time::Duration;

use bytes::Bytes;
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

const TEST_DIR: &str = "cas_operations/distributed_lock";
const DB_ROOT_DIR: &str = "./db/cas_operations/distributed_lock";
const LOG_DIR: &str = "./logs/cas_operations/distributed_lock";

/// Test distributed lock using CAS with standalone mode (gRPC)
///
/// Scenario:
/// 1. Start 3-node cluster via gRPC
/// 2. Multiple clients compete for lock via CAS
/// 3. Verify exactly one client acquires lock (mutual exclusion)
/// 4. Lock holder releases, another acquires
/// 5. Verify lock state persists across operations
#[tokio::test]
#[traced_test]
async fn test_distributed_lock_standalone() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    info!("Starting 3-node cluster for CAS lock test");
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

    info!("Cluster ready. Testing CAS lock operations");

    let urls = create_bootstrap_urls(ports);
    let client = Client::builder(urls).connect_timeout(Duration::from_secs(5)).build().await?;

    let lock_key = b"distributed_lock";

    // Test 1: Client A acquires lock (CAS: None -> "client_a")
    info!("Test 1: Client A acquiring lock");
    let acquired_a = client.compare_and_swap(lock_key, None::<&[u8]>, b"client_a").await?;
    assert!(acquired_a, "Client A should acquire lock");
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;

    // Test 2: Client B tries to acquire (should fail, lock held)
    info!("Test 2: Client B competing for lock (should fail)");
    let acquired_b = client.compare_and_swap(lock_key, None::<&[u8]>, b"client_b").await?;
    assert!(
        !acquired_b,
        "Client B should NOT acquire lock while A holds it"
    );

    // Test 3: Verify lock holder is client_a
    info!("Test 3: Verify lock holder");
    let holder = client.get(lock_key).await?;
    assert_eq!(
        holder,
        Some(Bytes::from(b"client_a".to_vec())),
        "Lock should be held by client_a"
    );

    // Test 4: Client A releases lock (CAS: "client_a" -> empty)
    info!("Test 4: Client A releasing lock");
    let released = client.compare_and_swap(lock_key, Some(b"client_a"), b"").await?;
    assert!(released, "Client A should release lock");
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;

    // Test 5: Client B acquires lock after release
    info!("Test 5: Client B acquiring lock after release");
    let acquired_b2 = client.compare_and_swap(lock_key, Some(b""), b"client_b").await?;
    assert!(acquired_b2, "Client B should acquire lock after A releases");

    // Test 6: Verify new lock holder
    info!("Test 6: Verify new lock holder");
    let new_holder = client.get_linearizable(lock_key).await?;
    assert_eq!(
        new_holder.map(|result| result.value),
        Some(Bytes::from(b"client_b".to_vec())),
        "Lock should be held by client_b"
    );

    // Test 7: Wrong client tries to release (should fail)
    info!("Test 7: Client A tries to release B's lock (should fail)");
    let wrong_release = client.compare_and_swap(lock_key, Some(b"client_a"), b"").await?;
    assert!(!wrong_release, "Wrong client should NOT release lock");

    // Test 8: Correct client releases
    info!("Test 8: Client B releases own lock");
    let correct_release = client.compare_and_swap(lock_key, Some(b"client_b"), b"").await?;
    assert!(correct_release, "Client B should release own lock");

    info!("All CAS distributed lock tests (standalone) passed");

    // Cleanup
    for tx in ctx.graceful_txs.iter() {
        let _ = tx.send(());
    }
    for handle in ctx.node_handles {
        let _ = handle.await;
    }

    Ok(())
}
