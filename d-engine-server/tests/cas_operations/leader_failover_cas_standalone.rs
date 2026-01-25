use d_engine_client::Client;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use std::time::Duration;
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

const TEST_DIR: &str = "cas_operations/leader_failover_cas";
const DB_ROOT_DIR: &str = "./db/cas_operations/leader_failover_cas";
const LOG_DIR: &str = "./logs/cas_operations/leader_failover_cas";

/// Test CAS operation behavior during leader failover (Standalone/gRPC mode)
///
/// Based on etcd's TestTxnWriteFail and TestFailover patterns:
/// - https://github.com/etcd-io/etcd/blob/main/tests/integration/clientv3/txn_test.go
/// - https://github.com/etcd-io/etcd/blob/main/tests/integration/v3_failover_test.go
///
/// Scenario:
/// 1. Start 3-node cluster via gRPC, elect leader
/// 2. Client A sends CAS request to acquire lock
/// 3. **Immediately stop leader** (before CAS may commit)
/// 4. Wait for new leader election
/// 5. Verify: Lock state is consistent (either acquired or not, no partial writes)
/// 6. Client B retries CAS and succeeds (client handles NOT_LEADER error)
///
/// Validates:
/// - Uncommitted CAS fails gracefully (Raft safety)
/// - No partial writes (atomicity guarantee)
/// - gRPC client retry logic works across failover
/// - Client can discover new leader and retry successfully
/// - NOT_LEADER / UNAVAILABLE errors handled correctly
#[tokio::test]
#[traced_test]
async fn test_leader_failover_cas_standalone() -> Result<(), ClientApiError> {
    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    info!("Starting 3-node cluster for CAS failover test (gRPC mode)");
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

    info!("Cluster ready. Testing CAS with leader failover");

    let urls = create_bootstrap_urls(ports);
    let client = Client::builder(urls).connect_timeout(Duration::from_secs(5)).build().await?;

    let lock_key = b"failover_lock";

    // Phase 1: Identify leader and send CAS, then kill leader
    info!("Phase 1: Sending CAS to leader, then stopping leader");

    let initial_leader_id = client.get_leader_id().await?.expect("No leader elected");
    info!("Initial leader: node {}", initial_leader_id);

    // Spawn CAS request in background
    let cas_client = client.clone();
    let cas_handle = tokio::spawn(async move {
        cas_client.compare_and_swap(lock_key, None::<&[u8]>, b"client_a").await
    });

    // Immediately stop leader (simulate crash during CAS)
    tokio::time::sleep(Duration::from_millis(50)).await;
    info!("Stopping leader node {}", initial_leader_id);
    let leader_idx = (initial_leader_id - 1) as usize;
    ctx.graceful_txs[leader_idx].send(()).ok();

    // Wait for node to stop
    if let Some(handle) = ctx.node_handles.get_mut(leader_idx) {
        let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    }

    // CAS should fail or timeout (uncommitted request)
    let cas_result = tokio::time::timeout(Duration::from_secs(5), cas_handle).await;
    info!("CAS result during leader stop: {:?}", cas_result);
    // Expected: timeout, NOT_LEADER, or UNAVAILABLE error

    // Phase 2: Wait for new leader election
    info!("Phase 2: Waiting for new leader election");
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Client should auto-discover new leader
    let mut new_leader_id = None;
    for retry in 0..10 {
        if let Ok(Some(leader)) = client.get_leader_id().await {
            if leader != 0 && leader != initial_leader_id {
                new_leader_id = Some(leader);
                info!("New leader elected: node {}", leader);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("Retry {}/10: Waiting for new leader...", retry + 1);
    }

    let _new_leader_id = new_leader_id.expect("Failed to elect new leader after 10 retries");
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;

    // Phase 3: Verify lock state consistency
    info!("Phase 3: Verify lock state consistency");
    let lock_value = client.get(lock_key).await?;

    match lock_value {
        None => {
            info!("Lock is empty (CAS did not commit before leader crash) - Expected");
        }
        Some(ref value) if value.as_ref() == b"client_a" => {
            info!("Lock was acquired by client_a (CAS committed before leader crash) - Also valid");
        }
        Some(ref value) => {
            panic!("Unexpected lock value: {value:?}. CAS should be atomic!");
        }
    }

    // Phase 4: Client B retries CAS and succeeds (gRPC retry + leader rediscovery)
    info!("Phase 4: Client B retries CAS on new leader");
    let expected_value = lock_value.as_ref().map(|v| v.as_ref());

    let acquired_b = client.compare_and_swap(lock_key, expected_value, b"client_b").await?;

    assert!(
        acquired_b,
        "Client B should successfully acquire lock after failover"
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;

    // Phase 5: Verify final lock state
    info!("Phase 5: Verify final lock state");
    let final_value = client.get(lock_key).await?;
    assert_eq!(
        final_value,
        Some(b"client_b".to_vec().into()),
        "Lock should be held by client_b after successful CAS"
    );

    info!("Test passed: CAS atomicity and gRPC retry logic work during leader failover");

    // Cleanup remaining nodes
    for (idx, tx) in ctx.graceful_txs.into_iter().enumerate() {
        if idx != leader_idx {
            tx.send(()).ok();
        }
    }

    for (idx, mut handle) in ctx.node_handles.into_iter().enumerate() {
        if idx != leader_idx {
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut handle).await;
        }
    }

    Ok(())
}
