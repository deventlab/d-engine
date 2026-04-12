use d_engine_client::Client;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use d_engine_proto::error::ErrorCode;
use std::time::Duration;
use tempfile::TempDir;
use tracing::info;
use tracing_test::traced_test;

use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::start_node;

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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_root_dir = temp_dir.path().join("db").to_string_lossy().to_string();
    let log_dir = temp_dir.path().join("logs").to_string_lossy().to_string();

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

    info!("Cluster ready. Testing CAS with leader failover");

    let urls = create_bootstrap_urls(ports);
    let client = Client::builder(urls)
        .connect_timeout(Duration::from_secs(5))
        .cluster_ready_timeout(Duration::from_secs(30))
        .build()
        .await?;

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

    // Phase 2: Wait for a *stable* new leader.
    //
    // Why a stability loop instead of a single refresh():
    //
    // After node 3 crashes, the surviving nodes (1 and 2) may undergo more than
    // one election round before settling. A common pattern observed in CI:
    //
    //   Node 1 & 2 both start term-3 elections (split vote)
    //   → Node 2 steps down, resets its election timer (random 300–3000 ms)
    //   → Node 1 wins term 3 and becomes leader                    ← refresh() returns here
    //   → Node 2's timer fires before receiving Node 1's heartbeat ← term-4 election starts
    //   → Node 1 steps down (Raft rule: any higher term forces step-down)
    //   → Phase 3 reads hit node 1 (stale cached leader) → "Not leader"
    //
    // refresh() only guarantees that a ready leader existed at the instant of the
    // probe — it does NOT guarantee leadership stability going forward.
    //
    // Fix: confirm the same leader across two consecutive refresh() calls separated
    // by 2× the heartbeat interval (200 ms > 2 × 100 ms). If a concurrent election
    // is in flight, the second call will observe a different leader_id or find no
    // leader, and the loop retries. Once both calls agree, the leader has survived
    // at least two full heartbeat cycles and can be considered stable.
    info!("Phase 2: Waiting for stable new leader election");
    let new_leader_id = loop {
        client.refresh(None).await?;
        let id_first = client
            .get_leader_id()
            .await?
            .expect("Leader must be known after successful refresh");

        // Sleep longer than two heartbeat intervals (heartbeat = 100 ms) so any
        // concurrent higher-term election has time to complete and become visible.
        tokio::time::sleep(Duration::from_millis(200)).await;

        client.refresh(None).await?;
        let id_second = client
            .get_leader_id()
            .await?
            .expect("Leader must be known after second refresh");

        if id_first == id_second {
            break id_first; // Same leader confirmed twice — election has settled
        }
        // Leaders differ: a second election occurred between the two probes.
        // Loop and wait for the next stable state.
        info!(
            "Leader changed from node {} to node {} during stability check — retrying",
            id_first, id_second
        );
    };
    info!("Stable leader confirmed: node {}", new_leader_id);

    // Phase 3: Verify lock state consistency
    //
    // Even with the stability loop above there is a residual window between the
    // second refresh() and the actual read RPC. Guard against it with a single
    // refresh-and-retry on StaleOperation so the assertion never fails spuriously.
    info!("Phase 3: Verify lock state consistency");
    let lock_value = match client.get(lock_key).await {
        Err(ClientApiError::Business {
            code: ErrorCode::StaleOperation,
            ..
        }) => {
            // A second leader change happened in the tiny gap after the stability
            // check. Refresh once and retry — this is not a test failure.
            client.refresh(None).await?;
            client.get(lock_key).await?
        }
        other => other?,
    };

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
