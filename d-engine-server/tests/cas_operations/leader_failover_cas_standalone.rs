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

    // Phase 2 + 3: Wait for a stable leader AND verify lock state consistency.
    //
    // Why these two phases are merged into a single retry loop:
    //
    // After node 3 crashes, the surviving nodes may undergo cascading elections
    // before settling on a stable leader. A pattern observed in both CI and local:
    //
    //   Node 1 & 2 both start elections (split vote, multiple rounds)
    //   → Node A wins term N and becomes leader   ← refresh() sees this and returns
    //   → Node B's election timer fires, starts term N+1 election
    //   → Node A receives term N+1, MUST step down (Raft protocol)
    //   → Phase 3 read hits node A (now a follower) → "Not leader"
    //
    // The key insight: checking leader_id consistency across two refresh() calls
    // does NOT reliably detect this. Cluster metadata (current_leader_id) reflects
    // what each node *last committed as leader*, not the live Raft state. A node
    // that just stepped down may still appear as "leader" in other nodes' metadata
    // until the new leader's noop is replicated.
    //
    // The only authoritative check is: can the leader actually serve a read right now?
    //
    // Fix: merge Phase 2 and Phase 3 into a refresh→read loop. refresh() discovers
    // the best-known leader; the subsequent get() is the live proof that the leader
    // is active. On StaleOperation the loop retries immediately. This converges once
    // the cluster elects a stable leader that can serve requests end-to-end.
    // refresh() internally handles cluster_ready_timeout, so the loop is bounded.
    info!("Phase 2+3: Waiting for stable leader and verifying lock state consistency");
    let lock_value = loop {
        client.refresh(None).await?;
        let new_leader_id = client
            .get_leader_id()
            .await?
            .expect("Leader must be known after successful refresh");
        info!("Candidate leader: node {}", new_leader_id);

        match client.get(lock_key).await {
            Ok(value) => {
                // Read succeeded — this leader is actively serving requests.
                info!("Stable leader confirmed: node {}", new_leader_id);
                break value;
            }
            Err(ClientApiError::Business {
                code: ErrorCode::StaleOperation,
                ..
            }) => {
                // The leader changed between refresh() and the read RPC.
                // A cascading election is still in progress — refresh and retry.
                info!(
                    "Node {} is no longer leader (cascading election in progress), retrying",
                    new_leader_id
                );
                continue;
            }
            Err(ClientApiError::Network {
                code: ErrorCode::ConnectionTimeout,
                ..
            }) => {
                // Node 1 just won the election but node 2 immediately started its own
                // candidacy (cascading elections).  A linearizable read requires a
                // heartbeat-quorum ACK from node 2; while node 2 is a candidate it
                // ignores node 1's heartbeats.  The pending read sits on the server
                // until node 2 steps down, which can exceed the client's default
                // request_timeout (3 s), causing Code::Cancelled / "Timeout expired".
                //
                // The cluster will stabilise once node 2 receives node 1's heartbeat
                // with the winning term and steps down.  Retry refresh() + get() so
                // we wait for a leader that can actually serve requests end-to-end.
                info!(
                    "get() timed out waiting for leader {} to achieve read quorum \
                     (cascading election still settling), retrying",
                    new_leader_id
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => {
                return Err(e);
            }
        }
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
