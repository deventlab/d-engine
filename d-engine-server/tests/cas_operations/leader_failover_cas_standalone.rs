use d_engine_client::Client;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use d_engine_core::client::ErrorCode;
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
use crate::common::wait_for_stable_leader;

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
        let node_data_dir = temp_dir.path().join(format!("node{}", i + 1));
        let mut node_cfg = node_config(
            &create_node_config(
                (i + 1) as u64,
                *port,
                ports,
                &node_data_dir.to_string_lossy(),
                &log_dir,
            )
            .await,
        );
        // TODO(#428): widen the election timeout for this test only. After the leader is
        // killed, the 2 surviving nodes re-elect; with the default 300ms min, slow CI can
        // livelock (the new leader's 100ms heartbeat slips past the follower's 300ms
        // timeout, so the follower votes it out and split-vote cascades). 3000/6000 gives
        // the leader time to establish — same rationale as `create_rejoin_node_config`.
        // Revert once #428 (leader lease) lands.
        node_cfg.raft.election.election_timeout_min = 3000;
        node_cfg.raft.election.election_timeout_max = 6000;
        let (graceful_tx, node_handle) = start_node(&node_data_dir, node_cfg, None, None).await?;
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

    // Phase 2 + 3: Wait for a stable leader, then verify lock state consistency.
    //
    // wait_for_stable_leader() is the authoritative check for "can the leader
    // actually serve a read right now?" — see its own doc comment for why
    // cheaper checks (e.g. leader_id consistency across refresh() calls) don't
    // reliably detect a cascading election still settling after node 3 crashes.
    info!("Phase 2+3: Waiting for stable leader and verifying lock state consistency");
    wait_for_stable_leader(&client).await?;
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
    //
    // Same cascading-election exposure as Phase 2+3/5: the leader confirmed stable a
    // moment ago in Phase 2+3 can still step down before this CAS lands.
    info!("Phase 4: Client B retries CAS on new leader");
    let expected_value = lock_value.as_ref().map(|v| v.as_ref());

    let acquired_b = loop {
        match client.compare_and_swap(lock_key, expected_value, b"client_b").await {
            Ok(acquired) => break acquired,
            Err(ClientApiError::Business {
                code: ErrorCode::StaleOperation,
                ..
            }) => continue,
            Err(ClientApiError::Network {
                code: ErrorCode::NotLeader,
                ..
            }) => {
                client.refresh(None).await?;
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(ClientApiError::Network {
                code: ErrorCode::ConnectionTimeout,
                ..
            }) => {
                // A ConnectionTimeout means the response was lost, not that the server
                // didn't act — the server may have already committed this exact CAS
                // before the timeout. Retrying blindly with the same `expected_value`
                // would then legitimately get `Ok(false)` (the value moved on from what
                // client_b expected), which this loop would misreport as "client_b's
                // CAS failed" even though it actually succeeded on the first attempt.
                // Read reality first: if the lock already shows client_b's own value,
                // the earlier attempt won — accept it instead of firing a second CAS.
                client.refresh(None).await?;
                match client.get(lock_key).await {
                    Ok(Some(v)) if v.as_ref() == b"client_b".as_slice() => break true,
                    _ => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }
            Err(e) => return Err(e),
        }
    };

    assert!(
        acquired_b,
        "Client B should successfully acquire lock after failover"
    );

    // Phase 5: Verify final lock state
    //
    // Same cascading-election resilience as Phase 2+3: a linearizable read requires
    // the leader to confirm quorum via heartbeat. If a cascading election is still
    // settling after Phase 4's CAS, that heartbeat quorum confirmation can timeout.
    // Retry refresh() + get() until a stable leader can serve the read end-to-end.
    info!("Phase 5: Verify final lock state");
    let final_value = loop {
        client.refresh(None).await?;
        match client.get(lock_key).await {
            Ok(value) => break value,
            Err(ClientApiError::Business {
                code: ErrorCode::StaleOperation,
                ..
            }) => continue,
            Err(ClientApiError::Network {
                code: ErrorCode::ConnectionTimeout,
                ..
            }) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(ClientApiError::Network {
                code: ErrorCode::NotLeader,
                ..
            }) => continue,
            Err(e) => return Err(e),
        }
    };
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
