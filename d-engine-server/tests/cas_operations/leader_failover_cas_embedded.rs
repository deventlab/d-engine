#![cfg(feature = "rocksdb")]
use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

use d_engine_core::ClientApi;
use d_engine_server::api::EmbeddedEngine;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;

/// Test CAS operation behavior during leader failover (Embedded mode)
///
/// Based on etcd's TestTxnWriteFail pattern:
/// https://github.com/etcd-io/etcd/blob/main/tests/integration/clientv3/txn_test.go
///
/// Scenario:
/// 1. Start 3-node cluster, elect leader
/// 2. Client A sends CAS request to acquire lock
/// 3. **Immediately stop leader** (before CAS may commit)
/// 4. Wait for new leader election
/// 5. Verify: Lock state is consistent (either acquired or not, no partial writes)
/// 6. Client B retries CAS and succeeds
///
/// Validates:
/// - Uncommitted CAS fails gracefully (Raft safety)
/// - No partial writes (atomicity guarantee)
/// - Lock state remains consistent after failover
/// - Clients can retry and succeed after new leader elected
#[tokio::test]
#[traced_test]
async fn test_leader_failover_cas_embedded() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for CAS failover test");

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root_dir.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let config = node_config(&config_str);
        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let storage_path = node_db_root.join("storage");
        let sm_path = node_db_root.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-cas-failover-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine =
            EmbeddedEngine::start_custom(storage, state_machine, Some(&config_path)).await?;
        engines.push(engine);
    }

    // Wait for initial leader election
    let leader_info = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("Failed to elect leader");
    info!("Initial leader elected: node {}", leader_info.leader_id);

    let initial_leader_id = leader_info.leader_id;
    let leader_idx = (initial_leader_id - 1) as usize;
    let lock_key = b"failover_lock";

    // Phase 1: Try CAS on leader, then immediately kill leader
    info!("Phase 1: Sending CAS to leader, then stopping leader");
    let leader_client = engines[leader_idx].client();

    // Spawn CAS request
    let cas_handle = {
        let client = leader_client.clone();
        let key = lock_key.to_vec();
        tokio::spawn(async move { client.compare_and_swap(&key, None::<&[u8]>, b"client_a").await })
    };

    // Immediately stop leader (before CAS may commit)
    tokio::time::sleep(Duration::from_millis(10)).await;
    info!("Stopping leader node {}", leader_info.leader_id);
    engines[leader_idx].stop().await?;

    // CAS should fail or timeout
    let cas_result = tokio::time::timeout(Duration::from_secs(3), cas_handle).await;
    info!("CAS result during leader stop: {:?}", cas_result);
    // Expected: timeout or error (uncommitted request fails)

    // Phase 2: Wait for new leader election
    info!("Phase 2: Waiting for new leader election");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Find new leader (from remaining nodes)
    let mut new_leader_idx = None;
    for (idx, engine) in engines.iter().enumerate() {
        if idx == leader_idx {
            continue; // Skip stopped node
        }
        if let Some(leader_info) = engine.leader_info() {
            let leader_id = leader_info.leader_id;
            if leader_id != 0 && leader_id != initial_leader_id {
                new_leader_idx = Some(idx);
                info!("New leader elected: node {} (idx {})", leader_id, idx);
                break;
            }
        }
    }

    let new_leader_idx = new_leader_idx.expect("Failed to elect new leader");
    let new_leader_client = engines[new_leader_idx].client();

    // Phase 3: Verify lock state consistency
    info!("Phase 3: Verify lock state consistency");
    let lock_value = new_leader_client.get(lock_key).await?;

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

    // Phase 4: Client B retries CAS and succeeds
    info!("Phase 4: Client B retries CAS on new leader");
    let expected_value = lock_value.as_ref().map(|v| v.as_ref());
    let acquired_b = new_leader_client
        .compare_and_swap(lock_key, expected_value, b"client_b")
        .await?;

    assert!(
        acquired_b,
        "Client B should successfully acquire lock after failover"
    );

    // Phase 5: Verify final lock state
    info!("Phase 5: Verify final lock state");
    let final_value = new_leader_client.get(lock_key).await?;
    assert_eq!(
        final_value,
        Some(b"client_b".to_vec().into()),
        "Lock should be held by client_b after successful CAS"
    );

    info!("Test passed: CAS atomicity maintained during leader failover");

    // Cleanup remaining nodes
    for engine in engines.iter_mut() {
        engine.stop().await?;
    }

    Ok(())
}
