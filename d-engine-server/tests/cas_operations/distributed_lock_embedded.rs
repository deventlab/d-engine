use std::sync::Arc;
use std::time::Duration;

use d_engine_core::ClientApi;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBStorageEngine;
use d_engine_server::api::EmbeddedEngine;
use tracing::info;
use tracing_test::traced_test;

use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;

/// Test distributed lock using CAS with embedded mode
///
/// Scenario:
/// 1. Start 3-node cluster
/// 2. Multiple clients compete for lock via CAS
/// 3. Verify exactly one client acquires lock (mutual exclusion)
/// 4. Lock holder releases, another acquires
/// 5. Verify lock state persists across operations
#[tokio::test]
#[traced_test]
async fn test_distributed_lock_embedded() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for CAS lock test");

    let mut engines = b"";

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

        let config_path = format!("/tmp/d-engine-cas-lock-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine =
            EmbeddedEngine::start_custom(storage, state_machine, Some(&config_path)).await?;
        engines.push(engine);
    }

    // Wait for leader election
    let leader_info = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("Failed to elect leader");
    info!("Leader elected: node {}", leader_info.leader_id);

    let leader_idx = (leader_info.leader_id - 1) as usize;
    let leader_client = engines[leader_idx].client();

    let lock_key = b"distributed_lock";

    // Test 1: Client A acquires lock (CAS: None -> "client_a")
    info!("Test 1: Client A acquiring lock");
    let acquired_a = leader_client.compare_and_swap(lock_key, None, b"client_a").await?;
    assert!(acquired_a, "Client A should acquire lock");

    // Test 2: Client B tries to acquire (should fail, lock held)
    info!("Test 2: Client B competing for lock (should fail)");
    let acquired_b = leader_client.compare_and_swap(lock_key, None, b"client_b").await?;
    assert!(
        !acquired_b,
        "Client B should NOT acquire lock while A holds it"
    );

    // Test 3: Verify lock holder is client_a
    info!("Test 3: Verify lock holder");
    let holder = leader_client.get(lock_key).await?;
    assert_eq!(
        holder,
        Some(b"client_a".to_vec()),
        "Lock should be held by client_a"
    );

    // Test 4: Client A releases lock (CAS: "client_a" -> empty)
    info!("Test 4: Client A releasing lock");
    let released = leader_client.compare_and_swap(lock_key, Some(b"client_a"), b"").await?;
    assert!(released, "Client A should release lock");

    // Test 5: Client B acquires lock after release
    info!("Test 5: Client B acquiring lock after release");
    let acquired_b2 = leader_client.compare_and_swap(lock_key, Some(b""), b"client_b").await?;
    assert!(acquired_b2, "Client B should acquire lock after A releases");

    // Test 6: Verify new lock holder
    info!("Test 6: Verify new lock holder");
    let new_holder = leader_client.get(lock_key).await?;
    assert_eq!(
        new_holder,
        Some(b"client_b".to_vec()),
        "Lock should be held by client_b"
    );

    // Test 7: Wrong client tries to release (should fail)
    info!("Test 7: Client A tries to release B's lock (should fail)");
    let wrong_release = leader_client.compare_and_swap(lock_key, Some(b"client_a"), b"").await?;
    assert!(!wrong_release, "Wrong client should NOT release lock");

    // Test 8: Correct client releases
    info!("Test 8: Client B releases own lock");
    let correct_release = leader_client.compare_and_swap(lock_key, Some(b"client_b"), b"").await?;
    assert!(correct_release, "Client B should release own lock");

    info!("All CAS distributed lock tests passed");
    Ok(())
}
