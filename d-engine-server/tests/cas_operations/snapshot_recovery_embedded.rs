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

/// Test CAS state survives snapshot and recovery (embedded mode)
///
/// Scenario:
/// 1. Start 3-node cluster
/// 2. Perform CAS operations (acquire lock)
/// 3. Trigger snapshot
/// 4. Restart node
/// 5. Verify CAS state persists (lock still held)
#[tokio::test]
#[traced_test]
async fn test_snapshot_recovery_embedded() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for CAS snapshot test");

    let mut engines = Vec::new();
    let mut storage_paths = Vec::new();
    let mut sm_paths = Vec::new();

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

        storage_paths.push(storage_path.clone());
        sm_paths.push(sm_path.clone());

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-cas-snapshot-node{node_id}.toml");
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

    let lock_key = b"persistent_lock";

    // Step 1: Acquire lock via CAS
    info!("Step 1: Acquiring lock via CAS");
    let acquired = leader_client.compare_and_swap(lock_key, None, b"owner_before_snapshot").await?;
    assert!(acquired, "Should acquire lock");

    // Verify lock state
    let holder = leader_client.get(lock_key).await?;
    assert_eq!(holder, Some(b"owner_before_snapshot".to_vec().into()));
    info!("Lock acquired and verified");

    // Step 2: Write additional data to trigger snapshot threshold
    info!("Step 2: Writing data to trigger snapshot");
    for i in 0..100 {
        leader_client
            .put(
                format!("key_{i}").as_bytes(),
                format!("value_{i}").as_bytes(),
            )
            .await?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 3: Stop and restart one follower node
    let follower_idx = if leader_idx == 0 { 1 } else { 0 };
    info!("Step 3: Stopping follower node {}", follower_idx + 1);

    engines[follower_idx].stop().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    info!("Restarting follower node {}", follower_idx + 1);
    let storage = Arc::new(RocksDBStorageEngine::new(
        storage_paths[follower_idx].clone(),
    )?);
    let state_machine = Arc::new(RocksDBStateMachine::new(sm_paths[follower_idx].clone())?);
    let config_path = format!("/tmp/d-engine-cas-snapshot-node{}.toml", follower_idx + 1);

    let new_engine =
        EmbeddedEngine::start_custom(storage, state_machine, Some(&config_path)).await?;
    engines[follower_idx] = new_engine;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Step 4: Verify lock persists after restart
    info!("Step 4: Verifying lock persists after restart");
    let recovered_holder = leader_client.get(lock_key).await?;
    assert_eq!(
        recovered_holder,
        Some(b"owner_before_snapshot".to_vec().into()),
        "Lock should persist after snapshot recovery"
    );

    // Step 5: Verify lock can be released and re-acquired
    info!("Step 5: Release and re-acquire lock");
    let released = leader_client
        .compare_and_swap(lock_key, Some(b"owner_before_snapshot"), b"")
        .await?;
    assert!(released, "Should release lock");

    let reacquired = leader_client.compare_and_swap(lock_key, Some(b""), b"new_owner").await?;
    assert!(reacquired, "Should re-acquire lock");

    let final_holder = leader_client.get(lock_key).await?;
    assert_eq!(final_holder, Some(b"new_owner".to_vec().into()));

    info!("CAS snapshot recovery test passed");
    Ok(())
}
