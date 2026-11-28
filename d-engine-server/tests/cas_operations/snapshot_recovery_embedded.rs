#![cfg(feature = "rocksdb")]
use std::sync::Arc;
use std::time::Duration;

use d_engine_core::ClientApi;
use d_engine_server::api::DefaultEmbeddedEngine;
use d_engine_server::open_rocksdb;
use tracing::info;
use tracing_test::traced_test;

use crate::common::get_available_ports;
use crate::common::wait_for_snapshot;

const SNAPSHOT_THRESHOLD: u64 = 50;
const RETAINED_LOGS: u64 = 10;
const FILLER_WRITES: u64 = 100;

fn node_config(
    node_id: u64,
    ports: &[u16],
    log_dir: &std::path::Path,
) -> String {
    format!(
        r#"
[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 1, status = 3 }}
]
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 10000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
"#,
        ports[node_id as usize - 1],
        ports[0],
        ports[1],
        ports[2],
        log_dir.join(format!("node{node_id}")).display(),
    )
}

async fn start_node(
    node_id: u64,
    data_dir: &std::path::Path,
    config_path: &str,
) -> Result<DefaultEmbeddedEngine, Box<dyn std::error::Error>> {
    let db_path = data_dir.join(format!("node{node_id}/db"));
    tokio::fs::create_dir_all(&db_path).await?;
    let (storage, state_machine) = open_rocksdb(&db_path)?;
    Ok(DefaultEmbeddedEngine::start_custom(
        &db_path,
        Arc::new(storage),
        state_machine,
        Some(config_path),
    )
    .await?)
}

/// A lock taken with CAS survives a restart of a node whose log was compacted into a snapshot.
///
/// Scenario:
/// 1. Start a 3-node cluster that snapshots every 50 entries.
/// 2. Take the lock with CAS, then write 100 more entries: the follower takes a snapshot and
///    purges the log entries it covers, the lock among them.
/// 3. Stop all three nodes, then restart only that follower. Alone it has no quorum and no
///    leader, so nothing can be replicated to it: whatever it serves came from its own disk.
/// 4. The lock and the oldest filler write must be readable on it. Both live only in the snapshot.
/// 5. Restart the other two nodes: the lock can still be released and taken again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[traced_test]
async fn test_snapshot_recovery_embedded() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");
    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice().to_vec();

    let mut config_paths = Vec::new();
    let mut engines = Vec::new();
    for node_id in 1..=3u64 {
        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, node_config(node_id, &ports, &log_dir)).await?;
        let config_path = config_path.to_string_lossy().to_string();
        engines.push(start_node(node_id, &data_dir, &config_path).await?);
        config_paths.push(config_path);
    }

    let leader_id = engines[0].wait_ready(Duration::from_secs(10)).await?.leader_id;
    let leader = engines
        .iter()
        .find(|e| e.node_id() == leader_id)
        .expect("leader is one of the nodes");
    let leader_client = leader.client().clone();
    let follower_id = (1..=3u64).find(|&id| id != u64::from(leader_id)).expect("a follower exists");
    info!("Leader is node {leader_id}; node {follower_id} will be restarted");

    let lock_key = b"persistent_lock";
    assert!(
        leader_client
            .compare_and_swap(lock_key, None::<&[u8]>, b"owner_before_snapshot")
            .await?,
        "Should acquire lock"
    );
    for i in 0..FILLER_WRITES {
        leader_client
            .put(
                format!("key_{i}").as_bytes(),
                format!("value_{i}").as_bytes(),
            )
            .await?;
    }

    let follower_snapshots = data_dir.join(format!("node{follower_id}/db/snapshots"));
    assert!(
        wait_for_snapshot(&follower_snapshots, Duration::from_secs(15)).await,
        "node {follower_id} must take a snapshot after {SNAPSHOT_THRESHOLD} entries"
    );

    // Stop only after the follower applied the last write, so every write is on its disk.
    let follower = &engines[follower_id as usize - 1];
    let last_key = format!("key_{}", FILLER_WRITES - 1);
    let mut caught_up = false;
    for _ in 0..50 {
        if follower.client().get_eventual(last_key.as_bytes()).await?.is_some() {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        caught_up,
        "node {follower_id} must apply every write before it is stopped"
    );

    for engine in engines.drain(..) {
        engine.stop().await?;
    }

    let follower = start_node(
        follower_id,
        &data_dir,
        &config_paths[follower_id as usize - 1],
    )
    .await?;
    let client = follower.client();
    assert_eq!(
        client.get_eventual(lock_key).await?,
        Some(b"owner_before_snapshot".to_vec().into()),
        "the lock must be recovered from node {follower_id}'s own snapshot"
    );
    assert_eq!(
        client.get_eventual(b"key_0").await?,
        Some(b"value_0".to_vec().into()),
        "the oldest write must be recovered from node {follower_id}'s own snapshot"
    );

    // The rest of the cluster comes back; the recovered lock still works as a lock.
    let mut engines = vec![follower];
    for node_id in (1..=3u64).filter(|&id| id != follower_id) {
        engines.push(start_node(node_id, &data_dir, &config_paths[node_id as usize - 1]).await?);
    }
    let leader_id = engines[0].wait_ready(Duration::from_secs(10)).await?.leader_id;
    let leader_client = engines
        .iter()
        .find(|e| e.node_id() == leader_id)
        .expect("leader is one of the nodes")
        .client()
        .clone();
    assert!(
        leader_client
            .compare_and_swap(lock_key, Some(b"owner_before_snapshot"), b"")
            .await?,
        "Should release lock"
    );
    assert!(
        leader_client.compare_and_swap(lock_key, Some(b""), b"new_owner").await?,
        "Should re-acquire lock"
    );
    assert_eq!(
        leader_client.get(lock_key).await?,
        Some(b"new_owner".to_vec().into())
    );

    for engine in engines {
        engine.stop().await?;
    }
    Ok(())
}
