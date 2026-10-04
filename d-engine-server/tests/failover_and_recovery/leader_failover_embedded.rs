use std::sync::Arc;
use std::time::Duration;

use d_engine_core::client::ClientApiError;
use d_engine_core::client::ErrorCode;
use d_engine_core::client::LeaderHint;
use d_engine_server::RocksDBUnifiedEngine;
use d_engine_server::api::DefaultEmbeddedEngine;
use tracing::info;
use tracing_test::traced_test;

use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::wait_for_new_leader;

/// Test 3-node cluster leader failover with DefaultEmbeddedEngine API
///
/// Scenario:
/// 1. Start 3-node cluster
/// 2. Kill leader node
/// 3. Verify re-election and data consistency
/// 4. Verify cluster operational with 2/3 nodes
#[tokio::test]
#[traced_test]
async fn test_embedded_leader_failover() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster");

    let mut engines = Vec::new();
    let mut configs = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            data_dir.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        // Each node needs its own storage directory to avoid RocksDB lock conflicts
        let node_db_root = data_dir.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");

        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;

        let config_path = format!("/tmp/d-engine-test-failover-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        configs.push((config_str, config_path));

        let engine = DefaultEmbeddedEngine::start_custom(
            &node_db_root,
            Arc::new(storage),
            Arc::new(state_machine),
            Some(&configs[i].1),
        )
        .await?;
        engines.push(engine);
    }

    // Wait for initial leader
    let initial_leader = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("Failed to elect initial leader");
    info!(
        "Initial leader elected: {} (term {})",
        initial_leader.leader_id, initial_leader.term
    );

    let leader_idx = (initial_leader.leader_id - 1) as usize;

    // Idle first: no client traffic for 10x election_timeout_min. Write admission depends on
    // heartbeat ACKs only, so the first write after a quiet period must still be admitted.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Write some data to initial leader
    engines[leader_idx]
        .client()
        .put(b"before-failover".to_vec(), b"initial-value".to_vec())
        .await?;

    // Wait for replication to all nodes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify data replicated to all nodes (eventual consistency)
    for engine in &engines {
        let val = engine.client().get_eventual(b"before-failover".to_vec()).await?;
        assert_eq!(val.as_deref(), Some(b"initial-value".as_slice()));
    }
    info!("Initial data written successfully");

    // Writing to a follower must be rejected with the real leader's address, so the
    // caller (e.g. d-lmdb/d-stream, running fully in-process against EmbeddedClient)
    // can redirect without ever touching gRPC/tonic. See ticket for #425 follow-up.
    let follower_idx = (0..3).find(|&i| i != leader_idx).expect("cluster has followers");
    let follower_result = engines[follower_idx]
        .client()
        .put(b"should-reject".to_vec(), b"wrong-node".to_vec())
        .await;
    match follower_result {
        Err(ClientApiError::Network {
            code: ErrorCode::NotLeader,
            leader_hint,
            ..
        }) => {
            assert_eq!(
                leader_hint,
                Some(LeaderHint {
                    leader_id: initial_leader.leader_id,
                    // Membership::get_address normalizes to a scheme-prefixed URI
                    // (see d-engine-server/src/utils/net.rs::address_str) so the
                    // hint is directly usable to build a GrpcClient.
                    address: format!("http://127.0.0.1:{}", ports[leader_idx]),
                }),
                "follower rejection must carry the real leader's id and address"
            );
        }
        other => panic!(
            "write to follower must return Network{{NotLeader, leader_hint}}, got: {other:?}"
        ),
    }
    info!("Confirmed: write to follower carries real leader_hint");

    // Kill the actual leader node
    let leader_idx = (initial_leader.leader_id - 1) as usize;
    info!("Killing leader node {}", initial_leader.leader_id);
    let killed_engine = engines.remove(leader_idx);
    let _killed_config = configs.remove(leader_idx);
    killed_engine.stop().await?;

    // Wait for re-election on any surviving node
    info!("Waiting for new leader election");
    let receivers = engines.iter().map(|e| e.leader_change_notifier()).collect();
    let new_leader_info = wait_for_new_leader(
        receivers,
        initial_leader.leader_id,
        Duration::from_secs(120),
    )
    .await;

    assert_ne!(
        new_leader_info.leader_id, initial_leader.leader_id,
        "New leader should not be the killed node"
    );
    info!(
        "New leader elected: {} (term {})",
        new_leader_info.leader_id, new_leader_info.term
    );

    // Find new leader engine to write
    let mut leader_client = None;
    for engine in &engines {
        if engine.node_id() == new_leader_info.leader_id {
            leader_client = Some(engine.client());
            break;
        }
    }
    let leader_client = leader_client.expect("New leader not found in engines");

    // Cluster should still be operational with 2/3 nodes
    leader_client.put(b"after-failover".to_vec(), b"still-works".to_vec()).await?;

    // Allow time for state machine application
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify old data still readable (from surviving follower)
    let old_val = engines[0].client().get_eventual(b"before-failover".to_vec()).await?;
    assert_eq!(
        old_val.as_deref(),
        Some(b"initial-value".as_slice()),
        "Old data should be preserved"
    );

    // Verify new data written successfully (read from Leader with strong consistency)
    let new_val = leader_client.get_linearizable(b"after-failover".to_vec()).await?;
    assert_eq!(
        new_val.as_deref(),
        Some(b"still-works".as_slice()),
        "New data should be written"
    );

    info!("Cluster operational with 2/3 nodes");

    // Cleanup
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}

/// Test node rejoin after temporary failure
///
/// Scenario:
/// 1. Start 3-node cluster
/// 2. Kill a follower node
/// 3. Verify cluster still operational (2/3 quorum)
/// 4. Restart killed follower
/// 5. Verify it rejoins and syncs data
#[tokio::test]
#[traced_test]
async fn test_embedded_node_rejoin() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for rejoin test");

    let mut engines = Vec::new();
    let mut configs = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let node_db_root = db_root.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");

        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;

        let config_path = format!("/tmp/d-engine-test-rejoin-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        configs.push((config_str, config_path));

        let engine = DefaultEmbeddedEngine::start_custom(
            &node_db_root,
            Arc::new(storage),
            Arc::new(state_machine),
            Some(&configs[i].1),
        )
        .await?;
        engines.push(engine);
    }

    let leader_info = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("Failed to elect leader");
    info!(
        "Leader elected: {} (term {})",
        leader_info.leader_id, leader_info.term
    );

    let leader_idx = (leader_info.leader_id - 1) as usize;

    // Write initial data
    engines[leader_idx]
        .client()
        .put(b"before-kill".to_vec(), b"initial".to_vec())
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Find a follower to kill (not the leader)
    let follower_idx = if leader_idx == 0 { 1 } else { 0 };
    let follower_id = (follower_idx + 1) as u64;

    info!("Killing follower node {}", follower_id);
    let killed_engine = engines.remove(follower_idx);
    let killed_config = configs.remove(follower_idx);
    killed_engine.stop().await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Cluster should still work with 2/3 nodes
    let remaining_leader_idx =
        engines.iter().position(|e| e.node_id() == leader_info.leader_id).unwrap();
    engines[remaining_leader_idx]
        .client()
        .put(b"after-kill".to_vec(), b"still-works".to_vec())
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    info!(
        "Cluster operational with 2/3 nodes, restarting follower {}",
        follower_id
    );

    // Restart the killed follower
    let node_db_root = db_root.join(format!("node{follower_id}"));
    let db_path = node_db_root.join("db");

    // Retry opening DB to tolerate async cleanup delays under parallel test load.
    // Arc<DB> release may lag behind stop() completion when tokio is under heavy load.
    let (storage, state_machine) = {
        let mut last_err: Option<Box<dyn std::error::Error>> = None;
        let mut opened = None;
        for attempt in 0..50 {
            match RocksDBUnifiedEngine::open(&db_path) {
                Ok(ok) => {
                    opened = Some(ok);
                    break;
                }
                Err(e) if e.to_string().contains("lock hold by current process") => {
                    info!("DB LOCK held (attempt {attempt}), retrying in 100ms...");
                    last_err = Some(e.into());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        opened.ok_or_else(|| last_err.unwrap())?
    };

    let restarted_engine = DefaultEmbeddedEngine::start_custom(
        &node_db_root,
        Arc::new(storage),
        Arc::new(state_machine),
        Some(&killed_config.1),
    )
    .await?;
    restarted_engine.wait_ready(Duration::from_secs(30)).await?;

    // Wait for sync
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify restarted follower synced all data
    let val1 = restarted_engine.client().get_eventual(b"before-kill".to_vec()).await?;
    assert_eq!(
        val1.as_deref(),
        Some(b"initial".as_slice()),
        "Should sync old data"
    );

    let val2 = restarted_engine.client().get_eventual(b"after-kill".to_vec()).await?;
    assert_eq!(
        val2.as_deref(),
        Some(b"still-works".as_slice()),
        "Should sync new data written while offline"
    );

    info!("Follower {} rejoined and synced successfully", follower_id);

    // Cleanup
    engines.push(restarted_engine);
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}

/// Test minority failure (2/3 nodes down) causes cluster unavailability
#[tokio::test]
#[traced_test]
async fn test_minority_failure_blocks_writes() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for minority failure test");

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let node_db_root = db_root.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");

        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;

        let config_path = format!("/tmp/d-engine-test-minority-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine = DefaultEmbeddedEngine::start_custom(
            &node_db_root,
            Arc::new(storage),
            Arc::new(state_machine),
            Some(&config_path),
        )
        .await?;
        engines.push(engine);
    }

    let leader_info = engines[0].wait_ready(Duration::from_secs(10)).await?;
    info!(
        "Leader elected successfully: node {}",
        leader_info.leader_id
    );

    // Write initial data to the actual leader
    info!(
        "Writing initial test data to leader (node {})",
        leader_info.leader_id
    );
    let leader_idx = (leader_info.leader_id - 1) as usize;
    engines[leader_idx]
        .client()
        .put(b"test-key".to_vec(), b"test-value".to_vec())
        .await?;
    info!("Initial data written successfully");

    info!("Killing 2 nodes to lose majority (keeping leader alive but unable to get quorum)");

    // Kill 2 non-leader nodes, leaving the leader isolated without majority
    // Indices: 0, 1, 2 -> nodes: 1, 2, 3
    let mut indices_to_kill = vec![0, 1, 2];
    indices_to_kill.remove(leader_idx); // Remove leader index
    indices_to_kill.truncate(2); // Take first 2 non-leader indices

    info!(
        "Killing nodes at engine indices: {:?} (leader is at index {})",
        indices_to_kill, leader_idx
    );

    // Remove in reverse order to avoid index shifting issues
    let mut killed_engines = Vec::new();
    for &idx in indices_to_kill.iter().rev() {
        let engine = engines.remove(idx);
        killed_engines.push(engine);
    }

    // Stop killed engines
    for engine in killed_engines {
        let _ = engine.stop().await;
    }

    info!("Sleeping 2 seconds for cluster to stabilize");
    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("2 nodes killed, verifying leader cannot serve writes without majority");
    info!("Remaining engine count: {}", engines.len());

    // The leader (now alone) should reject writes since it can't reach quorum
    info!("Attempting write on isolated leader (should be rejected with NotLeader)");
    let write_result = tokio::time::timeout(
        Duration::from_secs(3),
        engines[0].client().put(b"should-fail".to_vec(), b"no-majority".to_vec()),
    )
    .await;

    info!("Write result: {:?}", write_result);

    // 2 s without a quorum ACK is far beyond election_timeout_min (300 ms): write admission must
    // answer at once with NotLeader instead of queueing the write until the client times out.
    match &write_result {
        Ok(Err(ClientApiError::Network {
            code: ErrorCode::NotLeader,
            ..
        })) => info!("Write correctly rejected with NotLeader"),
        other => panic!("expected an immediate NotLeader from write admission, got: {other:?}"),
    }

    // CheckQuorum: after election_timeout_max x multiple (3 s x 2) of silence the leader steps
    // down, and the client keeps seeing NotLeader (no leader to redirect to).
    let step_down_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while engines[0].is_leader() {
        assert!(
            tokio::time::Instant::now() < step_down_deadline,
            "leader without a quorum must step down within election_timeout_max x multiple"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after_step_down = engines[0].client().put(b"after-step-down".to_vec(), b"x".to_vec()).await;
    assert!(
        matches!(
            after_step_down,
            Err(ClientApiError::Network {
                code: ErrorCode::NotLeader,
                ..
            })
        ),
        "a stepped-down node must answer NotLeader, got: {after_step_down:?}"
    );

    info!("Minority failure test passed - cluster correctly refused writes");

    // Cleanup
    let remaining_engine = engines.remove(0);
    let _ = remaining_engine.stop().await;

    Ok(())
}

/// Test an isolated node does not inflate its term, so it cannot disturb the cluster later (#423)
///
/// Scenario:
/// 1. Start node 3 alone: its peers are not running, so it cannot get a quorum (the repo has no
///    fault injection; "started before its peers" is the isolation)
/// 2. Let it time out many times (15 s, at least 5 rounds at election_timeout_max = 3 s)
/// 3. Start nodes 1 and 2 and wait for a leader
/// 4. The leader's term must be small: without PreVote node 3 would have raised its term once
///    per round and the whole cluster would have adopted it (term >= 6)
#[tokio::test]
#[traced_test]
async fn test_isolated_node_does_not_inflate_term() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    async fn start_node(
        node_id: u64,
        ports: &[u16],
        db_root: &std::path::Path,
        log_dir: &std::path::Path,
    ) -> Result<DefaultEmbeddedEngine, Box<dyn std::error::Error>> {
        let config_str = create_node_config(
            node_id,
            ports[(node_id - 1) as usize],
            ports,
            db_root.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;
        let node_db_root = db_root.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");
        tokio::fs::create_dir_all(&db_path).await?;
        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;
        let config_path = format!("/tmp/d-engine-test-isolated-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;
        Ok(DefaultEmbeddedEngine::start_custom(
            &node_db_root,
            Arc::new(storage),
            Arc::new(state_machine),
            Some(&config_path),
        )
        .await?)
    }

    info!("Starting node 3 alone");
    let node3 = start_node(3, ports, &db_root, &log_dir).await?;
    tokio::time::sleep(Duration::from_secs(15)).await;

    info!("Starting nodes 1 and 2");
    let node1 = start_node(1, ports, &db_root, &log_dir).await?;
    let node2 = start_node(2, ports, &db_root, &log_dir).await?;

    let leader = node1.wait_ready(Duration::from_secs(30)).await?;
    info!("Leader {} at term {}", leader.leader_id, leader.term);
    assert!(
        leader.term <= 4,
        "an isolated node must not inflate the cluster's term, leader term was {}",
        leader.term
    );

    node1
        .client()
        .put(b"after-isolation".to_vec(), b"ok".to_vec())
        .await
        .or_else(|e| match e {
            // The write may land on a follower right after election: only the term matters here.
            ClientApiError::Network {
                code: ErrorCode::NotLeader,
                ..
            } => Ok(()),
            other => Err(other),
        })?;

    for engine in [node1, node2, node3] {
        engine.stop().await?;
    }
    Ok(())
}
