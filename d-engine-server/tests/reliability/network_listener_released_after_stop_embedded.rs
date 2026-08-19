//! Regression test for #438: graceful stop() doesn't guarantee the network layer
//! (and its existing peer connections) has actually closed before returning.

use std::sync::Arc;
use std::time::Duration;

use d_engine_core::capture_logs_globally_filtered;
use d_engine_core::logs_contain_globally;
use d_engine_server::DefaultEmbeddedEngine;
use d_engine_server::RocksDBUnifiedEngine;
use tracing::info;

use crate::common::get_available_ports;

/// Purpose: when a follower calls `stop()`, the leader's *already-open* replication
/// connection to it must be noticed as broken within a short, bounded window —
/// not "eventually, whenever the transport layer's keepalive happens to fire" (#438).
///
/// This reuses the 3-node cluster bootstrap pattern from
/// `snapshot_and_recovery/snapshot_correctness_after_install_embedded.rs`, stripped
/// down to the minimum needed to exercise #438: no data writes, no snapshot config —
/// just start a cluster, stop a follower, and check how fast the leader notices.
///
/// Poll window is generous (a few seconds) purely as a CI/OS-jitter tolerance — a
/// correct fix should make this near-instant, not "eventually within the window."
#[tokio::test]
async fn test_leader_detects_follower_stop_within_bounded_window()
-> Result<(), Box<dyn std::error::Error>> {
    let logs = capture_logs_globally_filtered(
        "info,d_engine_core=debug,d_engine_server=debug,h2=off,tonic=warn,hyper=warn",
    );
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut engines: Vec<Option<DefaultEmbeddedEngine>> = Vec::new();

    for node_id in 1u64..=3 {
        let config = format!(
            r#"
[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 1, status = 3 }}
]

[raft]
general_raft_timeout_duration_in_ms = 5000
"#,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
        );

        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, &config).await?;

        let db_path = data_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let engine = DefaultEmbeddedEngine::start_custom(
            &db_path,
            Arc::new(storage),
            Arc::new(sm),
            Some(config_path.to_str().unwrap()),
        )
        .await?;
        engines.push(Some(engine));
    }

    let leader_info = engines[0].as_ref().unwrap().wait_ready(Duration::from_secs(15)).await?;
    let leader_idx = engines
        .iter()
        .position(|e| e.as_ref().map(|e| e.node_id() == leader_info.leader_id).unwrap_or(false))
        .expect("leader must be one of the 3 engines");
    let leader_client = engines[leader_idx].as_ref().unwrap().client().clone();

    let follower_idx = engines
        .iter()
        .position(|e| e.as_ref().map(|e| !e.is_leader()).unwrap_or(false))
        .expect("must have at least one non-leader");
    let follower_node_id = engines[follower_idx].as_ref().unwrap().node_id();
    info!("Stopping follower node {follower_node_id}");

    // Force definite replication activity (not just relying on heartbeats) so we
    // can conclusively tell whether logs_contain_globally sees worker-task logs at all.
    leader_client.put(b"diag_key".to_vec(), b"diag_value".to_vec()).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let stopped = engines[follower_idx].take().unwrap();
    stopped.stop().await?;

    // The leader's worker for this peer should notice the connection is gone.
    // A graceful stop ends the stream cleanly (EOF), not with an error — so check
    // for every way the worker's recv_handle can report termination, not just the
    // error path. Poll window is a CI tolerance, not an expected delay: see doc
    // comment above.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut detected = false;
    while tokio::time::Instant::now() < deadline {
        if logs_contain_globally(&logs, "Bidi stream recv error")
            || logs_contain_globally(&logs, "Bidi stream sender closed")
            || logs_contain_globally(&logs, "Bidi stream ended (EOF)")
            || logs_contain_globally(&logs, "Replication stream receiver exited")
        {
            detected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        detected,
        "leader did not detect follower {follower_node_id}'s stop() within 3s — \
         the replication stream should have errored out promptly, not been left \
         dangling waiting on a transport-level keepalive timeout (#438)"
    );

    Ok(())
}
