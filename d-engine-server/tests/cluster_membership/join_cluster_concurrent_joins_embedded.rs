//! Two nodes join a running cluster at the same time and catch up from the leader's snapshot,
//! while the leader keeps serving reads and writes (Embedded mode).

#![cfg(feature = "rocksdb")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use d_engine_core::capture_logs_globally_filtered;
use d_engine_core::logs_contain_globally_since;
use d_engine_server::DefaultEmbeddedEngine;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::StateMachine;
use d_engine_server::install_snapshot_transfer_gate;
use d_engine_server::open_rocksdb;
use serial_test::serial;
use tracing::info;

use crate::common::SharedStateMachine;
use crate::common::get_available_ports;

const SNAPSHOT_THRESHOLD: u64 = 64;
const RETAINED_LOGS: u64 = 8;
const BASELINE_ENTRIES: u64 = 80;
const VALUE_SIZE: usize = 16 * 1024;
const CHUNK_SIZE: usize = 64 * 1024;
const WRITES_DURING_TRANSFER: u64 = 20;

/// Incompressible payload, so the snapshot keeps a realistic size under gzip.
fn deterministic_value(
    seed: u64,
    len: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..len {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        out.push((s & 0xFF) as u8);
    }
    out
}

/// TOML config for one node. `joining` lists the new nodes this node knows about.
fn node_toml(
    node_id: u64,
    ports: &[u16],
    joining: &[u64],
) -> String {
    let mut cluster = String::new();
    for id in 1u64..=3 {
        cluster.push_str(&format!(
            "    {{ id = {id}, name = 'n{id}', address = '127.0.0.1:{}', role = 1, status = 3 }},\n",
            ports[id as usize - 1]
        ));
    }
    for &id in joining {
        // Learner (role 4) that is promoted to voter once it has caught up (status 1).
        cluster.push_str(&format!(
            "    {{ id = {id}, name = 'n{id}', address = '127.0.0.1:{}', role = 4, status = 1 }},\n",
            ports[id as usize - 1]
        ));
    }
    format!(
        r#"
[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{}'
initial_cluster = [
{cluster}]

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
chunk_size = {CHUNK_SIZE}
push_queue_size = 1
"#,
        ports[node_id as usize - 1],
    )
}

/// Two nodes join a running 3-node cluster at the same time. The leader has purged the log
/// they need, so it must send each of them a snapshot, and it keeps serving reads and writes
/// while both transfers are in flight.
///
/// # What this proves
/// - The leader handles two join requests that arrive together; neither is lost.
/// - The leader streams a snapshot to two new nodes at once.
/// - While both transfers are in flight, the leader still commits writes and answers
///   linearizable reads.
/// - Both new nodes install the snapshot, catch up, and are promoted: the cluster ends with five
///   voters and the same leader.
///
/// # How "in flight" is observed
/// A test-only transfer gate for each new node holds the real push right before its first
/// chunk. The test waits until both gates report "started", so both transfers are known to be
/// in flight at the same time, with no reliance on timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_two_nodes_join_concurrently_and_leader_serves_during_snapshot_transfer()
-> Result<(), Box<dyn std::error::Error>> {
    // A snapshot file on disk does not mean the log is purged. The new nodes must join only
    // after the leader logs its purge, otherwise they could catch up through AppendEntries and
    // the snapshot path would not be tested. The baseline is taken before any write, because the
    // purge runs in the background and can finish while the writes are still going.
    let logs = capture_logs_globally_filtered(
        "info,d_engine_core=debug,d_engine_core::state_machine_handler=trace,d_engine_server=debug,h2=off,tonic=warn,hyper=warn",
    );
    let since = logs.lock().unwrap().len();

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");

    let mut port_guard = get_available_ports(5).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // 1. Start the 3-node cluster.
    let mut engines = Vec::new();
    let mut sms: Vec<Arc<RocksDBStateMachine>> = Vec::new();
    for node_id in 1u64..=3 {
        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, node_toml(node_id, ports, &[])).await?;
        let db_path = data_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;
        let (storage, sm) = open_rocksdb(&db_path)?;
        let sm = Arc::new(sm);
        sms.push(Arc::clone(&sm));
        engines.push(
            DefaultEmbeddedEngine::start_custom(
                &db_path,
                Arc::new(storage),
                SharedStateMachine::new(sm),
                Some(config_path.to_str().unwrap()),
            )
            .await?,
        );
    }

    let leader_id = engines[0].wait_ready(Duration::from_secs(15)).await?.leader_id;
    let leader_idx = engines
        .iter()
        .position(|e| e.node_id() == leader_id)
        .expect("leader is a voter");
    let leader = engines[leader_idx].client().clone();
    let leader_sm = Arc::clone(&sms[leader_idx]);

    // 2. Write enough to make the leader snapshot and purge its log.
    info!("Writing {BASELINE_ENTRIES} large entries to trigger snapshot + purge");
    for i in 0..BASELINE_ENTRIES {
        leader.put(format!("key_{i}"), deterministic_value(i, VALUE_SIZE)).await?;
    }
    // The capture buffer is shared by the whole process: match the leader's own purge line.
    // SNAPSHOT_THRESHOLD > RETAINED_LOGS, so a purge line here always means a real purge.
    let purge_needle = format!("node_id={leader_id} purge_upto_index=");
    let purged = tokio::time::timeout(Duration::from_secs(30), async {
        while !logs_contain_globally_since(&logs, since, &purge_needle) {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(
        purged.is_ok(),
        "leader never purged its log; the new nodes could catch up without a snapshot"
    );

    // 3. Start nodes 4 and 5 at the same time. Each gate is registered before its node starts,
    // so the leader cannot finish a transfer before the test can hold it.
    let (gate4, _guard4) = install_snapshot_transfer_gate(4);
    let (gate5, _guard5) = install_snapshot_transfer_gate(5);

    let mut joiner_sms = Vec::new();
    let mut joiner_starts = Vec::new();
    for node_id in [4u64, 5] {
        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, node_toml(node_id, ports, &[node_id])).await?;
        let db_path = data_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;
        let (storage, sm) = open_rocksdb(&db_path)?;
        let sm = Arc::new(sm);
        joiner_sms.push(Arc::clone(&sm));
        joiner_starts.push(async move {
            DefaultEmbeddedEngine::start_custom(
                &db_path,
                Arc::new(storage),
                SharedStateMachine::new(sm),
                Some(config_path.to_str().unwrap()),
            )
            .await
        });
    }
    let mut joiner_starts = joiner_starts.into_iter();
    let (node4, node5) = tokio::join!(joiner_starts.next().unwrap(), joiner_starts.next().unwrap());
    let (node4, node5) = (node4?, node5?);
    info!("Nodes 4 and 5 started together");

    // 4. Both transfers are in flight at the same time.
    tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(gate4.wait_started(), gate5.wait_started())
    })
    .await
    .expect("the leader did not start a snapshot transfer to both new nodes");
    info!("Snapshot transfers to nodes 4 and 5 are both held mid-flight");

    // 5. The leader serves writes and linearizable reads while both transfers are held.
    let applied_before = leader_sm.last_applied().index;
    for i in 0..WRITES_DURING_TRANSFER {
        leader.put(format!("during_{i}"), format!("value_{i}")).await?;
        let read = leader.get_linearizable(format!("during_{i}")).await?;
        assert_eq!(
            read.as_deref(),
            Some(format!("value_{i}").as_bytes()),
            "a linearizable read during the snapshot transfer must see the write before it"
        );
    }
    assert!(
        leader_sm.last_applied().index >= applied_before + WRITES_DURING_TRANSFER,
        "the leader must apply the writes while the transfers are in flight"
    );
    assert!(
        !gate4.is_completed() && !gate5.is_completed(),
        "both transfers must still be in flight; otherwise the writes above did not overlap them"
    );

    // 6. Let both transfers finish. Both new nodes catch up and become voters.
    gate4.release();
    gate5.release();

    let target = leader_sm.last_applied().index;
    let membership = engines[leader_idx].watch_membership();
    let all_voters: BTreeSet<u32> = (1..=5).collect();
    let caught_up = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let synced = joiner_sms.iter().all(|sm| sm.last_applied().index >= target);
            let promoted = membership.borrow().members == all_voters;
            if synced && promoted {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(
        caught_up.is_ok(),
        "nodes 4 and 5 must catch up to index {target} and become voters; applied: 4={}, 5={}, \
         voters: {:?}",
        joiner_sms[0].last_applied().index,
        joiner_sms[1].last_applied().index,
        membership.borrow().members
    );

    // The snapshot carried the data written before the join; the log carried the rest.
    for sm in &joiner_sms {
        assert_eq!(
            sm.get(b"key_0")?.as_deref(),
            Some(deterministic_value(0, VALUE_SIZE).as_slice()),
            "data written before the join must reach the new node through the snapshot"
        );
        let last = WRITES_DURING_TRANSFER - 1;
        assert_eq!(
            sm.get(format!("during_{last}").as_bytes())?.as_deref(),
            Some(format!("value_{last}").as_bytes()),
            "writes made during the transfer must reach the new node"
        );
    }

    // No leader change while the nodes joined.
    let leader_now = engines[0].wait_ready(Duration::from_secs(5)).await?.leader_id;
    assert_eq!(
        leader_now, leader_id,
        "the leader must not change while nodes join"
    );

    for engine in engines.iter().chain([&node4, &node5]) {
        engine.stop().await?;
    }
    Ok(())
}
