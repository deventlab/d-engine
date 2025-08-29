//! Case 1: Verify that Node 3 has snapshot generated
//!
//! Scenario:
//!
//! 1. Create a cluster with 3 nodes (1, 2, 3).
//! 2. Node 1 appends 3 log entries with Term=1.
//! 3. Node 2 appends 4 log entries with Term=1.
//! 4. Node 3 appends 10 log entries with Term=2.
//! 5. All three node state machine has log1, 2, 3
//! 6. Node 3 will be Leader
//! 7. According to the config `max_log_entries_before_snapshot = 1`, snapshot should be generated
//!    in node 3
//!
//! Expected Result:
//!
//! - Node 3 becomes the leader
//! - last_commit_index is 10
//! - Node 1 and 2's log-3's term is 2

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use d_engine::client::ClientApiError;
use d_engine::convert::safe_kv;
use d_engine::storage::StateMachine;
use d_engine::storage::StorageEngine;
use d_engine::LogStore;
use tokio::time::sleep;

use crate::common::check_cluster_is_ready;
use crate::common::check_path_contents;
use crate::common::create_node_config;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_state_machine;
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::SNAPSHOT_PORT_BASE;

// Constants for test configuration
const SNAPSHOT_DIR: &str = "./snapshots/snapshot/case1";
const SNAPSHOT_CASE1_DIR: &str = "snapshot/case1";
const SNAPSHOT_CASE1_DB_ROOT_DIR: &str = "./db/snapshot/case1";
const SNAPSHOT_CASE1_LOG_DIR: &str = "./logs/snapshot/case1";

/// The current test relies on the following snapshot configuration:
/// When the number of log entries exceeds 1, a snapshot will be triggered.
/// [raft.snapshot]
/// max_log_entries_before_snapshot = 1
#[tokio::test]
async fn test_snapshot_scenario() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset(SNAPSHOT_CASE1_DIR).await?;

    let ports = [
        SNAPSHOT_PORT_BASE + 1,
        SNAPSHOT_PORT_BASE + 2,
        SNAPSHOT_PORT_BASE + 3,
    ];

    // Prepare state machines
    let sm1 =
        Arc::new(prepare_state_machine(1, &format!("{}/cs/1", SNAPSHOT_CASE1_DB_ROOT_DIR)).await);
    let sm2 =
        Arc::new(prepare_state_machine(2, &format!("{}/cs/2", SNAPSHOT_CASE1_DB_ROOT_DIR)).await);
    let sm3 =
        Arc::new(prepare_state_machine(3, &format!("{}/cs/3", SNAPSHOT_CASE1_DB_ROOT_DIR)).await);

    // Prepare raft logs
    let r1 = prepare_storage_engine(1, &format!("{}/cs/1", SNAPSHOT_CASE1_DB_ROOT_DIR), 0);
    let r2 = prepare_storage_engine(2, &format!("{}/cs/2", SNAPSHOT_CASE1_DB_ROOT_DIR), 0);
    let r3 = prepare_storage_engine(3, &format!("{}/cs/3", SNAPSHOT_CASE1_DB_ROOT_DIR), 0);

    let last_log_id: u64 = 10;
    manipulate_log(&r1, vec![1, 2, 3], 1).await;
    init_hard_state(&r1, 1, None);
    manipulate_log(&r2, vec![1, 2, 3, 4], 1).await;
    init_hard_state(&r2, 1, None);
    manipulate_log(&r3, (1..=3).collect(), 1).await;
    init_hard_state(&r3, 2, None);
    manipulate_log(&r3, (4..=last_log_id).collect(), 2).await;

    // Start cluster nodes
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    // To maintain the last included index of the snapshot, because of the configure:
    // retained_log_entries. e.g. if leader local raft log has 10 entries. but
    // retained_log_entries=1 , then the last included index of the snapshot should be 9.
    let mut snapshot_last_included_id: Option<u64> = None;
    for (i, port) in ports.iter().enumerate() {
        let node_id = (i + 1) as u64;
        let config = create_node_config(
            node_id,
            *port,
            &ports,
            &format!("{}/cs/{}", SNAPSHOT_CASE1_DB_ROOT_DIR, i + 1),
            SNAPSHOT_CASE1_LOG_DIR,
        )
        .await;

        let (state_machine, raft_log) = match i {
            0 => (Some(sm1.clone()), Some(r1.clone())),
            1 => (Some(sm2.clone()), Some(r2.clone())),
            2 => (Some(sm3.clone()), Some(r3.clone())),
            _ => (None, None),
        };

        let mut node_config = node_config(&config);

        node_config.raft.snapshot.snapshots_dir =
            PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, node_id));
        //Dirty code: could leave it like this for now.
        snapshot_last_included_id =
            Some(last_log_id.saturating_sub(node_config.raft.snapshot.retained_log_entries));

        let (graceful_tx, node_handle) = start_node(node_config, state_machine, raft_log).await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }
    let last_included = snapshot_last_included_id.unwrap();

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[test_snapshot_scenario] Cluster started. Running tests...");

    sleep(Duration::from_secs(3)).await;
    let leader_snapshot_metadata = sm3.snapshot_metadata().unwrap();

    // Verify snapshot file exists
    let snapshot_path = "./snapshots/snapshot/case1/3";
    assert!(check_path_contents(snapshot_path).unwrap_or(false));

    // Verify snapshot metadata
    assert!(leader_snapshot_metadata.last_included.unwrap().index >= last_included);
    assert!(!leader_snapshot_metadata.checksum.is_empty());

    // Verify state machine status
    let value = sm3.get(&safe_kv(3)).unwrap();
    assert_eq!(value, Some(safe_kv(3).to_vec()));

    // Verify raft log been purged
    for i in 1..=3 {
        assert!(r3.log_store().entry(i).await.unwrap().is_none());
    }

    // Clean up
    ctx.shutdown().await
}
