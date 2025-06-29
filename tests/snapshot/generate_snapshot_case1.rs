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

use std::sync::Arc;
use std::time::Duration;

use d_engine::client::ClientApiError;
use d_engine::convert::safe_kv;
use d_engine::storage::RaftLog;
use d_engine::storage::StateMachine;
use tokio::time::sleep;

use crate::common::check_cluster_is_ready;
use crate::common::check_path_contents;
use crate::common::init_state_storage;
use crate::common::manipulate_log;
use crate::common::manipulate_state_machine;
use crate::common::prepare_raft_log;
use crate::common::prepare_state_machine;
use crate::common::prepare_state_storage;
use crate::common::reset;
use crate::common::start_node;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::SNAPSHOT_PORT_BASE;

/// The current test relies on the following snapshot configuration:
/// When the number of log entries exceeds 1, a snapshot will be triggered.
/// [raft.snapshot]
/// max_log_entries_before_snapshot = 1
#[tracing::instrument]
#[tokio::test]
async fn test_snapshot_scenario() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset("snapshot/case1").await?;

    let port1 = SNAPSHOT_PORT_BASE + 1;
    let port2 = SNAPSHOT_PORT_BASE + 2;
    let port3 = SNAPSHOT_PORT_BASE + 3;

    // 1. Prepare state machine for node 1 so that we could read out the last applied id in this test
    println!("1. Prepare state_machine & raft_log");
    let sm1 = Arc::new(prepare_state_machine(1, "./db/snapshot/case1/cs/1"));
    let sm2 = Arc::new(prepare_state_machine(2, "./db/snapshot/case1/cs/2"));
    let sm3 = Arc::new(prepare_state_machine(3, "./db/snapshot/case1/cs/3"));
    let r1 = Arc::new(prepare_raft_log("./db/snapshot/case1/cs/1", 0));
    manipulate_log(&r1, vec![1, 2, 3], 1);
    manipulate_state_machine(&r1, &sm1, 1..=3);

    let r2 = Arc::new(prepare_raft_log("./db/snapshot/case1/cs/2", 0));
    manipulate_log(&r2, vec![1, 2, 3, 4], 1);
    manipulate_state_machine(&r2, &sm2, 1..=3);

    let r3 = Arc::new(prepare_raft_log("./db/snapshot/case1/cs/3", 0));
    manipulate_log(&r3, (1..=10).collect(), 2);
    manipulate_state_machine(&r3, &sm3, 1..=3);

    let ss1 = Arc::new(prepare_state_storage("./db/snapshot/case1/cs/1"));
    init_state_storage(&ss1, 1, None);
    let ss2 = Arc::new(prepare_state_storage("./db/snapshot/case1/cs/2"));
    init_state_storage(&ss2, 1, None);
    let ss3 = Arc::new(prepare_state_storage("./db/snapshot/case1/cs/3"));
    init_state_storage(&ss3, 2, None);

    // 2. Start a 3-node cluster
    println!("2. Start a 3-node cluster and artificially create inconsistent states");
    let (graceful_tx1, node_n1) =
        start_node("./tests/snapshot/case1/n1", Some(sm1.clone()), Some(r1), Some(ss1)).await?;
    let (graceful_tx2, node_n2) =
        start_node("./tests/snapshot/case1/n2", Some(sm2.clone()), Some(r2), Some(ss2)).await?;
    let (graceful_tx3, node_n3) = start_node(
        "./tests/snapshot/case1/n3",
        Some(sm3.clone()),
        Some(r3.clone()),
        Some(ss3),
    )
    .await?;

    // Combine all log layers
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [port1, port2, port3] {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("Cluster started. Running tests...");

    sleep(Duration::from_secs(3)).await;
    let leader_snapshot_metadata = sm3.snapshot_metadata().unwrap();

    // Verify snapshot file exists
    let snapshot_path = "./snapshots/snapshot/case1/3";
    assert!(check_path_contents(snapshot_path).unwrap_or(false));

    // Verify snapshot metadata
    assert_eq!(leader_snapshot_metadata.last_included.unwrap().index, 13);
    // Last log index
    assert!(!leader_snapshot_metadata.checksum.is_empty()); // Checksum is valid

    // Verify state machine status is preserved
    let value = sm3.get(&safe_kv(3)).unwrap();
    assert_eq!(value, Some(safe_kv(3).to_vec()));

    // Verify raft log been purged
    for i in 1..=3 {
        assert!(r3.get_entry_by_index(i).is_none());
    }

    // Wait nodes shutdown
    graceful_tx3
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    graceful_tx2
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    graceful_tx1
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    node_n3.await??;
    node_n2.await??;
    node_n1.await??;

    Ok(()) // Return Result type
}
