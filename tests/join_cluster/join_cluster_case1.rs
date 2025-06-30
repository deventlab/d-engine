//! This test focuses on the scenario where a new node joins an existing cluster,
//! receives a snapshot, and successfully installs it.  
//! The test completes when the node transitions its role to `Follower`.

use std::sync::Arc;
use std::time::Duration;

use d_engine::client::ClientApiError;
use d_engine::convert::safe_kv;
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
use crate::JOIN_CLUSTER_PORT_BASE;

#[tracing::instrument]
#[tokio::test]
async fn test_join_cluster_scenario() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset("join_cluster/case1").await?;

    let port1 = JOIN_CLUSTER_PORT_BASE + 1;
    let port2 = JOIN_CLUSTER_PORT_BASE + 2;
    let port3 = JOIN_CLUSTER_PORT_BASE + 3;

    // 1. Prepare state machine for node 1 so that we could read out the last applied id in this test
    println!("1. Prepare state_machine & raft_log");
    let sm1 = Arc::new(prepare_state_machine(1, "./db/join_cluster/case1/cs/1"));
    let sm2 = Arc::new(prepare_state_machine(2, "./db/join_cluster/case1/cs/2"));
    let sm3 = Arc::new(prepare_state_machine(3, "./db/join_cluster/case1/cs/3"));
    let sm4 = Arc::new(prepare_state_machine(4, "./db/join_cluster/case1/cs/4"));
    let r1 = Arc::new(prepare_raft_log("./db/join_cluster/case1/cs/1", 0));
    manipulate_log(&r1, vec![1, 2, 3], 1);
    manipulate_state_machine(&r1, &sm1, 1..=3);

    let r2 = Arc::new(prepare_raft_log("./db/join_cluster/case1/cs/2", 0));
    manipulate_log(&r2, vec![1, 2, 3, 4], 1);
    manipulate_state_machine(&r2, &sm2, 1..=3);

    let r3 = Arc::new(prepare_raft_log("./db/join_cluster/case1/cs/3", 0));
    manipulate_log(&r3, (1..=10).collect(), 2);
    manipulate_state_machine(&r3, &sm3, 1..=3);

    let r4 = Arc::new(prepare_raft_log("./db/join_cluster/case1/cs/4", 0));

    let ss1 = Arc::new(prepare_state_storage("./db/join_cluster/case1/cs/1"));
    init_state_storage(&ss1, 1, None);
    let ss2 = Arc::new(prepare_state_storage("./db/join_cluster/case1/cs/2"));
    init_state_storage(&ss2, 1, None);
    let ss3 = Arc::new(prepare_state_storage("./db/join_cluster/case1/cs/3"));
    init_state_storage(&ss3, 2, None);

    // 2. Start a 3-node cluster
    println!("2. Start a 3-node cluster and artificially create inconsistent states");
    let (graceful_tx1, node_n1) =
        start_node("./tests/join_cluster/case1/n1", Some(sm1.clone()), Some(r1), Some(ss1)).await?;
    let (graceful_tx2, node_n2) =
        start_node("./tests/join_cluster/case1/n2", Some(sm2.clone()), Some(r2), Some(ss2)).await?;
    let (graceful_tx3, node_n3) = start_node(
        "./tests/join_cluster/case1/n3",
        Some(sm3.clone()),
        Some(r3.clone()),
        Some(ss3),
    )
    .await?;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [port1, port2, port3] {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[test_join_cluster_scenario] Cluster started. Running tests...");

    // 3. Wait for snapshot generation
    sleep(Duration::from_secs(3)).await;
    let leader_snapshot_metadata = sm3.snapshot_metadata().unwrap();
    // Verify snapshot file exists
    let snapshot_path = "./snapshots/join_cluster/case1/3";
    assert!(check_path_contents(snapshot_path).unwrap_or(false));
    // Verify snapshot metadata
    assert_eq!(leader_snapshot_metadata.last_included.unwrap().index, 13);
    // Last log index
    assert!(!leader_snapshot_metadata.checksum.is_empty()); // Checksum is valid

    // 4. Start a new node and try to join the cluster
    println!("Start a new node and try to join the cluster...");
    let (graceful_tx4, node_n4) = start_node(
        "./tests/join_cluster/case1/n4",
        Some(sm4.clone()),
        Some(r4.clone()),
        None,
    )
    .await?;

    sleep(Duration::from_secs(3)).await;

    // 5. Validate if node 4 has snapshot installed
    let snapshot_path = "./snapshots/join_cluster/case1/4";
    assert!(check_path_contents(snapshot_path).unwrap_or(false));

    // 6. Validate if node 4 state machine has log-14
    for i in 1..=10 {
        println!("{i} | get entry from state machine");
        let value = sm4.get(&safe_kv(i)).unwrap();
        assert_eq!(value, Some(safe_kv(i).to_vec()));
    }

    // Wait nodes shutdown
    graceful_tx4
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
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
    node_n4.await??;
    Ok(()) // Return Result type
}
