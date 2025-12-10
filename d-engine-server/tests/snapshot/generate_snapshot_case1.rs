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

use d_engine_client::ClientApiError;
use d_engine_server::LogStore;
use d_engine_server::StorageEngine;
use tokio::time::sleep;
use tracing_test::traced_test;

use crate::client_manager::ClientManager;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::check_path_contents;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_state_machine;
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;

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
#[traced_test]
async fn test_snapshot_scenario() -> Result<(), ClientApiError> {
    reset(SNAPSHOT_CASE1_DIR).await?;

    let _port_guard = get_available_ports(3).await;
    let ports = _port_guard.as_slice();

    // Prepare state machine directories (do not pre-allocate Arc to avoid ownership issues)
    prepare_state_machine(1, &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/1")).await;
    prepare_state_machine(2, &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/2")).await;
    prepare_state_machine(3, &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/3")).await;

    // Prepare raft logs
    let r1 = prepare_storage_engine(1, &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/1"), 0);
    let r2 = prepare_storage_engine(2, &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/2"), 0);
    let r3 = prepare_storage_engine(3, &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/3"), 0);

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
            ports,
            &format!("{}/cs/{}", SNAPSHOT_CASE1_DB_ROOT_DIR, i + 1),
            SNAPSHOT_CASE1_LOG_DIR,
        )
        .await;

        // Create fresh Arc for state machine to ensure single ownership
        let state_machine = Arc::new(
            prepare_state_machine(
                node_id as u32,
                &format!("{SNAPSHOT_CASE1_DB_ROOT_DIR}/cs/{node_id}"),
            )
            .await,
        );

        let raft_log = match i {
            0 => Some(r1.clone()),
            1 => Some(r2.clone()),
            2 => Some(r3.clone()),
            _ => None,
        };

        let mut node_config = node_config(&config);

        node_config.raft.snapshot.snapshots_dir =
            PathBuf::from(format!("{SNAPSHOT_DIR}/{node_id}"));
        //Dirty code: could leave it like this for now.
        snapshot_last_included_id =
            Some(last_log_id.saturating_sub(node_config.raft.snapshot.retained_log_entries));

        let (graceful_tx, node_handle) =
            start_node(node_config, Some(state_machine), raft_log).await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }
    let _last_included = snapshot_last_included_id.unwrap();

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[test_snapshot_scenario] Cluster started. Running tests...");

    sleep(Duration::from_secs(3)).await;

    // Verify snapshot file exists on leader (node 3)
    let snapshot_path = "./snapshots/snapshot/case1/3";
    assert!(check_path_contents(snapshot_path).unwrap_or(false));

    // Verify state machine data via client API (snapshot has been applied to leader)
    let mut client_manager = ClientManager::new(&create_bootstrap_urls(ports)).await?;

    // Verify data via client API - this confirms snapshot was applied and committed
    test_put_get(&mut client_manager, 3, 3).await?;

    // Verify raft log been purged (log entries before snapshot should be deleted)
    for i in 1..=3 {
        assert!(r3.log_store().entry(i).await.unwrap().is_none());
    }

    // Clean up
    ctx.shutdown().await
}
