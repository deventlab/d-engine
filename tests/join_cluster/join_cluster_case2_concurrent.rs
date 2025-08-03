//! This test focuses on the scenario where a new node joins an existing cluster,
//! receives a snapshot, and successfully installs it.
//! The test completes when the node transitions its role to `Follower`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use d_engine::client::ClientApiError;
use d_engine::convert::safe_kv;
use d_engine::proto::common::NodeStatus;
use d_engine::storage::StateMachine;
use tokio::time::sleep;

use crate::client_manager::ClientManager;
use crate::common;
use crate::common::check_cluster_is_ready;
use crate::common::check_path_contents;
use crate::common::create_bootstrap_urls;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_raft_log;
use crate::common::prepare_state_machine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::JOIN_CLUSTER_PORT_BASE;
// Constants for test configuration
const JOIN_CLUSTER_CASE2_DIR: &str = "join_cluster/case2";
const SNAPSHOT_DIR: &str = "./snapshots/join_cluster/case2";
const JOIN_CLUSTER_CASE2_DB_ROOT_DIR: &str = "./db/join_cluster/case2";
const JOIN_CLUSTER_CASE2_LOG_DIR: &str = "./logs/join_cluster/case2";

#[tokio::test]
async fn test_join_cluster_scenario2() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset(JOIN_CLUSTER_CASE2_DIR).await?;

    let ports = [
        JOIN_CLUSTER_PORT_BASE + 11,
        JOIN_CLUSTER_PORT_BASE + 12,
        JOIN_CLUSTER_PORT_BASE + 13,
    ];
    let new_node_port4 = JOIN_CLUSTER_PORT_BASE + 14;
    let new_node_port5 = JOIN_CLUSTER_PORT_BASE + 15;

    // Prepare state machines
    let sm1 = Arc::new(prepare_state_machine(
        1,
        &format!("{}/cs/1", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
    ));
    let sm2 = Arc::new(prepare_state_machine(
        2,
        &format!("{}/cs/2", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
    ));
    let sm3 = Arc::new(prepare_state_machine(
        3,
        &format!("{}/cs/3", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
    ));
    let sm4 = Arc::new(prepare_state_machine(
        4,
        &format!("{}/cs/4", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
    ));
    let sm5 = Arc::new(prepare_state_machine(
        5,
        &format!("{}/cs/5", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
    ));

    // Prepare raft logs
    let r1 = prepare_raft_log(1, &format!("{}/cs/1", JOIN_CLUSTER_CASE2_DB_ROOT_DIR), 0);
    let r2 = prepare_raft_log(2, &format!("{}/cs/2", JOIN_CLUSTER_CASE2_DB_ROOT_DIR), 0);
    let r3 = prepare_raft_log(3, &format!("{}/cs/3", JOIN_CLUSTER_CASE2_DB_ROOT_DIR), 0);
    let r4 = prepare_raft_log(4, &format!("{}/cs/4", JOIN_CLUSTER_CASE2_DB_ROOT_DIR), 0);
    let r5 = prepare_raft_log(5, &format!("{}/cs/5", JOIN_CLUSTER_CASE2_DB_ROOT_DIR), 0);

    let last_log_id: u64 = 10;
    manipulate_log(&r1, vec![1, 2, 3], 1).await;
    init_hard_state(&r1, 1, None);
    manipulate_log(&r2, vec![1, 2, 3, 4], 1).await;
    init_hard_state(&r2, 1, None);
    manipulate_log(&r3, (1..=3).collect(), 1).await;
    init_hard_state(&r3, 2, None);
    manipulate_log(&r3, (4..=last_log_id).collect(), 2).await;

    // Start initial cluster nodes
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    let cluster_nodes = &[
        (JOIN_CLUSTER_PORT_BASE + 11, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 12, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 13, 1, 2),
    ];

    // To maintain the last included index of the snapshot, because of the configure:
    // retained_log_entries. e.g. if leader local raft log has 10 entries. but
    // retained_log_entries=1 , Leader will also generate a noop entry. So leader has 11 total
    // entries. then the last included index of the snapshot should be 10 = 11-1.
    let mut snapshot_last_included_id: Option<u64> = None;
    for (i, port) in ports.iter().enumerate() {
        let node_id = (i + 1) as u64;
        let config = create_node_config(
            node_id,
            *port,
            cluster_nodes,
            &format!("{}/cs/{}", JOIN_CLUSTER_CASE2_DB_ROOT_DIR, i + 1),
            JOIN_CLUSTER_CASE2_LOG_DIR,
        )
        .await;

        let (state_machine, raft_log) = match i {
            0 => (Some(sm1.clone()), Some(r1.clone())),
            1 => (Some(sm2.clone()), Some(r2.clone())),
            2 => (Some(sm3.clone()), Some(r3.clone())),
            _ => (None, None),
        };

        let mut node_config = node_config(&config);
        node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
        node_config.raft.snapshot.cleanup_retain_count = 2;
        node_config.raft.snapshot.snapshots_dir =
            PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, node_id));
        node_config.raft.snapshot.chunk_size = 100;
        //Dirty code: could leave it like this for now.
        snapshot_last_included_id =
            Some(last_log_id.saturating_sub(node_config.raft.snapshot.retained_log_entries));

        let (graceful_tx, node_handle) = start_node(node_config, state_machine, raft_log).await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }
    let last_included = snapshot_last_included_id.unwrap();

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify initial cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[test_join_cluster_scenario] Initial cluster started. Running tests...");

    // Wait for snapshot generation
    sleep(Duration::from_secs(3)).await;
    let leader_snapshot_metadata = sm3.snapshot_metadata().unwrap();

    // Verify snapshot file exists
    let snapshot_path = format!("{}/3", SNAPSHOT_DIR);
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));
    assert!(leader_snapshot_metadata.last_included.unwrap().index >= last_included);
    assert!(!leader_snapshot_metadata.checksum.is_empty());

    // Start new node 4 and join cluster
    println!("Starting new node 4 and joining cluster...");
    let cluster_nodes = &[
        (JOIN_CLUSTER_PORT_BASE + 11, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 12, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 13, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 14, 3, 0),
    ];
    let config = create_node_config(
        4,
        new_node_port4,
        cluster_nodes,
        &format!("{}/cs/4", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
        JOIN_CLUSTER_CASE2_LOG_DIR,
    )
    .await;

    let mut node_config = node_config(&config);
    node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
    node_config.raft.snapshot.cleanup_retain_count = 2;
    node_config.raft.snapshot.snapshots_dir = PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, 4));
    node_config.raft.snapshot.chunk_size = 100;

    let (graceful_tx4, node_n4) =
        start_node(node_config, Some(sm4.clone()), Some(r4.clone())).await?;

    ctx.graceful_txs.push(graceful_tx4);
    ctx.node_handles.push(node_n4);

    sleep(Duration::from_secs(3)).await;

    // Start new node 5 and join cluster
    println!("Starting new node 5 and joining cluster...");
    let cluster_nodes = &[
        (JOIN_CLUSTER_PORT_BASE + 11, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 12, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 13, 1, 2),
        (JOIN_CLUSTER_PORT_BASE + 14, 3, 0),
        (JOIN_CLUSTER_PORT_BASE + 15, 3, 0),
    ];
    let config = create_node_config(
        5,
        new_node_port5,
        cluster_nodes,
        &format!("{}/cs/5", JOIN_CLUSTER_CASE2_DB_ROOT_DIR),
        JOIN_CLUSTER_CASE2_LOG_DIR,
    )
    .await;

    let mut node_config = common::node_config(&config);
    node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
    node_config.raft.snapshot.cleanup_retain_count = 2;
    node_config.raft.snapshot.snapshots_dir = PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, 5));
    node_config.raft.snapshot.chunk_size = 100;

    let (graceful_tx5, node_n5) =
        start_node(node_config.clone(), Some(sm5.clone()), Some(r5.clone())).await?;

    ctx.graceful_txs.push(graceful_tx5);
    ctx.node_handles.push(node_n5);

    sleep(Duration::from_secs(3)).await;

    // Validate node 4
    let snapshot_path = format!("{}/4", SNAPSHOT_DIR);
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));

    for i in 1..=last_included {
        let value = sm4.get(&safe_kv(i)).unwrap();
        assert_eq!(value, Some(safe_kv(i).to_vec()));
    }

    // Validate node 5
    let snapshot_path = format!("{}/5", SNAPSHOT_DIR);
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));

    for i in 1..=last_included {
        let value = sm5.get(&safe_kv(i)).unwrap();
        assert_eq!(value, Some(safe_kv(i).to_vec()));
    }

    // Verify the active nodes number is 5
    let ports = [
        JOIN_CLUSTER_PORT_BASE + 11,
        JOIN_CLUSTER_PORT_BASE + 12,
        JOIN_CLUSTER_PORT_BASE + 13,
        JOIN_CLUSTER_PORT_BASE + 14,
        JOIN_CLUSTER_PORT_BASE + 15,
    ];
    let mut client_manager = ClientManager::new(&create_bootstrap_urls(&ports)).await?;

    // Bugfix: TODO: has to insert a new entry to invoke the commit handler to work:
    test_put_get(&mut client_manager, 11, 200).await?;
    assert_eq!(sm1.len(), 11);

    assert_eq!(client_manager.list_leader_id().await.unwrap(), 3);
    let members = client_manager.list_members().await.unwrap();
    assert_eq!(members.len(), 5);
    for m in members {
        println!("Check member status: {:?}", &m);
        assert_eq!(m.status, NodeStatus::Active as i32);
    }

    // Clean up
    ctx.shutdown().await
}

async fn create_node_config(
    node_id: u64,
    port: u16,
    cluster_nodes: &[(u16, u8, u8)], //  (port, role, status)
    db_root_dir: &str,
    log_dir: &str,
) -> String {
    println!("Port: {}", port);

    let initial_cluster_entries = cluster_nodes
        .iter()
        .enumerate()
        .map(|(i, &(p, role, status))| {
            let id = i as u64 + 1;
            format!("{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = {role}, status = {status} }}")
        })
        .collect::<Vec<_>>()
        .join(",\n            ");

    format!(
        r#"
        node_id = {node_id}
        listen_address = '127.0.0.1:{port}'
        initial_cluster = [
            {initial_cluster_entries}
        ]
        db_root_dir = '{db_root_dir}'
        log_dir = '{log_dir}'
        "#
    )
}
