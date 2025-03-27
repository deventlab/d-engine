use crate::{
    test_utils::{
        insert_raft_log, insert_state_machine, insert_state_storage, reset_dbs, settings,
    },
    Error, NodeBuilder, RaftLog, RaftStateMachine, SledRaftLog, SledStateStorage, StateMachine,
    StateStorage,
};
use std::sync::Arc;
use tokio::sync::watch;

#[test]
fn test_new_initializes_default_components() {
    let settings = settings("/tmp/test_new_initializes_default_components");

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new(settings, shutdown_rx);

    assert!(builder.raft_log.is_some());
    assert!(builder.state_machine.is_some());
    assert!(builder.state_storage.is_some());
    assert!(builder.raft_log.is_some());
    assert!(builder.membership.is_some());
    assert!(builder.state_machine_handler.is_some());
    assert!(builder.commit_handler.is_none());
    assert!(builder.node.is_none());
}

#[test]
fn test_set_raft_log_replaces_default() {
    // Prepare RaftTypeConfig components
    let db_path = "/tmp/test_set_raft_log_replaces_default";
    let settings = settings(db_path);

    let (raft_log_db, state_machine_db, state_storage_db, _snapshot_storage_db) =
        reset_dbs(db_path);

    let id = 1;
    let raft_log_db = Arc::new(raft_log_db);
    let state_machine_db = Arc::new(state_machine_db);
    let state_storage_db = Arc::new(state_storage_db);

    let sled_raft_log = SledRaftLog::new(raft_log_db, None);
    // diff customization raft_log with orgional one
    let expected_raft_log_ids = vec![1, 2];
    insert_raft_log(&sled_raft_log, expected_raft_log_ids.clone(), 1);

    let sled_state_machine = RaftStateMachine::new(id, state_machine_db.clone());
    let expected_state_machine_ids = vec![1, 2, 3];
    insert_state_machine(&sled_state_machine, expected_state_machine_ids.clone(), 1);

    let sled_state_storage = SledStateStorage::new(state_storage_db);
    let expected_state_storage_ids = vec![1, 2, 3, 4, 5];
    insert_state_storage(&sled_state_storage, expected_state_storage_ids.clone());

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new(settings, shutdown_rx)
        .raft_log(sled_raft_log)
        .state_storage(sled_state_storage)
        .state_machine(sled_state_machine);

    // Verify that raft_log is replaced with customization one
    assert_eq!(
        builder.raft_log.as_ref().unwrap().len(),
        expected_raft_log_ids.len()
    );
    assert_eq!(
        builder.state_machine.as_ref().unwrap().len(),
        expected_state_machine_ids.len()
    );
    assert_eq!(
        builder.state_storage.as_ref().unwrap().len(),
        expected_state_storage_ids.len()
    );
}

#[tokio::test]
async fn test_build_creates_node() {
    let settings = settings("/tmp/test_build_creates_node");

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new(settings, shutdown_rx).build();

    // Verify that the node instance is generated
    assert!(builder.node.is_some());
}

#[test]
fn test_ready_fails_without_build() {
    let settings = settings("/tmp/test_ready_fails_without_build");

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new(settings, shutdown_rx);

    let result = builder.ready();
    assert!(matches!(result, Err(Error::ServerFailedToStartError)));
}

#[tokio::test]
#[should_panic(expected = "failed to start RPC server")]
async fn test_start_rpc_panics_without_node() {
    let settings = settings("/tmp/test_start_rpc_panics_without_node");

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new(settings, shutdown_rx);

    // If start the RPC service directly without calling build(), the service should panic.
    let _ = builder.start_rpc_server().await;
}

// No panic
#[tokio::test]
async fn test_metrics_server_starts_on_correct_port() {
    let mut settings = settings("/tmp/test_metrics_server_starts_on_correct_port");
    settings.cluster.prometheus_metrics_port = 12345; // Set the test port

    let (shutdown_tx, shutdown_rx) = watch::channel(());

    NodeBuilder::new(settings, shutdown_rx)
        .build()
        .start_metrics_server(shutdown_tx.subscribe());
}
