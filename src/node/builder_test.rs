use crate::test_utils::insert_raft_log;
use crate::test_utils::insert_state_machine;
use crate::test_utils::insert_state_storage;
use crate::test_utils::reset_dbs;
use crate::test_utils::settings;
use crate::Error;
use crate::NodeBuilder;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::RaftStateMachine;
use crate::SledRaftLog;
use crate::SledStateStorage;
use crate::StateMachine;
use crate::StateStorage;
use crate::SystemError;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::watch;

/// These components should not be initialized during builder setup; developers should have the
/// highest priority to customize them first.
#[test]
fn test_new_initializes_default_components_with_none() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new_from_db_path("/tmp/test_new_initializes_default_components", shutdown_rx);

    assert!(builder.raft_log.is_none());
    assert!(builder.state_machine.is_none());
    assert!(builder.state_storage.is_none());
    assert!(builder.raft_log.is_none());
    assert!(builder.membership.is_none());
    assert!(builder.state_machine_handler.is_none());
    assert!(builder.commit_handler.is_none());
    assert!(builder.node.is_none());
}

#[test]
fn test_set_raft_log_replaces_default() {
    // Prepare RaftTypeConfig components
    let db_path = "/tmp/test_set_raft_log_replaces_default";

    let (raft_log_db, state_machine_db, state_storage_db, _snapshot_storage_db) = reset_dbs(db_path);

    let id = 1;
    let raft_log_db = Arc::new(raft_log_db);
    let state_machine_db = Arc::new(state_machine_db);
    let state_storage_db = Arc::new(state_storage_db);

    let sled_raft_log = SledRaftLog::new(raft_log_db, None);
    // diff customization raft_log with orgional one
    let expected_raft_log_ids = vec![1, 2];
    insert_raft_log(&sled_raft_log, expected_raft_log_ids.clone(), 1);

    let sled_state_machine = Arc::new(RaftStateMachine::new(id, state_machine_db.clone()));
    let expected_state_machine_ids = vec![1, 2, 3];
    insert_state_machine(&sled_state_machine, expected_state_machine_ids.clone(), 1);

    let sled_state_storage = SledStateStorage::new(state_storage_db);
    let expected_state_storage_ids = vec![1, 2, 3, 4, 5];
    insert_state_storage(&sled_state_storage, expected_state_storage_ids.clone());

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new_from_db_path(db_path, shutdown_rx)
        .raft_log(sled_raft_log)
        .state_storage(sled_state_storage)
        .state_machine(sled_state_machine);

    // Verify that raft_log is replaced with customization one
    assert_eq!(builder.raft_log.as_ref().unwrap().len(), expected_raft_log_ids.len());
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
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new_from_db_path("/tmp/test_build_creates_node", shutdown_rx).build();

    // Verify that the node instance is generated
    assert!(builder.node.is_some());
}

#[test]
fn test_ready_fails_without_build() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new_from_db_path("/tmp/test_ready_fails_without_build", shutdown_rx);

    let result = builder.ready();
    assert!(matches!(result, Err(Error::System(SystemError::NodeStartFailed(_)))));
}

#[tokio::test]
#[should_panic(expected = "failed to start RPC server")]
async fn test_start_rpc_panics_without_node() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::new_from_db_path("/tmp/test_start_rpc_panics_without_node", shutdown_rx);

    // If start the RPC service directly without calling build(), the service should
    // panic.
    let _ = builder.start_rpc_server().await;
}

// No panic
#[tokio::test]
async fn test_metrics_server_starts_on_correct_port() {
    let mut settings = settings("/tmp/test_metrics_server_starts_on_correct_port");
    settings.monitoring.prometheus_port = 12345; // Set the test port

    let (shutdown_tx, shutdown_rx) = watch::channel(());

    NodeBuilder::init(settings, shutdown_rx)
        .build()
        .start_metrics_server(shutdown_tx.subscribe());
}

// Test helper function: create a temporary configuration file
fn create_temp_config(content: &str) -> (PathBuf, String) {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_config.toml");
    std::fs::write(&file_path, content).unwrap();
    (dir.into_path(), file_path.to_str().unwrap().to_string())
}

#[test]
fn test_config_override_success() {
    // Prepare test configuration
    let (_dir, config_path) = create_temp_config(
        r#"
    [cluster]
    db_root_dir = "./custom_db"
    "#,
    );

    // Use temporary environment variables
    temp_env::with_var("CONFIG_PATH", Some(config_path.clone()), || {
        let base_config = RaftNodeConfig::new().unwrap();
        let updated_config = base_config.with_override_config(&config_path).unwrap();

        // Verify path coverage
        assert_eq!(
            updated_config.cluster.db_root_dir,
            PathBuf::from("./custom_db"),
            "DB root dir not overridden"
        );

        // Verify that other fields remain default
        assert_eq!(updated_config.cluster.node_id, 1, "Node ID should remain default");
    });
}

#[test]
fn test_config_override_invalid_path() {
    let base_config = RaftNodeConfig::new().unwrap();
    let result = base_config.with_override_config("non_existent.toml");

    assert!(
        matches!(result, Err(Error::Config(_))),
        "Should return ConfigError for invalid path"
    );
}

#[test]
fn test_config_override_invalid_format() {
    let (_dir, config_path) = create_temp_config(
        r#" 
    invalid_toml_format 
    cluster: { 
    db_root_dir: "./custom_db" 
    } 
    "#,
    );

    let base_config = RaftNodeConfig::new().unwrap();
    let result = base_config.with_override_config(&config_path);

    assert!(
        matches!(result, Err(Error::Config(_))),
        "Should return ConfigError for invalid format"
    );
}

#[test]
fn test_config_override_priority() {
    let (_dir, config_path) = create_temp_config(
        r#" 
    [cluster] 
    db_root_dir = "./file_db" 
    "#,
    );

    temp_env::with_vars(
        vec![
            ("CONFIG_PATH", Some(config_path.as_str())),
            ("RAFT__CLUSTER__DB_ROOT_DIR", Some("/env/db")),
        ],
        || {
            let config = RaftNodeConfig::new().unwrap();

            // Verify that environment variables have higher priority than configuration
            // files
            assert_eq!(
                config.cluster.db_root_dir,
                PathBuf::from("/env/db"),
                "Environment variable should override file config"
            );
        },
    );
}

#[test]
fn test_partial_override() {
    let (_dir, config_path) = create_temp_config(
        r#" 
    [raft.election] 
    election_timeout_min = 500 
    "#,
    );

    let base_config = RaftNodeConfig::new().unwrap();
    let updated_config = base_config.with_override_config(&config_path).unwrap();

    // Validate covered fields
    assert_eq!(
        updated_config.raft.election.election_timeout_min, 500,
        "Raft config should be overridden"
    );

    // Verify unmodified fields
    assert_eq!(
        updated_config.cluster.db_root_dir, base_config.cluster.db_root_dir,
        "Cluster config should remain unchanged"
    );
}
