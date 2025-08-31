use crate::test_utils::insert_raft_log;
use crate::test_utils::insert_state_machine;
use crate::test_utils::mock_state_machine;
use crate::test_utils::MockStorageEngine;
use crate::BufferedRaftLog;
use crate::Error;
use crate::FileStateMachine;
use crate::FileStorageEngine;
use crate::FlushPolicy;
use crate::LogStore;
use crate::MockStateMachine;
use crate::NodeBuilder;
use crate::PersistenceConfig;
use crate::PersistenceStrategy;
use crate::RaftNodeConfig;
use crate::RaftTypeConfig;
use crate::StateMachine;
use crate::StorageEngine;
use crate::SystemError;
use serial_test::serial;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;
use tempfile::TempDir;
use tokio::sync::watch;

/// These components should not be initialized during builder setup; developers should have the
/// highest priority to customize them first.
#[test]
fn test_new_initializes_default_components_with_none() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::new_from_db_path(
        "/tmp/test_new_initializes_default_components",
        shutdown_rx,
    );

    assert!(builder.storage_engine.is_none());
    assert!(builder.state_machine.is_none());
    assert!(builder.transport.is_none());
    assert!(builder.membership.is_none());
    assert!(builder.state_machine_handler.is_none());
    assert!(builder.snapshot_policy.is_none());
    assert!(builder.node.is_none());
}

#[tokio::test]
async fn test_set_raft_log_replaces_default() {
    // Prepare RaftTypeConfig components
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let id = 1;

    let mock_storage_engine =
        Arc::new(FileStorageEngine::new(temp_dir.path().join("storage_engine")).unwrap());

    let (buffered_raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            id,
            PersistenceConfig {
                strategy: PersistenceStrategy::DiskFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
            },
            mock_storage_engine.clone(),
        );
    let buffered_raft_log = buffered_raft_log.start(receiver);

    // diff customization raft_log with orgional one
    let expected_raft_log_ids = vec![1, 2];
    insert_raft_log(&buffered_raft_log, expected_raft_log_ids.clone(), 1).await;

    let mock_state_machine =
        Arc::new(FileStateMachine::new(temp_dir.path().join("state_machine"), id).await.unwrap());

    let expected_state_machine_ids = vec![1, 2, 3];
    insert_state_machine(&mock_state_machine, expected_state_machine_ids.clone(), 1).await;

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<FileStorageEngine, FileStateMachine>::new_from_db_path(
        temp_dir.path().join("db_path").to_str().unwrap(),
        shutdown_rx,
    )
    .storage_engine(mock_storage_engine)
    .state_machine(mock_state_machine);

    // Verify that raft_log is replaced with customization one
    assert_eq!(
        builder.storage_engine.as_ref().unwrap().log_store().last_index(),
        expected_raft_log_ids[1]
    );
    assert_eq!(
        builder.state_machine.as_ref().unwrap().len(),
        expected_state_machine_ids.len()
    );
}

#[tokio::test]
async fn test_build_creates_node() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::new_from_db_path(
        "/tmp/test_build_creates_node",
        shutdown_rx,
    )
    .state_machine(Arc::new(mock_state_machine()))
    .storage_engine(Arc::new(MockStorageEngine::new()))
    .build();

    // Verify that the node instance is generated
    assert!(builder.node.is_some());
}

#[test]
fn test_ready_fails_without_build() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::new_from_db_path(
        "/tmp/test_ready_fails_without_build",
        shutdown_rx,
    );

    let result = builder.ready();
    assert!(matches!(
        result,
        Err(Error::System(SystemError::NodeStartFailed(_)))
    ));
}

#[tokio::test]
#[should_panic(expected = "failed to start RPC server")]
async fn test_start_rpc_panics_without_node() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::new_from_db_path(
        "/tmp/test_start_rpc_panics_without_node",
        shutdown_rx,
    );

    // If start the RPC service directly without calling build(), the service should
    // panic.
    let _ = builder.start_rpc_server().await;
}

// Test helper function: create a temporary configuration file
fn create_temp_config(content: &str) -> (PathBuf, String) {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_config.toml");
    std::fs::write(&file_path, content).unwrap();
    (dir.keep(), file_path.to_str().unwrap().to_string())
}

#[test]
#[serial]
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
        assert_eq!(
            updated_config.cluster.node_id, 1,
            "Node ID should remain default"
        );
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
#[serial]
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
#[serial]
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
#[serial]
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
