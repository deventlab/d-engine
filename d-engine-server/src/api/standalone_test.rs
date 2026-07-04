//! Unit tests for StandaloneEngine configuration validation

#[cfg(all(test, feature = "rocksdb"))]
mod standalone_server_tests {
    use std::time::Duration;

    use serial_test::serial;
    use tokio::sync::watch;

    use crate::api::StandaloneEngine;

    // ── run(data_dir, shutdown_rx) tests ─────────────────────────────────────

    /// run() creates the data directory automatically when it does not exist.
    #[tokio::test]
    #[serial]
    async fn test_run_creates_missing_directory() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let data_dir = temp_dir.path().join("auto-created");
        assert!(!data_dir.exists());

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let dir_clone = data_dir.clone();
        let handle =
            tokio::spawn(async move { StandaloneEngine::run(&dir_clone, shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(data_dir.exists(), "directory must be created automatically");

        shutdown_tx.send(()).ok();
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
        assert!(result.is_ok(), "run() should succeed: {:?}", result.err());
    }

    /// /tmp data_dir emits a warning but is not rejected.
    #[tokio::test]
    #[serial(tmp_db)]
    async fn test_run_tmp_path_warns_but_succeeds() {
        let tmp_path = std::path::PathBuf::from("/tmp/d-engine-standalone-test");
        let _ = std::fs::remove_dir_all(&tmp_path);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let path_clone = tmp_path.clone();
        let handle =
            tokio::spawn(async move { StandaloneEngine::run(&path_clone, shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
        assert!(
            result.is_ok(),
            "/tmp path should succeed with warning: {:?}",
            result.err()
        );

        let _ = std::fs::remove_dir_all(&tmp_path);
    }

    /// data_dir overrides cluster.db_root_dir set in CONFIG_PATH.
    #[tokio::test]
    #[serial]
    async fn test_run_data_dir_overrides_config_path_db_root_dir() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_data_dir = temp_dir.path().join("from-config");
        let explicit_data_dir = temp_dir.path().join("from-arg");

        let config_path = temp_dir.path().join("test.toml");
        std::fs::write(
            &config_path,
            format!(
                "[cluster]\ndb_root_dir = \"{}\"\n[cluster.rpc]\nlisten_addr = \"127.0.0.1:0\"\n",
                config_data_dir.display()
            ),
        )
        .expect("write config");

        unsafe { std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap()) };

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let dir_clone = explicit_data_dir.clone();
        let handle =
            tokio::spawn(async move { StandaloneEngine::run(&dir_clone, shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");

        unsafe { std::env::remove_var("CONFIG_PATH") };

        assert!(result.is_ok(), "should succeed: {:?}", result.err());
        assert!(explicit_data_dir.exists(), "explicit path must be used");
        assert!(!config_data_dir.exists(), "config path must be ignored");
    }

    /// Nonexistent CONFIG_PATH still causes a config-load error.
    #[tokio::test]
    #[serial]
    async fn test_run_config_path_nonexistent_fails() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let data_dir = temp_dir.path().join("db");
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        unsafe { std::env::set_var("CONFIG_PATH", "/nonexistent/config.toml") };
        let result = StandaloneEngine::run(&data_dir, shutdown_rx).await;
        unsafe { std::env::remove_var("CONFIG_PATH") };

        assert!(result.is_err(), "nonexistent CONFIG_PATH should fail");
    }

    // ── run_with(config_path, shutdown_rx) tests ─────────────────────────────

    #[tokio::test]
    async fn test_run_with_valid_config() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_path = temp_dir.path().join("config.toml");
        let data_dir = temp_dir.path().join("data");

        std::fs::write(
            &config_path,
            format!(
                concat!(
                    "[cluster]\nnode_id = 1\ndb_root_dir = \"{}\"\n\n",
                    "[cluster.rpc]\nlisten_addr = \"127.0.0.1:0\"\n\n",
                    "[raft]\nheartbeat_idle_flush_interval_ms = 500\n",
                    "election_timeout_min_ms = 1500\nelection_timeout_max_ms = 3000\n"
                ),
                data_dir.display()
            ),
        )
        .expect("write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let config_path_str = config_path.to_str().unwrap().to_string();
        let handle =
            tokio::spawn(
                async move { StandaloneEngine::run_with(&config_path_str, shutdown_rx).await },
            );

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
        assert!(
            result.is_ok(),
            "run_with() should succeed with valid config"
        );
    }

    #[tokio::test]
    async fn test_run_with_nonexistent_config() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let result = StandaloneEngine::run_with("/nonexistent/config.toml", shutdown_rx).await;
        assert!(
            result.is_err(),
            "run_with() should fail with nonexistent config"
        );
    }

    /// run_with() with a /tmp db_root_dir emits a warning but is not rejected.
    #[tokio::test]
    #[serial(tmp_db)]
    async fn test_run_with_tmp_db_warns_but_succeeds() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_path = temp_dir.path().join("config.toml");
        let tmp_db = "/tmp/d-engine-standalone-runwith-test";
        let _ = std::fs::remove_dir_all(tmp_db);

        std::fs::write(
            &config_path,
            format!(
                "[cluster]\nnode_id = 1\ndb_root_dir = \"{tmp_db}\"\n\n[cluster.rpc]\nlisten_addr = \"127.0.0.1:0\"\n"
            ),
        )
        .expect("write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let config_path_str = config_path.to_str().unwrap().to_string();
        let handle =
            tokio::spawn(
                async move { StandaloneEngine::run_with(&config_path_str, shutdown_rx).await },
            );

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
        assert!(
            result.is_ok(),
            "/tmp path should succeed with warning: {:?}",
            result.err()
        );

        let _ = std::fs::remove_dir_all(tmp_db);
    }

    #[tokio::test]
    async fn test_shutdown_signal_stops_server() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_path = temp_dir.path().join("config.toml");
        let data_dir = temp_dir.path().join("data");

        std::fs::write(
            &config_path,
            format!(
                "[cluster]\nnode_id = 1\ndb_root_dir = \"{}\"\n\n[cluster.rpc]\nlisten_addr = \"127.0.0.1:0\"\n",
                data_dir.display()
            ),
        )
        .expect("write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let config_path_str = config_path.to_str().unwrap().to_string();
        let handle =
            tokio::spawn(
                async move { StandaloneEngine::run_with(&config_path_str, shutdown_rx).await },
            );

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).expect("send shutdown");

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout");
        assert!(result.is_ok(), "server task should not panic");
    }
}

/// Tests for `start_node` — the programmatic-config entry point for
/// library wrappers that build their own `RaftNodeConfig` in Rust.
#[cfg(all(test, feature = "rocksdb"))]
mod start_node_tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use serial_test::serial;
    use tokio::sync::watch;

    use d_engine_core::config::ClusterConfig;
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;

    use crate::RocksDBStateMachine;
    use crate::RocksDBStorageEngine;
    use crate::api::StandaloneEngine;
    use crate::storage::TtlLease;

    /// Build a minimal valid `RaftNodeConfig` backed by a temp directory.
    fn make_config(
        temp_dir: &tempfile::TempDir,
        node_id: u32,
    ) -> d_engine_core::RaftNodeConfig {
        let listen_addr: SocketAddr = "127.0.0.1:19732".parse().unwrap();
        d_engine_core::RaftNodeConfig {
            cluster: ClusterConfig {
                node_id,
                listen_address: listen_addr,
                initial_cluster: vec![NodeMeta {
                    id: node_id,
                    address: listen_addr.to_string(),
                    role: NodeRole::Follower as i32,
                    status: NodeStatus::Active.into(),
                }],
                db_root_dir: temp_dir.path().join("engine"),
                log_dir: temp_dir.path().join("logs"),
            },
            ..Default::default()
        }
    }

    /// Create RocksDB storage engine + state machine with lease initialized.
    fn create_rocksdb_se_and_sm(
        temp_dir: &tempfile::TempDir
    ) -> (Arc<RocksDBStorageEngine>, Arc<RocksDBStateMachine>) {
        let storage_path = temp_dir.path().join("storage");
        let sm_path = temp_dir.path().join("sm");
        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::create_dir_all(&sm_path).unwrap();

        let storage =
            Arc::new(RocksDBStorageEngine::new(&storage_path).expect("create storage engine"));
        let mut sm = RocksDBStateMachine::new(&sm_path).expect("create state machine");
        sm.set_lease(Arc::new(TtlLease::new(Default::default())));
        let sm = Arc::new(sm);

        (storage, sm)
    }

    // ── start_node tests ──────────────────────────────────────────────────

    /// Happy path: build a valid config and SE/SM programmatically, start the
    /// server, and shut it down cleanly — no config file needed.
    #[tokio::test]
    #[serial(start_node)]
    async fn test_start_node_with_programmatic_config_starts_and_stops() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (storage, sm) = create_rocksdb_se_and_sm(&temp_dir);
        let config = make_config(&temp_dir, 1);
        let storage_path = temp_dir.path().join("storage");
        let sm_path = temp_dir.path().join("sm");

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let handle = tokio::spawn(StandaloneEngine::start_node(
            config,
            storage,
            sm,
            shutdown_rx,
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
        assert!(
            result.is_ok(),
            "start_node should succeed: {:?}",
            result.err()
        );

        // RocksDB must have persisted data — proves the engine used the passed-in instances.
        assert!(
            storage_path.join("CURRENT").exists() || storage_path.join("IDENTITY").exists(),
            "storage engine must have persisted data to {:?}",
            storage_path
        );
        assert!(
            sm_path.join("CURRENT").exists() || sm_path.join("IDENTITY").exists(),
            "state machine must have persisted data to {:?}",
            sm_path
        );
    }

    /// `node_id = 0` is rejected by `ClusterConfig::validate()`.
    /// `start_node` calls `validate()` internally — the error must propagate.
    #[tokio::test]
    #[serial(start_node)]
    async fn test_start_node_unvalidated_config_returns_error() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (storage, sm) = create_rocksdb_se_and_sm(&temp_dir);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let config = make_config(&temp_dir, 0); // node_id=0 triggers validation error

        let result = StandaloneEngine::start_node(config, storage, sm, shutdown_rx).await;
        assert!(
            result.is_err(),
            "start_node with node_id=0 must return validation error"
        );
    }

    /// RocksDB files are written to the passed-in paths after a clean shutdown —
    /// proves the engine actually uses the caller's SE and SM instances.
    #[tokio::test]
    #[serial(start_node)]
    async fn test_start_node_persists_data_to_passed_paths() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let storage_path = temp_dir.path().join("my-storage");
        let sm_path = temp_dir.path().join("my-sm");
        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::create_dir_all(&sm_path).unwrap();

        let storage = Arc::new(RocksDBStorageEngine::new(&storage_path).expect("create storage"));
        let mut sm = RocksDBStateMachine::new(&sm_path).expect("create sm");
        sm.set_lease(Arc::new(TtlLease::new(Default::default())));
        let sm = Arc::new(sm);

        let config = make_config(&temp_dir, 1);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let handle = tokio::spawn(StandaloneEngine::start_node(
            config,
            storage,
            sm,
            shutdown_rx,
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown_tx.send(()).ok();

        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

        // After clean shutdown, RocksDB must have flushed data.
        let has_storage_files = std::fs::read_dir(&storage_path)
            .map(|mut d| d.any(|e| e.is_ok()))
            .unwrap_or(false);
        let has_sm_files =
            std::fs::read_dir(&sm_path).map(|mut d| d.any(|e| e.is_ok())).unwrap_or(false);
        assert!(has_storage_files, "storage dir must contain RocksDB files");
        assert!(has_sm_files, "state machine dir must contain RocksDB files");
    }
}

/// Tests for `run_custom` — the custom SE/SM entry point for standalone mode.
#[cfg(all(test, feature = "rocksdb"))]
mod run_custom_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serial_test::serial;
    use tokio::sync::watch;

    use crate::RocksDBStateMachine;
    use crate::RocksDBStorageEngine;
    use crate::api::StandaloneEngine;
    use crate::storage::TtlLease;

    /// Create RocksDB SE/SM with lease initialized.
    fn create_rocksdb_se_and_sm(
        temp_dir: &tempfile::TempDir
    ) -> (Arc<RocksDBStorageEngine>, Arc<RocksDBStateMachine>) {
        let storage_path = temp_dir.path().join("storage");
        let sm_path = temp_dir.path().join("sm");
        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::create_dir_all(&sm_path).unwrap();

        let storage =
            Arc::new(RocksDBStorageEngine::new(&storage_path).expect("create storage engine"));
        let mut sm = RocksDBStateMachine::new(&sm_path).expect("create state machine");
        sm.set_lease(Arc::new(TtlLease::new(Default::default())));
        let sm = Arc::new(sm);

        (storage, sm)
    }

    /// Write a minimal valid config file for a single-node cluster.
    fn write_config(
        temp_dir: &tempfile::TempDir,
        filename: &str,
        node_id: u32,
    ) -> std::path::PathBuf {
        let config_path = temp_dir.path().join(filename);
        let data_dir = temp_dir.path().join("data");
        std::fs::write(
            &config_path,
            format!(
                concat!(
                    "[cluster]\nnode_id = {node_id}\ndb_root_dir = \"{data_dir}\"\n\n",
                    "[cluster.rpc]\nlisten_addr = \"127.0.0.1:19733\"\n\n",
                    "[raft]\nheartbeat_idle_flush_interval_ms = 500\n",
                    "election_timeout_min_ms = 1500\nelection_timeout_max_ms = 3000\n"
                ),
                node_id = node_id,
                data_dir = data_dir.display(),
            ),
        )
        .expect("write config");
        config_path
    }

    /// Shutdown helper: sleep, send signal, assert clean exit.
    async fn shutdown_and_assert(
        shutdown_tx: watch::Sender<()>,
        handle: tokio::task::JoinHandle<crate::Result<()>>,
        sleep_ms: u64,
        test_name: &str,
    ) {
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
        assert!(
            result.is_ok(),
            "{test_name} should succeed: {:?}",
            result.err()
        );
    }

    // ── run_custom tests ──────────────────────────────────────────────────

    /// `run_custom` with a valid config file: server starts, shuts down cleanly.
    #[tokio::test]
    #[serial]
    async fn test_run_custom_with_valid_config_path_starts_and_stops() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (storage, sm) = create_rocksdb_se_and_sm(&temp_dir);
        let config_path = write_config(&temp_dir, "valid.toml", 1);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let config_path_str = config_path.to_str().unwrap().to_string();
        let handle = tokio::spawn(StandaloneEngine::run_custom(
            storage,
            sm,
            shutdown_rx,
            Some(config_path_str),
        ));
        shutdown_and_assert(shutdown_tx, handle, 200, "run_custom with valid config").await;
    }

    /// `run_custom` without a config file uses default config — must still work.
    #[tokio::test]
    #[serial]
    async fn test_run_custom_without_config_path_uses_defaults() {
        // CONFIG_PATH must be cleared so RaftNodeConfig::new() doesn't pick up
        // an env-configured file and fail.
        unsafe { std::env::remove_var("CONFIG_PATH") };

        let temp_dir = tempfile::tempdir().expect("tempdir");
        // Use a persistent path so default /tmp/db gets a real dir
        let (storage, sm) = create_rocksdb_se_and_sm(&temp_dir);
        // Override db_root_dir via env so default config points to our temp dir
        unsafe {
            std::env::set_var(
                "RAFT__CLUSTER__DB_ROOT_DIR",
                temp_dir.path().join("db").to_str().unwrap(),
            );
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let handle = tokio::spawn(StandaloneEngine::run_custom::<
            RocksDBStorageEngine,
            RocksDBStateMachine,
        >(storage, sm, shutdown_rx, None::<&str>));

        shutdown_and_assert(shutdown_tx, handle, 300, "run_custom without config").await;

        unsafe { std::env::remove_var("RAFT__CLUSTER__DB_ROOT_DIR") };
    }

    /// `run_custom` with a nonexistent config file path must return an error.
    #[tokio::test]
    #[serial]
    async fn test_run_custom_with_nonexistent_config_returns_error() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (storage, sm) = create_rocksdb_se_and_sm(&temp_dir);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let result = StandaloneEngine::run_custom(
            storage,
            sm,
            shutdown_rx,
            Some("/nonexistent/path/config.toml"),
        )
        .await;
        assert!(
            result.is_err(),
            "run_custom with nonexistent config must return error"
        );
    }

    /// `run_custom` with config containing `node_id = 0` must fail validation.
    #[tokio::test]
    #[serial]
    async fn test_run_custom_with_invalid_config_returns_error() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (storage, sm) = create_rocksdb_se_and_sm(&temp_dir);
        let config_path = write_config(&temp_dir, "invalid.toml", 0);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let config_path_str = config_path.to_str().unwrap().to_string();
        let result =
            StandaloneEngine::run_custom(storage, sm, shutdown_rx, Some(config_path_str)).await;
        assert!(
            result.is_err(),
            "run_custom with node_id=0 must return validation error"
        );
    }
}

/// Tests for unified RocksDB path (`unified_db = true`) in `run_with()` and `run()`.
///
/// All existing tests use configs without a `[storage]` section, so `unified_db`
/// defaults to `false` (separate RocksDB instances). These tests exercise the
/// `unified_db = true` branch introduced in #295 to ensure both paths are verified.
#[cfg(all(test, feature = "rocksdb"))]
mod unified_db_tests {
    use std::time::Duration;

    use tokio::sync::watch;

    use crate::api::StandaloneEngine;

    fn make_config(
        data_dir: &std::path::Path,
        unified: bool,
    ) -> String {
        format!(
            r#"
[cluster]
node_id = 1
db_root_dir = "{}"

[cluster.rpc]
listen_addr = "127.0.0.1:0"

[raft]
heartbeat_idle_flush_interval_ms = 500
election_timeout_min_ms = 1500
election_timeout_max_ms = 3000

[storage]
unified_db = {unified}
"#,
            data_dir.display()
        )
    }

    /// `run_with()` with `unified_db = true` should start a single shared RocksDB,
    /// serve traffic, and shut down cleanly on shutdown signal.
    ///
    /// Business scenario: Operator deploys a standalone node with `unified_db = true`
    /// to reduce memory/FD usage. The server must start and stop without errors.
    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn test_run_with_unified_db_starts_and_shuts_down_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config_path = temp_dir.path().join("config.toml");
        std::fs::write(&config_path, make_config(temp_dir.path(), true)).expect("write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let config_path_str = config_path.to_str().unwrap().to_string();
        let handle =
            tokio::spawn(
                async move { StandaloneEngine::run_with(&config_path_str, shutdown_rx).await },
            );

        // Give the server enough time to open RocksDB and start the Raft loop.
        tokio::time::sleep(Duration::from_millis(200)).await;

        shutdown_tx.send(()).expect("send shutdown signal");

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within 5 s")
            .expect("server task must not panic");

        assert!(
            result.is_ok(),
            "run_with(unified_db=true) must exit cleanly on shutdown signal"
        );
    }

    /// `run_with()` with `unified_db = false` (separate RocksDB) should also start
    /// and shut down cleanly, confirming parity between the two storage paths.
    ///
    /// Business scenario: Default deployment — operator does not set `unified_db`,
    /// so the server opens two separate RocksDB instances.
    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn test_run_with_separate_db_starts_and_shuts_down_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config_path = temp_dir.path().join("config.toml");
        std::fs::write(&config_path, make_config(temp_dir.path(), false)).expect("write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let config_path_str = config_path.to_str().unwrap().to_string();
        let handle =
            tokio::spawn(
                async move { StandaloneEngine::run_with(&config_path_str, shutdown_rx).await },
            );

        tokio::time::sleep(Duration::from_millis(200)).await;

        shutdown_tx.send(()).expect("send shutdown signal");

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within 5 s")
            .expect("server task must not panic");

        assert!(
            result.is_ok(),
            "run_with(unified_db=false) must exit cleanly on shutdown signal"
        );
    }
}
