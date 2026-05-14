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

        unsafe { std::env::remove_var("CONFIG_PATH") };

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("server must stop within timeout")
            .expect("server task must not panic");
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
