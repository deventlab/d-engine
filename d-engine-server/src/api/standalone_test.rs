//! Unit tests for StandaloneServer configuration validation

#[cfg(all(test, feature = "rocksdb"))]
mod standalone_server_tests {
    #[cfg(debug_assertions)]
    use std::time::Duration;

    use serial_test::serial;
    use tokio::sync::watch;

    use crate::api::StandaloneServer;

    // Tests for run() method (reads CONFIG_PATH env var)

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_run_with_config_path_env_valid() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");
        let data_dir = temp_dir.path().join("data");

        // Create valid config with custom db_root_dir
        let config_content = format!(
            r#"
[cluster]
node_id = 1
db_root_dir = "{}"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#,
            data_dir.display()
        );
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Set CONFIG_PATH env var
        unsafe {
            std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
        }

        // Spawn server in background
        let server_handle = tokio::spawn(async move { StandaloneServer::run(shutdown_rx).await });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());

        // Cleanup env var
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout")
            .expect("Server task should not panic");

        assert!(
            result.is_ok(),
            "run() should succeed with valid CONFIG_PATH"
        );
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_run_with_config_path_env_nonexistent() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // Set CONFIG_PATH to nonexistent file
        unsafe {
            std::env::set_var("CONFIG_PATH", "/nonexistent/config.toml");
        }

        let result = StandaloneServer::run(shutdown_rx).await;

        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        assert!(
            result.is_err(),
            "run() should fail with nonexistent CONFIG_PATH"
        );
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_run_with_config_path_env_tmp_db_allows_in_debug() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");

        // Clean up /tmp/db before test
        let _ = std::fs::remove_dir_all("/tmp/db");

        // Create config with /tmp/db
        let config_content = r#"
[cluster]
node_id = 1
db_root_dir = "/tmp/db"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        unsafe {
            std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
        }

        // Spawn server in background
        let server_handle = tokio::spawn(async move { StandaloneServer::run(shutdown_rx).await });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());

        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout")
            .expect("Server task should not panic");

        // In debug mode, should succeed with warning
        assert!(
            result.is_ok(),
            "run() should allow /tmp/db in debug mode with CONFIG_PATH"
        );

        // Clean up after test
        let _ = std::fs::remove_dir_all("/tmp/db");
    }

    #[tokio::test]
    #[cfg(not(debug_assertions))]
    #[serial]
    async fn test_run_with_config_path_env_tmp_db_rejects_in_release() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");

        // Create config with /tmp/db
        let config_content = r#"
[cluster]
node_id = 1
db_root_dir = "/tmp/db"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        unsafe {
            std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
        }

        // In release mode, should reject immediately
        let result = StandaloneServer::run(shutdown_rx).await;

        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        assert!(
            result.is_err(),
            "run() should reject /tmp/db in release mode with CONFIG_PATH"
        );

        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
        }
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_run_without_config_path_env_allows_in_debug() {
        // No CONFIG_PATH env var - uses default config with /tmp/db
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        // Clean up /tmp/db before test
        let _ = std::fs::remove_dir_all("/tmp/db");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Spawn server in background
        let server_handle = tokio::spawn(async move { StandaloneServer::run(shutdown_rx).await });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout")
            .expect("Server task should not panic");

        assert!(
            result.is_ok(),
            "run() should allow default /tmp/db in debug mode without CONFIG_PATH"
        );

        // Clean up after test
        let _ = std::fs::remove_dir_all("/tmp/db");
    }

    #[tokio::test]
    #[cfg(not(debug_assertions))]
    #[serial]
    async fn test_run_without_config_path_env_rejects_in_release() {
        // No CONFIG_PATH env var - should reject default /tmp/db in release
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // In release mode, should reject immediately
        let result = StandaloneServer::run(shutdown_rx).await;

        assert!(
            result.is_err(),
            "run() should reject default /tmp/db in release mode without CONFIG_PATH"
        );

        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
        }
    }

    // Tests for run_with() method (explicit config path)

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_run_with_default_db_root_dir_allows_in_debug() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");

        // Clean up /tmp/db before test
        let _ = std::fs::remove_dir_all("/tmp/db");

        // Create config without db_root_dir (will use default /tmp/db)
        let config_content = r#"
[cluster]
node_id = 1

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Spawn server in background
        let server_handle = tokio::spawn(async move {
            StandaloneServer::run_with(config_path.to_str().unwrap(), shutdown_rx).await
        });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout")
            .expect("Server task should not panic");

        // In debug mode, should succeed (allows /tmp/db with warning)
        assert!(
            result.is_ok(),
            "run_with() should succeed in debug mode with default /tmp/db"
        );

        // Clean up after test
        let _ = std::fs::remove_dir_all("/tmp/db");
    }

    #[tokio::test]
    #[cfg(not(debug_assertions))]
    #[serial]
    async fn test_run_with_default_db_root_dir_rejects_in_release() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");

        // Create config without db_root_dir (will use default /tmp/db)
        let config_content = r#"
[cluster]
node_id = 1

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // In release mode, should reject /tmp/db immediately
        let result = StandaloneServer::run_with(config_path.to_str().unwrap(), shutdown_rx).await;

        assert!(
            result.is_err(),
            "run_with() should reject /tmp/db in release mode"
        );

        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
        }
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn test_run_with_valid_config() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");
        let data_dir = temp_dir.path().join("data");

        // Create valid config with custom db_root_dir
        let config_content = format!(
            r#"
[cluster]
node_id = 1
db_root_dir = "{}"

[cluster.rpc]
listen_addr = "127.0.0.1:0"

[raft]
heartbeat_interval_ms = 500
election_timeout_min_ms = 1500
election_timeout_max_ms = 3000
"#,
            data_dir.display()
        );
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Spawn server in background
        let server_handle = tokio::spawn(async move {
            StandaloneServer::run_with(config_path.to_str().unwrap(), shutdown_rx).await
        });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout")
            .expect("Server task should not panic");

        assert!(
            result.is_ok(),
            "run_with() should succeed with valid config"
        );
    }

    #[tokio::test]
    async fn test_run_with_nonexistent_config() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let result = StandaloneServer::run_with("/nonexistent/config.toml", shutdown_rx).await;

        assert!(
            result.is_err(),
            "run_with() should fail with nonexistent config"
        );
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_run_with_tmp_db_allows_in_debug() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");

        // Clean up /tmp/db before test
        let _ = std::fs::remove_dir_all("/tmp/db");

        // Create config with /tmp/db
        let config_content = r#"
[cluster]
node_id = 1
db_root_dir = "/tmp/db"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Spawn server in background
        let server_handle = tokio::spawn(async move {
            StandaloneServer::run_with(config_path.to_str().unwrap(), shutdown_rx).await
        });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout")
            .expect("Server task should not panic");

        // In debug mode, should succeed with warning
        assert!(
            result.is_ok(),
            "run_with() should allow /tmp/db in debug mode"
        );

        // Clean up after test
        let _ = std::fs::remove_dir_all("/tmp/db");
    }

    #[tokio::test]
    #[cfg(not(debug_assertions))]
    #[serial]
    async fn test_run_with_tmp_db_rejects_in_release() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");

        // Create config with /tmp/db
        let config_content = r#"
[cluster]
node_id = 1
db_root_dir = "/tmp/db"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // In release mode, should reject immediately
        let result = StandaloneServer::run_with(config_path.to_str().unwrap(), shutdown_rx).await;

        assert!(
            result.is_err(),
            "run_with() should reject /tmp/db in release mode"
        );

        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
        }
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_shutdown_signal_stops_server() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = temp_dir.path().join("test_config.toml");
        let data_dir = temp_dir.path().join("data");

        let config_content = format!(
            r#"
[cluster]
node_id = 1
db_root_dir = "{}"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#,
            data_dir.display()
        );
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let server_handle = tokio::spawn(async move {
            StandaloneServer::run_with(config_path.to_str().unwrap(), shutdown_rx).await
        });

        // Let server start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let send_result = shutdown_tx.send(());
        assert!(send_result.is_ok(), "Should send shutdown signal");

        // Server should stop gracefully
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server should stop within timeout");

        assert!(result.is_ok(), "Server task should not panic");
    }
}
