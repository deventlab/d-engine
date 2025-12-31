//! Sequential tests for EmbeddedEngine that modify global environment variables
//!
//! These tests must run sequentially (--test-threads=1) to avoid race conditions
//! from concurrent modifications to process-level CONFIG_PATH environment variable.

#[cfg(test)]
#[cfg(feature = "rocksdb")]
mod config_env_tests {
    use serial_test::serial;

    use crate::api::EmbeddedEngine;

    // Tests for start() method with CONFIG_PATH environment variable

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_start_with_config_path_env_valid() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = _temp_dir.path().join("test_config.toml");
        let data_dir = _temp_dir.path().join("data");

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

        // Set CONFIG_PATH env var and test
        unsafe {
            std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
        }
        let result = EmbeddedEngine::start().await;
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        assert!(
            result.is_ok(),
            "start() should succeed with valid CONFIG_PATH"
        );

        if let Ok(engine) = result {
            engine.stop().await.ok();
        }
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial]
    async fn test_start_with_config_path_env_nonexistent() {
        // Set CONFIG_PATH to nonexistent file
        unsafe {
            std::env::set_var("CONFIG_PATH", "/nonexistent/config.toml");
        }
        let result = EmbeddedEngine::start().await;
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        assert!(
            result.is_err(),
            "start() should fail with nonexistent CONFIG_PATH"
        );
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[serial(tmp_db)]
    async fn test_start_with_config_path_env_tmp_db_allows_in_debug() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = _temp_dir.path().join("test_config.toml");

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

        // In debug mode, should succeed with warning
        unsafe {
            std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
        }
        let result = EmbeddedEngine::start().await;
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        assert!(
            result.is_ok(),
            "start() should allow /tmp/db in debug mode with CONFIG_PATH"
        );

        if let Ok(engine) = result {
            engine.stop().await.ok();
        }

        // Clean up after test
        let _ = std::fs::remove_dir_all("/tmp/db");
    }

    #[tokio::test]
    #[cfg(not(debug_assertions))]
    #[serial]
    async fn test_start_with_config_path_env_tmp_db_rejects_in_release() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config_path = _temp_dir.path().join("test_config.toml");

        // Create config with /tmp/db
        let config_content = r#"
[cluster]
node_id = 1
db_root_dir = "/tmp/db"

[cluster.rpc]
listen_addr = "127.0.0.1:0"
"#;
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        // In release mode, should reject
        unsafe {
            std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
        }
        let result = EmbeddedEngine::start().await;
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        assert!(
            result.is_err(),
            "start() should reject /tmp/db in release mode with CONFIG_PATH"
        );

        if let Err(e) = result {
            let err_msg = format!("{:?}", e);
            assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
        }
    }
}
