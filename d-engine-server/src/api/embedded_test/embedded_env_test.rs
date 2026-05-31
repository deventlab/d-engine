//! Tests for EmbeddedEngine::start(data_dir) behaviour
//!
//! Sequential execution required to avoid env-var race conditions.

#[cfg(test)]
#[cfg(feature = "rocksdb")]
mod start_data_dir_tests {
    use serial_test::serial;

    use crate::api::EmbeddedEngine;

    /// data_dir is created automatically when it does not exist.
    #[tokio::test]
    #[serial]
    async fn test_start_creates_missing_directory() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let data_dir = temp_dir.path().join("auto-created");

        assert!(!data_dir.exists());
        let result = EmbeddedEngine::start(&data_dir).await;
        assert!(
            result.is_ok(),
            "should create dir automatically: {:?}",
            result.err()
        );
        assert!(data_dir.exists());

        if let Ok(engine) = result {
            engine.stop().await.ok();
        }
    }

    /// Opening an existing data directory is idempotent (data is preserved).
    #[tokio::test]
    #[serial]
    async fn test_start_existing_directory_is_idempotent() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let data_dir = temp_dir.path().join("db");

        // First start: write a key
        {
            let engine = EmbeddedEngine::start(&data_dir).await.expect("first start");
            engine.wait_ready(std::time::Duration::from_secs(5)).await.expect("ready");
            engine.client().put(b"k".to_vec(), b"v".to_vec()).await.expect("put");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            engine.stop().await.ok();
        }

        // Second start: data must still be there
        {
            let engine = EmbeddedEngine::start(&data_dir).await.expect("second start");
            engine.wait_ready(std::time::Duration::from_secs(5)).await.expect("ready");
            let val = engine.client().get_linearizable(b"k".to_vec()).await.expect("get");
            assert_eq!(val.as_deref(), Some(b"v".as_ref()), "data must persist");
            engine.stop().await.ok();
        }
    }

    /// data_dir overrides cluster.db_root_dir set in CONFIG_PATH.
    #[tokio::test]
    #[serial]
    async fn test_start_data_dir_overrides_config_path_db_root_dir() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_data_dir = temp_dir.path().join("from-config");
        let explicit_data_dir = temp_dir.path().join("from-arg");

        // Write a config that points at a different directory
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

        let result = EmbeddedEngine::start(&explicit_data_dir).await;

        unsafe { std::env::remove_var("CONFIG_PATH") };

        assert!(result.is_ok(), "should succeed: {:?}", result.err());
        assert!(explicit_data_dir.exists(), "explicit path must be used");
        assert!(!config_data_dir.exists(), "config path must be ignored");

        if let Ok(engine) = result {
            engine.stop().await.ok();
        }
    }

    /// /tmp paths emit a warning but are not rejected.
    #[tokio::test]
    #[serial(tmp_db)]
    async fn test_start_tmp_path_warns_but_succeeds() {
        let tmp_path = std::path::PathBuf::from("/tmp/d-engine-env-test");
        let _ = std::fs::remove_dir_all(&tmp_path);

        let result = EmbeddedEngine::start(&tmp_path).await;

        assert!(
            result.is_ok(),
            "/tmp path should succeed with warn, not error: {:?}",
            result.err()
        );

        if let Ok(engine) = result {
            engine.stop().await.ok();
        }
        let _ = std::fs::remove_dir_all(&tmp_path);
    }
}
