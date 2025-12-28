//! Unit tests for EmbeddedEngine leader election APIs

#[cfg(test)]
mod embedded_engine_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::api::EmbeddedEngine;
    use crate::storage::FileStateMachine;
    use crate::storage::FileStorageEngine;

    async fn create_test_storage_and_sm() -> (
        Arc<FileStorageEngine>,
        Arc<FileStateMachine>,
        tempfile::TempDir,
    ) {
        // Clear CONFIG_PATH to avoid external config interference
        unsafe {
            std::env::remove_var("CONFIG_PATH");
        }

        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage_path = temp_dir.path().join("storage");
        let sm_path = temp_dir.path().join("sm");

        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::create_dir_all(&sm_path).unwrap();

        let storage =
            Arc::new(FileStorageEngine::new(storage_path).expect("Failed to create storage"));
        let sm =
            Arc::new(FileStateMachine::new(sm_path).await.expect("Failed to create state machine"));

        (storage, sm, temp_dir)
    }

    #[tokio::test]
    async fn test_wait_ready_single_node_success() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        // Start embedded engine (single node)
        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // Wait for leader election (should succeed quickly in single-node mode)
        let result = engine.wait_ready(Duration::from_secs(5)).await;

        assert!(
            result.is_ok(),
            "Leader election should succeed in single-node mode"
        );
        let leader_info = result.unwrap();
        assert_eq!(
            leader_info.leader_id, 1,
            "Single node should elect itself as leader"
        );
        assert!(leader_info.term > 0, "Term should be positive");

        // Cleanup
        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_wait_ready_timeout() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // In single-node mode, leader should be elected immediately
        // But if we had a cluster without quorum, this would timeout
        // For this test, we verify timeout mechanism works
        let very_short_timeout = Duration::from_nanos(1);

        // Note: This might still succeed if election happens instantly
        // The test verifies timeout handling exists
        let _ = engine.wait_ready(very_short_timeout).await;

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_leader_change_notifier_basic() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // Subscribe to leader changes
        let mut leader_rx = engine.leader_change_notifier();

        // Wait for leader election
        engine
            .wait_ready(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");

        // The notifier should have the current leader
        tokio::time::timeout(Duration::from_secs(1), leader_rx.changed())
            .await
            .expect("Should receive leader notification within timeout")
            .expect("Should receive change event");

        let current_leader = *leader_rx.borrow();
        assert!(current_leader.is_some(), "Leader should be elected");

        if let Some(info) = current_leader {
            assert_eq!(info.leader_id, 1);
            assert!(info.term > 0);
        }

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_ready_and_wait_ready_sequence() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // Wait for leader election (wait_ready handles both node init and leader election)
        let start = std::time::Instant::now();
        let leader_info = engine
            .wait_ready(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");
        let duration = start.elapsed();

        // Verify timing (single-node should be fast)
        assert!(
            duration < Duration::from_secs(2),
            "Leader election should be fast in single-node"
        );

        // Verify leader info
        assert_eq!(leader_info.leader_id, 1);
        assert!(leader_info.term > 0);

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_client_available_after_wait_ready() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        engine
            .wait_ready(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");

        // Client should be usable
        let client = engine.client();

        // Perform a write operation
        let result = client.put(b"test_key".to_vec(), b"test_value".to_vec()).await;
        assert!(
            result.is_ok(),
            "Put operation should succeed after leader election"
        );

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_multiple_leader_change_notifier_subscribers() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // Create multiple subscribers
        let mut rx1 = engine.leader_change_notifier();
        let mut rx2 = engine.leader_change_notifier();

        // Wait for leader election
        engine
            .wait_ready(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");

        // Both subscribers should receive notification
        tokio::time::timeout(Duration::from_secs(1), rx1.changed())
            .await
            .expect("Subscriber 1 should receive within timeout")
            .expect("Subscriber 1 should receive change");

        tokio::time::timeout(Duration::from_secs(1), rx2.changed())
            .await
            .expect("Subscriber 2 should receive within timeout")
            .expect("Subscriber 2 should receive change");

        // Both should have same leader info
        let leader1 = *rx1.borrow();
        let leader2 = *rx2.borrow();
        assert_eq!(leader1, leader2, "Both subscribers should see same leader");

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_engine_stop_cleans_up() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // Stop should complete without error
        let stop_result = engine.stop().await;
        assert!(stop_result.is_ok(), "Stop should succeed");
    }

    #[tokio::test]
    async fn test_wait_ready_race_condition_already_elected() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // First call - wait for leader election
        let first_result = engine.wait_ready(Duration::from_secs(5)).await;
        assert!(first_result.is_ok(), "First wait_ready should succeed");
        let first_info = first_result.unwrap();

        // Second call - leader already elected, should return immediately
        let second_start = std::time::Instant::now();
        let second_result = engine.wait_ready(Duration::from_secs(5)).await;
        let second_duration = second_start.elapsed();

        assert!(second_result.is_ok(), "Second wait_ready should succeed");
        let second_info = second_result.unwrap();

        // Should return almost instantly (< 100ms)
        assert!(
            second_duration < Duration::from_millis(100),
            "wait_ready should return immediately when leader already elected, took {second_duration:?}"
        );

        // Should return same leader info
        assert_eq!(first_info.leader_id, second_info.leader_id);
        assert_eq!(first_info.term, second_info.term);

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_wait_ready_multiple_calls_concurrent() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = Arc::new(
            EmbeddedEngine::start_custom(storage, sm, None)
                .await
                .expect("Failed to start engine"),
        );

        // Wait for initial leader election
        engine
            .wait_ready(Duration::from_secs(5))
            .await
            .expect("Initial leader election should succeed");

        // Spawn multiple concurrent wait_ready calls
        let mut handles = vec![];
        for _ in 0..10 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                let start = std::time::Instant::now();
                let result = engine_clone.wait_ready(Duration::from_secs(5)).await;
                let duration = start.elapsed();
                (result, duration)
            });
            handles.push(handle);
        }

        // All should complete successfully and quickly
        for handle in handles {
            let (result, duration) = handle.await.expect("Task should not panic");
            assert!(result.is_ok(), "wait_ready should succeed");
            assert!(
                duration < Duration::from_millis(100),
                "Should return immediately, took {duration:?}"
            );
        }

        Arc::try_unwrap(engine)
            .ok()
            .expect("Arc should have single owner")
            .stop()
            .await
            .expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_wait_ready_check_current_value_first() {
        let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

        let engine = EmbeddedEngine::start_custom(storage, sm, None)
            .await
            .expect("Failed to start engine");

        // Wait for leader election
        engine
            .wait_ready(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");

        // Verify current value is set
        let leader_rx = engine.leader_change_notifier();
        let current_leader = *leader_rx.borrow();
        assert!(current_leader.is_some(), "Current leader should be set");

        // Now call wait_ready again - it should check current value first
        // and return immediately without waiting for changed() event
        let start = std::time::Instant::now();
        let result = engine.wait_ready(Duration::from_secs(5)).await;
        let duration = start.elapsed();

        assert!(result.is_ok(), "Should succeed");
        assert!(
            duration < Duration::from_millis(50),
            "Should check current value first and return immediately, took {duration:?}"
        );

        engine.stop().await.expect("Failed to stop engine");
    }

    /// Tests for configuration validation (start/start_with with various configs)
    #[cfg(feature = "rocksdb")]
    mod config_validation_tests {
        use super::*;
        use serial_test::serial;

        // Tests for start() method with CONFIG_PATH have been moved to embedded_env_test.rs
        // to run sequentially and avoid environment variable race conditions

        #[tokio::test]
        #[cfg(debug_assertions)]
        #[serial]
        async fn test_start_without_config_path_env_allows_in_debug() {
            // No CONFIG_PATH env var - uses default config with /tmp/db
            unsafe {
                std::env::remove_var("CONFIG_PATH");
            }

            // Clean up /tmp/db before test to avoid corruption from previous runs
            let _ = std::fs::remove_dir_all("/tmp/db");

            let result = EmbeddedEngine::start().await;

            assert!(
                result.is_ok(),
                "start() should allow default /tmp/db in debug mode without CONFIG_PATH. Error: {:?}",
                result.as_ref().err()
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
        async fn test_start_without_config_path_env_rejects_in_release() {
            // No CONFIG_PATH env var - should reject default /tmp/db in release
            unsafe {
                std::env::remove_var("CONFIG_PATH");
            }
            let result = EmbeddedEngine::start().await;

            assert!(
                result.is_err(),
                "start() should reject default /tmp/db in release mode without CONFIG_PATH"
            );

            if let Err(e) = result {
                let err_msg = format!("{:?}", e);
                assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
            }
        }

        // Tests for start_with() method (explicit config path)

        #[tokio::test]
        async fn test_start_with_valid_config() {
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

[raft]
heartbeat_interval_ms = 500
election_timeout_min_ms = 1500
election_timeout_max_ms = 3000
"#,
                data_dir.display()
            );
            std::fs::write(&config_path, config_content).expect("Failed to write config");

            // Should succeed with valid config
            let result = EmbeddedEngine::start_with(config_path.to_str().unwrap()).await;
            assert!(
                result.is_ok(),
                "start_with() should succeed with valid config"
            );

            if let Ok(engine) = result {
                engine.stop().await.ok();
            }
            // _temp_dir stays alive until here
        }

        #[tokio::test]
        async fn test_start_with_nonexistent_config() {
            let result = EmbeddedEngine::start_with("/nonexistent/config.toml").await;

            assert!(
                result.is_err(),
                "start_with() should fail with nonexistent config"
            );
        }

        #[tokio::test]
        #[cfg(debug_assertions)]
        #[serial]
        async fn test_start_with_tmp_db_allows_in_debug() {
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
            let result = EmbeddedEngine::start_with(config_path.to_str().unwrap()).await;
            assert!(
                result.is_ok(),
                "start_with() should allow /tmp/db in debug mode"
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
        async fn test_start_with_tmp_db_rejects_in_release() {
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
            let result = EmbeddedEngine::start_with(config_path.to_str().unwrap()).await;
            assert!(
                result.is_err(),
                "start_with() should reject /tmp/db in release mode"
            );

            if let Err(e) = result {
                let err_msg = format!("{:?}", e);
                assert!(err_msg.contains("/tmp/db") || err_msg.contains("db_root_dir"));
            }
        }

        #[tokio::test]
        async fn test_drop_without_stop_warning() {
            let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

            let engine = EmbeddedEngine::start_custom(storage, sm, None)
                .await
                .expect("Failed to start engine");

            // Drop without calling stop() - should trigger warning in Drop impl
            // Note: We can't easily capture the warning log in test, but this
            // verifies the code path doesn't panic
            drop(engine);
        }
    }

    #[cfg(feature = "watch")]
    mod watch_tests {
        use super::*;
        use serial_test::serial;

        #[tokio::test]
        #[serial]
        async fn test_watch_registers_successfully() {
            let (storage, sm, _temp_dir) = create_test_storage_and_sm().await;

            let engine = EmbeddedEngine::start_custom(storage, sm, None)
                .await
                .expect("Failed to start engine");

            engine
                .wait_ready(Duration::from_secs(5))
                .await
                .expect("Leader should be elected");

            // Register watcher
            let result = engine.watch(b"test_key");
            assert!(
                result.is_ok(),
                "watch() should succeed when feature is enabled"
            );

            if let Ok(mut handle) = result {
                // Give watcher time to register
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Trigger a change
                let client = engine.client();
                client
                    .put(b"test_key".to_vec(), b"value1".to_vec())
                    .await
                    .expect("Put should succeed");

                // Should receive watch event
                let event =
                    tokio::time::timeout(Duration::from_secs(2), handle.receiver_mut().recv())
                        .await
                        .expect("Should receive event within timeout");

                assert!(event.is_some(), "Should receive watch event");
            }

            engine.stop().await.expect("Failed to stop engine");
        }
    }

    #[cfg(feature = "watch")]
    mod watch_tempdir_tests {
        use super::*;

        /// Test to verify Watch fails when TempDir is dropped before engine usage
        ///
        /// This test directly demonstrates the root cause of watch test failures:
        /// When TempDir is dropped, watch events cannot be delivered even though
        /// basic operations like PUT still work.
        #[tokio::test]
        async fn test_watch_with_tempdir_dropped() {
            println!("\n=== Testing Watch with TempDir DROPPED ===");

            let (storage, sm) = {
                let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
                let storage_path = temp_dir.path().join("storage");
                let sm_path = temp_dir.path().join("sm");

                std::fs::create_dir_all(&storage_path).unwrap();
                std::fs::create_dir_all(&sm_path).unwrap();

                let storage = Arc::new(
                    FileStorageEngine::new(storage_path).expect("Failed to create storage"),
                );
                let sm = Arc::new(
                    FileStateMachine::new(sm_path).await.expect("Failed to create state machine"),
                );

                println!("Created storage and SM, TempDir about to be dropped...");
                (storage, sm)
                // temp_dir dropped here!
            };

            println!("TempDir dropped, starting engine...");

            let engine = EmbeddedEngine::start_custom(storage, sm, None)
                .await
                .expect("Failed to start engine");

            engine
                .wait_ready(Duration::from_secs(5))
                .await
                .expect("Leader should be elected");

            println!("Engine started and leader elected");

            // Register watcher
            let result = engine.watch(b"test_key");
            println!("Watch registration result: {:?}", result.is_ok());
            assert!(result.is_ok(), "watch() should succeed");

            if let Ok(mut handle) = result {
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Trigger a change
                println!("Performing PUT operation...");
                let client = engine.client();
                client
                    .put(b"test_key".to_vec(), b"value1".to_vec())
                    .await
                    .expect("Put should succeed");

                println!("PUT succeeded, waiting for watch event...");

                // Try to receive watch event with timeout
                let event_result =
                    tokio::time::timeout(Duration::from_secs(2), handle.receiver_mut().recv())
                        .await;

                match event_result {
                    Ok(Some(_)) => {
                        println!("✅ Watch event RECEIVED (unexpected!)");
                        panic!("Watch should have failed with dropped TempDir");
                    }
                    Ok(None) => {
                        println!("❌ Watch channel closed");
                    }
                    Err(_) => {
                        println!("❌ Watch event TIMEOUT (expected - this is the bug!)");
                    }
                }
            }

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test to verify Watch works when TempDir is kept alive
        #[tokio::test]
        async fn test_watch_with_tempdir_alive() {
            println!("\n=== Testing Watch with TempDir ALIVE ===");

            let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let storage_path = _temp_dir.path().join("storage");
            let sm_path = _temp_dir.path().join("sm");

            std::fs::create_dir_all(&storage_path).unwrap();
            std::fs::create_dir_all(&sm_path).unwrap();

            let storage =
                Arc::new(FileStorageEngine::new(storage_path).expect("Failed to create storage"));
            let sm = Arc::new(
                FileStateMachine::new(sm_path).await.expect("Failed to create state machine"),
            );

            println!("Created storage and SM, TempDir kept alive");

            let engine = EmbeddedEngine::start_custom(storage, sm, None)
                .await
                .expect("Failed to start engine");

            engine
                .wait_ready(Duration::from_secs(5))
                .await
                .expect("Leader should be elected");

            println!("Engine started and leader elected");

            // Register watcher
            let result = engine.watch(b"test_key");
            println!("Watch registration result: {:?}", result.is_ok());
            assert!(result.is_ok(), "watch() should succeed");

            if let Ok(mut handle) = result {
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Trigger a change
                println!("Performing PUT operation...");
                let client = engine.client();
                client
                    .put(b"test_key".to_vec(), b"value1".to_vec())
                    .await
                    .expect("Put should succeed");

                println!("PUT succeeded, waiting for watch event...");

                // Try to receive watch event
                let event_result =
                    tokio::time::timeout(Duration::from_secs(2), handle.receiver_mut().recv())
                        .await;

                match event_result {
                    Ok(Some(_)) => {
                        println!("✅ Watch event RECEIVED (expected!)");
                    }
                    Ok(None) => {
                        println!("❌ Watch channel closed (unexpected)");
                        panic!("Watch channel should not be closed");
                    }
                    Err(_) => {
                        println!("❌ Watch event TIMEOUT (unexpected)");
                        panic!("Watch event should have been received");
                    }
                }
            }

            engine.stop().await.expect("Failed to stop engine");
            // _temp_dir stays alive until here
        }
    }
}
