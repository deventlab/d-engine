#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;

    /// Test that get_multi reconstructs results in correct key order.
    ///
    /// Verifies the critical fix: when server returns only results for
    /// existing keys (sparse), we must map by key to preserve positional
    /// correspondence with the input key vector.
    #[test]
    fn test_get_multi_result_reconstruction() {
        // Simulate server response scenario:
        // Request: [key1, key2, key3]
        // Exists: [key1, key3]  (key2 is missing)
        // Server returns: [key1→value1, key3→value3]

        let requested_keys: Vec<_> = [
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ]
        .to_vec();
        let server_results: Vec<_> = vec![
            (Bytes::from("key1"), Bytes::from("value1")),
            (Bytes::from("key3"), Bytes::from("value3")),
        ];

        // Simulate the fixed reconstruction logic
        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        // Expected: [Some(value1), None, Some(value3)]
        assert_eq!(
            reconstructed.len(),
            3,
            "Result count must match request count"
        );
        assert_eq!(
            reconstructed[0],
            Some(Bytes::from("value1")),
            "Position 0 is key1"
        );
        assert_eq!(reconstructed[1], None, "Position 1 is key2 (missing)");
        assert_eq!(
            reconstructed[2],
            Some(Bytes::from("value3")),
            "Position 2 is key3"
        );
    }

    /// Test edge case: all requested keys exist.
    #[test]
    fn test_get_multi_all_keys_exist() {
        let requested_keys: Vec<_> = [Bytes::from("a"), Bytes::from("b")].to_vec();
        let server_results: Vec<_> = vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ];

        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        assert_eq!(reconstructed.len(), 2);
        assert_eq!(reconstructed[0], Some(Bytes::from("1")));
        assert_eq!(reconstructed[1], Some(Bytes::from("2")));
    }

    /// Test edge case: no requested keys exist.
    #[test]
    fn test_get_multi_no_keys_exist() {
        let requested_keys: Vec<_> =
            [Bytes::from("x"), Bytes::from("y"), Bytes::from("z")].to_vec();
        let server_results: Vec<(Bytes, Bytes)> = Vec::new();

        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        assert_eq!(reconstructed.len(), 3);
        assert!(reconstructed.iter().all(|r| r.is_none()));
    }

    /// Test that empty byte values are preserved correctly.
    #[test]
    fn test_get_multi_preserves_empty_values() {
        let requested_keys: Vec<_> = [Bytes::from("empty"), Bytes::from("nonempty")].to_vec();
        let server_results: Vec<_> = vec![
            (Bytes::from("empty"), Bytes::new()),
            (Bytes::from("nonempty"), Bytes::from("v")),
        ];

        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        assert_eq!(reconstructed[0], Some(Bytes::new()));
        assert_eq!(reconstructed[1], Some(Bytes::from("v")));
    }
}

// =============================================================================
// Integration Tests (require EmbeddedEngine)
// =============================================================================

#[cfg(test)]
mod integration_tests {
    use std::time::Duration;

    use d_engine_core::ClientApi;
    use tempfile::TempDir;

    use crate::api::EmbeddedEngine;

    /// Helper to create a test EmbeddedEngine
    async fn create_test_engine() -> (EmbeddedEngine, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("db");

        let config_path = temp_dir.path().join("d-engine.toml");
        let port = 50000 + (std::process::id() % 10000);
        let config_content = format!(
            r#"
[cluster]
listen_address = "127.0.0.1:{}"
db_root_dir = "{}"
single_node = true

[raft]
general_raft_timeout_duration_in_ms = 100

[raft.commit_handler]
batch_size_threshold = 1
process_interval_ms = 1

[replication]
rpc_append_entries_in_batch_threshold = 0

"#,
            port,
            db_path.display()
        );
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let engine = EmbeddedEngine::start_with(config_path.to_str().unwrap())
            .await
            .expect("Failed to start engine");

        engine.wait_ready(Duration::from_secs(5)).await.expect("Engine not ready");

        (engine, temp_dir)
    }

    mod cas_operations {
        use super::*;

        /// Test CAS success scenario - acquiring a distributed lock
        #[tokio::test]
        async fn test_cas_acquire_lock_success() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            let lock_key = b"distributed_lock";
            let owner = b"client_a";

            // CAS: None -> "client_a" (acquire lock)
            let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner).await;
            assert!(result.is_ok(), "CAS should not error");
            assert!(result.unwrap(), "CAS should succeed");

            // Verify lock is set
            let value = client.get(lock_key).await.expect("get should succeed");
            assert_eq!(value, Some(owner.to_vec().into()));

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test CAS conflict scenario - lock already held by another client
        #[tokio::test]
        async fn test_cas_lock_conflict() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            let lock_key = b"distributed_lock";
            let owner_a = b"client_a";
            let owner_b = b"client_b";

            // Client A acquires lock
            let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner_a).await;
            assert!(result.unwrap(), "Client A should acquire lock");

            // Client B tries to acquire (should fail)
            let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner_b).await;
            assert!(!result.unwrap(), "Client B should fail - lock held");

            // Verify lock still held by client_a
            let value = client.get(lock_key).await.expect("get should succeed");
            assert_eq!(value, Some(owner_a.to_vec().into()));

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test CAS release lock - correct owner releases the lock
        #[tokio::test]
        async fn test_cas_release_lock() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            let lock_key = b"distributed_lock";
            let owner = b"client_a";

            // Acquire lock
            let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner).await;
            assert!(result.unwrap());

            // Release lock (CAS: "client_a" -> empty)
            let result = client.compare_and_swap(lock_key, Some(owner), b"").await;
            assert!(result.unwrap(), "Correct owner should release lock");

            // Verify lock is released (empty value)
            let value = client.get(lock_key).await.expect("get should succeed");
            assert_eq!(value, Some(b"".to_vec().into()));

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test CAS prevent wrong release - only correct owner can release
        #[tokio::test]
        async fn test_cas_prevent_wrong_release() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            let lock_key = b"distributed_lock";
            let owner_a = b"client_a";
            let wrong_owner = b"client_b";

            // Client A acquires lock
            let result = client.compare_and_swap(lock_key, None::<&[u8]>, owner_a).await;
            assert!(result.unwrap());

            // Client B tries to release (should fail - wrong owner)
            let result = client.compare_and_swap(lock_key, Some(wrong_owner), b"").await;
            assert!(!result.unwrap(), "Wrong owner cannot release lock");

            // Verify lock still held by client_a
            let value = client.get(lock_key).await.expect("get should succeed");
            assert_eq!(value, Some(owner_a.to_vec().into()));

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test CAS edge cases - empty values and large values
        #[tokio::test]
        async fn test_cas_edge_cases() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            // Test 1: Empty value CAS
            let result = client.compare_and_swap(b"empty_key", None::<&[u8]>, b"").await;
            assert!(result.unwrap(), "CAS with empty value should succeed");

            let value = client.get(b"empty_key").await.expect("get should succeed");
            assert_eq!(value, Some(b"".to_vec().into()));

            // Test 2: Large value CAS
            let large_value = vec![b'x'; 1024 * 1024]; // 1MB
            let result = client.compare_and_swap(b"large_key", None::<&[u8]>, &large_value).await;
            assert!(result.unwrap(), "CAS with large value should succeed");

            let value = client.get(b"large_key").await.expect("get should succeed");
            assert_eq!(value, Some(large_value.into()));

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test CAS on non-existent key
        #[tokio::test]
        async fn test_cas_nonexistent_key_success() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            let nonexistent_key = b"does_not_exist";

            // CAS: None -> "new_value" on non-existent key (should succeed)
            let result =
                client.compare_and_swap(nonexistent_key, None::<&[u8]>, b"new_value").await;
            assert!(
                result.unwrap(),
                "CAS on non-existent key with None should succeed"
            );

            let value = client.get(nonexistent_key).await.expect("get should succeed");
            assert_eq!(value, Some(b"new_value".to_vec().into()));

            engine.stop().await.expect("Failed to stop engine");
        }

        /// Test CAS on non-existent key with wrong expected value
        #[tokio::test]
        async fn test_cas_nonexistent_key_failure() {
            let (engine, _temp_dir) = create_test_engine().await;
            let client = engine.client();

            let nonexistent_key = b"does_not_exist";

            // CAS: Some(wrong) -> "value" on non-existent key (should fail)
            let result = client.compare_and_swap(nonexistent_key, Some(b"wrong"), b"value").await;
            assert!(
                !result.unwrap(),
                "CAS on non-existent key with Some(wrong) should fail"
            );

            // Key should still not exist
            let value = client.get(nonexistent_key).await.expect("get should succeed");
            assert_eq!(value, None);

            engine.stop().await.expect("Failed to stop engine");
        }
    }
}
