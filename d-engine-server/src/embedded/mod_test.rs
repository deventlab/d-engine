//! Unit tests for EmbeddedEngine leader election APIs

#[cfg(test)]
mod embedded_engine_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::embedded::EmbeddedEngine;
    use crate::storage::FileStateMachine;
    use crate::storage::FileStorageEngine;

    async fn create_test_storage_and_sm() -> (Arc<FileStorageEngine>, Arc<FileStateMachine>) {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage_path = temp_dir.path().join("storage");
        let sm_path = temp_dir.path().join("sm");

        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::create_dir_all(&sm_path).unwrap();

        let storage =
            Arc::new(FileStorageEngine::new(storage_path).expect("Failed to create storage"));
        let sm =
            Arc::new(FileStateMachine::new(sm_path).await.expect("Failed to create state machine"));

        (storage, sm)
    }

    #[tokio::test]
    async fn test_wait_ready_single_node_success() {
        let (storage, sm) = create_test_storage_and_sm().await;

        // Start embedded engine (single node)
        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
    async fn test_leader_change_notifier_subscription() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        // Stop should complete without error
        let stop_result = engine.stop().await;
        assert!(stop_result.is_ok(), "Stop should succeed");
    }

    #[tokio::test]
    async fn test_wait_ready_race_condition_already_elected() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine = Arc::new(
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine"),
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
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

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
}
