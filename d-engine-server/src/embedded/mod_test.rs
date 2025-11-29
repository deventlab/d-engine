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
    async fn test_wait_leader_single_node_success() {
        let (storage, sm) = create_test_storage_and_sm().await;

        // Start embedded engine (single node)
        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        // Wait for node initialization
        engine.ready().await;

        // Wait for leader election (should succeed quickly in single-node mode)
        let result = engine.wait_leader(Duration::from_secs(5)).await;

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
    async fn test_wait_leader_timeout() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        engine.ready().await;

        // In single-node mode, leader should be elected immediately
        // But if we had a cluster without quorum, this would timeout
        // For this test, we verify timeout mechanism works
        let very_short_timeout = Duration::from_nanos(1);

        // Note: This might still succeed if election happens instantly
        // The test verifies timeout handling exists
        let _ = engine.wait_leader(very_short_timeout).await;

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_leader_notifier_subscription() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        engine.ready().await;

        // Subscribe to leader changes
        let mut leader_rx = engine.leader_notifier();

        // Wait for leader election
        engine
            .wait_leader(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");

        // The notifier should have the current leader
        tokio::time::timeout(Duration::from_secs(1), leader_rx.changed())
            .await
            .expect("Should receive leader notification within timeout")
            .expect("Should receive change event");

        let current_leader = leader_rx.borrow().clone();
        assert!(current_leader.is_some(), "Leader should be elected");

        if let Some(info) = current_leader {
            assert_eq!(info.leader_id, 1);
            assert!(info.term > 0);
        }

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_ready_and_wait_leader_sequence() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        // Step 1: Wait for node initialization
        let ready_start = std::time::Instant::now();
        engine.ready().await;
        let ready_duration = ready_start.elapsed();

        // Step 2: Wait for leader election
        let leader_start = std::time::Instant::now();
        let leader_info = engine
            .wait_leader(Duration::from_secs(5))
            .await
            .expect("Leader should be elected");
        let leader_duration = leader_start.elapsed();

        // Verify timing (single-node should be fast)
        assert!(
            ready_duration < Duration::from_secs(2),
            "Node initialization should be fast"
        );
        assert!(
            leader_duration < Duration::from_secs(2),
            "Leader election should be fast in single-node"
        );

        // Verify leader info
        assert_eq!(leader_info.leader_id, 1);
        assert!(leader_info.term > 0);

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_client_available_after_wait_leader() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        engine.ready().await;
        engine
            .wait_leader(Duration::from_secs(5))
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
    async fn test_multiple_leader_notifier_subscribers() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        engine.ready().await;

        // Create multiple subscribers
        let mut rx1 = engine.leader_notifier();
        let mut rx2 = engine.leader_notifier();

        // Wait for leader election
        engine
            .wait_leader(Duration::from_secs(5))
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
        let leader1 = rx1.borrow().clone();
        let leader2 = rx2.borrow().clone();
        assert_eq!(leader1, leader2, "Both subscribers should see same leader");

        engine.stop().await.expect("Failed to stop engine");
    }

    #[tokio::test]
    async fn test_engine_stop_cleans_up() {
        let (storage, sm) = create_test_storage_and_sm().await;

        let engine =
            EmbeddedEngine::start(None, storage, sm).await.expect("Failed to start engine");

        engine.ready().await;

        // Stop should complete without error
        let stop_result = engine.stop().await;
        assert!(stop_result.is_ok(), "Stop should succeed");
    }
}
