use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use tempfile::TempDir;

use crate::ApplyEntry;
use crate::Command;
use crate::Error;
use crate::storage::StateMachine;

/// Test suite for StateMachine implementations
///
/// This suite provides comprehensive tests that can be used to validate
/// any StateMachine implementation. Developers should implement the
/// `StateMachineBuilder` trait and then call `run_all_state_machine_tests`
/// with their builder.
pub struct StateMachineTestSuite;

/// Builder trait for creating StateMachine instances for testing
#[async_trait]
pub trait StateMachineBuilder: Send + Sync {
    /// Create a new StateMachine instance for testing
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error>;

    /// Clean up any resources after testing
    async fn cleanup(&self) -> Result<(), Error>;
}

impl StateMachineTestSuite {
    /// Run all state machine tests
    pub async fn run_all_tests<B: StateMachineBuilder>(builder: B) -> Result<(), Error> {
        Self::test_start_stop(builder.build().await?).await?;
        Self::test_basic_kv_operations(builder.build().await?).await?;
        Self::test_apply_chunk_functionality(builder.build().await?).await?;
        Self::test_cas_operations(builder.build().await?).await?;
        Self::test_last_applied_detection(builder.build().await?).await?;
        Self::test_snapshot_operations(builder.build().await?).await?;
        Self::test_persistence(builder.build().await?).await?;

        Self::test_drop_flushes_data(&builder).await?;
        Self::test_drop_persists_last_applied(&builder).await?;
        Self::test_data_survives_reopen(&builder).await?;
        Self::test_ungraceful_shutdown_recovery(&builder).await?;
        Self::test_reset_operation(builder.build().await?).await?;

        Self::test_get_multi_returns_values_in_key_order(builder.build().await?).await?;
        Self::test_get_multi_absent_keys_return_none(builder.build().await?).await?;
        Self::test_get_multi_empty_keys_returns_empty(builder.build().await?).await?;
        Self::test_get_multi_partial_hit_some_keys_missing(builder.build().await?).await?;
        Self::test_get_multi_service_registry_coherence(builder.build().await?).await?;
        Self::test_get_multi_election_state_coherence(builder.build().await?).await?;
        Self::test_get_multi_quota_management_coherence(builder.build().await?).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Run performance tests (optional, not included in run_all_tests)
    pub async fn run_performance_tests<B: StateMachineBuilder>(builder: B) -> Result<(), Error> {
        Self::test_apply_chunk_performance_smoke(builder.build().await?).await?;
        Self::test_apply_chunk_scalability(builder.build().await?).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Test start/stop functionality
    pub async fn test_start_stop(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
        // Test default state
        assert!(state_machine.is_running());

        // Test explicit start/stop
        state_machine.start().await?;
        assert!(state_machine.is_running());
        state_machine.stop()?;
        assert!(!state_machine.is_running());
        state_machine.start().await?;
        assert!(state_machine.is_running());

        Ok(())
    }

    /// Test basic key-value operations
    pub async fn test_basic_kv_operations(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let test_key = b"test_key";
        let test_value = Bytes::from(b"test_value".to_vec());

        // Create an insert entry
        let entries = vec![create_insert_entry(
            1,
            Bytes::from(test_key.to_vec()),
            test_value.clone(),
        )];
        state_machine.apply_chunk(&entries).await?;

        // Verify the value was inserted
        match state_machine.get(test_key)? {
            Some(value) => assert_eq!(value, test_value),
            None => panic!("Value not found after insert"),
        }

        // Create a delete entry
        let entries = vec![create_delete_entry(2, Bytes::from(test_key.to_vec()))];
        state_machine.apply_chunk(&entries).await?;

        // Verify the value was deleted
        assert!(state_machine.get(test_key)?.is_none());

        Ok(())
    }

    /// Test CAS operations
    pub async fn test_cas_operations(sm: Arc<dyn StateMachine>) -> Result<(), Error> {
        sm.start().await?;

        // Test 1: CAS on non-existent key (expected None)
        let entry1 = create_cas_entry(1, b"lock".to_vec().into(), None, b"owner1".to_vec().into());
        sm.apply_chunk(&[entry1]).await?;
        assert_eq!(sm.get(b"lock")?, Some(b"owner1".to_vec().into()));

        // Test 2: CAS success (expected matches)
        let entry2 = create_cas_entry(
            2,
            b"lock".to_vec().into(),
            Some(b"owner1".to_vec().into()),
            b"owner2".to_vec().into(),
        );
        sm.apply_chunk(&[entry2]).await?;
        assert_eq!(sm.get(b"lock")?, Some(b"owner2".to_vec().into()));

        // Test 3: CAS failure (expected mismatch) - value should not change
        let entry3 = create_cas_entry(
            3,
            b"lock".to_vec().into(),
            Some(b"wrong".to_vec().into()),
            b"owner3".to_vec().into(),
        );
        sm.apply_chunk(&[entry3]).await?;
        assert_eq!(sm.get(b"lock")?, Some(b"owner2".to_vec().into())); // Still owner2

        sm.stop()?;
        Ok(())
    }

    /// Test chunk application functionality
    pub async fn test_apply_chunk_functionality(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Create a mix of insert and delete operations
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_delete_entry(3, Bytes::from(b"key1".to_vec())),
            create_insert_entry(
                4,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];

        state_machine.apply_chunk(&entries).await?;

        // Verify the final state
        assert!(state_machine.get(b"key1")?.is_none());
        assert_eq!(
            state_machine.get(b"key2")?,
            Some(Bytes::from(b"value2".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key3")?,
            Some(Bytes::from(b"value3".to_vec()))
        );
        assert_eq!(state_machine.last_applied(), LogId { index: 4, term: 1 });

        Ok(())
    }

    /// Test last applied index detection
    pub async fn test_last_applied_detection(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        assert!(state_machine.reset().await.is_ok());
        // Initial state
        assert_eq!(state_machine.last_applied(), LogId { index: 0, term: 0 });

        // Apply entries with different terms
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_insert_entry(
                3,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];

        state_machine.apply_chunk(&entries).await?;
        assert_eq!(state_machine.last_applied(), LogId { index: 3, term: 1 });

        Ok(())
    }

    /// Test snapshot operations
    pub async fn test_snapshot_operations(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Add some test data
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_insert_entry(
                3,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&entries).await?;

        // Create a temporary directory for the snapshot
        let temp_dir = TempDir::new()?;
        let snapshot_dir = temp_dir.path().join("snapshot");

        // Generate snapshot
        let last_included = LogId { index: 3, term: 1 };
        let checksum = state_machine
            .generate_snapshot_data(snapshot_dir.clone(), last_included)
            .await?;

        // Verify snapshot metadata was updated
        let metadata = state_machine.snapshot_metadata();
        assert!(metadata.is_some());
        assert_eq!(metadata.unwrap().last_included, Some(last_included));

        // Apply snapshot (simulate receiving from leader)
        // let snapshot_path = snapshot_dir.join("snapshot.bin");
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::from(checksum.to_vec()),
        };

        //Reset State Machine to make sure it is fresh
        state_machine.reset().await?;
        assert_eq!(state_machine.get(b"key1")?, None);
        assert_eq!(state_machine.get(b"key2")?, None);
        assert_eq!(state_machine.get(b"key3")?, None);
        assert_eq!(state_machine.last_applied(), LogId::default());

        // Assume there were some entries. After applying snapshot, all the old ones should be
        // cleared.
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"old_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"old_key2".to_vec()),
                Bytes::from(b"vold_alue2".to_vec()),
            ),
            create_insert_entry(
                3,
                Bytes::from(b"old_key3".to_vec()),
                Bytes::from(b"old_value3".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&entries).await?;

        state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await?;

        // Verify state was preserved after snapshot application
        assert_eq!(
            state_machine.get(b"key1")?,
            Some(Bytes::from(b"value1".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key2")?,
            Some(Bytes::from(b"value2".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key3")?,
            Some(Bytes::from(b"value3".to_vec()))
        );
        assert_eq!(state_machine.get(b"old_key2")?, None);
        assert_eq!(state_machine.get(b"old_key3")?, None);
        assert_eq!(state_machine.last_applied(), last_included);

        Ok(())
    }

    /// Test data persistence
    pub async fn test_persistence(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
        // Add test data
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&entries).await?;

        // Update last applied
        let last_applied = LogId { index: 2, term: 1 };
        state_machine.persist_last_applied(last_applied)?;

        // Update snapshot metadata
        let snapshot_metadata = SnapshotMetadata {
            last_included: Some(last_applied),
            checksum: Bytes::from(vec![0; 32]),
        };
        state_machine.persist_last_snapshot_metadata(&snapshot_metadata)?;

        // Flush to ensure persistence
        state_machine.flush()?;

        Ok(())
    }

    /// Performance smoke test: apply_chunk baseline
    ///
    /// Ensures apply_chunk doesn't have catastrophic performance issues.
    /// Threshold: 100 entries in < 1 second (generous for CI stability).
    pub async fn test_apply_chunk_performance_smoke(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let entries: Vec<_> = (1..=100)
            .map(|i| {
                create_insert_entry(
                    i,
                    Bytes::from(format!("perf_key_{i}")),
                    Bytes::from(format!("perf_value_{i}")),
                )
            })
            .collect();

        let start = Instant::now();
        state_machine.apply_chunk(&entries).await?;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "Performance regression: apply_chunk(100) took {elapsed:?} (expected < 1s)",
        );

        Ok(())
    }

    /// Performance scalability test: verify O(N) complexity
    ///
    /// Tests that 1000 entries take ~10x longer than 100 entries (not 100x).
    /// Detects algorithmic issues (e.g., O(N²) instead of O(N)).
    pub async fn test_apply_chunk_scalability(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Baseline: 100 entries
        let small_entries: Vec<_> = (1..=100)
            .map(|i| {
                create_insert_entry(
                    i,
                    Bytes::from(format!("scale_key_{i}")),
                    Bytes::from(format!("scale_value_{i}")),
                )
            })
            .collect();

        let start_small = Instant::now();
        state_machine.apply_chunk(&small_entries).await?;
        let elapsed_small = start_small.elapsed();

        // Large batch: 1000 entries
        let large_entries: Vec<_> = (101..=1100)
            .map(|i| {
                create_insert_entry(
                    i,
                    Bytes::from(format!("scale_key_{i}")),
                    Bytes::from(format!("scale_value_{i}")),
                )
            })
            .collect();

        let start_large = Instant::now();
        state_machine.apply_chunk(&large_entries).await?;
        let elapsed_large = start_large.elapsed();

        // Verify linear scalability: 1000 entries should be 5x-15x slower (not 100x)
        let ratio = elapsed_large.as_micros() as f64 / elapsed_small.as_micros().max(1) as f64;

        // Relax threshold in CI environment (resource-constrained)
        let threshold = if std::env::var("CI").is_ok() {
            100.0 // CI: Allow up to 100x (disk I/O can be slow)
        } else {
            20.0 // Local: Expect near-linear scalability
        };

        assert!(
            ratio < threshold,
            "Scalability issue: 1000 entries took {ratio:.1}x longer than 100 entries (expected ~10x). \
             Possible O(N²) complexity. Threshold: {threshold}x"
        );

        Ok(())
    }

    /// Test that stop() persists data correctly
    ///
    /// This test verifies that:
    /// 1. Calling stop() flushes all data to disk
    /// 2. Data survives across StateMachine instances (reopen from same path)
    /// 3. Stop triggers proper cleanup including flush
    ///
    /// This catches bugs where:
    /// - stop() doesn't call flush
    /// - Data lives only in memory buffers
    ///
    /// Test that stop() properly persists data
    ///
    /// This test verifies that calling stop() explicitly will persist both
    /// data and metadata. This is the "graceful shutdown" path.
    pub async fn test_drop_flushes_data<B: StateMachineBuilder>(builder: &B) -> Result<(), Error> {
        // Step 1: Create state machine, write data, call stop()
        let sm = builder.build().await?;
        sm.start().await?;

        // Write test data
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"drop_test_key1".to_vec()),
                Bytes::from(b"drop_test_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"drop_test_key2".to_vec()),
                Bytes::from(b"drop_test_value2".to_vec()),
            ),
        ];
        sm.apply_chunk(&entries).await?;

        // Verify data exists in memory
        assert_eq!(
            sm.get(b"drop_test_key1")?,
            Some(Bytes::from(b"drop_test_value1".to_vec())),
            "Data should exist before stop"
        );

        // Call stop() for graceful shutdown - stop() must persist data
        sm.stop()?;

        // Explicitly drop Arc to release database locks (critical for RocksDB)
        drop(sm);
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Step 2: Reopen state machine from same path
        let sm2 = builder.build().await?;
        sm2.start().await?;

        // Step 3: Verify data survived
        let value1 = sm2.get(b"drop_test_key1")?;
        let value2 = sm2.get(b"drop_test_key2")?;

        assert_eq!(
            value1,
            Some(Bytes::from(b"drop_test_value1".to_vec())),
            "BUG: Data lost after stop! StateMachine.stop() must persist data."
        );
        assert_eq!(
            value2,
            Some(Bytes::from(b"drop_test_value2".to_vec())),
            "BUG: Data lost after stop! StateMachine.stop() must persist data."
        );

        Ok(())
    }

    /// Test that Drop ALONE persists last_applied metadata
    ///
    /// This is a critical test that verifies Drop implementation correctness.
    /// When a StateMachine is dropped (without explicit stop/flush), it must
    /// still persist last_applied index/term to prevent replay of already
    /// applied entries on restart.
    ///
    /// Bug scenario if Drop is incorrect:
    /// 1. Apply entries 1-10
    /// 2. Crash before persisting last_applied=10
    /// 3. On restart, last_applied reads as 0
    /// 4. Raft replays entries 1-10 again -> data corruption
    pub async fn test_drop_persists_last_applied<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        // Step 1: Write data - NO explicit stop() or flush() before drop
        {
            let sm = builder.build().await?;
            sm.start().await?;

            let entries = vec![
                create_insert_entry(
                    1,
                    Bytes::from(b"drop_meta_key1".to_vec()),
                    Bytes::from(b"drop_meta_value1".to_vec()),
                ),
                create_insert_entry(
                    2,
                    Bytes::from(b"drop_meta_key2".to_vec()),
                    Bytes::from(b"drop_meta_value2".to_vec()),
                ),
                create_insert_entry(
                    3,
                    Bytes::from(b"drop_meta_key3".to_vec()),
                    Bytes::from(b"drop_meta_value3".to_vec()),
                ),
            ];
            sm.apply_chunk(&entries).await?;

            // Verify last_applied in memory
            assert_eq!(
                sm.last_applied().index,
                3,
                "last_applied should be 3 after applying 3 entries"
            );

            // ⚠️ NO stop() call here!
            // ⚠️ NO flush() call here!
            // Just let Drop run when sm goes out of scope
        } // <- Drop runs here

        // Small delay to ensure file system sync (especially for macOS)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Reopen
        let sm2 = builder.build().await?;
        sm2.start().await?;

        // Step 3: Critical assertion - last_applied must be persisted by Drop
        assert_eq!(
            sm2.last_applied().index,
            3,
            "BUG: last_applied not persisted by Drop! \
             Drop implementation must call save_hard_state() (not just flush()) \
             to persist last_applied index/term. Without this, Raft will replay \
             already-applied entries after restart, causing data corruption."
        );

        // Also verify the data itself survived
        assert_eq!(
            sm2.get(b"drop_meta_key1")?,
            Some(Bytes::from(b"drop_meta_value1".to_vec())),
            "Data should also be persisted by Drop"
        );
        assert_eq!(
            sm2.get(b"drop_meta_key3")?,
            Some(Bytes::from(b"drop_meta_value3".to_vec())),
            "Data should also be persisted by Drop"
        );

        Ok(())
    }

    /// Test that data survives across stop() and reopen
    ///
    /// This test verifies:
    /// 1. Graceful stop() persists all data
    /// 2. Reopening from same path recovers all data
    /// 3. last_applied index is correctly restored
    pub async fn test_data_survives_reopen<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        let last_applied_before: LogId;

        // Step 1: Create, write, stop gracefully
        {
            let sm = builder.build().await?;
            sm.start().await?;

            let entries = vec![
                create_insert_entry(
                    1,
                    Bytes::from(b"reopen_key1".to_vec()),
                    Bytes::from(b"reopen_value1".to_vec()),
                ),
                create_insert_entry(
                    2,
                    Bytes::from(b"reopen_key2".to_vec()),
                    Bytes::from(b"reopen_value2".to_vec()),
                ),
                create_insert_entry(
                    3,
                    Bytes::from(b"reopen_key3".to_vec()),
                    Bytes::from(b"reopen_value3".to_vec()),
                ),
            ];
            sm.apply_chunk(&entries).await?;

            last_applied_before = sm.last_applied();
            assert_eq!(last_applied_before.index, 3);

            // Graceful stop
            sm.stop()?;
        }

        // Step 2: Reopen and verify
        let sm2 = builder.build().await?;
        sm2.start().await?;

        // Verify all data
        assert_eq!(
            sm2.get(b"reopen_key1")?,
            Some(Bytes::from(b"reopen_value1".to_vec())),
            "Data should survive graceful stop"
        );
        assert_eq!(
            sm2.get(b"reopen_key2")?,
            Some(Bytes::from(b"reopen_value2".to_vec()))
        );
        assert_eq!(
            sm2.get(b"reopen_key3")?,
            Some(Bytes::from(b"reopen_value3".to_vec()))
        );

        // Verify last_applied is restored
        assert_eq!(
            sm2.last_applied(),
            last_applied_before,
            "last_applied index should be restored after reopen"
        );

        Ok(())
    }

    /// Test recovery from ungraceful shutdown (crash simulation)
    ///
    /// This test simulates a crash by:
    /// 1. Writing data
    /// 2. Dropping without stop() - simulates SIGKILL/power loss
    /// 3. Verifying data recovery on reopen
    ///
    /// This is critical for production scenarios where:
    /// - Process receives SIGKILL
    /// - Server crashes/panics
    /// - Power loss occurs
    ///
    /// Test ungraceful shutdown recovery (crash simulation)
    ///
    /// This test simulates a crash by dropping the state machine without
    /// calling stop(). The Drop implementation must ensure that both data
    /// and metadata (especially last_applied) are persisted.
    ///
    /// This is different from test_drop_persists_last_applied because:
    /// - This test focuses on the "crash and recover" narrative
    /// - This test verifies both data AND metadata recovery
    /// - This explicitly models the ungraceful shutdown scenario
    pub async fn test_ungraceful_shutdown_recovery<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        // Step 1: Write data and drop ungracefully (no stop())
        let sm = builder.build().await?;
        sm.start().await?;

        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"crash_key1".to_vec()),
                Bytes::from(b"crash_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"crash_key2".to_vec()),
                Bytes::from(b"crash_value2".to_vec()),
            ),
        ];
        sm.apply_chunk(&entries).await?;

        // Verify data exists in memory
        assert!(sm.get(b"crash_key1")?.is_some());
        assert_eq!(sm.last_applied().index, 2);

        // ⚠️ NO flush() call here!
        // ⚠️ NO stop() call here!
        // 🔥 Simulate crash: drop without graceful shutdown
        // Drop implementation MUST handle persistence
        drop(sm);

        // Small delay to ensure file system sync
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Simulate restart - create new instance from same path
        let sm2 = builder.build().await?;
        sm2.start().await?;

        // Step 3: Verify crash recovery - both data and metadata
        let value1 = sm2.get(b"crash_key1")?;
        let value2 = sm2.get(b"crash_key2")?;

        assert_eq!(
            value1,
            Some(Bytes::from(b"crash_value1".to_vec())),
            "BUG: Data lost after ungraceful shutdown! \
             Drop implementation must persist data. Consider: \
             1) Implement Drop to call save_hard_state() \
             2) Use sync writes for critical data"
        );
        assert_eq!(
            value2,
            Some(Bytes::from(b"crash_value2".to_vec())),
            "BUG: Data lost after ungraceful shutdown!"
        );

        // Verify last_applied is recovered
        assert_eq!(
            sm2.last_applied().index,
            2,
            "BUG: last_applied not recovered after crash! \
             Drop must persist last_applied to prevent entry replay."
        );

        Ok(())
    }

    ///
    /// This test verifies that the reset operation:
    /// 1. Clears all data from memory
    /// 2. Resets Raft state to initial values
    /// 3. Clears all persisted files
    /// 4. Maintains operational state (running status)
    pub async fn test_reset_operation(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
        // Add test data
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_insert_entry(
                3,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&entries).await?;

        // Verify data exists
        assert_eq!(
            state_machine.get(b"key1")?,
            Some(Bytes::from(b"value1".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key2")?,
            Some(Bytes::from(b"value2".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key3")?,
            Some(Bytes::from(b"value3".to_vec()))
        );
        assert_eq!(state_machine.last_applied(), LogId { index: 3, term: 1 });
        // Note: snapshot_metadata may or may not be None depending on previous tests
        // The test focuses on verifying reset() clears everything, not initial state

        // Store running state for verification
        let was_running = state_machine.is_running();

        // Perform reset
        state_machine.reset().await?;

        // Verify all data is cleared
        assert!(state_machine.get(b"key1")?.is_none());
        assert!(state_machine.get(b"key2")?.is_none());
        assert!(state_machine.get(b"key3")?.is_none());

        // Verify Raft state is reset
        assert_eq!(state_machine.last_applied(), LogId { index: 0, term: 0 });
        assert!(state_machine.snapshot_metadata().is_none());

        // Verify operational state is maintained
        assert_eq!(state_machine.is_running(), was_running);

        // Test that we can add new data after reset
        let new_entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"new_key1".to_vec()),
                Bytes::from(b"new_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"new_key2".to_vec()),
                Bytes::from(b"new_value2".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&new_entries).await?;

        // Verify new data exists
        assert_eq!(
            state_machine.get(b"new_key1")?,
            Some(Bytes::from(b"new_value1".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"new_key2")?,
            Some(Bytes::from(b"new_value2".to_vec()))
        );
        assert_eq!(state_machine.last_applied(), LogId { index: 2, term: 1 });

        Ok(())
    }

    // ── get_multi tests ───────────────────────────────────────────────────────

    /// Verify that get_multi preserves input key order in its output.
    ///
    /// The caller maps result[i] back to keys[i] without a key lookup.
    /// Any reordering silently breaks that mapping.
    pub async fn test_get_multi_returns_values_in_key_order(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        sm.apply_chunk(&[
            create_insert_entry(1, Bytes::from("key_a"), Bytes::from("value_a")),
            create_insert_entry(2, Bytes::from("key_b"), Bytes::from("value_b")),
            create_insert_entry(3, Bytes::from("key_c"), Bytes::from("value_c")),
        ])
        .await?;

        // Request in reverse alphabetical order to catch any accidental sorting.
        let keys = vec![
            Bytes::from("key_c"),
            Bytes::from("key_a"),
            Bytes::from("key_b"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3, "result length must match key count");
        assert_eq!(
            values[0],
            Some(Bytes::from("value_c")),
            "position 0 = key_c"
        );
        assert_eq!(
            values[1],
            Some(Bytes::from("value_a")),
            "position 1 = key_a"
        );
        assert_eq!(
            values[2],
            Some(Bytes::from("value_b")),
            "position 2 = key_b"
        );
        Ok(())
    }

    /// Verify that absent keys produce None entries, not errors or panics.
    ///
    /// Callers rely on None to distinguish "key not found" from a storage failure.
    /// Returning Err for a missing key would force callers to treat a normal business
    /// state (no registration yet) as an error.
    pub async fn test_get_multi_absent_keys_return_none(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let keys = vec![Bytes::from("ghost_1"), Bytes::from("ghost_2")];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 2);
        assert!(values[0].is_none(), "absent key must return None, not Err");
        assert!(values[1].is_none(), "absent key must return None, not Err");
        Ok(())
    }

    /// Verify that an empty key slice returns an empty result without error.
    ///
    /// Callers may pass an empty slice when the coordinator has no keys to fetch
    /// (e.g., an empty service namespace). The implementation must not panic or
    /// return an error in this case.
    pub async fn test_get_multi_empty_keys_returns_empty(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let values = sm.get_multi(&[])?;
        assert!(
            values.is_empty(),
            "empty key list must produce empty result"
        );
        Ok(())
    }

    /// Verify that a mix of present and absent keys returns Some/None correctly.
    ///
    /// The position contract must hold even when some slots are None.
    /// A naive implementation that skips missing keys and compacts the result
    /// would shift positions, silently misrouting values to wrong fields.
    pub async fn test_get_multi_partial_hit_some_keys_missing(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        sm.apply_chunk(&[create_insert_entry(
            1,
            Bytes::from("present"),
            Bytes::from("v"),
        )])
        .await?;

        let keys = vec![
            Bytes::from("missing_before"),
            Bytes::from("present"),
            Bytes::from("missing_after"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(
            values.len(),
            3,
            "result length must equal key count even with gaps"
        );
        assert!(
            values[0].is_none(),
            "missing_before must be None at position 0"
        );
        assert_eq!(
            values[1],
            Some(Bytes::from("v")),
            "present must have value at position 1"
        );
        assert!(
            values[2].is_none(),
            "missing_after must be None at position 2"
        );
        Ok(())
    }

    /// Service registry coordinator scenario: batch read must not produce a torn state.
    ///
    /// In distributed service routing, (addr, version, health) must all come from
    /// the same applied state. A torn read — addr from apply index 100, version from
    /// index 105 — would cause traffic to route to the old instance while the client
    /// believes it runs the new version, breaking gray-release correctness.
    ///
    /// This test verifies that after a full v2 deployment, get_multi returns all three
    /// fields from v2 with no v1 values mixed in.
    pub async fn test_get_multi_service_registry_coherence(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // v1: initial deployment
        sm.apply_chunk(&[
            create_insert_entry(
                1,
                Bytes::from("/service/payment/addr"),
                Bytes::from("10.0.0.1:8080"),
            ),
            create_insert_entry(
                2,
                Bytes::from("/service/payment/version"),
                Bytes::from("v2.2"),
            ),
            create_insert_entry(
                3,
                Bytes::from("/service/payment/health"),
                Bytes::from("healthy"),
            ),
        ])
        .await?;

        // v2: new deployment — all three fields transition together
        sm.apply_chunk(&[
            create_insert_entry(
                4,
                Bytes::from("/service/payment/addr"),
                Bytes::from("10.0.0.2:8080"),
            ),
            create_insert_entry(
                5,
                Bytes::from("/service/payment/version"),
                Bytes::from("v2.3"),
            ),
            create_insert_entry(
                6,
                Bytes::from("/service/payment/health"),
                Bytes::from("starting"),
            ),
        ])
        .await?;

        let keys = vec![
            Bytes::from("/service/payment/addr"),
            Bytes::from("/service/payment/version"),
            Bytes::from("/service/payment/health"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3);
        // All three must reflect the v2 state consistently.
        // Any mix (e.g., addr=v2 + version=v2.2) is a torn read.
        assert_eq!(
            values[0],
            Some(Bytes::from("10.0.0.2:8080")),
            "addr must be v2.3 instance"
        );
        assert_eq!(values[1], Some(Bytes::from("v2.3")), "version must be v2.3");
        assert_eq!(
            values[2],
            Some(Bytes::from("starting")),
            "health must be v2.3 startup value"
        );
        Ok(())
    }

    /// Leader election coordinator scenario: batch read must not produce a phantom state.
    ///
    /// (leader, term, lease_expire) must be internally consistent. A torn read can
    /// produce a combination that never existed — e.g., leader=node-3 (old term) with
    /// term=43 (new term). A coordinator observing this phantom state may incorrectly
    /// conclude that node-3 holds a lease in term 43 and take split-brain actions.
    ///
    /// This test verifies that after a term transition, get_multi returns all three
    /// fields from the new term with no old-term values mixed in.
    pub async fn test_get_multi_election_state_coherence(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Term 42: node-3 is leader
        sm.apply_chunk(&[
            create_insert_entry(1, Bytes::from("/election/leader"), Bytes::from("node-3")),
            create_insert_entry(2, Bytes::from("/election/term"), Bytes::from("42")),
            create_insert_entry(
                3,
                Bytes::from("/election/lease_expire"),
                Bytes::from("1748600000"),
            ),
        ])
        .await?;

        // Term 43: node-5 wins new election
        sm.apply_chunk(&[
            create_insert_entry(4, Bytes::from("/election/leader"), Bytes::from("node-5")),
            create_insert_entry(5, Bytes::from("/election/term"), Bytes::from("43")),
            create_insert_entry(
                6,
                Bytes::from("/election/lease_expire"),
                Bytes::from("1748603600"),
            ),
        ])
        .await?;

        let keys = vec![
            Bytes::from("/election/leader"),
            Bytes::from("/election/term"),
            Bytes::from("/election/lease_expire"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3);
        // Must reflect term-43 state consistently.
        // leader=node-3 + term=43 is a phantom state that never existed.
        assert_eq!(
            values[0],
            Some(Bytes::from("node-5")),
            "leader must be node-5 (term 43)"
        );
        assert_eq!(values[1], Some(Bytes::from("43")), "term must be 43");
        assert_eq!(
            values[2],
            Some(Bytes::from("1748603600")),
            "lease_expire must be term-43 value"
        );
        Ok(())
    }

    /// Quota management coordinator scenario: batch read must return coherent counters.
    ///
    /// (limit, used, window_start) from different apply indexes produce incorrect
    /// rate-limit calculations — a pure business correctness failure that is independent
    /// of Linearizable semantics. Reading used=850 with a new window_start makes the
    /// rate limiter reject requests that should be allowed in the new window.
    ///
    /// This test verifies that after a window reset, get_multi returns all three
    /// counters from the new window with no old-window values mixed in.
    pub async fn test_get_multi_quota_management_coherence(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Window 1: tenant-A has consumed 850 of their 1000 quota
        sm.apply_chunk(&[
            create_insert_entry(1, Bytes::from("/quota/tenant-A/limit"), Bytes::from("1000")),
            create_insert_entry(2, Bytes::from("/quota/tenant-A/used"), Bytes::from("850")),
            create_insert_entry(
                3,
                Bytes::from("/quota/tenant-A/window_start"),
                Bytes::from("1748599900"),
            ),
        ])
        .await?;

        // Window 2: quota window resets — used counter goes back to 0
        sm.apply_chunk(&[
            create_insert_entry(4, Bytes::from("/quota/tenant-A/limit"), Bytes::from("1000")),
            create_insert_entry(5, Bytes::from("/quota/tenant-A/used"), Bytes::from("0")),
            create_insert_entry(
                6,
                Bytes::from("/quota/tenant-A/window_start"),
                Bytes::from("1748603500"),
            ),
        ])
        .await?;

        let keys = vec![
            Bytes::from("/quota/tenant-A/limit"),
            Bytes::from("/quota/tenant-A/used"),
            Bytes::from("/quota/tenant-A/window_start"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3);
        // Must read window-2 consistently.
        // used=850 + window_start from window-2 is a torn read that wrongly
        // blocks requests that should be free in the new window.
        assert_eq!(
            values[0],
            Some(Bytes::from("1000")),
            "limit is unchanged across windows"
        );
        assert_eq!(
            values[1],
            Some(Bytes::from("0")),
            "used must be 0 after window reset"
        );
        assert_eq!(
            values[2],
            Some(Bytes::from("1748603500")),
            "window_start must be from window-2"
        );
        Ok(())
    }
}

/// Helper function to create an Insert ApplyEntry
fn create_insert_entry(
    index: u64,
    key: Bytes,
    value: Bytes,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Insert {
            key,
            value,
            ttl_secs: None,
        },
    }
}

/// Helper function to create a Delete ApplyEntry
fn create_delete_entry(
    index: u64,
    key: Bytes,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Delete { key },
    }
}

/// Helper function to create a CAS ApplyEntry
fn create_cas_entry(
    index: u64,
    key: Bytes,
    expected: Option<Bytes>,
    value: Bytes,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::CompareAndSwap {
            key,
            expected,
            value,
        },
    }
}

// ============================================================================
// Default scan_prefix implementation test
// ============================================================================
//
// The default scan_prefix on the trait returns an error because not all state
// machines support prefix scans.  This test exercises that code path through a
// minimal concrete type that delegates everything else but does NOT override
// scan_prefix, ensuring the default arm is reachable and exercised.
#[cfg(test)]
mod default_scan_prefix_tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::storage::SnapshotMetadata;

    use crate::storage::state_machine::{ApplyResult, StateMachine};
    use crate::{ApplyEntry, Error};

    struct MinimalSm {
        running: AtomicBool,
    }

    #[async_trait]
    impl StateMachine for MinimalSm {
        async fn start(&self) -> Result<(), Error> {
            self.running.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn stop(&self) -> Result<(), Error> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }

        fn get(
            &self,
            _key_buffer: &[u8],
        ) -> Result<Option<Bytes>, Error> {
            Ok(None)
        }

        fn entry_term(
            &self,
            _entry_id: u64,
        ) -> Option<u64> {
            None
        }

        async fn apply_chunk(
            &self,
            _chunk: &[ApplyEntry],
        ) -> Result<Vec<ApplyResult>, Error> {
            Ok(vec![])
        }

        fn len(&self) -> usize {
            0
        }

        fn update_last_applied(
            &self,
            _last_applied: LogId,
        ) {
        }

        fn last_applied(&self) -> LogId {
            LogId::default()
        }

        fn persist_last_applied(
            &self,
            _last_applied: LogId,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn update_last_snapshot_metadata(
            &self,
            _snapshot_metadata: &SnapshotMetadata,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn snapshot_metadata(&self) -> Option<SnapshotMetadata> {
            None
        }

        fn persist_last_snapshot_metadata(
            &self,
            _snapshot_metadata: &SnapshotMetadata,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn apply_snapshot_from_file(
            &self,
            _metadata: &SnapshotMetadata,
            _snapshot_path: std::path::PathBuf,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn generate_snapshot_data(
            &self,
            _new_snapshot_dir: std::path::PathBuf,
            _last_included: LogId,
        ) -> Result<Bytes, Error> {
            Ok(Bytes::new())
        }

        fn save_hard_state(&self) -> Result<(), Error> {
            Ok(())
        }

        fn flush(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn flush_async(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn reset(&self) -> Result<(), Error> {
            Ok(())
        }
        // scan_prefix intentionally omitted → exercises the trait default implementation
    }

    /// Test that the default scan_prefix returns an error.
    ///
    /// State machines that do not support prefix scans must return an error so
    /// callers can surface a clear message instead of panicking.
    #[tokio::test]
    async fn test_scan_prefix_default_returns_error() {
        let sm = MinimalSm {
            running: AtomicBool::new(true),
        };

        let result = sm.scan_prefix(b"/test/prefix/");

        assert!(
            result.is_err(),
            "default scan_prefix must return Err; got Ok unexpectedly"
        );
        // The error type is StateMachineError — check the Debug representation which
        // includes the nested message (Display only shows the outermost variant).
        let err_debug = format!("{:?}", result.unwrap_err());
        assert!(
            err_debug.contains("scan_prefix not supported"),
            "error should mention 'scan_prefix not supported', got: {err_debug}"
        );
    }
}
