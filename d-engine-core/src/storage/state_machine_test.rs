use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Delete;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::SnapshotMetadata;
use prost::Message;
use tempfile::TempDir;
use tonic::async_trait;

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
        Self::test_last_applied_detection(builder.build().await?).await?;
        Self::test_snapshot_operations(builder.build().await?).await?;
        Self::test_persistence(builder.build().await?).await?;

        Self::test_drop_flushes_data(&builder).await?;
        Self::test_drop_persists_last_applied(&builder).await?;
        Self::test_data_survives_reopen(&builder).await?;
        Self::test_ungraceful_shutdown_recovery(&builder).await?;
        Self::test_reset_operation(builder.build().await?).await?;

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
        state_machine.apply_chunk(entries).await?;

        // Verify the value was inserted
        match state_machine.get(test_key)? {
            Some(value) => assert_eq!(value, test_value),
            None => panic!("Value not found after insert"),
        }

        // Create a delete entry
        let entries = vec![create_delete_entry(2, Bytes::from(test_key.to_vec()))];
        state_machine.apply_chunk(entries).await?;

        // Verify the value was deleted
        assert!(state_machine.get(test_key)?.is_none());

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

        state_machine.apply_chunk(entries).await?;

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

        state_machine.apply_chunk(entries).await?;
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
        state_machine.apply_chunk(entries).await?;

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
        state_machine.apply_chunk(entries).await?;

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
        state_machine.apply_chunk(entries).await?;

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
        state_machine.apply_chunk(entries).await?;
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
        state_machine.apply_chunk(small_entries).await?;
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
        state_machine.apply_chunk(large_entries).await?;
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
        sm.apply_chunk(entries).await?;

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
            sm.apply_chunk(entries).await?;

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
            sm.apply_chunk(entries).await?;

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
        sm.apply_chunk(entries).await?;

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
        state_machine.apply_chunk(entries).await?;

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
        state_machine.apply_chunk(new_entries).await?;

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
}

/// Helper function to create an insert entry
fn create_insert_entry(
    index: u64,
    key: Bytes,
    value: Bytes,
) -> Entry {
    // 1. Build the WriteCommand
    let insert = Insert {
        key,
        value,
        ttl_secs: 0,
    };
    let operation = Operation::Insert(insert);
    let write_cmd = WriteCommand {
        operation: Some(operation),
    };

    // 2. Serialize WriteCommand to Bytes
    let mut buf = Vec::new();
    write_cmd.encode(&mut buf).expect("Failed to encode WriteCommand");
    let cmd_bytes = Bytes::from(buf); // convert Vec<u8> to Bytes

    // 3. Wrap in Payload::Command
    let payload = EntryPayload {
        payload: Some(Payload::Command(cmd_bytes)),
    };

    // 4. Build the Entry
    Entry {
        index,
        term: 1,
        payload: Some(payload),
    }
}

/// Helper function to create a delete entry
fn create_delete_entry(
    index: u64,
    key: Bytes,
) -> Entry {
    let delete = Delete { key };
    let operation = Operation::Delete(delete);
    let write_cmd = WriteCommand {
        operation: Some(operation),
    };

    let mut buf = Vec::new();
    write_cmd.encode(&mut buf).expect("Failed to encode WriteCommand");
    let cmd_bytes = Bytes::from(buf);

    Entry {
        index,
        term: 1,
        payload: Some(EntryPayload {
            payload: Some(Payload::Command(cmd_bytes)),
        }),
    }
}
