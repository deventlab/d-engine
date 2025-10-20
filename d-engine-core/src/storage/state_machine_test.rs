use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tempfile::TempDir;
use tonic::async_trait;

use crate::Error;
use crate::storage::StateMachine;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Delete;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::SnapshotMetadata;

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
        Self::test_reset_operation(builder.build().await?).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Test start/stop functionality
    async fn test_start_stop(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
        // Test default state
        assert!(state_machine.is_running());

        // Test explicit start/stop
        state_machine.start()?;
        assert!(state_machine.is_running());
        state_machine.stop()?;
        assert!(!state_machine.is_running());
        state_machine.start()?;
        assert!(state_machine.is_running());

        Ok(())
    }

    /// Test basic key-value operations
    async fn test_basic_kv_operations(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
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
    async fn test_apply_chunk_functionality(
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
    async fn test_last_applied_detection(
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
    async fn test_snapshot_operations(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
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
        let snapshot_dir = temp_dir.path().to_path_buf();

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
    async fn test_persistence(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
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

    /// Test reset operation functionality
    ///
    /// This test verifies that the reset operation:
    /// 1. Clears all data from memory
    /// 2. Resets Raft state to initial values
    /// 3. Clears all persisted files
    /// 4. Maintains operational state (running status)
    async fn test_reset_operation(state_machine: Arc<dyn StateMachine>) -> Result<(), Error> {
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
        assert!(state_machine.snapshot_metadata().is_none());

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
    let insert = Insert { key, value };
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
