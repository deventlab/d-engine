use crate::{Error, StateMachine};
use d_engine_core::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use std::sync::Arc;
use tempfile::TempDir;
use tonic::async_trait;

use super::RocksDBStateMachine;

struct RocksDBStateMachineBuilder {
    temp_dir: TempDir,
}

impl RocksDBStateMachineBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("Failed to create temp dir"),
        }
    }
}

#[async_trait]
impl StateMachineBuilder for RocksDBStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        let path = self.temp_dir.path().join("rocksdb_sm");
        let sm = RocksDBStateMachine::new(path)?;
        Ok(Arc::new(sm))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // TempDir automatically cleans up on drop
        Ok(())
    }
}

#[tokio::test]
async fn test_rocksdb_state_machine_suite() {
    let builder = RocksDBStateMachineBuilder::new();
    StateMachineTestSuite::run_all_tests(builder)
        .await
        .expect("RocksDBStateMachine should pass all tests");
}

#[tokio::test]
async fn test_rocksdb_state_machine_performance() {
    let builder = RocksDBStateMachineBuilder::new();
    StateMachineTestSuite::run_performance_tests(builder)
        .await
        .expect("RocksDBStateMachine should pass performance tests");
}

/// Test that read operations are rejected during snapshot restoration
///
/// This test verifies the fix for concurrent read safety during apply_snapshot_from_file().
/// Without the is_serving check in get(), reads could access the temporary empty database
/// during PHASE 2.5, returning incorrect empty results instead of proper errors.
///
/// Test flow:
/// 1. Create state machine with data
/// 2. Stop serving (simulating snapshot restoration PHASE 1)
/// 3. Attempt read operation
/// 4. Verify NotServing error is returned (not empty result)
/// 5. Resume serving
/// 6. Verify read succeeds
#[tokio::test]
async fn test_get_rejected_when_not_serving() {
    use crate::StateMachine;
    use bytes::Bytes;
    use d_engine_proto::client::WriteCommand;
    use d_engine_proto::client::write_command::{Insert, Operation};
    use d_engine_proto::common::Entry;
    use d_engine_proto::common::entry_payload::Payload;
    use prost::Message;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_not_serving");
    let state_machine = RocksDBStateMachine::new(db_path).expect("Failed to create state machine");

    // Start state machine
    state_machine.start().await.expect("Failed to start");

    // Insert test data
    let test_key = b"test_key";
    let test_value = b"test_value";

    let write_cmd = WriteCommand {
        operation: Some(Operation::Insert(Insert {
            key: test_key.to_vec().into(),
            value: test_value.to_vec().into(),
            ttl_secs: 0,
        })),
    };

    let mut buf = Vec::new();
    write_cmd.encode(&mut buf).expect("Failed to encode");

    let entry = Entry {
        index: 1,
        term: 1,
        payload: Some(d_engine_proto::common::EntryPayload {
            payload: Some(Payload::Command(buf.into())),
        }),
    };

    state_machine.apply_chunk(vec![entry]).await.expect("Failed to apply entry");

    // Verify data exists before stopping
    let value = state_machine.get(test_key).expect("Failed to get");
    assert_eq!(value, Some(Bytes::from(test_value.to_vec())));

    // Simulate snapshot restoration: stop serving
    state_machine.stop().expect("Failed to stop");

    // Attempt read while not serving - should return NotServing error
    let result = state_machine.get(test_key);
    assert!(result.is_err(), "Read should fail when not serving");

    // Check the error chain for NotServing
    let err = result.unwrap_err();
    let err_debug = format!("{err:?}"); // Use Debug format to see full error chain
    assert!(
        err_debug.contains("NotServing")
            || err_debug.contains("not serving")
            || err_debug.contains("restoring from snapshot"),
        "Error should indicate not serving state, got: {err_debug}"
    );

    // Resume serving
    state_machine.start().await.expect("Failed to restart");

    // Read should succeed now
    let value = state_machine.get(test_key).expect("Failed to get after resuming");
    assert_eq!(
        value,
        Some(Bytes::from(test_value.to_vec())),
        "Data should be accessible after resuming service"
    );
}
