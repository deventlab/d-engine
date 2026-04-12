use super::RocksDBStorageEngine;
use super::RocksDBUnifiedEngine;
use crate::{Error, StateMachine};
use bytes::Bytes;
use d_engine_core::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::{CompareAndSwap, Delete, Insert, Operation};
use d_engine_proto::common::Entry;
use d_engine_proto::common::entry_payload::Payload;
use prost::Message;
use std::sync::Arc;
use std::sync::Mutex;
use tempfile::TempDir;
use tonic::async_trait;

struct RocksDBStateMachineBuilder {
    temp_dir: TempDir,
    // Partner storage kept alive alongside SM; must be dropped before the next build().
    storage: Mutex<Option<RocksDBStorageEngine>>,
}

impl RocksDBStateMachineBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("Failed to create temp dir"),
            storage: Mutex::new(None),
        }
    }
}

#[async_trait]
impl StateMachineBuilder for RocksDBStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        // Drop old storage so Arc<DB> refcount falls to 0 and RocksDB releases the lock.
        {
            *self.storage.lock().unwrap() = None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let path = self.temp_dir.path().join("rocksdb_sm");
        let (storage, sm) = RocksDBUnifiedEngine::open(&path)?;
        *self.storage.lock().unwrap() = Some(storage);
        Ok(Arc::new(sm))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        *self.storage.lock().unwrap() = None;
        let delay = if std::env::var("CI").is_ok() {
            std::time::Duration::from_millis(500)
        } else {
            std::time::Duration::from_millis(100)
        };
        tokio::time::sleep(delay).await;
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

// TODO: test_apply_chunk_scalability uses wall-clock I/O time ratio to detect O(N²) complexity,
// which is unreliable in CI due to disk I/O spikes. Needs redesign (e.g., measure pure in-memory
// ops separately from WAL writes) before re-enabling.
#[tokio::test]
#[ignore]
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
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_not_serving");
    let (_storage, state_machine) =
        RocksDBUnifiedEngine::open(&db_path).expect("Failed to open unified DB");

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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn encode_insert(
    key: &[u8],
    value: &[u8],
    ttl_secs: u64,
) -> Entry {
    let cmd = WriteCommand {
        operation: Some(Operation::Insert(Insert {
            key: key.to_vec().into(),
            value: value.to_vec().into(),
            ttl_secs,
        })),
    };
    encode_entry(cmd, 1, 1)
}

fn encode_delete(
    key: &[u8],
    index: u64,
) -> Entry {
    let cmd = WriteCommand {
        operation: Some(Operation::Delete(Delete {
            key: key.to_vec().into(),
        })),
    };
    encode_entry(cmd, index, 1)
}

fn encode_cas(
    key: &[u8],
    expected: Option<&[u8]>,
    new_value: &[u8],
    index: u64,
) -> Entry {
    let cmd = WriteCommand {
        operation: Some(Operation::CompareAndSwap(CompareAndSwap {
            key: key.to_vec().into(),
            expected_value: expected.map(|v| v.to_vec().into()),
            new_value: new_value.to_vec().into(),
        })),
    };
    encode_entry(cmd, index, 1)
}

fn encode_entry(
    cmd: WriteCommand,
    index: u64,
    term: u64,
) -> Entry {
    let mut buf = Vec::new();
    cmd.encode(&mut buf).expect("encode");
    Entry {
        index,
        term,
        payload: Some(d_engine_proto::common::EntryPayload {
            payload: Some(Payload::Command(buf.into())),
        }),
    }
}

// ── CAS tests ─────────────────────────────────────────────────────────────────

/// CAS succeeds when current value matches expected: result.succeeded == true
/// and the new value is visible via get().
#[tokio::test]
async fn test_apply_chunk_cas_match_succeeds_and_writes_new_value() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    // Pre-populate key
    sm.apply_chunk(vec![encode_insert(b"k", b"old", 0)]).await.unwrap();

    // CAS with matching expected value
    let results = sm.apply_chunk(vec![encode_cas(b"k", Some(b"old"), b"new", 2)]).await.unwrap();

    assert!(
        results[0].succeeded,
        "CAS should succeed when current == expected"
    );
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from("new")));
}

/// CAS fails when current value does not match expected: result.succeeded == false
/// and the original value is unchanged.
#[tokio::test]
async fn test_apply_chunk_cas_mismatch_returns_failure_and_preserves_value() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![encode_insert(b"k", b"current", 0)]).await.unwrap();

    let results = sm
        .apply_chunk(vec![encode_cas(b"k", Some(b"wrong_expected"), b"new", 2)])
        .await
        .unwrap();

    assert!(
        !results[0].succeeded,
        "CAS should fail when current != expected"
    );
    // Original value must be preserved
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from("current")));
}

/// CAS with expected=None on a non-existent key: create-if-absent pattern succeeds.
#[tokio::test]
async fn test_apply_chunk_cas_none_expected_on_absent_key_succeeds() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    let results = sm.apply_chunk(vec![encode_cas(b"new_key", None, b"value", 1)]).await.unwrap();

    assert!(
        results[0].succeeded,
        "CAS(None→value) on absent key should succeed"
    );
    assert_eq!(sm.get(b"new_key").unwrap(), Some(Bytes::from("value")));
}

/// CAS with expected=None fails when key already exists.
#[tokio::test]
async fn test_apply_chunk_cas_none_expected_on_existing_key_fails() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![encode_insert(b"k", b"exists", 0)]).await.unwrap();

    let results = sm.apply_chunk(vec![encode_cas(b"k", None, b"new", 2)]).await.unwrap();

    assert!(
        !results[0].succeeded,
        "CAS(None) should fail when key exists"
    );
    // Value must remain unchanged
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from("exists")));
}

/// delete() removes a key successfully; subsequent get returns None.
#[tokio::test]
async fn test_apply_chunk_delete_removes_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![encode_insert(b"to_delete", b"v", 0)]).await.unwrap();
    sm.apply_chunk(vec![encode_delete(b"to_delete", 2)]).await.unwrap();

    assert_eq!(sm.get(b"to_delete").unwrap(), None);
}
