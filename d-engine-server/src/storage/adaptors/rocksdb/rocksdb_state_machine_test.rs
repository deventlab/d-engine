use super::RocksDBStorageEngine;
use super::RocksDBUnifiedEngine;
use crate::{Error, StateMachine};
use async_trait::async_trait;
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

// ── scan_prefix tests (#378) ──────────────────────────────────────────────────

/// scan_prefix returns exactly the entries whose key starts with the given prefix,
/// excluding all keys from other namespaces.
///
/// This is the core correctness guarantee: prefix matching must be exact —
/// no adjacent namespace keys may leak through even if they share a substring.
#[tokio::test]
async fn test_scan_prefix_returns_only_matching_keys() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![
        encode_insert_at(b"/services/node1", b"10.0.0.1", 1),
        encode_insert_at(b"/services/node2", b"10.0.0.2", 2),
        encode_insert_at(b"/other/key", b"must_not_appear", 3),
    ])
    .await
    .unwrap();

    let result = sm.scan_prefix(b"/services/").unwrap();

    assert_eq!(
        result.entries.len(),
        2,
        "only /services/ keys should be returned"
    );
    let keys: Vec<&Bytes> = result.entries.iter().map(|(k, _)| k).collect();
    assert!(keys.contains(&&Bytes::from_static(b"/services/node1")));
    assert!(keys.contains(&&Bytes::from_static(b"/services/node2")));
}

/// scan_prefix stops at prefix_successor (last byte + 1), not at the first
/// key that does not share the prefix string — these are different.
///
/// Without set_iterate_upper_bound, a hand-rolled break would stop at the first
/// non-matching key after the prefix range, but RocksDB's block-level skipping
/// would not apply, making the scan O(total_keys) in the worst case.
/// This test verifies that a key in a lexicographically adjacent namespace
/// (/t/ > /s/) is not returned and that the upper-bound mechanism is effective.
#[tokio::test]
async fn test_scan_prefix_upper_bound_excludes_adjacent_namespace() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![
        encode_insert_at(b"/services/last", b"v", 1),
        // '/t' > '/s' in byte order — sits right after '/services/' in the keyspace
        encode_insert_at(b"/t/trap", b"must_not_appear", 2),
    ])
    .await
    .unwrap();

    let result = sm.scan_prefix(b"/services/").unwrap();

    assert_eq!(
        result.entries.len(),
        1,
        "/t/trap must not appear in /services/ scan"
    );
    assert_eq!(result.entries[0].0, Bytes::from_static(b"/services/last"));
}

/// scan_prefix on a prefix with no matching keys returns an empty entries list,
/// not an error. An empty namespace is a valid business state (e.g. no services
/// registered yet), and callers must not need to distinguish it from an error.
#[tokio::test]
async fn test_scan_prefix_empty_namespace_returns_empty_vec() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![encode_insert_at(b"/other/key", b"v", 1)]).await.unwrap();

    let result = sm.scan_prefix(b"/missing/").unwrap();

    assert!(
        result.entries.is_empty(),
        "missing prefix must return empty entries, not an error"
    );
}

/// scan_prefix revision is >= the applied index at the time of the call.
///
/// This is the linearizability anchor for the watch→scan pattern:
/// after scan returns revision=R, callers filter watch events with
/// event.revision <= R (already in the snapshot) vs > R (must be applied).
/// If revision were stale, the filter boundary would be wrong and events
/// would be silently double-applied or missed.
#[tokio::test]
async fn test_scan_prefix_revision_reflects_applied_index() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    // Apply 3 entries as a single chunk; last_applied_index becomes 3
    sm.apply_chunk(vec![
        encode_insert_at(b"/s/a", b"1", 1),
        encode_insert_at(b"/s/b", b"2", 2),
        encode_insert_at(b"/s/c", b"3", 3),
    ])
    .await
    .unwrap();

    let result = sm.scan_prefix(b"/s/").unwrap();

    assert_eq!(result.entries.len(), 3);
    assert_eq!(
        result.revision, 3,
        "revision must equal the applied index after the writes"
    );
}

/// scan_prefix correctly handles a prefix whose last byte is 0xFF.
///
/// The naive `last_byte + 1` implementation wraps 0xFF → 0x00, producing an upper
/// bound that is lexicographically *less than* the prefix itself — RocksDB's iterator
/// stops immediately and returns no rows. prefix_successor must propagate the carry:
/// [0x2F, 0xFF] (b"/\xFF") → [0x30] (b"0"), ensuring the scan covers all matching keys.
#[tokio::test]
async fn test_scan_prefix_0xff_suffix_carry_propagation() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    // Key: b"/\xFF/node1" — prefix b"/\xFF/" ends with 0xFF then 0x2F
    // Use a simpler case: prefix = b"a\xFF", key = b"a\xFFnode"
    let prefix: &[u8] = b"\x61\xFF";
    let key: &[u8] = b"\x61\xFFnode";
    let decoy: &[u8] = b"\x62decoy"; // sits above [0x62], must not appear

    sm.apply_chunk(vec![
        encode_insert_at(key, b"value", 1),
        encode_insert_at(decoy, b"must_not_appear", 2),
    ])
    .await
    .unwrap();

    let result = sm.scan_prefix(prefix).unwrap();

    assert_eq!(
        result.entries.len(),
        1,
        "only key under 0xFF prefix should appear"
    );
    assert_eq!(result.entries[0].0, Bytes::copy_from_slice(key));
}

/// scan_prefix with an empty prefix returns an empty result immediately.
///
/// An empty prefix would otherwise match every key in the database, which
/// is an unsafe operation for an arbitrarily large keyspace. The implementation
/// short-circuits and returns `ScanResult { entries: vec![], revision }` so
/// callers receive a well-defined result without iterating the whole DB.
#[tokio::test]
async fn test_scan_prefix_empty_prefix_returns_empty() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![encode_insert_at(b"/services/node1", b"v", 1)])
        .await
        .unwrap();

    let result = sm.scan_prefix(b"").unwrap();

    assert!(
        result.entries.is_empty(),
        "empty prefix should return empty entries (not all keys)"
    );
}

/// scan_prefix with an all-0xFF prefix has no successor, so no upper bound is
/// set and the iterator uses the starts_with guard to exclude non-matching keys.
///
/// prefix_successor([0xFF, 0xFF]) returns None because all bytes are 0xFF —
/// there is no lexicographically larger byte string that could serve as an
/// upper bound.  The scan must still return all keys that start with the prefix
/// and exclude everything else.
#[tokio::test]
async fn test_scan_prefix_all_0xff_no_upper_bound() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    // Key that starts with [0xFF, 0xFF] — sits at the very top of the keyspace.
    let prefix: &[u8] = b"\xFF\xFF";
    let key: &[u8] = b"\xFF\xFF\x01";
    let decoy: &[u8] = b"\xFE\xAA"; // lexicographically below the prefix

    sm.apply_chunk(vec![
        encode_insert_at(key, b"top_value", 1),
        encode_insert_at(decoy, b"must_not_appear", 2),
    ])
    .await
    .unwrap();

    let result = sm.scan_prefix(prefix).unwrap();

    assert_eq!(
        result.entries.len(),
        1,
        "only key under all-0xFF prefix should appear"
    );
    assert_eq!(result.entries[0].0, Bytes::copy_from_slice(key));
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

/// Like encode_insert but with an explicit log index, required when placing
/// multiple entries in a single apply_chunk call (indices must be strictly increasing).
fn encode_insert_at(
    key: &[u8],
    value: &[u8],
    index: u64,
) -> Entry {
    let cmd = WriteCommand {
        operation: Some(Operation::Insert(Insert {
            key: key.to_vec().into(),
            value: value.to_vec().into(),
            ttl_secs: 0,
        })),
    };
    encode_entry(cmd, index, 1)
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

/// Two CAS ops targeting the same key in a single apply_chunk must be applied
/// sequentially: the second CAS must see the first CAS's write, not the stale DB value.
///
/// Failure mode without fix: db.get_cf() returns the pre-batch value for both reads,
/// both report succeeded=true, and CAS2's write silently overwrites CAS1's value,
/// causing a linearizability violation (acknowledged write disappears).
#[tokio::test]
async fn test_apply_chunk_two_cas_same_key_second_must_see_first_write() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(vec![encode_insert(b"k", b"v0", 0)]).await.unwrap();

    // CAS1: v0 → v1  (should succeed)
    // CAS2: v0 → v2  (must fail — CAS1 already changed value to v1)
    let results = sm
        .apply_chunk(vec![
            encode_cas(b"k", Some(b"v0"), b"v1", 2),
            encode_cas(b"k", Some(b"v0"), b"v2", 3),
        ])
        .await
        .unwrap();

    assert!(results[0].succeeded, "CAS1 (v0→v1) must succeed");
    assert!(
        !results[1].succeeded,
        "CAS2 (v0→v2) must fail: CAS1 already wrote v1, so expected=v0 no longer matches"
    );
    assert_eq!(
        sm.get(b"k").unwrap(),
        Some(Bytes::from("v1")),
        "final value must be v1; CAS2 must not overwrite CAS1's committed write"
    );
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

/// apply_chunk returns Err when the underlying DB is replaced with a read-only instance.
///
/// This covers the write_wbwi error path that is only reachable when RocksDB itself fails.
/// Deleting the directory does not work (Unix keeps open FDs valid), so we inject a
/// read-only DB via swap_db_for_test to reliably trigger the error.
#[tokio::test]
async fn test_apply_chunk_returns_error_on_read_only_db() {
    use rocksdb::{DB, Options};

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("rocksdb");
    let (_storage, sm) = RocksDBUnifiedEngine::open(&db_path).unwrap();

    // Confirm the state machine works normally first
    sm.apply_chunk(vec![encode_insert(b"k", b"v", 0)]).await.unwrap();

    // Open the same DB as read-only and inject it — writes will now return NotSupported
    let ro_db = DB::open_cf_for_read_only(
        &Options::default(),
        &db_path,
        [super::STATE_MACHINE_CF, super::STATE_MACHINE_META_CF],
        false,
    )
    .unwrap();
    sm.swap_db_for_test(ro_db);

    // Write to a read-only DB must fail
    let result = sm.apply_chunk(vec![encode_insert(b"k2", b"v2", 0)]).await;
    assert!(
        result.is_err(),
        "apply_chunk should return Err when DB is read-only"
    );
}

/// map_snapshot_join_error produces "panicked" message when the blocking task panics.
#[tokio::test]
async fn test_snapshot_join_error_reports_panic() {
    use super::RocksDBStateMachine;
    let handle = tokio::task::spawn_blocking(|| -> Result<(), crate::Error> {
        panic!("intentional test panic");
    });
    let join_err = handle.await.unwrap_err();
    let storage_err = RocksDBStateMachine::map_snapshot_join_error(join_err);
    assert!(
        storage_err.to_string().contains("panicked"),
        "expected 'panicked' in error, got: {storage_err}"
    );
}

/// map_snapshot_join_error produces "cancelled" message when the blocking task is aborted.
#[tokio::test]
async fn test_snapshot_join_error_reports_cancellation() {
    use super::RocksDBStateMachine;
    let handle = tokio::task::spawn_blocking(|| -> Result<(), crate::Error> {
        std::thread::sleep(std::time::Duration::from_secs(60));
        Ok(())
    });
    handle.abort();
    let join_err = handle.await.unwrap_err();
    let storage_err = RocksDBStateMachine::map_snapshot_join_error(join_err);
    assert!(
        storage_err.to_string().contains("cancelled"),
        "expected 'cancelled' in error, got: {storage_err}"
    );
}
