use super::RocksDBStorageEngine;
use super::RocksDBUnifiedEngine;
use crate::{Error, StateMachine};
use async_trait::async_trait;
use bytes::Bytes;
use d_engine_core::file_io::compute_checksum_from_folder_path;
use d_engine_core::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use d_engine_core::{ApplyEntry, Command, SnapshotApplyResult};
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
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

    let entry = ApplyEntry {
        index: 1,
        term: 1,
        command: Command::Insert {
            key: Bytes::from(test_key.to_vec()),
            value: Bytes::from(test_value.to_vec()),
            ttl_secs: None,
        },
    };

    state_machine.apply_chunk(&[entry]).await.expect("Failed to apply entry");

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

    sm.apply_chunk(&[
        insert_at(b"/services/node1", b"10.0.0.1", 1),
        insert_at(b"/services/node2", b"10.0.0.2", 2),
        insert_at(b"/other/key", b"must_not_appear", 3),
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

    sm.apply_chunk(&[
        insert_at(b"/services/last", b"v", 1),
        // '/t' > '/s' in byte order — sits right after '/services/' in the keyspace
        insert_at(b"/t/trap", b"must_not_appear", 2),
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

    sm.apply_chunk(&[insert_at(b"/other/key", b"v", 1)]).await.unwrap();

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
    sm.apply_chunk(&[
        insert_at(b"/s/a", b"1", 1),
        insert_at(b"/s/b", b"2", 2),
        insert_at(b"/s/c", b"3", 3),
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

    sm.apply_chunk(&[
        insert_at(key, b"value", 1),
        insert_at(decoy, b"must_not_appear", 2),
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

    sm.apply_chunk(&[insert_at(b"/services/node1", b"v", 1)]).await.unwrap();

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

    sm.apply_chunk(&[
        insert_at(key, b"top_value", 1),
        insert_at(decoy, b"must_not_appear", 2),
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

// ── get_multi tests ───────────────────────────────────────────────────────────

/// RocksDB get_multi uses db.snapshot() to guarantee snapshot isolation.
///
/// A snapshot taken at the start of the batch read freezes the visible state:
/// writes committed after the snapshot is taken are invisible to that batch.
/// This means every (key, value) pair in the result comes from the same
/// point-in-time state, preventing torn reads across apply indexes.
///
/// This test verifies two things:
///   1. get_multi returns the current state accurately after each apply.
///   2. Each result set is internally consistent — no v1/v2 mix is possible.
#[tokio::test]
async fn test_get_multi_rocksdb_snapshot_reads_consistent_state() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    // Apply v1: initial deployment state
    sm.apply_chunk(&[
        insert_at(b"/svc/addr", b"10.0.0.1:8080", 1),
        insert_at(b"/svc/version", b"v1", 2),
        insert_at(b"/svc/health", b"healthy", 3),
    ])
    .await
    .unwrap();

    let keys = vec![
        Bytes::from_static(b"/svc/addr"),
        Bytes::from_static(b"/svc/version"),
        Bytes::from_static(b"/svc/health"),
    ];

    // Read after v1 — must see full v1 state
    let v1_result = sm.get_multi(&keys).unwrap();
    assert_eq!(
        v1_result[0],
        Some(Bytes::from_static(b"10.0.0.1:8080")),
        "addr must be v1"
    );
    assert_eq!(
        v1_result[1],
        Some(Bytes::from_static(b"v1")),
        "version must be v1"
    );
    assert_eq!(
        v1_result[2],
        Some(Bytes::from_static(b"healthy")),
        "health must be v1"
    );

    // Apply v2: new deployment — all three fields transition
    sm.apply_chunk(&[
        insert_at(b"/svc/addr", b"10.0.0.2:8080", 4),
        insert_at(b"/svc/version", b"v2", 5),
        insert_at(b"/svc/health", b"starting", 6),
    ])
    .await
    .unwrap();

    // Read after v2 — must see full v2 state, no v1 values mixed in
    let v2_result = sm.get_multi(&keys).unwrap();
    assert_eq!(
        v2_result[0],
        Some(Bytes::from_static(b"10.0.0.2:8080")),
        "addr must be v2"
    );
    assert_eq!(
        v2_result[1],
        Some(Bytes::from_static(b"v2")),
        "version must be v2"
    );
    assert_eq!(
        v2_result[2],
        Some(Bytes::from_static(b"starting")),
        "health must be v2"
    );

    // Verify each result set is internally consistent (snapshot isolation guarantee).
    // A torn read — e.g., addr=v2 + version=v1 — would indicate the snapshot is not
    // held for the full batch, which would be a correctness violation.
    for (label, result) in [("v1_result", &v1_result), ("v2_result", &v2_result)] {
        let is_v1 = result[0] == Some(Bytes::from_static(b"10.0.0.1:8080"))
            && result[1] == Some(Bytes::from_static(b"v1"))
            && result[2] == Some(Bytes::from_static(b"healthy"));
        let is_v2 = result[0] == Some(Bytes::from_static(b"10.0.0.2:8080"))
            && result[1] == Some(Bytes::from_static(b"v2"))
            && result[2] == Some(Bytes::from_static(b"starting"));
        assert!(
            is_v1 || is_v2,
            "{label}: torn read detected — result is neither pure v1 nor pure v2: {result:?}"
        );
    }
}

// ── get_multi single-key fast path tests ───────────────────────────────────────
//
// `get_multi` skips `db.snapshot()` when `keys.len() == 1`, reading directly via
// `db.get_cf()` instead. Cross-key point-in-time consistency (the reason the
// snapshot exists) is meaningless for a single key, so the fast path must be
// behaviorally identical to the snapshot path for every case a caller can hit —
// these tests exist to pin that equivalence down explicitly, not just performance.

/// A single existing key must resolve to `Some(value)` on the fast path, the
/// same as it would via the (now-skipped) snapshot path.
#[tokio::test]
async fn test_get_multi_single_key_present_returns_value() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(&[insert_at(b"k", b"v", 1)]).await.unwrap();

    let result = sm.get_multi(&[Bytes::from_static(b"k")]).unwrap();
    assert_eq!(result, vec![Some(Bytes::from_static(b"v"))]);
}

/// A single absent key must resolve to `None`, not an error — `db.get_cf()`
/// returning `Ok(None)` for a missing key must be distinguished from the
/// `Err` path (CF missing / SM not serving).
#[tokio::test]
async fn test_get_multi_single_key_absent_returns_none() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    let result = sm.get_multi(&[Bytes::from_static(b"missing")]).unwrap();
    assert_eq!(result, vec![None]);
}

/// `get_multi(&[key])` and `get(key)` must return identical results for both
/// a present and an absent key — the fast path is documented as "equivalent to
/// a single get_cf() call", so this pins that claim down as a regression test
/// rather than leaving it as an unverified comment.
#[tokio::test]
async fn test_get_multi_single_key_matches_get_for_present_and_absent_keys() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(&[insert_at(b"k", b"v", 1)]).await.unwrap();

    let present_key = Bytes::from_static(b"k");
    let absent_key = Bytes::from_static(b"missing");

    assert_eq!(
        sm.get_multi(std::slice::from_ref(&present_key)).unwrap(),
        vec![sm.get(&present_key).unwrap()]
    );
    assert_eq!(
        sm.get_multi(std::slice::from_ref(&absent_key)).unwrap(),
        vec![sm.get(&absent_key).unwrap()]
    );
}

/// Mirrors `test_get_multi_rocksdb_snapshot_reads_consistent_state` but for the
/// single-key fast path: after a second `apply_chunk` updates the key, a
/// subsequent `get_multi` must observe the new value, not a stale one. Confirms
/// the fast path reads live state and isn't accidentally holding on to
/// anything resembling a cached view.
#[tokio::test]
async fn test_get_multi_single_key_reflects_latest_applied_value() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(&[insert_at(b"/svc/version", b"v1", 1)]).await.unwrap();
    let v1_result = sm.get_multi(&[Bytes::from_static(b"/svc/version")]).unwrap();
    assert_eq!(v1_result, vec![Some(Bytes::from_static(b"v1"))]);

    sm.apply_chunk(&[insert_at(b"/svc/version", b"v2", 2)]).await.unwrap();
    let v2_result = sm.get_multi(&[Bytes::from_static(b"/svc/version")]).unwrap();
    assert_eq!(v2_result, vec![Some(Bytes::from_static(b"v2"))]);
}

/// `keys.len() == 0` falls through to the snapshot path (the `== 1` check is
/// false), which must still handle an empty key list gracefully — no
/// snapshot-on-nothing panic, just an empty result vec.
#[tokio::test]
async fn test_get_multi_empty_keys_returns_empty_vec() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    let result = sm.get_multi(&[]).unwrap();
    assert_eq!(result, Vec::<Option<Bytes>>::new());
}

/// Boundary check one key above the fast-path threshold: exactly two keys must
/// still take the `db.snapshot()` path and preserve cross-key point-in-time
/// consistency — guards against an off-by-one in the `keys.len() == 1` check
/// silently widening the fast path to cases that need the snapshot guarantee.
#[tokio::test]
async fn test_get_multi_two_keys_still_uses_snapshot_isolation() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();

    sm.apply_chunk(&[
        insert_at(b"/svc/addr", b"10.0.0.1:8080", 1),
        insert_at(b"/svc/version", b"v1", 2),
    ])
    .await
    .unwrap();

    let keys = vec![
        Bytes::from_static(b"/svc/addr"),
        Bytes::from_static(b"/svc/version"),
    ];

    let v1_result = sm.get_multi(&keys).unwrap();
    assert_eq!(v1_result[0], Some(Bytes::from_static(b"10.0.0.1:8080")));
    assert_eq!(v1_result[1], Some(Bytes::from_static(b"v1")));

    sm.apply_chunk(&[
        insert_at(b"/svc/addr", b"10.0.0.2:8080", 3),
        insert_at(b"/svc/version", b"v2", 4),
    ])
    .await
    .unwrap();

    let v2_result = sm.get_multi(&keys).unwrap();
    assert_eq!(v2_result[0], Some(Bytes::from_static(b"10.0.0.2:8080")));
    assert_eq!(v2_result[1], Some(Bytes::from_static(b"v2")));

    for (label, result) in [("v1_result", &v1_result), ("v2_result", &v2_result)] {
        let is_v1 = result[0] == Some(Bytes::from_static(b"10.0.0.1:8080"))
            && result[1] == Some(Bytes::from_static(b"v1"));
        let is_v2 = result[0] == Some(Bytes::from_static(b"10.0.0.2:8080"))
            && result[1] == Some(Bytes::from_static(b"v2"));
        assert!(
            is_v1 || is_v2,
            "{label}: torn read detected — result is neither pure v1 nor pure v2: {result:?}"
        );
    }
}

// ── close_db() tests ──────────────────────────────────────────────────────────
//
// close_db() is a permanent, one-way shutdown that releases the RocksDB LOCK file
// immediately, without waiting for Arc<SM> to be the sole owner.  It is separate
// from stop() because stop()/start() must remain a reversible cycle (used during
// snapshot restoration).

/// close_db() marks SM not-running and makes reads return an error.
#[tokio::test]
async fn test_close_db_prevents_reads() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();
    sm.start().await.unwrap();
    sm.apply_chunk(&[insert_at(b"k", b"v", 1)]).await.unwrap();
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from_static(b"v")));

    sm.close_db();

    assert!(!sm.is_running(), "is_running must be false after close_db");
    assert!(sm.get(b"k").is_err(), "get must fail after close_db");
    assert!(
        sm.get_multi(&[Bytes::from_static(b"k")]).is_err(),
        "get_multi must fail after close_db"
    );
}

/// stop() after close_db() must succeed without panicking.
/// Node::stop() calls sm.stop() after EmbeddedEngine has already called close_db();
/// that sequence must be safe.
#[test]
fn test_stop_after_close_db_is_safe() {
    let dir = TempDir::new().unwrap();
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();
    sm.close_db();
    assert!(sm.stop().is_ok(), "stop after close_db must not error");
}

/// Drop after close_db() must not panic or double-flush.
/// EmbeddedEngine::stop() calls close_db() before the Raft loop exits; the SM
/// is then dropped — Drop must detect the closed state and skip the flush.
#[tokio::test]
async fn test_drop_after_close_db_does_not_panic() {
    let dir = TempDir::new().unwrap();
    {
        let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).unwrap();
        sm.start().await.unwrap();
        sm.apply_chunk(&[insert_at(b"k", b"v", 1)]).await.unwrap();
        sm.close_db();
        // sm drops here — Drop must be a no-op
    }
    // reaching here without panic means the test passes
}

fn insert_at(
    key: &[u8],
    value: &[u8],
    index: u64,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Insert {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            ttl_secs: None,
        },
    }
}

fn insert_with_ttl(
    key: &[u8],
    value: &[u8],
    ttl_secs: u64,
    index: u64,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Insert {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            ttl_secs: if ttl_secs > 0 { Some(ttl_secs) } else { None },
        },
    }
}

fn delete_at(
    key: &[u8],
    index: u64,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Delete {
            key: Bytes::copy_from_slice(key),
        },
    }
}

fn cas_at(
    key: &[u8],
    expected: Option<&[u8]>,
    new_value: &[u8],
    index: u64,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::CompareAndSwap {
            key: Bytes::copy_from_slice(key),
            expected: expected.map(Bytes::copy_from_slice),
            value: Bytes::copy_from_slice(new_value),
        },
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
    sm.apply_chunk(&[insert_with_ttl(b"k", b"old", 0, 1)]).await.unwrap();

    // CAS with matching expected value
    let results = sm.apply_chunk(&[cas_at(b"k", Some(b"old"), b"new", 2)]).await.unwrap();

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

    sm.apply_chunk(&[insert_with_ttl(b"k", b"current", 0, 1)]).await.unwrap();

    let results = sm
        .apply_chunk(&[cas_at(b"k", Some(b"wrong_expected"), b"new", 2)])
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

    let results = sm.apply_chunk(&[cas_at(b"new_key", None, b"value", 1)]).await.unwrap();

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

    sm.apply_chunk(&[insert_with_ttl(b"k", b"exists", 0, 1)]).await.unwrap();

    let results = sm.apply_chunk(&[cas_at(b"k", None, b"new", 2)]).await.unwrap();

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

    sm.apply_chunk(&[insert_with_ttl(b"k", b"v0", 0, 1)]).await.unwrap();

    // CAS1: v0 → v1  (should succeed)
    // CAS2: v0 → v2  (must fail — CAS1 already changed value to v1)
    let results = sm
        .apply_chunk(&[
            cas_at(b"k", Some(b"v0"), b"v1", 2),
            cas_at(b"k", Some(b"v0"), b"v2", 3),
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

    sm.apply_chunk(&[insert_with_ttl(b"to_delete", b"v", 0, 1)]).await.unwrap();
    sm.apply_chunk(&[delete_at(b"to_delete", 2)]).await.unwrap();

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
    sm.apply_chunk(&[insert_with_ttl(b"k", b"v", 0, 1)]).await.unwrap();

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
    let result = sm.apply_chunk(&[insert_with_ttl(b"k2", b"v2", 0, 2)]).await;
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

// ── stale snapshot install (#436) ─────────────────────────────────────

/// A stale snapshot (`last_included` behind current `last_applied`) must not be
/// installed — doing so rolls back already-applied data.
///
/// Currently RED: `apply_snapshot_from_file` doesn't check `last_included` against
/// `last_applied` before installing. #436.
#[tokio::test]
async fn test_apply_snapshot_from_file_rejects_stale_snapshot() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_stale_snapshot");
    let (_storage, state_machine) =
        RocksDBUnifiedEngine::open(&db_path).expect("Failed to open unified DB");
    state_machine.start().await.expect("Failed to start");

    // 1. Apply entries 1..=20 — the state a snapshot generated "now" would capture.
    let early_entries: Vec<ApplyEntry> = (1..=20)
        .map(|i| insert_at(format!("key_{i}").as_bytes(), b"early", i))
        .collect();
    state_machine.apply_chunk(&early_entries).await.expect("apply early entries");

    // 2. Generate a real snapshot reflecting state as of index 20 — genuinely complete
    // and correct for its own last_included, just old by the time it gets applied later.
    let stale_snapshot_dir = temp_dir.path().join("stale_snapshot");
    let stale_last_included = LogId { term: 1, index: 20 };
    state_machine
        .generate_snapshot_data(stale_snapshot_dir.clone(), stale_last_included)
        .await
        .expect("generate stale snapshot");

    // 3. Keep advancing past what the snapshot captured — the follower catching up via
    // normal AppendEntries while the stale snapshot is still "in flight".
    let later_entries: Vec<ApplyEntry> = (21..=33)
        .map(|i| insert_at(format!("key_{i}").as_bytes(), b"later", i))
        .collect();
    state_machine.apply_chunk(&later_entries).await.expect("apply later entries");

    assert_eq!(
        state_machine.last_applied().index,
        33,
        "sanity: advanced past the stale snapshot's index"
    );
    let value_before = state_machine.get(b"key_23").expect("read before install");
    assert_eq!(
        value_before,
        Some(Bytes::from_static(b"later")),
        "sanity: key_23 present before stale install"
    );

    // 4. The stale snapshot (last_included=20) arrives late and gets installed — exactly
    // what happened in the flaky integration test.
    let stale_metadata = SnapshotMetadata {
        last_included: Some(stale_last_included),
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = state_machine
        .apply_snapshot_from_file(&stale_metadata, stale_snapshot_dir)
        .await
        .expect("apply_snapshot_from_file should not error even when rejecting a stale snapshot");
    assert!(
        matches!(result, SnapshotApplyResult::IgnoredStale { current } if current.index == 33),
        "expected IgnoredStale{{current.index: 33}}, got: {result:?}"
    );

    // 5. A stale snapshot must be a no-op: state must not roll back, key_23 must survive.
    assert_eq!(
        state_machine.last_applied().index,
        33,
        "installing a snapshot older than current progress must not roll back last_applied"
    );
    let value_after = state_machine.get(b"key_23").expect("read after install");
    assert_eq!(
        value_after,
        Some(Bytes::from_static(b"later")),
        "key_23 (applied after the stale snapshot was generated) must survive installing that stale snapshot"
    );
}

/// A duplicate snapshot (`last_included` == current `last_applied`, same term) is an
/// idempotent no-op — not an error, and must not re-run the destructive install.
///
/// #436.
#[tokio::test]
async fn test_apply_snapshot_from_file_ignores_duplicate_snapshot() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_duplicate_snapshot");
    let (_storage, state_machine) =
        RocksDBUnifiedEngine::open(&db_path).expect("Failed to open unified DB");
    state_machine.start().await.expect("Failed to start");

    let entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    state_machine.apply_chunk(&entries).await.expect("apply entries");

    let last_included = LogId { term: 1, index: 5 };
    let snapshot_dir = temp_dir.path().join("snapshot");
    state_machine
        .generate_snapshot_data(snapshot_dir.clone(), last_included)
        .await
        .expect("generate snapshot");

    // Same (index, term) as current progress — installing it again must be a no-op,
    // not an error, and must not touch the data.
    let metadata = SnapshotMetadata {
        last_included: Some(last_included),
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = state_machine
        .apply_snapshot_from_file(&metadata, snapshot_dir)
        .await
        .expect("duplicate snapshot must not error");
    assert!(
        matches!(result, SnapshotApplyResult::IgnoredDuplicate { current } if current.index == 5 && current.term == 1),
        "expected IgnoredDuplicate{{current: (5,1)}}, got: {result:?}"
    );
    assert_eq!(
        state_machine.last_applied(),
        last_included,
        "duplicate install must not change last_applied"
    );
}

/// Same index, DIFFERENT term than current `last_applied` — this can't happen under a
/// correctly-functioning Raft cluster (Log Matching Property), so it must be treated as
/// a hard inconsistency: reject with an error, don't silently ignore or auto-install.
///
/// #436.
#[tokio::test]
async fn test_apply_snapshot_from_file_rejects_boundary_conflict() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_boundary_conflict");
    let (_storage, state_machine) =
        RocksDBUnifiedEngine::open(&db_path).expect("Failed to open unified DB");
    state_machine.start().await.expect("Failed to start");

    let entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    state_machine.apply_chunk(&entries).await.expect("apply entries");
    assert_eq!(state_machine.last_applied(), LogId { term: 1, index: 5 });

    let snapshot_dir = temp_dir.path().join("snapshot");
    // Same index (5) as current progress, but a different term (2) — a boundary conflict,
    // not a duplicate.
    state_machine
        .generate_snapshot_data(snapshot_dir.clone(), LogId { term: 2, index: 5 })
        .await
        .expect("generate snapshot");
    let conflicting_metadata = SnapshotMetadata {
        last_included: Some(LogId { term: 2, index: 5 }),
        checksum: Bytes::from_static(&[0; 32]),
    };

    let result = state_machine
        .apply_snapshot_from_file(&conflicting_metadata, snapshot_dir)
        .await;
    assert!(
        result.is_err(),
        "same index, different term must be rejected as a boundary conflict, not silently accepted"
    );
    assert_eq!(
        state_machine.last_applied(),
        LogId { term: 1, index: 5 },
        "a rejected boundary conflict must not change last_applied"
    );
}

/// A genuinely newer snapshot (`last_included.index` > current `last_applied`) must
/// actually run the destructive install and report `Applied`.
///
/// #436 — dedicated RocksDB-level check that the `Applied` variant
/// carries the right `last_included`, complementing the generic conformance suite
/// (`state_machine_test.rs`) which already exercises this path end-to-end.
#[tokio::test]
async fn test_apply_snapshot_from_file_reports_applied_for_newer_snapshot() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_applied_snapshot");
    let (_storage, state_machine) =
        RocksDBUnifiedEngine::open(&db_path).expect("Failed to open unified DB");
    state_machine.start().await.expect("Failed to start");

    let entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    state_machine.apply_chunk(&entries).await.expect("apply entries");

    let last_included = LogId { term: 1, index: 5 };
    let snapshot_dir = temp_dir.path().join("snapshot");
    state_machine
        .generate_snapshot_data(snapshot_dir.clone(), last_included)
        .await
        .expect("generate snapshot");

    // Fall behind, then receive the (now newer-than-current) snapshot.
    state_machine.reset().await.expect("reset");
    assert_eq!(state_machine.last_applied(), LogId::default());

    let checksum = compute_checksum_from_folder_path(&snapshot_dir)
        .await
        .expect("compute checksum");
    let metadata = SnapshotMetadata {
        last_included: Some(last_included),
        checksum: Bytes::copy_from_slice(&checksum),
    };
    let result = state_machine
        .apply_snapshot_from_file(&metadata, snapshot_dir)
        .await
        .expect("newer snapshot must install successfully");
    assert!(
        matches!(result, SnapshotApplyResult::Applied { last_included: li } if li == last_included),
        "expected Applied{{last_included: {last_included:?}}}, got: {result:?}"
    );
    assert_eq!(state_machine.last_applied(), last_included);
}

/// #436-adjacent: `last_included: None` (e.g. a malformed/corrupted message off the
/// wire) must be rejected outright, not silently fall through to an unconditional
/// install. Before the fix, `None` skipped the entire classification `if let` block
/// and installed anyway — this pinned down the found bug; now pins down the fix.
#[tokio::test]
async fn test_apply_snapshot_from_file_rejects_missing_last_included() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_none_last_included");
    let (_storage, state_machine) =
        RocksDBUnifiedEngine::open(&db_path).expect("Failed to open unified DB");
    state_machine.start().await.expect("Failed to start");

    // Snapshot captured early, containing only key_1..key_5.
    let early_entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    state_machine.apply_chunk(&early_entries).await.expect("apply early entries");
    let snapshot_dir = temp_dir.path().join("snapshot");
    state_machine
        .generate_snapshot_data(snapshot_dir.clone(), LogId { term: 1, index: 5 })
        .await
        .expect("generate snapshot");

    // Node keeps going past the snapshot boundary: key_6..key_8, last_applied=8.
    let later_entries: Vec<ApplyEntry> =
        (6..=8).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    state_machine.apply_chunk(&later_entries).await.expect("apply later entries");
    assert_eq!(state_machine.last_applied(), LogId { term: 1, index: 8 });

    let metadata = SnapshotMetadata {
        last_included: None,
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await;

    assert!(
        result.is_err(),
        "a snapshot with no boundary must be rejected, not installed — got: {result:?}"
    );

    // Nothing must have changed: no destructive install happened.
    assert!(
        state_machine.get(b"key_6").unwrap().is_some(),
        "key_6 (applied after the snapshot boundary) must survive a rejected install"
    );
    assert_eq!(
        state_machine.last_applied(),
        LogId { term: 1, index: 8 },
        "last_applied must be unchanged after a rejected install"
    );
}
