use bytes::Bytes;
use d_engine_core::{ApplyEntry, Command, SnapshotApplyResult, StateMachine};
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use crate::storage::adaptors::file::FileStateMachine;
use async_trait::async_trait;
use d_engine_core::Error;
use d_engine_core::storage::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use tempfile::TempDir;

// ── close_storage() tests ─────────────────────────────────────────────────────

/// close_storage() must mark the SM as not running.
/// FileStateMachine has no exclusive OS lock, so close_storage() is a lifecycle
/// signal — subsequent reads must fail with NotServing.
#[tokio::test]
async fn test_file_sm_close_storage_marks_not_running() {
    let dir = TempDir::new().unwrap();
    let sm = FileStateMachine::new(dir.path().to_path_buf()).await.unwrap();
    sm.start().await.unwrap();
    assert!(sm.is_running());

    sm.close_storage();

    assert!(!sm.is_running(), "close_storage must mark SM not running");
}

/// stop() after close_storage() must not panic (idempotent shutdown).
#[tokio::test]
async fn test_file_sm_stop_after_close_storage_is_safe() {
    let dir = TempDir::new().unwrap();
    let sm = FileStateMachine::new(dir.path().to_path_buf()).await.unwrap();
    sm.close_storage();
    assert!(sm.stop().is_ok(), "stop after close_storage must not error");
}

/// Builder for FileStateMachine test instances
struct FileStateMachineBuilder {
    temp_dir: TempDir,
}

impl FileStateMachineBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("Failed to create temp dir"),
        }
    }
}

#[async_trait]
impl StateMachineBuilder for FileStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        // Use fixed path to support restart recovery testing
        let path = self.temp_dir.path().to_path_buf().join("file_sm");
        let sm = FileStateMachine::new(path).await?;
        Ok(Arc::new(sm))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // TempDir automatically cleans up on drop
        Ok(())
    }
}

#[tokio::test]
async fn test_file_state_machine_suite() {
    let builder = FileStateMachineBuilder::new();
    StateMachineTestSuite::run_all_tests(builder)
        .await
        .expect("FileStateMachine should pass all tests");
}

// TODO: test_apply_chunk_scalability uses wall-clock I/O time ratio to detect O(N²) complexity,
// which is unreliable in CI due to disk I/O spikes. Needs redesign (e.g., measure pure in-memory
// ops separately from WAL writes) before re-enabling.
#[tokio::test]
#[ignore]
async fn test_file_state_machine_performance() {
    let builder = FileStateMachineBuilder::new();
    StateMachineTestSuite::run_performance_tests(builder)
        .await
        .expect("FileStateMachine should pass performance tests");
}

#[tokio::test]
async fn test_wal_replay_after_crash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf().to_path_buf();

    let sm = FileStateMachine::new(data_dir.clone()).await.unwrap();

    let entries = vec![
        ApplyEntry {
            index: 1,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
                ttl_secs: None,
            },
        },
        ApplyEntry {
            index: 2,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
                ttl_secs: None,
            },
        },
    ];

    // Apply entries (this writes WAL + updates memory)
    sm.apply_chunk(&entries).await.unwrap();

    // Verify data is in memory
    assert_eq!(sm.get(b"key1").unwrap(), Some(Bytes::from("value1")));
    assert_eq!(sm.get(b"key2").unwrap(), Some(Bytes::from("value2")));

    // Manually append to WAL without updating memory (simulate partial write before crash)
    let crash_entries = vec![ApplyEntry {
        index: 3,
        term: 1,
        command: Command::Insert {
            key: Bytes::from("key3"),
            value: Bytes::from("value3"),
            ttl_secs: None,
        },
    }];
    let dummy_outcomes = vec![false; crash_entries.len()]; // non-CAS entries: outcome unused
    sm.append_to_wal(&crash_entries, &dummy_outcomes).await.unwrap();

    // Don't call flush - simulate crash before persistence
    drop(sm);

    // Recovery: should replay WAL
    let sm_recovered = FileStateMachine::new(data_dir.clone()).await.unwrap();

    // Verify: key1, key2 from state.data; key3 from WAL replay
    assert_eq!(
        sm_recovered.get(b"key1").unwrap(),
        Some(Bytes::from("value1"))
    );
    assert_eq!(
        sm_recovered.get(b"key2").unwrap(),
        Some(Bytes::from("value2"))
    );
    assert_eq!(
        sm_recovered.get(b"key3").unwrap(),
        Some(Bytes::from("value3"))
    );
}

// ── CAS WAL crash-safety tests ────────────────────────────────────────────────

/// A failed CAS must NOT corrupt data when the node crashes and replays WAL.
///
/// Before the fix, apply_chunk wrote all entries (including CAS) to WAL before
/// evaluating the comparison. On replay the CAS was applied unconditionally,
/// overwriting data that should never have changed.
#[tokio::test]
async fn test_cas_failure_wal_replay_does_not_corrupt_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf().to_path_buf();
    let sm = FileStateMachine::new(data_dir.clone()).await.unwrap();

    // Establish initial value
    sm.apply_chunk(&[ApplyEntry {
        index: 1,
        term: 1,
        command: Command::Insert {
            key: Bytes::from("k"),
            value: Bytes::from("original"),
            ttl_secs: None,
        },
    }])
    .await
    .unwrap();

    // CAS with wrong expected value — must fail at runtime
    let results = sm
        .apply_chunk(&[ApplyEntry {
            index: 2,
            term: 1,
            command: Command::CompareAndSwap {
                key: Bytes::from("k"),
                expected: Some(Bytes::from("wrong_expected")),
                value: Bytes::from("should_not_appear"),
            },
        }])
        .await
        .unwrap();

    assert!(
        !results[0].succeeded,
        "CAS should fail when expected != current"
    );
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from("original")));

    // Simulate crash + WAL replay
    drop(sm);
    let sm2 = FileStateMachine::new(data_dir).await.unwrap();

    assert_eq!(
        sm2.get(b"k").unwrap(),
        Some(Bytes::from("original")),
        "WAL replay of failed CAS must not corrupt data"
    );
}

/// CAS that depends on an earlier Insert in the same chunk must succeed.
///
/// The shadow-map simulation must process entries in order so the CAS sees
/// the Insert's effect even though self.data hasn't been updated yet.
#[tokio::test]
async fn test_cas_in_same_chunk_as_preceding_insert() {
    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf().to_path_buf())
        .await
        .unwrap();

    let results = sm
        .apply_chunk(&[
            ApplyEntry {
                index: 1,
                term: 1,
                command: Command::Insert {
                    key: Bytes::from("k"),
                    value: Bytes::from("v1"),
                    ttl_secs: None,
                },
            },
            ApplyEntry {
                index: 2,
                term: 1,
                command: Command::CompareAndSwap {
                    key: Bytes::from("k"),
                    expected: Some(Bytes::from("v1")),
                    value: Bytes::from("v2"),
                },
            },
        ])
        .await
        .unwrap();

    assert!(results[1].succeeded, "CAS must see Insert from same chunk");
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from("v2")));
}

/// A successful CAS must be correctly applied after WAL replay.
#[tokio::test]
async fn test_cas_success_wal_replay_applies_new_value() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf().to_path_buf();
    let sm = FileStateMachine::new(data_dir.clone()).await.unwrap();

    sm.apply_chunk(&[ApplyEntry {
        index: 1,
        term: 1,
        command: Command::Insert {
            key: Bytes::from("k"),
            value: Bytes::from("v1"),
            ttl_secs: None,
        },
    }])
    .await
    .unwrap();

    let results = sm
        .apply_chunk(&[ApplyEntry {
            index: 2,
            term: 1,
            command: Command::CompareAndSwap {
                key: Bytes::from("k"),
                expected: Some(Bytes::from("v1")),
                value: Bytes::from("v2"),
            },
        }])
        .await
        .unwrap();

    assert!(
        results[0].succeeded,
        "CAS should succeed when expected matches"
    );
    assert_eq!(sm.get(b"k").unwrap(), Some(Bytes::from("v2")));

    // Simulate crash + WAL replay
    drop(sm);
    let sm2 = FileStateMachine::new(data_dir).await.unwrap();

    assert_eq!(
        sm2.get(b"k").unwrap(),
        Some(Bytes::from("v2")),
        "Successful CAS must survive WAL replay"
    );
}

/// WAL replay must return an error when the WAL contains an unrecognised opcode byte.
///
/// An unknown opcode means the WAL is corrupt or was written by a future/incompatible
/// binary. Silently treating it as a no-op would hide the corruption and leave the
/// state machine in an undefined state. The correct behaviour is to fail loudly so
/// the operator can investigate rather than continuing with silently missing entries.
#[tokio::test]
async fn test_replay_wal_unknown_opcode_returns_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf().to_path_buf();

    // Write one valid entry so the WAL file exists and has a known-good prefix.
    let sm = FileStateMachine::new(data_dir.clone()).await.unwrap();
    sm.apply_chunk(&[ApplyEntry {
        index: 1,
        term: 1,
        command: Command::Insert {
            key: Bytes::from("k"),
            value: Bytes::from("v"),
            ttl_secs: None,
        },
    }])
    .await
    .unwrap();
    drop(sm);

    // Append a syntactically complete WAL entry whose opcode byte (99) is not
    // defined in WalOpCode. Format: index(8) + term(8) + opcode(1) + key_len(8)
    // + val_len(8) + expire_at(8) = 41 bytes total (matches the Noop/CasFailed layout).
    let wal_path = data_dir.join("wal.log");
    let mut file = tokio::fs::OpenOptions::new().append(true).open(&wal_path).await.unwrap();
    let mut corrupt_entry = Vec::with_capacity(41);
    corrupt_entry.extend_from_slice(&2u64.to_be_bytes()); // index = 2
    corrupt_entry.extend_from_slice(&1u64.to_be_bytes()); // term  = 1
    corrupt_entry.push(99u8); // unknown opcode
    corrupt_entry.extend_from_slice(&0u64.to_be_bytes()); // key_len = 0
    corrupt_entry.extend_from_slice(&0u64.to_be_bytes()); // val_len = 0
    corrupt_entry.extend_from_slice(&0u64.to_be_bytes()); // expire_at = 0
    file.write_all(&corrupt_entry).await.unwrap();
    file.flush().await.unwrap();
    drop(file);

    // Recovery must fail — unknown opcode must not be silently skipped.
    let result = FileStateMachine::new(data_dir).await;
    assert!(
        result.is_err(),
        "WAL replay must return an error on unknown opcode, not silently skip it"
    );
}

// ── scan_prefix tests ─────────────────────────────────────────────────────────

/// scan_prefix returns only keys that start with the prefix, not all keys.
#[tokio::test]
async fn test_file_sm_scan_prefix_returns_matching_keys() {
    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf().to_path_buf())
        .await
        .unwrap();

    let entries = vec![
        ApplyEntry {
            index: 1,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/services/node1"),
                value: Bytes::from("10.0.0.1"),
                ttl_secs: None,
            },
        },
        ApplyEntry {
            index: 2,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/services/node2"),
                value: Bytes::from("10.0.0.2"),
                ttl_secs: None,
            },
        },
        ApplyEntry {
            index: 3,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/other/key"),
                value: Bytes::from("must_not_appear"),
                ttl_secs: None,
            },
        },
    ];

    sm.apply_chunk(&entries).await.unwrap();

    let result = sm.scan_prefix(b"/services/").unwrap();

    assert_eq!(
        result.entries.len(),
        2,
        "only /services/ keys should appear"
    );
    let keys: Vec<_> = result.entries.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains(&Bytes::from("/services/node1")));
    assert!(keys.contains(&Bytes::from("/services/node2")));
}

/// scan_prefix on a missing prefix returns empty entries, not an error.
#[tokio::test]
async fn test_file_sm_scan_prefix_empty_namespace() {
    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf().to_path_buf())
        .await
        .unwrap();

    sm.apply_chunk(&[ApplyEntry {
        index: 1,
        term: 1,
        command: Command::Insert {
            key: Bytes::from("/other/key"),
            value: Bytes::from("v"),
            ttl_secs: None,
        },
    }])
    .await
    .unwrap();

    let result = sm.scan_prefix(b"/missing/").unwrap();

    assert!(
        result.entries.is_empty(),
        "missing prefix should return empty entries"
    );
}

// ── get_multi tests ───────────────────────────────────────────────────────────

/// FileStateMachine get_multi holds a read lock for the entire batch, ensuring
/// that state transitions are atomic from the reader's perspective.
///
/// Because apply_chunk acquires a write lock when updating self.data, a concurrent
/// write cannot interleave between the individual key reads inside get_multi.
/// The result is always either the full pre-write state or the full post-write state —
/// never a mix of values from different apply indexes.
///
/// This test verifies sequential coherence: after each state transition, all keys
/// in the batch reflect the same version with no cross-version contamination.
#[tokio::test]
async fn test_get_multi_file_sm_atomic_state_transitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf().to_path_buf())
        .await
        .unwrap();

    // v1: initial state
    sm.apply_chunk(&[
        ApplyEntry {
            index: 1,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/svc/addr"),
                value: Bytes::from("10.0.0.1"),
                ttl_secs: None,
            },
        },
        ApplyEntry {
            index: 2,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/svc/version"),
                value: Bytes::from("v1"),
                ttl_secs: None,
            },
        },
    ])
    .await
    .unwrap();

    let keys = vec![Bytes::from("/svc/addr"), Bytes::from("/svc/version")];

    // Read after v1 — both fields must be from v1
    let v1_result = sm.get_multi(&keys).unwrap();
    assert_eq!(
        v1_result[0],
        Some(Bytes::from("10.0.0.1")),
        "addr must be v1"
    );
    assert_eq!(v1_result[1], Some(Bytes::from("v1")), "version must be v1");

    // Apply v2: both fields transition together
    sm.apply_chunk(&[
        ApplyEntry {
            index: 3,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/svc/addr"),
                value: Bytes::from("10.0.0.2"),
                ttl_secs: None,
            },
        },
        ApplyEntry {
            index: 4,
            term: 1,
            command: Command::Insert {
                key: Bytes::from("/svc/version"),
                value: Bytes::from("v2"),
                ttl_secs: None,
            },
        },
    ])
    .await
    .unwrap();

    // Read after v2 — both fields must be from v2
    let v2_result = sm.get_multi(&keys).unwrap();
    assert_eq!(
        v2_result[0],
        Some(Bytes::from("10.0.0.2")),
        "addr must be v2"
    );
    assert_eq!(v2_result[1], Some(Bytes::from("v2")), "version must be v2");

    // Verify internal consistency of each result: addr and version must come from
    // the same version. A torn read (addr=v2 + version=v1) would indicate that
    // the read lock is not held across the full batch, which is a correctness bug.
    for (label, result) in [("v1_result", &v1_result), ("v2_result", &v2_result)] {
        let is_v1 =
            result[0] == Some(Bytes::from("10.0.0.1")) && result[1] == Some(Bytes::from("v1"));
        let is_v2 =
            result[0] == Some(Bytes::from("10.0.0.2")) && result[1] == Some(Bytes::from("v2"));
        assert!(
            is_v1 || is_v2,
            "{label}: torn read — result is neither pure v1 nor pure v2: {result:?}"
        );
    }
}

/// scan_prefix revision equals last_applied_index at scan time.
#[tokio::test]
async fn test_file_sm_scan_prefix_revision_reflects_applied_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf().to_path_buf())
        .await
        .unwrap();

    let entries: Vec<ApplyEntry> = (1u64..=3)
        .map(|i| ApplyEntry {
            index: i,
            term: 1,
            command: Command::Insert {
                key: Bytes::from(format!("/s/{i}")),
                value: Bytes::from(format!("{i}")),
                ttl_secs: None,
            },
        })
        .collect();

    sm.apply_chunk(&entries).await.unwrap();

    let result = sm.scan_prefix(b"/s/").unwrap();

    assert_eq!(result.entries.len(), 3);
    assert_eq!(result.revision, 3, "revision must equal last_applied_index");
}

// ── apply_snapshot_from_file classification (#436) ───────────
//
// FileStateMachine had zero coverage of this before today — it previously did an
// unconditional restore with no stale/duplicate/boundary check at all. These tests
// mirror the RocksDB adaptor's equivalents 1:1 (same contract, same StateMachine trait).

fn insert_at(
    key: &[u8],
    value: &[u8],
    index: u64,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Insert {
            key: Bytes::from(key.to_vec()),
            value: Bytes::from(value.to_vec()),
            ttl_secs: None,
        },
    }
}

/// A stale snapshot (`last_included` behind current `last_applied`) must not be
/// installed — doing so rolls back already-applied data.
#[tokio::test]
async fn test_file_apply_snapshot_from_file_rejects_stale_snapshot() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(dir.path().join("sm")).await.expect("create sm");
    sm.start().await.expect("start");

    let early: Vec<ApplyEntry> = (1..=20)
        .map(|i| insert_at(format!("key_{i}").as_bytes(), b"early", i))
        .collect();
    sm.apply_chunk(&early).await.expect("apply early entries");

    let stale_snapshot_dir = dir.path().join("stale_snapshot");
    let stale_last_included = LogId { term: 1, index: 20 };
    sm.generate_snapshot_data(stale_snapshot_dir.clone(), stale_last_included)
        .await
        .expect("generate stale snapshot");

    let later: Vec<ApplyEntry> = (21..=33)
        .map(|i| insert_at(format!("key_{i}").as_bytes(), b"later", i))
        .collect();
    sm.apply_chunk(&later).await.expect("apply later entries");
    assert_eq!(
        sm.last_applied().index,
        33,
        "sanity: advanced past the stale snapshot's index"
    );

    let stale_metadata = SnapshotMetadata {
        last_included: Some(stale_last_included),
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = sm
        .apply_snapshot_from_file(&stale_metadata, stale_snapshot_dir)
        .await
        .expect("apply_snapshot_from_file should not error even when rejecting a stale snapshot");
    assert!(
        matches!(result, SnapshotApplyResult::IgnoredStale { current } if current.index == 33),
        "expected IgnoredStale{{current.index: 33}}, got: {result:?}"
    );
    assert_eq!(
        sm.last_applied().index,
        33,
        "must not roll back last_applied"
    );
    assert_eq!(
        sm.get(b"key_23").expect("read after install"),
        Some(Bytes::from_static(b"later")),
        "key_23 must survive installing a stale snapshot"
    );
}

/// A duplicate snapshot (`last_included` == current `last_applied`, same term) is an
/// idempotent no-op — not an error, and must not re-run the destructive install.
#[tokio::test]
async fn test_file_apply_snapshot_from_file_ignores_duplicate_snapshot() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(dir.path().join("sm")).await.expect("create sm");
    sm.start().await.expect("start");

    let entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    sm.apply_chunk(&entries).await.expect("apply entries");

    let last_included = LogId { term: 1, index: 5 };
    let snapshot_dir = dir.path().join("snapshot");
    sm.generate_snapshot_data(snapshot_dir.clone(), last_included)
        .await
        .expect("generate snapshot");

    let metadata = SnapshotMetadata {
        last_included: Some(last_included),
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = sm
        .apply_snapshot_from_file(&metadata, snapshot_dir)
        .await
        .expect("duplicate snapshot must not error");
    assert!(
        matches!(result, SnapshotApplyResult::IgnoredDuplicate { current } if current.index == 5 && current.term == 1),
        "expected IgnoredDuplicate{{current: (5,1)}}, got: {result:?}"
    );
    assert_eq!(
        sm.last_applied(),
        last_included,
        "duplicate install must not change last_applied"
    );
}

/// Same index, DIFFERENT term than current `last_applied` — can't happen under a
/// correctly-functioning Raft cluster (Log Matching Property); must be rejected as a
/// hard inconsistency, not silently ignored or auto-installed.
#[tokio::test]
async fn test_file_apply_snapshot_from_file_rejects_boundary_conflict() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(dir.path().join("sm")).await.expect("create sm");
    sm.start().await.expect("start");

    let entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    sm.apply_chunk(&entries).await.expect("apply entries");
    assert_eq!(sm.last_applied(), LogId { term: 1, index: 5 });

    let snapshot_dir = dir.path().join("snapshot");
    sm.generate_snapshot_data(snapshot_dir.clone(), LogId { term: 2, index: 5 })
        .await
        .expect("generate snapshot");
    let conflicting_metadata = SnapshotMetadata {
        last_included: Some(LogId { term: 2, index: 5 }),
        checksum: Bytes::from_static(&[0; 32]),
    };

    let result = sm.apply_snapshot_from_file(&conflicting_metadata, snapshot_dir).await;
    assert!(
        result.is_err(),
        "same index, different term must be rejected as a boundary conflict, not silently accepted"
    );
    assert_eq!(
        sm.last_applied(),
        LogId { term: 1, index: 5 },
        "a rejected boundary conflict must not change last_applied"
    );
}

/// A genuinely newer snapshot (`last_included.index` > current `last_applied`) must
/// actually run the destructive install and report `Applied`.
#[tokio::test]
async fn test_file_apply_snapshot_from_file_reports_applied_for_newer_snapshot() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(dir.path().join("sm")).await.expect("create sm");
    sm.start().await.expect("start");

    let entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    sm.apply_chunk(&entries).await.expect("apply entries");

    let last_included = LogId { term: 1, index: 5 };
    let snapshot_dir = dir.path().join("snapshot");
    sm.generate_snapshot_data(snapshot_dir.clone(), last_included)
        .await
        .expect("generate snapshot");

    sm.reset().await.expect("reset");
    assert_eq!(sm.last_applied(), LogId::default());

    let metadata = SnapshotMetadata {
        last_included: Some(last_included),
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = sm
        .apply_snapshot_from_file(&metadata, snapshot_dir)
        .await
        .expect("newer snapshot must install successfully");
    assert!(
        matches!(result, SnapshotApplyResult::Applied { last_included: li } if li == last_included),
        "expected Applied{{last_included: {last_included:?}}}, got: {result:?}"
    );
    assert_eq!(sm.last_applied(), last_included);
}

/// #436-adjacent: same as the RocksDB adaptor's
/// `test_apply_snapshot_from_file_rejects_missing_last_included` — `last_included: None`
/// must be rejected outright, not silently fall through to an unconditional install.
#[tokio::test]
async fn test_file_apply_snapshot_from_file_rejects_missing_last_included() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(dir.path().join("sm")).await.expect("create sm");
    sm.start().await.expect("start");

    let early_entries: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    sm.apply_chunk(&early_entries).await.expect("apply early entries");
    let snapshot_dir = dir.path().join("snapshot");
    sm.generate_snapshot_data(snapshot_dir.clone(), LogId { term: 1, index: 5 })
        .await
        .expect("generate snapshot");

    let later_entries: Vec<ApplyEntry> =
        (6..=8).map(|i| insert_at(format!("key_{i}").as_bytes(), b"v", i)).collect();
    sm.apply_chunk(&later_entries).await.expect("apply later entries");
    assert_eq!(sm.last_applied(), LogId { term: 1, index: 8 });

    let metadata = SnapshotMetadata {
        last_included: None,
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = sm.apply_snapshot_from_file(&metadata, snapshot_dir).await;

    assert!(
        result.is_err(),
        "a snapshot with no boundary must be rejected, not installed — got: {result:?}"
    );
    assert!(
        sm.get(b"key_6").unwrap().is_some(),
        "key_6 (applied after the snapshot boundary) must survive a rejected install"
    );
    assert_eq!(
        sm.last_applied(),
        LogId { term: 1, index: 8 },
        "last_applied must be unchanged after a rejected install"
    );
}

/// A snapshot advances `last_applied` past the follower's pre-snapshot WAL entries.
/// If a crash leaves those stale entries in the WAL, replay must skip them
/// (`index <= last_applied`) instead of clobbering the newer snapshot content (#442).
#[tokio::test]
async fn test_replay_wal_skips_stale_entries_below_last_applied() {
    let dir = TempDir::new().expect("Failed to create temp dir");

    // Follower applies key_1..key_5 = "old" at indexes 1..=5.
    let sm = FileStateMachine::new(dir.path().join("sm")).await.expect("create follower");
    sm.start().await.expect("start follower");
    let stale: Vec<ApplyEntry> =
        (1..=5).map(|i| insert_at(format!("key_{i}").as_bytes(), b"old", i)).collect();
    sm.apply_chunk(&stale).await.expect("apply pre-snapshot entries");

    // Snapshot at index 10 overwrites the same keys with newer values.
    let gen_sm = FileStateMachine::new(dir.path().join("gen")).await.expect("create generator");
    gen_sm.start().await.expect("start generator");
    let newer: Vec<ApplyEntry> =
        (1..=10).map(|i| insert_at(format!("key_{i}").as_bytes(), b"new", i)).collect();
    gen_sm.apply_chunk(&newer).await.expect("apply generator entries");
    let snapshot_dir = dir.path().join("snapshot");
    gen_sm
        .generate_snapshot_data(snapshot_dir.clone(), LogId { term: 1, index: 10 })
        .await
        .expect("generate snapshot");

    // Install the snapshot into the follower (jumps from 5 to 10).
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { term: 1, index: 10 }),
        checksum: Bytes::from_static(&[0; 32]),
    };
    let result = sm
        .apply_snapshot_from_file(&metadata, snapshot_dir)
        .await
        .expect("install snapshot");
    assert!(
        matches!(result, SnapshotApplyResult::Applied { last_included } if last_included.index == 10),
        "expected Applied{{index:10}}, got: {result:?}"
    );

    // Simulate the crash window: stale pre-snapshot WAL entries survive while
    // last_applied is already 10 — the state the old persist order could leave.
    let stale_outcomes = vec![false; stale.len()];
    sm.append_to_wal(&stale, &stale_outcomes).await.expect("append stale WAL");

    // "Crash": drop and reload from disk.
    drop(sm);

    let recovered = FileStateMachine::new(dir.path().join("sm")).await.expect("reload after crash");
    recovered.start().await.expect("start recovered");

    // The snapshot's newer values must survive; stale WAL must NOT clobber them.
    assert_eq!(
        recovered.get(b"key_1").expect("read key_1"),
        Some(Bytes::from_static(b"new")),
        "key_1 must keep the snapshot value, not be clobbered by the stale WAL"
    );
    assert_eq!(
        recovered.get(b"key_5").expect("read key_5"),
        Some(Bytes::from_static(b"new")),
        "key_5 must keep the snapshot value, not be clobbered by the stale WAL"
    );
    assert_eq!(
        recovered.last_applied(),
        LogId { term: 1, index: 10 },
        "last_applied must remain at the snapshot boundary after recovery"
    );
}
