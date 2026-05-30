use bytes::Bytes;
use d_engine_core::{ApplyEntry, Command, StateMachine};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use crate::storage::adaptors::file::FileStateMachine;
use async_trait::async_trait;
use d_engine_core::Error;
use d_engine_core::storage::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use tempfile::TempDir;

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
        let path = self.temp_dir.path().join("file_sm");
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
    let data_dir = temp_dir.path().to_path_buf();

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
    let data_dir = temp_dir.path().to_path_buf();
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
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

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
    let data_dir = temp_dir.path().to_path_buf();
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
    let data_dir = temp_dir.path().to_path_buf();

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
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

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
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

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
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

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
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

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
