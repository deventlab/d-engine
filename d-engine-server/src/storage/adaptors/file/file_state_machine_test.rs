use bytes::Bytes;
use d_engine_core::StateMachine;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::entry_payload::Payload;
use prost::Message;
use std::sync::Arc;

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

    // Create proper Raft entries with payloads
    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(
                    WriteCommand {
                        operation: Some(Operation::Insert(Insert {
                            ttl_secs: 0,
                            key: Bytes::from("key1"),
                            value: Bytes::from("value1"),
                        })),
                    }
                    .encode_to_vec()
                    .into(),
                )),
            }),
        },
        Entry {
            index: 2,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(
                    WriteCommand {
                        operation: Some(Operation::Insert(Insert {
                            ttl_secs: 0,
                            key: Bytes::from("key2"),
                            value: Bytes::from("value2"),
                        })),
                    }
                    .encode_to_vec()
                    .into(),
                )),
            }),
        },
    ];

    // Apply entries (this writes WAL + updates memory)
    sm.apply_chunk(entries).await.unwrap();

    // Verify data is in memory
    assert_eq!(sm.get(b"key1").unwrap(), Some(Bytes::from("value1")));
    assert_eq!(sm.get(b"key2").unwrap(), Some(Bytes::from("value2")));

    // Now test crash recovery with WAL

    // Manually append to WAL without updating memory (simulate partial write)
    let crash_entries = vec![(
        Entry {
            index: 3,
            term: 1,
            payload: None,
        },
        "INSERT".to_string(),
        Bytes::from("key3"),
        Some(Bytes::from("value3")),
        0, // No TTL
    )];
    sm.append_to_wal(crash_entries).await.unwrap();

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

// ── scan_prefix tests ─────────────────────────────────────────────────────────

/// scan_prefix returns only keys that start with the prefix, not all keys.
#[tokio::test]
async fn test_file_sm_scan_prefix_returns_matching_keys() {
    use d_engine_core::StateMachine;

    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(
                    WriteCommand {
                        operation: Some(Operation::Insert(Insert {
                            key: Bytes::from("/services/node1"),
                            value: Bytes::from("10.0.0.1"),
                            ttl_secs: 0,
                        })),
                    }
                    .encode_to_vec()
                    .into(),
                )),
            }),
        },
        Entry {
            index: 2,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(
                    WriteCommand {
                        operation: Some(Operation::Insert(Insert {
                            key: Bytes::from("/services/node2"),
                            value: Bytes::from("10.0.0.2"),
                            ttl_secs: 0,
                        })),
                    }
                    .encode_to_vec()
                    .into(),
                )),
            }),
        },
        Entry {
            index: 3,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(
                    WriteCommand {
                        operation: Some(Operation::Insert(Insert {
                            key: Bytes::from("/other/key"),
                            value: Bytes::from("must_not_appear"),
                            ttl_secs: 0,
                        })),
                    }
                    .encode_to_vec()
                    .into(),
                )),
            }),
        },
    ];

    sm.apply_chunk(entries).await.unwrap();

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
    use d_engine_core::StateMachine;

    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

    sm.apply_chunk(vec![Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload {
            payload: Some(Payload::Command(
                WriteCommand {
                    operation: Some(Operation::Insert(Insert {
                        key: Bytes::from("/other/key"),
                        value: Bytes::from("v"),
                        ttl_secs: 0,
                    })),
                }
                .encode_to_vec()
                .into(),
            )),
        }),
    }])
    .await
    .unwrap();

    let result = sm.scan_prefix(b"/missing/").unwrap();

    assert!(
        result.entries.is_empty(),
        "missing prefix should return empty entries"
    );
}

/// scan_prefix revision equals last_applied_index at scan time.
#[tokio::test]
async fn test_file_sm_scan_prefix_revision_reflects_applied_index() {
    use d_engine_core::StateMachine;

    let temp_dir = tempfile::tempdir().unwrap();
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

    let entries: Vec<Entry> = (1u64..=3)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(
                    WriteCommand {
                        operation: Some(Operation::Insert(Insert {
                            key: Bytes::from(format!("/s/{i}")),
                            value: Bytes::from(format!("{i}")),
                            ttl_secs: 0,
                        })),
                    }
                    .encode_to_vec()
                    .into(),
                )),
            }),
        })
        .collect();

    sm.apply_chunk(entries).await.unwrap();

    let result = sm.scan_prefix(b"/s/").unwrap();

    assert_eq!(result.entries.len(), 3);
    assert_eq!(result.revision, 3, "revision must equal last_applied_index");
}
