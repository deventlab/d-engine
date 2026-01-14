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
use d_engine_core::Error;
use d_engine_core::storage::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use tempfile::TempDir;
use tonic::async_trait;

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

#[tokio::test]
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
