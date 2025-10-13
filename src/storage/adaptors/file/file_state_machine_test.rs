use bytes::Bytes;
use prost::Message;

use crate::{
    proto::{
        client::{
            write_command::{Insert, Operation},
            WriteCommand,
        },
        common::{entry_payload::Payload, Entry, EntryPayload},
    },
    FileStateMachine, StateMachine,
};

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
