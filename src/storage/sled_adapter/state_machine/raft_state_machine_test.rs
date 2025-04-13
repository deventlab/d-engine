use super::*;
use crate::convert::kv;
use crate::init_sled_storages;
use crate::proto::ClientCommand;
use crate::proto::Entry;
use crate::proto::SnapshotEntry;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::setup_raft_components;
use crate::test_utils::{self};
use crate::StateMachine;
use crate::COMMITTED_LOG_METRIC;
use prost::Message;
use sled::Batch;
use std::sync::Arc;

#[test]
fn test_start_stop() {
    let root_path = "/tmp/test_start_stop";
    let context = setup_raft_components(root_path, None, false);

    // Test default is running
    assert!(context.state_machine.is_running());

    context.state_machine.start().expect("should succeed");
    assert!(context.state_machine.is_running());
    context.state_machine.stop().expect("should succeed");
    assert!(!context.state_machine.is_running());
    context.state_machine.start().expect("should succeed");
    assert!(context.state_machine.is_running());
}
#[test]
fn test_apply_committed_raft_logs_in_batch() {
    let root_path = "/tmp/test_apply_committed_raft_logs_in_batch";
    let context = setup_raft_components(root_path, None, false);

    //step1: prepare some entries inside state machine
    let mut entries = Vec::new();
    for id in 1..=3 {
        let log = Entry {
            index: id,
            term: 1,
            command: generate_insert_commands(vec![id]),
        };
        entries.push(log);
    }
    context.state_machine.apply_chunk(entries).expect("should succeed");
    assert_eq!(context.state_machine.last_entry_index(), Some(3));
}

fn init(path: &str) -> Arc<sled::Db> {
    let (_raft_log_db, state_machine_db, _state_storage_db, _snapshot_storage_db) =
        init_sled_storages(path.to_string()).unwrap();
    Arc::new(state_machine_db)
}

/// # Case 1: test if node restart, the state machine entries should load from disk
#[test]
fn test_state_machine_flush() {
    test_utils::enable_logger();

    let p = "/tmp/test_state_machine_flush";
    {
        let _ = std::fs::remove_dir_all(p);
        println!("Test setup ...");
        let state_machine_db = init(p);
        let state_machine = Arc::new(RaftStateMachine::new(1, state_machine_db));
        let mut batch = Batch::default();
        batch.insert(kv(1), kv(1));
        batch.insert(kv(2), kv(2));
        state_machine.apply_batch(batch).expect("should succeed");
        state_machine.flush().expect("should succeed");
        println!(">>state_machine disk length: {:?}", state_machine.len());
    }

    {
        let state_machine_db = init(p);
        let state_machine = RaftStateMachine::new(1, state_machine_db);
        assert_eq!(state_machine.len(), 2);
        assert_eq!(state_machine.get(&kv(2)).unwrap_or(Some(kv(0))), Some(kv(2)));
    }
}

#[tokio::test]
async fn test_basic_kv_operations() {
    let root_path = "/tmp/test_basic_kv_operations";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();

    let test_key = 42u64;
    let test_value = kv(test_key);

    // Test insert and read
    let mut batch = Batch::default();
    batch.insert(test_value.clone(), test_value.clone());
    sm.apply_batch(batch).unwrap();

    match sm.get(&test_value) {
        Ok(Some(v)) => assert_eq!(v, test_value),
        _ => panic!("Value not found"),
    }

    // Test delete
    let mut batch = Batch::default();
    batch.remove(test_value.clone());
    sm.apply_batch(batch).unwrap();

    assert_eq!(sm.get(&test_value).unwrap(), None);
}

#[tokio::test]
async fn test_snapshot_operations() {
    let root_path = "/tmp/test_snapshot_operations";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();
    let test_entry = SnapshotEntry {
        key: kv(100),
        value: kv(100),
    };

    // Test successful snapshot application
    sm.stop().unwrap();
    sm.apply_snapshot(test_entry.clone()).unwrap();
    assert_eq!(sm.get(&test_entry.key).unwrap(), Some(test_entry.value));

    // Test error handling
    sm.start().unwrap();
    let test_entry = SnapshotEntry {
        key: kv(100),
        value: kv(100),
    };
    assert!(sm.apply_snapshot(test_entry).is_err());
}

#[test]
fn test_last_entry_detection() {
    let root_path = "/tmp/test_last_entry_detection";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();

    // Insert test data
    let mut entries = Vec::new();
    for id in 1..=5 {
        let log = Entry {
            index: id,
            term: 1,
            command: generate_insert_commands(vec![id]),
        };
        entries.push(log);
    }
    sm.apply_chunk(entries).unwrap();

    // Verify last entry
    assert_eq!(sm.last_entry_index(), Some(5));
    assert_eq!(sm.last_applied(), 5);
}

#[tokio::test]
async fn test_batch_error_handling() {
    let root_path = "/tmp/test_batch_error_handling";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();

    // Create invalid batch (simulate error)
    let mut batch = Batch::default();
    batch.insert(b"bad-key".to_vec(), b"bad-value".to_vec());

    // This test might need mocking for specific error conditions
    // For demonstration purposes:
    assert!(sm.apply_batch(batch).is_ok());
}

#[test]
fn test_iter_functionality() {
    let root_path = "/tmp/test_iter_functionality";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();

    // Insert test data
    let mut batch = Batch::default();
    for i in 1..=3 {
        let key = kv(i);
        batch.insert(key.clone(), key);
    }
    sm.apply_batch(batch).unwrap();

    // Verify iterator
    let mut count = 0;
    for item in sm.iter() {
        let (k, v) = item.unwrap();
        assert_eq!(k.to_vec(), v.to_vec());
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_apply_chunk_functionality() {
    let root_path = "/tmp/test_apply_chunk_functionality";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();
    let test_entries = vec![
        Entry {
            index: 1,
            command: ClientCommand::insert(kv(1), kv(2)).encode_to_vec(),
            term: 1,
        },
        Entry {
            index: 2,
            command: ClientCommand::delete(kv(1)).encode_to_vec(),
            term: 1,
        },
    ];

    // Test chunk application
    sm.apply_chunk(test_entries).unwrap();

    // Verify results
    assert_eq!(sm.get(&kv(1)).unwrap(), None);
    assert_eq!(sm.last_applied(), 2);
}

#[test]
fn test_metrics_integration() {
    let root_path = "/tmp/test_metrics_integration";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();
    let test_entry = Entry {
        index: 1,
        command: ClientCommand::insert(kv(1), kv(2)).encode_to_vec(),
        term: 1,
    };

    // Verify metric increment
    COMMITTED_LOG_METRIC.reset();
    let initial = COMMITTED_LOG_METRIC.with_label_values(&["1", "1"]).get();

    sm.apply_chunk(vec![test_entry]).unwrap();

    let post = COMMITTED_LOG_METRIC.with_label_values(&["1", "1"]).get();

    assert_eq!(post - initial, 1);
}
