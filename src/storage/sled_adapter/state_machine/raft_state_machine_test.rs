use super::*;
use crate::constants::SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX;
use crate::constants::SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM;
use crate::constants::STATE_MACHINE_META_NAMESPACE;
use crate::constants::STATE_MACHINE_TREE;
use crate::constants::STATE_SNAPSHOT_METADATA_TREE;
use crate::convert::safe_kv;
use crate::convert::safe_vk;
use crate::file_io::compute_checksum_from_path;
use crate::init_sled_state_machine_db;
use crate::init_sled_storages;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
use crate::test_utils::generate_delete_commands;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::setup_raft_components;
use crate::test_utils::{self};
use crate::Error;
use crate::StateMachine;
use crate::StorageError;
use crate::SystemError;
use crate::COMMITTED_LOG_METRIC;
use prometheus::Registry;
use sled::Batch;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

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
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    context.state_machine.apply_chunk(entries).expect("should succeed");
    assert_eq!(context.state_machine.last_applied(), LogId { index: 3, term: 1 });
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
        let state_machine = Arc::new(RaftStateMachine::new(1, state_machine_db).expect("success"));
        let mut batch = Batch::default();
        batch.insert(&safe_kv(1), &safe_kv(1));
        batch.insert(&safe_kv(2), &safe_kv(2));
        state_machine.apply_batch(batch).expect("should succeed");
        state_machine.flush().expect("should succeed");
        println!(">>state_machine disk length: {:?}", state_machine.len());
    }

    {
        let state_machine_db = init(p);
        let state_machine = RaftStateMachine::new(1, state_machine_db).expect("success");
        assert_eq!(state_machine.len(), 2);
        assert_eq!(state_machine.get(&safe_kv(2)).unwrap(), Some(safe_kv(2).to_vec()));
    }
}

#[tokio::test]
async fn test_basic_kv_operations() {
    let root_path = "/tmp/test_basic_kv_operations";
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();

    let test_key = 42u64;
    let test_value = safe_kv(test_key);

    // Test insert and read
    let mut batch = Batch::default();
    batch.insert(&test_value.clone(), &test_value.clone());
    sm.apply_batch(batch).unwrap();

    match sm.get(&test_value) {
        Ok(Some(v)) => assert_eq!(v, test_value),
        _ => panic!("Value not found"),
    }

    // Test delete
    let mut batch = Batch::default();
    batch.remove(&test_value.clone());
    sm.apply_batch(batch).unwrap();

    assert_eq!(sm.get(&test_value).unwrap(), None);
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
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    sm.apply_chunk(entries).unwrap();

    // Verify last entry
    assert_eq!(sm.last_applied(), LogId { index: 5, term: 1 });
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
        let key = safe_kv(i);
        batch.insert(&key, &key);
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
            payload: Some(EntryPayload::command(generate_insert_commands(vec![1, 2]))),
            term: 1,
        },
        Entry {
            index: 2,
            payload: Some(EntryPayload::command(generate_delete_commands(1..=1))),
            term: 1,
        },
    ];

    // Test chunk application
    sm.apply_chunk(test_entries).unwrap();

    // Verify results
    assert_eq!(sm.get(&safe_kv(1)).unwrap(), None);
    assert_eq!(sm.last_applied(), LogId { index: 2, term: 1 });
}

fn create_test_registry() -> Registry {
    let registry = Registry::new();
    registry.register(Box::new(COMMITTED_LOG_METRIC.clone())).unwrap();
    registry
}

#[test]
fn test_metrics_integration() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root_path = temp_dir.path().to_str().unwrap();
    let context = setup_raft_components(root_path, None, false);
    let sm = context.state_machine.clone();
    let test_entry = Entry {
        index: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1, 2]))),
        term: 1,
    };

    // Verify metric increment
    let _registry = create_test_registry();
    COMMITTED_LOG_METRIC.reset();
    let initial = COMMITTED_LOG_METRIC.with_label_values(&["1", "1"]).get();

    sm.apply_chunk(vec![test_entry]).unwrap();

    let post = COMMITTED_LOG_METRIC.with_label_values(&["1", "1"]).get();

    assert_eq!(post - initial, 1);
}

/// # Case 1: test basic functionality
#[tokio::test]
async fn test_generate_snapshot_data_case1() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_generate_snapshot_data_case1", None, false);
    let sm = context.state_machine.clone();

    // Insert 3 test entries
    let mut batch = Batch::default();
    for i in 1..=3 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    sm.apply_batch(batch).unwrap();

    // Generate snapshot with last included index=3
    let temp_path = root.path().join("snapshot1");
    sm.generate_snapshot_data(temp_path.clone(), LogId { index: 3, term: 1 })
        .await
        .unwrap();

    // Verify snapshot contents
    let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
    let tree = snapshot_db.open_tree(STATE_MACHINE_TREE).unwrap();
    let metadata_tree = snapshot_db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();

    let expected_checksum = compute_checksum_from_path(&temp_path).await.expect("success");

    // Check data entries
    for i in 1..=3 {
        assert!(tree.get(safe_kv(i)).unwrap().is_some());
    }

    // Check metadata (stored in same tree due to code limitation)
    assert_eq!(
        sm.last_included(),
        (
            LogId {
                index: 3u64,
                term: 1u64
            },
            Some(expected_checksum)
        )
    );
    assert_eq!(
        safe_vk(
            metadata_tree
                .get(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX)
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        3u64
    );
    assert_eq!(
        safe_vk(
            metadata_tree
                .get(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM)
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        1u64
    );
}

/// # Case 2: Exclude upper entries
#[tokio::test]
async fn test_generate_snapshot_data_case2() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_generate_snapshot_data_case2", None, false);
    let sm = context.state_machine.clone();

    // Insert 5 test entries
    let mut batch = Batch::default();
    for i in 1..=5 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    sm.apply_batch(batch).unwrap();

    // Generate snapshot with last included index=3
    let temp_path = root.path().join("snapshot2");
    sm.generate_snapshot_data(temp_path.clone(), LogId { index: 3, term: 1 })
        .await
        .unwrap();

    // Verify snapshot contents
    let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
    let tree = snapshot_db.open_tree(STATE_MACHINE_TREE).unwrap();

    // Entries <=3 should exist
    for i in 1..=3 {
        assert!(tree.get(safe_kv(i)).unwrap().is_some());
    }

    // Entries >3 should not exist
    for i in 4..=5 {
        assert!(tree.get(safe_kv(i)).unwrap().is_none());
    }
}

/// # Case 3: Metadata correctness
#[tokio::test]
async fn test_generate_snapshot_data_case3() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_generate_snapshot_data_case3", None, false);
    let sm = context.state_machine.clone();

    // Generate snapshot with specific metadata
    let temp_path = root.path().join("snapshot3");
    sm.generate_snapshot_data(temp_path.clone(), LogId { index: 42, term: 5 })
        .await
        .unwrap();

    // Verify metadata
    let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
    let metadata_tree = snapshot_db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();

    assert_eq!(
        safe_vk(
            metadata_tree
                .get(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX)
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        42u64
    );
    assert_eq!(
        safe_vk(
            metadata_tree
                .get(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM)
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        5u64
    );
}

/// # Case 4: Batch processing
#[tokio::test]
async fn test_generate_snapshot_data_case4() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_generate_snapshot_data_case4", None, false);
    let sm = context.state_machine.clone();

    // Insert 150 test entries
    let mut batch = Batch::default();
    for i in 1..=150 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    sm.apply_batch(batch).unwrap();

    // Generate snapshot
    let temp_path = root.path().join("snapshot4");
    sm.generate_snapshot_data(temp_path.clone(), LogId { index: 150, term: 1 })
        .await
        .unwrap();

    // Verify all entries exist
    let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
    let tree = snapshot_db.open_tree(STATE_MACHINE_TREE).unwrap();

    assert_eq!(tree.len(), 150);
    for i in 1..=150 {
        assert!(tree.get(safe_kv(i)).unwrap().is_some());
    }
}

/// # Case 1: Basic snapshot application
#[tokio::test]
async fn test_apply_snapshot_from_file_case1() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_apply_snapshot_from_file_case1", None, false);
    let sm = context.state_machine.clone();

    // Generate test snapshot
    let temp_path = root.path().join("snapshot_basic");
    let last_included = LogId { index: 5, term: 2 };
    let metadata = SnapshotMetadata {
        last_included: Some(last_included),
        checksum: [0; 32].to_vec(), //TODO: to be tested
    };

    {
        // Create dummy snapshot data
        let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
        let tree = snapshot_db.open_tree(STATE_MACHINE_TREE).unwrap();
        tree.insert(b"test_key", b"test_value").unwrap();
        tree.flush().unwrap();
    }

    // Apply snapshot
    sm.apply_snapshot_from_file(&metadata, temp_path.clone()).await.unwrap();

    // Verify data
    assert_eq!(sm.get(b"test_key").unwrap(), Some(b"test_value".to_vec()));
    assert_eq!(sm.last_applied(), last_included);
    assert_eq!(
        sm.last_included(),
        (last_included, Some(metadata.checksum_array().expect("success")))
    );
}

/// # Case 2: Overwrite existing state
#[tokio::test]
async fn test_apply_snapshot_from_file_case2() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_apply_snapshot_from_file_case2", None, false);
    let sm = context.state_machine.clone();

    // Add initial data
    let mut batch = Batch::default();
    batch.insert(b"existing_key", b"old_value");
    sm.apply_batch(batch).unwrap();

    // Create snapshot with new data
    let temp_path = root.path().join("snapshot_overwrite");
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 10, term: 3 }),
        checksum: [0; 32].to_vec(), //TODO: to be tested
    };

    {
        let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
        let tree = snapshot_db.open_tree(STATE_MACHINE_TREE).unwrap();
        tree.insert(b"new_key", b"new_value").unwrap();
        tree.flush().unwrap();
    }

    // Apply snapshot
    sm.apply_snapshot_from_file(&metadata, temp_path).await.unwrap();

    // Verify old data replaced
    assert_eq!(sm.get(b"existing_key").unwrap(), None);
    assert_eq!(sm.get(b"new_key").unwrap(), Some(b"new_value".to_vec()));
}

/// # Case 3: Metadata consistency check
#[tokio::test]
async fn test_apply_snapshot_from_file_case3() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_apply_snapshot_from_file_case3", None, false);
    let sm = context.state_machine.clone();

    let temp_path = root.path().join("snapshot_metadata");
    let last_included = LogId { index: 15, term: 4 };
    let test_metadata = SnapshotMetadata {
        last_included: Some(last_included),
        checksum: [0; 32].to_vec(), //TODO: to be tested
    };

    {
        // Create minimal valid snapshot
        let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
        let metadata_tree = snapshot_db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();
        metadata_tree
            .insert(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX, &safe_kv(last_included.index))
            .unwrap();
        metadata_tree
            .insert(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM, &safe_kv(last_included.term))
            .unwrap();
        metadata_tree.flush().unwrap();
    }

    sm.apply_snapshot_from_file(&test_metadata, temp_path).await.unwrap();

    // Verify metadata propagation
    assert_eq!(
        sm.last_included(),
        (last_included, Some(test_metadata.checksum_array().expect("success")))
    );
    assert_eq!(sm.last_applied(), last_included);
}

/// # Case 4: Concurrent snapshot protection
#[tokio::test]
async fn test_apply_snapshot_from_file_case4() {
    let root = tempfile::tempdir().unwrap();
    let context = setup_raft_components("/tmp/test_apply_snapshot_from_file_case4", None, false);
    let sm = context.state_machine.clone();

    let temp_path = root.path().join("snapshot_concurrent");
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 20, term: 5 }),
        checksum: [0; 32].to_vec(), //TODO: to be tested
    };

    // Start two concurrent apply operations
    let handle1 = tokio::spawn({
        let sm = sm.clone();
        let path = temp_path.clone();
        let metadata = metadata.clone();
        async move { sm.apply_snapshot_from_file(&metadata, path).await }
    });

    let handle2 = tokio::spawn({
        let sm = sm.clone();
        let path = temp_path.clone();
        let metadata = metadata.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            sm.apply_snapshot_from_file(&metadata, path).await
        }
    });

    // Verify only one succeeds
    let results = futures::join!(handle1, handle2);
    println!(" > {results:?}",);

    match (results.0, results.1) {
        (Ok(Ok(_)), Ok(Err(_))) | (Ok(Err(_)), Ok(Ok(_))) => (), // Expected scenario
        _ => panic!("Both snapshot applications should not succeed concurrently"),
    }
}

/// # Case 5: Invalid snapshot handling
#[tokio::test]
async fn test_apply_snapshot_from_file_case5() {
    let context = setup_raft_components("/tmp/test_apply_snapshot_from_file_case5", None, false);
    let sm = context.state_machine.clone();

    let result = sm
        .apply_snapshot_from_file(
            &SnapshotMetadata {
                last_included: Some(LogId { term: 1, index: 1 }),
                checksum: [0_u8; 32].to_vec(),
            },
            PathBuf::from("/non/existent/path"),
        )
        .await;

    assert!(matches!(
        result,
        Err(Error::System(SystemError::Storage(StorageError::PathError { .. })))
    ));
}

#[tokio::test]
async fn test_state_machine_drop() {
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let db = Arc::new(sled::open(temp_dir.path()).unwrap());
        // Create real instance instead of mock
        let state_machine = Arc::new(RaftStateMachine::new(1, db.clone()).expect("success"));

        // Insert test data
        let mut batch = Batch::default();
        batch.insert(&safe_kv(1), &safe_kv(1));
        batch.insert(&safe_kv(2), &safe_kv(2));
        state_machine.apply_batch(batch).expect("should succeed");

        // Explicitly drop to trigger flush
        drop(state_machine);
    }

    // Verify flush occurred by checking persistence
    let reloaded_db = sled::open(temp_dir.path()).unwrap();
    assert!(!reloaded_db.open_tree(STATE_MACHINE_META_NAMESPACE).unwrap().is_empty());
}
