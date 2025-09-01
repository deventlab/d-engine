use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sled::Batch;
use tracing::debug;
use tracing_test::traced_test;

use super::*;
use d_engine::constants::LAST_SNAPSHOT_METADATA_KEY;
use d_engine::constants::STATE_MACHINE_META_NAMESPACE;
use d_engine::constants::STATE_MACHINE_TREE;
use d_engine::constants::STATE_SNAPSHOT_METADATA_TREE;
use d_engine::convert::safe_kv;
use d_engine::file_io::compute_checksum_from_folder_path;
use d_engine::init_sled_state_machine_db;
use d_engine::init_sled_storages;
use d_engine::proto::common::Entry;
use d_engine::proto::common::EntryPayload;
use d_engine::proto::common::LogId;
use d_engine::proto::storage::SnapshotMetadata;
use d_engine::test_utils::generate_delete_commands;
use d_engine::test_utils::generate_insert_commands;
use d_engine::test_utils::reset_dbs;
use d_engine::test_utils::reuse_dbs;
use d_engine::StateMachine;

pub fn setup_raft_components(
    db_path: &str,
    restart: bool,
) -> SledStateMachine {
    println!("Test setup_raft_components ...");
    //start from fresh
    let (_storage_engine_db, state_machine_db) = if restart {
        reuse_dbs(db_path)
    } else {
        reset_dbs(db_path)
    };
    SledStateMachine::new(1, Arc::new(state_machine_db)).unwrap()
}
#[tokio::test]
#[traced_test]
async fn test_start_stop() {
    let root_path = "/tmp/test_start_stop";
    let state_machine = setup_raft_components(root_path, false);

    // Test default is running
    assert!(state_machine.is_running());

    state_machine.start().expect("should succeed");
    assert!(state_machine.is_running());
    state_machine.stop().expect("should succeed");
    assert!(!state_machine.is_running());
    state_machine.start().expect("should succeed");
    assert!(state_machine.is_running());
}

#[tokio::test]
#[traced_test]
async fn test_apply_committed_raft_logs_in_batch() {
    let root_path = "/tmp/test_apply_committed_raft_logs_in_batch";
    let state_machine = setup_raft_components(root_path, false);

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
    state_machine.apply_chunk(entries).await.expect("should succeed");
    assert_eq!(state_machine.last_applied(), LogId { index: 3, term: 1 });
}

fn init(path: &str) -> Arc<sled::Db> {
    let (_raft_log_db, state_machine_db) = init_sled_storages(path.to_string()).unwrap();
    Arc::new(state_machine_db)
}

/// # Case 1: test if node restart, the state machine entries should load from disk
#[test]
fn test_state_machine_flush() {
    let p = "/tmp/test_state_machine_flush";
    {
        let _ = std::fs::remove_dir_all(p);
        println!("Test setup ...");
        let state_machine_db = init(p);
        let state_machine = Arc::new(SledStateMachine::new(1, state_machine_db).expect("success"));
        let mut batch = Batch::default();
        batch.insert(&safe_kv(1), &safe_kv(1));
        batch.insert(&safe_kv(2), &safe_kv(2));
        state_machine.apply_batch(batch).expect("should succeed");
        state_machine.flush().expect("should succeed");
        println!(">>state_machine disk length: {:?}", state_machine.len());
    }

    {
        let state_machine_db = init(p);
        let state_machine = SledStateMachine::new(1, state_machine_db).expect("success");
        assert_eq!(state_machine.len(), 2);
        assert_eq!(
            state_machine.get(&safe_kv(2)).unwrap(),
            Some(safe_kv(2).to_vec())
        );
    }
}

#[tokio::test]
#[traced_test]
async fn test_basic_kv_operations() {
    let root_path = "/tmp/test_basic_kv_operations";
    let state_machine = setup_raft_components(root_path, false);

    let test_key = 42u64;
    let test_value = safe_kv(test_key);

    // Test insert and read
    let mut batch = Batch::default();
    batch.insert(&test_value.clone(), &test_value.clone());
    state_machine.apply_batch(batch).unwrap();

    match state_machine.get(&test_value) {
        Ok(Some(v)) => assert_eq!(v, test_value),
        _ => panic!("Value not found"),
    }

    // Test delete
    let mut batch = Batch::default();
    batch.remove(&test_value.clone());
    state_machine.apply_batch(batch).unwrap();

    assert_eq!(state_machine.get(&test_value).unwrap(), None);
}

#[tokio::test]
#[traced_test]
async fn test_last_entry_detection() {
    let root_path = "/tmp/test_last_entry_detection";
    let state_machine = setup_raft_components(root_path, false);

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
    state_machine.apply_chunk(entries).await.unwrap();

    // Verify last entry
    assert_eq!(state_machine.last_applied(), LogId { index: 5, term: 1 });
}

#[tokio::test]
#[traced_test]
async fn test_batch_error_handling() {
    let root_path = "/tmp/test_batch_error_handling";
    let state_machine = setup_raft_components(root_path, false);

    // Create invalid batch (simulate error)
    let mut batch = Batch::default();
    batch.insert(&safe_kv(999), b"bad-value".to_vec());

    // This test might need mocking for specific error conditions
    // For demonstration purposes:
    assert!(state_machine.apply_batch(batch).is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_iter_functionality() {
    let root_path = "/tmp/test_iter_functionality";
    let state_machine = setup_raft_components(root_path, false);

    // Insert test data
    let mut batch = Batch::default();
    for i in 1..=3 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    state_machine.apply_batch(batch).unwrap();

    // Verify iterator
    let mut count = 0;
    for item in state_machine.iter() {
        let (k, v) = item.unwrap();
        assert_eq!(k.to_vec(), v.to_vec());
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
#[traced_test]
async fn test_apply_chunk_functionality() {
    let root_path = "/tmp/test_apply_chunk_functionality";
    let state_machine = setup_raft_components(root_path, false);

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
    state_machine.apply_chunk(test_entries).await.unwrap();

    // Verify results
    assert_eq!(state_machine.get(&safe_kv(1)).unwrap(), None);
    assert_eq!(state_machine.last_applied(), LogId { index: 2, term: 1 });
}

/// # Case 1: test basic functionality
#[tokio::test]
#[traced_test]
async fn test_generate_snapshot_data_case1() {
    let root = tempfile::tempdir().unwrap();
    let state_machine = setup_raft_components("/tmp/test_generate_snapshot_data_case1", false);

    // Insert 3 test entries
    let mut batch = Batch::default();
    for i in 1..=3 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    state_machine.apply_batch(batch).unwrap();

    // Generate snapshot with last included index=3
    let temp_path = root.path().join("snapshot1");
    state_machine
        .generate_snapshot_data(temp_path.clone(), LogId { index: 3, term: 1 })
        .await
        .unwrap();

    // Verify snapshot contents
    let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
    let tree = snapshot_db.open_tree(STATE_MACHINE_TREE).unwrap();
    let metadata_tree = snapshot_db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();

    let expected_checksum = compute_checksum_from_folder_path(&temp_path).await.expect("success");

    // Check data entries
    for i in 1..=3 {
        assert!(tree.get(safe_kv(i)).unwrap().is_some());
    }

    // Check metadata (stored in same tree due to code limitation)
    assert_eq!(
        state_machine.snapshot_metadata(),
        (Some(SnapshotMetadata {
            last_included: Some(LogId {
                index: 3u64,
                term: 1u64
            }),
            checksum: expected_checksum.to_vec()
        }))
    );

    let last_included = SledStateMachine::load_snapshot_metadata(&metadata_tree)
        .unwrap()
        .unwrap()
        .last_included
        .unwrap();
    assert_eq!(last_included.index, 3u64);
    assert_eq!(last_included.term, 1u64);
}

/// # Case 2: Exclude upper entries
#[tokio::test]
#[traced_test]
async fn test_generate_snapshot_data_case2() {
    let root = tempfile::tempdir().unwrap();
    let state_machine = setup_raft_components("/tmp/test_generate_snapshot_data_case2", false);

    // Insert 5 test entries
    let mut batch = Batch::default();
    for i in 1..=5 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    state_machine.apply_batch(batch).unwrap();

    // Generate snapshot with last included index=3
    let temp_path = root.path().join("snapshot2");
    state_machine
        .generate_snapshot_data(temp_path.clone(), LogId { index: 3, term: 1 })
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
#[traced_test]
async fn test_generate_snapshot_data_case3() {
    let root = tempfile::tempdir().unwrap();
    let state_machine = setup_raft_components("/tmp/test_generate_snapshot_data_case3", false);

    // Generate snapshot with specific metadata
    let temp_path = root.path().join("snapshot3");
    state_machine
        .generate_snapshot_data(temp_path.clone(), LogId { index: 42, term: 5 })
        .await
        .unwrap();

    // Verify metadata
    let snapshot_db = init_sled_state_machine_db(&temp_path).unwrap();
    let metadata_tree = snapshot_db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();

    let last_included = SledStateMachine::load_snapshot_metadata(&metadata_tree)
        .unwrap()
        .unwrap()
        .last_included
        .unwrap();
    assert_eq!(last_included.index, 42u64);
    assert_eq!(last_included.term, 5u64);
}

/// # Case 4: Batch processing
#[tokio::test]
#[traced_test]
async fn test_generate_snapshot_data_case4() {
    let root = tempfile::tempdir().unwrap();
    let state_machine = setup_raft_components("/tmp/test_generate_snapshot_data_case4", false);

    // Insert 150 test entries
    let mut batch = Batch::default();
    for i in 1..=150 {
        let key = safe_kv(i);
        batch.insert(&key, &key);
    }
    state_machine.apply_batch(batch).unwrap();

    // Generate snapshot
    let temp_path = root.path().join("snapshot4");
    state_machine
        .generate_snapshot_data(
            temp_path.clone(),
            LogId {
                index: 150,
                term: 1,
            },
        )
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

#[cfg(test)]
mod apply_snapshot_from_file_tests {
    use std::path::Path;

    use super::*;
    use d_engine::file_io::create_valid_snapshot;

    /// # Case 1: Basic snapshot application
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case1() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_apply_snapshot_from_file_case1");
        let state_machine = setup_raft_components("/tmp/test_apply_snapshot_case1", false);

        // Generate test snapshot - UNCOMPRESSED DIRECTORY
        let snapshot_dir = case_path.join("snapshot_basic");
        let checksum = create_valid_snapshot(&snapshot_dir, |db| {
            let tree = db.open_tree(STATE_MACHINE_TREE).unwrap();
            tree.insert(safe_kv(456), b"test_value").unwrap();
            db.flush().unwrap();
        })
        .await;

        let last_included = LogId { index: 5, term: 2 };
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };

        // Apply snapshot
        state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await.unwrap();

        // Verify data and metadata
        assert_eq!(
            state_machine.get(&safe_kv(456)).unwrap(),
            Some(b"test_value".to_vec())
        );
        assert_eq!(state_machine.last_applied(), last_included);
        assert_eq!(state_machine.snapshot_metadata(), Some(metadata.clone()));
    }

    /// # Case 2: Overwrite existing state
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case2() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_apply_snapshot_from_file_case2");
        let state_machine = setup_raft_components(case_path.to_str().unwrap(), false);

        // Add initial data
        let mut batch = Batch::default();
        batch.insert(&safe_kv(232), b"old_value");
        state_machine.apply_batch(batch).unwrap();

        // Create UNCOMPRESSED snapshot directory with new data
        let snapshot_dir = case_path.join("snapshot_overwrite");
        let last_included = LogId { index: 10, term: 3 };

        let checksum = create_valid_snapshot(&snapshot_dir, |db| {
            let tree = db.open_tree(STATE_MACHINE_TREE).unwrap();
            tree.insert(safe_kv(5654), b"new_value").unwrap();
            db.flush().unwrap();
        })
        .await;

        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };

        // Apply snapshot
        state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await.unwrap();

        // Verify state overwrite
        assert_eq!(state_machine.get(&safe_kv(232)).unwrap(), None);
        assert_eq!(
            state_machine.get(&safe_kv(5654)).unwrap(),
            Some(b"new_value".to_vec())
        );
    }

    /// # Case 3: Metadata consistency check
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case3() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_apply_snapshot_from_file_case3");
        let state_machine = setup_raft_components(case_path.to_str().unwrap(), false);

        // Create COMPRESSED snapshot
        let temp_path = case_path.join("snapshot_metadata.tar.gz");
        let last_included = LogId { index: 15, term: 4 };

        let checksum = create_valid_snapshot(&temp_path, |db| {
            let metadata_tree = db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();
            let value = bincode::serialize(&SnapshotMetadata {
                last_included: Some(last_included),
                checksum: vec![],
            })
            .unwrap();
            metadata_tree.insert(LAST_SNAPSHOT_METADATA_KEY, value).unwrap();
            db.flush().unwrap();
        })
        .await;

        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };

        state_machine.apply_snapshot_from_file(&metadata, temp_path).await.unwrap();

        // Verify metadata propagation
        assert_eq!(state_machine.snapshot_metadata(), Some(metadata.clone()));
        assert_eq!(state_machine.last_applied(), last_included);
    }

    /// # Case 4: Concurrent snapshot protection
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case4() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_apply_snapshot_from_file_case4");
        let state_machine = Arc::new(setup_raft_components(case_path.to_str().unwrap(), false));

        let snapshot_dir = case_path.join("snapshot_basic");
        let last_included = LogId { index: 20, term: 5 };
        let checksum = create_valid_snapshot(&snapshot_dir, |db| {
            let tree = db.open_tree(STATE_MACHINE_TREE).unwrap();
            tree.insert(safe_kv(456), b"test_value").unwrap();
            db.flush().unwrap();
        })
        .await;

        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };

        // Start two concurrent apply operations
        let handle1 = tokio::spawn({
            let sm = state_machine.clone();
            let path = snapshot_dir.clone();
            let metadata = metadata.clone();
            async move { sm.apply_snapshot_from_file(&metadata, path).await }
        });

        let handle2 = tokio::spawn({
            let sm = state_machine.clone();
            let path = snapshot_dir.clone();
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
        let state_machine =
            setup_raft_components("/tmp/test_apply_snapshot_from_file_case5", false);

        let result = state_machine
            .apply_snapshot_from_file(
                &SnapshotMetadata {
                    last_included: Some(LogId { term: 1, index: 1 }),
                    checksum: [0_u8; 32].to_vec(),
                },
                PathBuf::from("/non/existent/path"),
            )
            .await;

        debug!(?result, "test_apply_snapshot_from_file_case5");
        assert!(result.is_err());
    }

    /// # Case 6: Checksum validation failure
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case6_checksum_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path =
            temp_dir.path().join("test_apply_snapshot_from_file_case6_checksum_failure");
        let state_machine = setup_raft_components(case_path.to_str().unwrap(), false);

        // Create valid snapshot directory
        let snapshot_dir = case_path.join("snapshot_checksum_failure");
        let checksum = create_valid_snapshot(&snapshot_dir, |_| {}).await;

        let last_included = LogId { index: 25, term: 6 };
        let mut metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };

        // Corrupt checksum
        metadata.checksum[0] = !metadata.checksum[0];

        // Should fail on checksum validation
        let result = state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await;
        assert!(result.is_err());
    }

    /// Creates an invalid snapshot directory (empty or corrupted)
    async fn create_invalid_snapshot(dir_path: &Path) {
        tokio::fs::create_dir_all(dir_path).await.unwrap();
        // Intentionally leave empty or create invalid files
    }

    /// # Case 7: Empty snapshot application
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case7_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_apply_snapshot_from_file_case7_empty");
        let state_machine = setup_raft_components(case_path.to_str().unwrap(), false);

        // Create empty snapshot directory
        let snapshot_dir = case_path.join("empty_snapshot");
        create_invalid_snapshot(&snapshot_dir).await;

        let last_included = LogId { index: 0, term: 0 };
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: vec![], // Special empty checksum
        };

        // Should handle empty snapshot
        let result = state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await;
        debug!(?result, "test_apply_snapshot_from_file_case7_empty");
        assert!(result.is_err());

        // Should reset to initial state
        assert_eq!(state_machine.get(&safe_kv(67)).unwrap(), None);
        assert_eq!(state_machine.last_applied(), last_included);
    }

    /// # Case 8: Invalid file format
    #[tokio::test]
    async fn test_apply_snapshot_from_file_case8_invalid_format() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_apply_snapshot_from_file_case8_invalid_format");
        let state_machine = setup_raft_components(case_path.to_str().unwrap(), false);

        // Create directory with invalid snapshot contents
        let invalid_dir = case_path.join("invalid_snapshot");
        tokio::fs::create_dir_all(&invalid_dir).await.unwrap();

        // Add some files that don't form a valid snapshot
        tokio::fs::write(invalid_dir.join("random.txt"), "Invalid snapshot content")
            .await
            .unwrap();

        let metadata = SnapshotMetadata {
            last_included: Some(LogId { index: 30, term: 8 }),
            checksum: vec![],
        };

        let result = state_machine.apply_snapshot_from_file(&metadata, invalid_dir).await;
        assert!(result.is_err());
    }
}

#[tokio::test]
#[traced_test]
async fn test_state_machine_drop() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_state_machine_drop");

    {
        let db = Arc::new(sled::open(case_path.clone()).unwrap());
        // Create real instance instead of mock
        let state_machine = Arc::new(SledStateMachine::new(1, db.clone()).expect("success"));

        // Insert test data
        let mut batch = Batch::default();
        batch.insert(&safe_kv(1), &safe_kv(1));
        batch.insert(&safe_kv(2), &safe_kv(2));
        state_machine.apply_batch(batch).expect("should succeed");

        // Explicitly drop to trigger flush
        drop(state_machine);
    }

    // Verify flush occurred by checking persistence
    let reloaded_db = sled::open(case_path).unwrap();
    assert!(!reloaded_db.open_tree(STATE_MACHINE_META_NAMESPACE).unwrap().is_empty());
}
