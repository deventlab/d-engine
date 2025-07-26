use super::*;
use crate::{
    init_sled_storage_engine_db,
    proto::common::{Entry, EntryPayload, LogId},
    storage::{StorageEngine, RAFT_LOG_NAMESPACE},
    test_utils::enable_logger,
    Error, ProstError, SystemError,
};
use std::ops::RangeInclusive;
use tempfile::TempDir;

// Helper to create test entries
fn create_entries(range: RangeInclusive<u64>) -> Vec<Entry> {
    range
        .map(|i| Entry {
            index: i,
            term: i,
            payload: Some(EntryPayload::command(vec![i as u8; 1024])), // 1KB payload
        })
        .collect()
}

// Test setup helper
fn setup_storage(node_id: u32) -> (SledStorageEngine, TempDir) {
    let tempdir = tempfile::tempdir().unwrap();
    let db = init_sled_storage_engine_db(tempdir.path()).unwrap();
    let storage = SledStorageEngine::new(node_id, db).unwrap();
    (storage, tempdir)
}

#[test]
fn test_empty_storage() {
    let (storage, _dir) = setup_storage(1);

    assert_eq!(storage.last_index(), 0);
    assert!(storage.entry(1).unwrap().is_none());
    assert!(storage.get_entries_range(1..=5).unwrap().is_empty());
    assert_eq!(storage.len(), 0);
}

#[test]
fn test_single_entry_persistence() {
    let (storage, _dir) = setup_storage(1);
    let entries = create_entries(1..=1);

    // Persist and retrieve
    storage.persist_entries(entries.clone()).unwrap();
    assert_eq!(storage.last_index(), 1);
    assert_eq!(storage.entry(1).unwrap().unwrap(), entries[0]);
    assert_eq!(storage.get_entries_range(1..=1).unwrap(), entries);
    assert_eq!(storage.len(), 1);
}

#[test]
fn test_batch_persistence() {
    enable_logger();
    let (storage, _dir) = setup_storage(1);
    let entries = create_entries(1..=100);

    storage.persist_entries(entries.clone()).unwrap();

    // Verify all entries
    assert_eq!(storage.last_index(), 100);
    assert_eq!(storage.len(), 100);

    // Spot check random entries
    assert_eq!(storage.entry(1).unwrap().unwrap(), entries[0]);
    assert_eq!(storage.entry(50).unwrap().unwrap(), entries[49]);
    assert_eq!(storage.entry(100).unwrap().unwrap(), entries[99]);

    // Verify range query
    let range = storage.get_entries_range(25..=75).unwrap();
    assert_eq!(range.len(), 51);
    assert_eq!(range[0], entries[24]);
    assert_eq!(range[50], entries[74]);
}

#[test]
fn test_purge_logs() {
    let (storage, _dir) = setup_storage(1);
    storage.persist_entries(create_entries(1..=100)).unwrap();

    // Purge first 50 entries
    storage.purge_logs(LogId { index: 50, term: 50 }).unwrap();

    assert_eq!(storage.last_index(), 100); // Last index should remain
    assert_eq!(storage.len(), 50);
    assert!(storage.entry(1).unwrap().is_none());
    assert!(storage.entry(50).unwrap().is_none());
    assert!(storage.entry(51).unwrap().is_some());
}

#[test]
fn test_truncation() {
    let (storage, _dir) = setup_storage(1);
    storage.persist_entries(create_entries(1..=100)).unwrap();

    // Truncate from index 76 onward
    storage.truncate(76).unwrap();

    assert_eq!(storage.last_index(), 75);
    assert_eq!(storage.len(), 75);
    assert!(storage.entry(76).unwrap().is_none());
    assert!(storage.entry(100).unwrap().is_none());
    assert!(storage.entry(75).unwrap().is_some());
}

#[test]
fn test_reset_operation() {
    let (storage, _dir) = setup_storage(1);
    storage.persist_entries(create_entries(1..=50)).unwrap();

    storage.reset().unwrap();

    assert_eq!(storage.last_index(), 0);
    assert_eq!(storage.len(), 0);
    assert!(storage.entry(1).unwrap().is_none());
}

#[test]
fn test_edge_cases() {
    let (storage, _dir) = setup_storage(1);

    // Empty persistence
    storage.persist_entries(vec![]).unwrap();
    assert_eq!(storage.len(), 0);

    // Out-of-range access
    assert!(storage.get_entries_range(100..=200).unwrap().is_empty());

    // Invalid range (start > end)
    assert!(storage.get_entries_range(0..=1).unwrap().is_empty());
}

#[test]
fn test_concurrent_instances() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_sled_storage_engine_db(dir.path()).unwrap();

    // Create two independent storage engines
    let storage1 = SledStorageEngine::new(1, db.clone()).unwrap();
    let storage2 = SledStorageEngine::new(2, db).unwrap();

    storage1.persist_entries(create_entries(1..=50)).unwrap();
    storage2.persist_entries(create_entries(1..=100)).unwrap();

    // Verify isolation
    assert_eq!(storage1.last_index(), 50);
    assert_eq!(storage2.last_index(), 100);
    assert_eq!(storage1.len(), 50);
    assert_eq!(storage2.len(), 100);
}

#[test]
fn test_key_encoding_decoding() {
    // Test key conversion roundtrip
    for index in [0, 1, u64::MAX, 123456789] {
        let key = SledStorageEngine::index_to_key(index);
        assert_eq!(SledStorageEngine::key_to_index(&key), index);
    }

    // Test key ordering
    let key1 = SledStorageEngine::index_to_key(10);
    let key2 = SledStorageEngine::index_to_key(20);
    assert!(key1 < key2);
}

#[test]
fn test_corrupted_data_handling() {
    let node_id = 100;
    let tempdir = tempfile::tempdir().unwrap();
    let db = init_sled_storage_engine_db(tempdir.path()).unwrap();
    let tree_name = format!("raft_log_{}_{}", RAFT_LOG_NAMESPACE, node_id);
    let tree = db.open_tree(&tree_name).unwrap();
    // Insert invalid protobuf data
    tree.insert(SledStorageEngine::index_to_key(1), b"invalid_data")
        .unwrap();
    let storage = SledStorageEngine::new(node_id, db).unwrap();

    // Should return decode error
    let r = storage.entry(1);
    println!("{:?}", r);
    match r {
        Err(Error::System(SystemError::Prost(ProstError::Decode(_)))) => {} // Expected
        other => panic!("Unexpected result: {:?}", other),
    }
}
