use std::ops::RangeInclusive;
use std::sync::Arc;

use tempfile::TempDir;
use tracing_test::traced_test;

use super::*;
use crate::init_sled_log_tree_and_meta_tree;
use crate::init_sled_storage_engine_db;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::proto::election::VotedFor;
use crate::Error;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::ProstError;
use crate::StorageEngine;
use crate::SystemError;

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
fn setup_storage(_node_id: u32) -> (Arc<SledLogStore>, Arc<SledMetaStore>, TempDir) {
    let tempdir = tempfile::tempdir().unwrap();
    let (log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(tempdir.path(), 1).unwrap();
    let storage = SledStorageEngine::new(log_tree, meta_tree);
    (storage.log_store(), storage.meta_store(), tempdir)
}

#[tokio::test]
#[traced_test]
async fn test_empty_storage() {
    let (log_store, _meta_store, _dir) = setup_storage(1);

    assert_eq!(log_store.last_index(), 0);
    assert!(log_store.entry(1).await.unwrap().is_none());
    assert!(log_store.get_entries(1..=5).unwrap().is_empty());
    assert_eq!(log_store.len(), 0);
}

#[tokio::test]
#[traced_test]
async fn test_single_entry_persistence() {
    let (log_store, _meta_store, _dir) = setup_storage(1);
    let entries = create_entries(1..=1);

    // Persist and retrieve
    log_store.persist_entries(entries.clone()).await.unwrap();
    assert_eq!(log_store.last_index(), 1);
    assert_eq!(log_store.entry(1).await.unwrap().unwrap(), entries[0]);
    assert_eq!(log_store.get_entries(1..=1).unwrap(), entries);
    assert_eq!(log_store.len(), 1);
}

#[tokio::test]
#[traced_test]
async fn test_batch_persistence() {
    let (log_store, _meta_store, _dir) = setup_storage(1);
    let entries = create_entries(1..=100);

    log_store.persist_entries(entries.clone()).await.unwrap();

    // Verify all entries
    assert_eq!(log_store.last_index(), 100);
    assert_eq!(log_store.len(), 100);

    // Spot check random entries
    assert_eq!(log_store.entry(1).await.unwrap().unwrap(), entries[0]);
    assert_eq!(log_store.entry(50).await.unwrap().unwrap(), entries[49]);
    assert_eq!(log_store.entry(100).await.unwrap().unwrap(), entries[99]);

    // Verify range query
    let range = log_store.get_entries(25..=75).unwrap();
    assert_eq!(range.len(), 51);
    assert_eq!(range[0], entries[24]);
    assert_eq!(range[50], entries[74]);
}

#[tokio::test]
#[traced_test]
async fn test_purge_logs() {
    let (log_store, _meta_store, _dir) = setup_storage(1);
    log_store.persist_entries(create_entries(1..=100)).await.unwrap();

    // Purge first 50 entries
    log_store
        .purge(LogId {
            index: 50,
            term: 50,
        })
        .await
        .unwrap();

    assert_eq!(log_store.last_index(), 100); // Last index should remain
    assert_eq!(log_store.len(), 50);
    assert!(log_store.entry(1).await.unwrap().is_none());
    assert!(log_store.entry(50).await.unwrap().is_none());
    assert!(log_store.entry(51).await.unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_truncation() {
    let (log_store, _meta_store, _dir) = setup_storage(1);
    log_store.persist_entries(create_entries(1..=100)).await.unwrap();

    // Truncate from index 76 onward
    log_store.truncate(76).await.unwrap();

    assert_eq!(log_store.last_index(), 75);
    assert_eq!(log_store.len(), 75);
    assert!(log_store.entry(76).await.unwrap().is_none());
    assert!(log_store.entry(100).await.unwrap().is_none());
    assert!(log_store.entry(75).await.unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_reset_operation() {
    let (log_store, _meta_store, _dir) = setup_storage(1);
    log_store.persist_entries(create_entries(1..=50)).await.unwrap();

    log_store.reset().await.unwrap();

    assert_eq!(log_store.last_index(), 0);
    assert_eq!(log_store.len(), 0);
    assert!(log_store.entry(1).await.unwrap().is_none());
}

#[tokio::test]
#[traced_test]
async fn test_edge_cases() {
    let (log_store, _meta_store, _dir) = setup_storage(1);

    // Empty persistence
    log_store.persist_entries(vec![]).await.unwrap();
    assert_eq!(log_store.len(), 0);

    // Out-of-range access
    assert!(log_store.get_entries(100..=200).unwrap().is_empty());

    // Invalid range (start > end)
    assert!(log_store.get_entries(0..=1).unwrap().is_empty());
}

#[tokio::test]
#[traced_test]
async fn test_concurrent_instances() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = init_sled_storage_engine_db(tempdir.path()).unwrap();
    let node_id1 = 1;
    let node_id2 = 2;
    let log_tree1 = db.open_tree(format!("raft_log_{node_id1}",)).unwrap();
    let meta_tree1 = db.open_tree(format!("raft_meta_{node_id1}")).unwrap();
    let log_tree2 = db.open_tree(format!("raft_log_{node_id2}")).unwrap();
    let meta_tree2 = db.open_tree(format!("raft_meta_{node_id2}")).unwrap();

    // Create two independent storage engines
    let storage1 = SledStorageEngine::new(log_tree1, meta_tree1);
    let storage2 = SledStorageEngine::new(log_tree2, meta_tree2);

    storage1.log_store().persist_entries(create_entries(1..=50)).await.unwrap();
    storage2.log_store().persist_entries(create_entries(1..=100)).await.unwrap();

    // Verify isolation
    assert_eq!(storage1.log_store().last_index(), 50);
    assert_eq!(storage2.log_store().last_index(), 100);
    assert_eq!(storage1.log_store().len(), 50);
    assert_eq!(storage2.log_store().len(), 100);
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

#[tokio::test]
#[traced_test]
async fn test_corrupted_data_handling() {
    let node_id = 100;
    let tempdir = tempfile::tempdir().unwrap();
    let (log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(tempdir.path(), node_id).unwrap();
    // Insert invalid protobuf data
    log_tree.insert(SledStorageEngine::index_to_key(1), b"invalid_data").unwrap();
    let storage = SledStorageEngine::new(log_tree, meta_tree);

    // Should return decode error
    let r = storage.log_store().entry(1).await;
    println!("{r:?}");
    match r {
        Err(Error::System(SystemError::Prost(ProstError::Decode(_)))) => {} // Expected
        other => panic!("Unexpected result: {other:?}"),
    }
}

#[test]
fn test_hard_state_persistence() {
    let (log_store, meta_store, dir) = setup_storage(1);
    let hard_state = HardState {
        current_term: 5,
        voted_for: Some(VotedFor {
            voted_for_id: 10,
            voted_for_term: 4,
        }),
    };

    // Save and verify in-memory
    meta_store.save_hard_state(&hard_state).unwrap();
    let loaded = meta_store.load_hard_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 5);

    // Test durability after restart
    drop(meta_store); // Release DB lock
    drop(log_store);

    let (log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(dir.path(), 1).unwrap();
    let storage2 = SledStorageEngine::new(log_tree, meta_tree);
    let reloaded = storage2.meta_store().load_hard_state().unwrap().unwrap();
    assert_eq!(reloaded.current_term, 5);
}

#[tokio::test]
#[traced_test]
async fn test_reset_preserves_meta() {
    let (log_store, meta_store, _dir) = setup_storage(1);
    let hard_state = HardState {
        current_term: 3,
        voted_for: Some(VotedFor {
            voted_for_id: 5,
            voted_for_term: 4,
        }),
    };
    meta_store.save_hard_state(&hard_state).unwrap();

    // Reset should clear logs but keep meta
    log_store.reset().await.unwrap();

    let loaded = meta_store.load_hard_state().unwrap();
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().current_term, 3);
}

#[tokio::test]
#[traced_test]
async fn test_flush_persists_all_data() {
    let (log_store, meta_store, dir) = setup_storage(1);

    // Write to both trees
    log_store.persist_entries(create_entries(1..=5)).await.unwrap();
    meta_store
        .save_hard_state(&HardState {
            current_term: 2,
            voted_for: Some(VotedFor {
                voted_for_id: 1,
                voted_for_term: 2,
            }),
        })
        .unwrap();

    // Manually flush and reopen
    log_store.flush().unwrap();
    drop(log_store);
    drop(meta_store);

    let (log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(dir.path(), 1).unwrap();
    let storage2 = SledStorageEngine::new(log_tree, meta_tree);

    // Verify both trees
    assert_eq!(storage2.log_store().last_index(), 5);
    assert_eq!(
        storage2.meta_store().load_hard_state().unwrap().unwrap().current_term,
        2
    );
}

#[test]
fn test_corrupted_meta_data() {
    let (_log_store, meta_store, _dir) = setup_storage(1);

    // Insert invalid data directly
    meta_store.insert(HARD_STATE_KEY, b"invalid_bincode_data").unwrap();

    assert!(meta_store.load_hard_state().unwrap().is_none());
}

#[test]
fn test_drop_impl_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let hs = HardState {
        current_term: 7,
        voted_for: Some(VotedFor {
            voted_for_id: 2,
            voted_for_term: 7,
        }),
    };

    {
        let (log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(dir.path(), 1).unwrap();
        let storage = SledStorageEngine::new(log_tree, meta_tree);
        storage.meta_store().save_hard_state(&hs).unwrap();
        // No explicit flush - rely on Drop
    } // Storage dropped here

    // Reopen and verify
    let (log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(dir.path(), 1).unwrap();
    let storage2 = SledStorageEngine::new(log_tree, meta_tree);
    assert_eq!(
        storage2.meta_store().load_hard_state().unwrap().unwrap().current_term,
        7
    );
}
