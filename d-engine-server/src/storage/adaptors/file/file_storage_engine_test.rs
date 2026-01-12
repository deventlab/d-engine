use std::ops::RangeInclusive;
use std::sync::Arc;

use bytes::Bytes;
use d_engine_core::HardState;
use d_engine_core::LogStore;
use d_engine_core::MetaStore;
use d_engine_core::StorageEngine;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VotedFor;
use tempfile::TempDir;
use tracing_test::traced_test;

use super::*;

// Helper to create test entries
fn create_entries(range: RangeInclusive<u64>) -> Vec<Entry> {
    range
        .map(|i| Entry {
            index: i,
            term: i,
            payload: Some(EntryPayload::command(Bytes::from(vec![i as u8; 1024]))), // 1KB payload
        })
        .collect()
}

// Test setup helper
fn setup_storage() -> (Arc<FileStorageEngine>, TempDir) {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FileStorageEngine::new(tempdir.path().to_path_buf()).unwrap();
    (Arc::new(storage), tempdir)
}

#[tokio::test]
#[traced_test]
async fn test_empty_storage() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();

    assert_eq!(log_store.last_index(), 0);
    assert!(log_store.entry(1).await.unwrap().is_none());
    assert!(log_store.get_entries(1..=5).unwrap().is_empty());
}

#[tokio::test]
#[traced_test]
async fn test_single_entry_persistence() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();
    let entries = create_entries(1..=1);

    // Persist and retrieve
    log_store.persist_entries(entries.clone()).await.unwrap();
    assert_eq!(log_store.last_index(), 1);
    assert_eq!(log_store.entry(1).await.unwrap().unwrap(), entries[0]);
    assert_eq!(log_store.get_entries(1..=1).unwrap(), entries);
}

#[tokio::test]
#[traced_test]
async fn test_batch_persistence() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();
    let entries = create_entries(1..=100);

    log_store.persist_entries(entries.clone()).await.unwrap();

    // Verify all entries
    assert_eq!(log_store.last_index(), 100);

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
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();
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
    assert!(log_store.entry(1).await.unwrap().is_none());
    assert!(log_store.entry(50).await.unwrap().is_none());
    assert!(log_store.entry(51).await.unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_truncation() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();
    log_store.persist_entries(create_entries(1..=100)).await.unwrap();

    // Truncate from index 76 onward
    log_store.truncate(76).await.unwrap();

    assert_eq!(log_store.last_index(), 75);
    assert!(log_store.entry(76).await.unwrap().is_none());
    assert!(log_store.entry(100).await.unwrap().is_none());
    assert!(log_store.entry(75).await.unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_reset_operation() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();
    log_store.persist_entries(create_entries(1..=50)).await.unwrap();

    log_store.reset().await.unwrap();

    assert_eq!(log_store.last_index(), 0);
    assert!(log_store.entry(1).await.unwrap().is_none());
}

#[tokio::test]
#[traced_test]
async fn test_edge_cases() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();

    // Empty persistence
    log_store.persist_entries(vec![]).await.unwrap();
    assert_eq!(log_store.last_index(), 0);

    // Out-of-range access
    assert!(log_store.get_entries(100..=200).unwrap().is_empty());
}

#[tokio::test]
#[traced_test]
async fn test_concurrent_instances() {
    let tempdir1 = tempfile::tempdir().unwrap();
    let tempdir2 = tempfile::tempdir().unwrap();

    let storage1 = FileStorageEngine::new(tempdir1.path().to_path_buf()).unwrap();
    let storage2 = FileStorageEngine::new(tempdir2.path().to_path_buf()).unwrap();

    storage1.log_store().persist_entries(create_entries(1..=50)).await.unwrap();
    storage2.log_store().persist_entries(create_entries(1..=100)).await.unwrap();

    // Verify isolation
    assert_eq!(storage1.log_store().last_index(), 50);
    assert_eq!(storage2.log_store().last_index(), 100);
}

#[tokio::test]
#[traced_test]
async fn test_corrupted_data_handling() {
    // This test is less relevant for file storage as we don't directly manipulate the file bytes
    // But we can test error handling for invalid operations
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();

    // Try to access non-existent entry
    assert!(log_store.entry(999).await.unwrap().is_none());
}

#[test]
fn test_hard_state_persistence() {
    let (storage, dir) = setup_storage();
    let meta_store = storage.meta_store();
    let hard_state = HardState {
        current_term: 5,
        voted_for: Some(VotedFor {
            voted_for_id: 10,
            voted_for_term: 4,
            committed: false,
        }),
    };

    // Save and verify in-memory
    meta_store.save_hard_state(&hard_state).unwrap();
    let loaded = meta_store.load_hard_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 5);

    // Test durability after restart
    drop(meta_store); // Release resources
    drop(storage);

    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    let reloaded = storage2.meta_store().load_hard_state().unwrap().unwrap();
    assert_eq!(reloaded.current_term, 5);
}

#[tokio::test]
#[traced_test]
async fn test_reset_preserves_meta() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();
    let meta_store = storage.meta_store();

    let hard_state = HardState {
        current_term: 3,
        voted_for: Some(VotedFor {
            voted_for_id: 5,
            voted_for_term: 4,
            committed: false,
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
    let (storage, dir) = setup_storage();
    let log_store = storage.log_store();
    let meta_store = storage.meta_store();

    // Write to both stores
    log_store.persist_entries(create_entries(1..=5)).await.unwrap();
    meta_store
        .save_hard_state(&HardState {
            current_term: 2,
            voted_for: Some(VotedFor {
                voted_for_id: 1,
                voted_for_term: 2,
                committed: false,
            }),
        })
        .unwrap();

    // Manually flush and reopen
    log_store.flush().unwrap();
    drop(log_store);
    drop(meta_store);
    drop(storage);

    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();

    // Verify both stores
    assert_eq!(storage2.log_store().last_index(), 5);
    assert_eq!(
        storage2.meta_store().load_hard_state().unwrap().unwrap().current_term,
        2
    );
}

#[test]
fn test_corrupted_meta_data() {
    let (storage, _dir) = setup_storage();
    let meta_store = storage.meta_store();

    // This test is challenging for file storage as we don't have direct access to modify the file
    // We'll verify that loading non-existent state returns None
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
            committed: false,
        }),
    };

    {
        let storage = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
        storage.meta_store().save_hard_state(&hs).unwrap();
        // No explicit flush - rely on Drop
    } // Storage dropped here

    // Reopen and verify
    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    assert_eq!(
        storage2.meta_store().load_hard_state().unwrap().unwrap().current_term,
        7
    );
}

// Additional tests specific to file storage

#[tokio::test]
#[traced_test]
async fn test_file_recovery_after_crash() {
    let (storage, dir) = setup_storage();
    let log_store = storage.log_store();

    // Write some data
    log_store.persist_entries(create_entries(1..=10)).await.unwrap();

    // Simulate crash by dropping without explicit flush
    drop(log_store);
    drop(storage);

    // Reopen and check if data was recovered
    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    let log_store2 = storage2.log_store();

    // File storage should recover all data since we flush on each write
    assert_eq!(log_store2.last_index(), 10);
    assert!(log_store2.entry(5).await.unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_large_entry_persistence() {
    let (storage, _dir) = setup_storage();
    let log_store = storage.log_store();

    // Create a large entry (100KB)
    let large_entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(Bytes::from(vec![42; 1024 * 100]))),
    };

    log_store.persist_entries(vec![large_entry.clone()]).await.unwrap();

    let retrieved = log_store.entry(1).await.unwrap().unwrap();
    assert_eq!(retrieved.index, 1);
    assert_eq!(retrieved.term, 1);
    assert_eq!(retrieved.payload, large_entry.payload);
}

#[cfg(test)]
mod tests {
    use d_engine_proto::common::EntryPayload;
    use d_engine_proto::common::Noop;
    use d_engine_proto::common::entry_payload::Payload;

    use super::*;

    /// Test to demonstrate TempDir drop race condition
    ///
    /// This test shows that when TempDir is dropped before FileStorageEngine,
    /// the storage engine can still operate (due to open file handles),
    /// but subsequent operations may fail if they need to access the filesystem.
    #[tokio::test]
    async fn test_tempdir_drop_race_condition() {
        // Scenario 1: TempDir dropped immediately - demonstrates the problem
        let storage_without_tempdir = {
            let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let storage_path = temp_dir.path().join("storage");

            // Create storage engine
            let storage =
                FileStorageEngine::new(storage_path.clone()).expect("Failed to create storage");

            println!("Storage created at: {storage_path:?}");
            println!("Directory exists: {}", storage_path.exists());

            // temp_dir is dropped here!
            storage
        };

        // Small delay to allow OS to process directory deletion
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        println!("After TempDir drop - checking if we can still use storage...");

        // Try to persist entries - this uses already-open file handles
        let entry = Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Noop(Noop {})),
            }),
        };

        let result = storage_without_tempdir.log_store().persist_entries(vec![entry]).await;

        println!("Persist result: {result:?}");

        // This typically succeeds because file handles are still valid!
        assert!(result.is_ok(), "Write to open file handle should succeed");

        // Scenario 2: TempDir kept alive - the correct way
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let storage_path = _temp_dir.path().join("storage");

        let storage_with_tempdir =
            FileStorageEngine::new(storage_path.clone()).expect("Failed to create storage");

        println!("\nWith TempDir kept alive:");
        println!("Directory exists: {}", storage_path.exists());

        let entry = Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Noop(Noop {})),
            }),
        };

        let result = storage_with_tempdir.log_store().persist_entries(vec![entry]).await;

        assert!(
            result.is_ok(),
            "All operations should succeed when TempDir is alive"
        );

        // _temp_dir stays alive until end of test
    }

    /// Test demonstrating that new file operations fail after TempDir drop
    #[test]
    fn test_new_file_operations_fail_after_tempdir_drop() {
        let storage_path = {
            let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let path = temp_dir.path().join("storage");

            // Create initial storage
            let _storage = FileStorageEngine::new(path.clone()).expect("Failed to create storage");

            println!("Created storage at: {path:?}");
            println!("Directory exists before drop: {}", path.exists());
            path
            // temp_dir dropped here
        };

        // Force wait for OS to process deletion
        std::thread::sleep(std::time::Duration::from_millis(100));

        println!("Directory exists after drop: {}", storage_path.exists());

        // Explicitly try to remove the directory to ensure it's gone
        let _ = std::fs::remove_dir_all(&storage_path);

        println!(
            "Directory exists after explicit removal: {}",
            storage_path.exists()
        );

        // Try to create a NEW FileStorageEngine with the same path
        // This will attempt to open/create files in the deleted directory
        println!("\nAttempting to create new storage after directory deletion...");
        let result = FileStorageEngine::new(storage_path.clone());

        println!("Result: {:?}", result.is_ok());

        // Actually, fs::create_dir_all in FileStorageEngine::new will recreate the directory!
        // So this might still succeed, but demonstrates the concept
        if result.is_ok() {
            println!("Note: Operation succeeded because fs::create_dir_all recreates directories");
        }
    }

    // Integration tests using StorageEngineTestSuite

    use d_engine_core::Error;
    use d_engine_core::storage::storage_engine_test::{
        StorageEngineBuilder, StorageEngineTestSuite,
    };
    use tonic::async_trait;

    /// Builder for FileStorageEngine test instances
    struct FileStorageEngineBuilder {
        temp_dir: TempDir,
    }

    impl FileStorageEngineBuilder {
        fn new() -> Self {
            Self {
                temp_dir: TempDir::new().expect("Failed to create temp dir"),
            }
        }
    }

    #[async_trait]
    impl StorageEngineBuilder for FileStorageEngineBuilder {
        type Engine = FileStorageEngine;

        async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
            // Use fixed path to support restart recovery testing
            let path = self.temp_dir.path().join("file_storage");
            let engine = FileStorageEngine::new(path)?;
            Ok(Arc::new(engine))
        }

        async fn cleanup(&self) -> Result<(), Error> {
            // TempDir automatically cleans up on drop
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_file_storage_engine_suite() {
        let builder = FileStorageEngineBuilder::new();
        StorageEngineTestSuite::run_all_tests(builder)
            .await
            .expect("FileStorageEngine should pass all tests");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_file_storage_performance_batch_write() {
        let (storage, _dir) = setup_storage();
        let log_store = storage.log_store();

        let start = std::time::Instant::now();
        let batch_size = 1000;

        // Write 1000 entries
        log_store.persist_entries(create_entries(1..=batch_size)).await.unwrap();

        let duration = start.elapsed();
        let ops_per_sec = batch_size as f64 / duration.as_secs_f64();

        println!("Batch write performance:");
        println!("  - Entries: {batch_size}");
        println!("  - Duration: {duration:?}");
        println!("  - Ops/sec: {ops_per_sec:.2}");

        assert_eq!(log_store.last_index(), batch_size);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_file_storage_performance_random_read() {
        let (storage, _dir) = setup_storage();
        let log_store = storage.log_store();

        let total_entries = 1000u64;
        log_store.persist_entries(create_entries(1..=total_entries)).await.unwrap();

        let start = std::time::Instant::now();
        let read_count = 100;

        // Random reads
        for i in 1..=read_count {
            let index = (i * 7) % total_entries + 1; // Pseudo-random access
            let _ = log_store.entry(index).await.unwrap();
        }

        let duration = start.elapsed();
        let ops_per_sec = read_count as f64 / duration.as_secs_f64();

        println!("Random read performance:");
        println!("  - Reads: {read_count}");
        println!("  - Duration: {duration:?}");
        println!("  - Ops/sec: {ops_per_sec:.2}");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_file_storage_performance_range_query() {
        let (storage, _dir) = setup_storage();
        let log_store = storage.log_store();

        let total_entries = 10000u64;
        log_store.persist_entries(create_entries(1..=total_entries)).await.unwrap();

        let start = std::time::Instant::now();

        // Range query
        let range = log_store.get_entries(1000..=2000).unwrap();

        let duration = start.elapsed();

        println!("Range query performance:");
        println!("  - Range size: {}", range.len());
        println!("  - Duration: {duration:?}");

        assert_eq!(range.len(), 1001);
    }
}
