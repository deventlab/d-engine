use d_engine_core::storage_engine_test::EntryAt;
use std::ops::RangeInclusive;
use std::sync::Arc;

use bytes::Bytes;
use d_engine_core::HardState;
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

    assert_eq!(storage.last_index(), 0);
    assert!(storage.entry_at(1).unwrap().is_none());
    assert!(storage.get_entries(1..=5).unwrap().is_empty());
}

#[tokio::test]
#[traced_test]
async fn test_single_entry_persistence() {
    let (storage, _dir) = setup_storage();
    let entries = create_entries(1..=1);

    // Persist and retrieve
    storage.persist_entries(entries.clone()).await.unwrap();
    assert_eq!(storage.last_index(), 1);
    assert_eq!(storage.entry_at(1).unwrap().unwrap(), entries[0]);
    assert_eq!(storage.get_entries(1..=1).unwrap(), entries);
}

#[tokio::test]
#[traced_test]
async fn test_batch_persistence() {
    let (storage, _dir) = setup_storage();
    let entries = create_entries(1..=100);

    storage.persist_entries(entries.clone()).await.unwrap();

    // Verify all entries
    assert_eq!(storage.last_index(), 100);

    // Spot check random entries
    assert_eq!(storage.entry_at(1).unwrap().unwrap(), entries[0]);
    assert_eq!(storage.entry_at(50).unwrap().unwrap(), entries[49]);
    assert_eq!(storage.entry_at(100).unwrap().unwrap(), entries[99]);

    // Verify range query
    let range = storage.get_entries(25..=75).unwrap();
    assert_eq!(range.len(), 51);
    assert_eq!(range[0], entries[24]);
    assert_eq!(range[50], entries[74]);
}

#[tokio::test]
#[traced_test]
async fn test_purge_logs() {
    let (storage, _dir) = setup_storage();
    storage.persist_entries(create_entries(1..=100)).await.unwrap();

    // Purge first 50 entries
    storage
        .purge(LogId {
            index: 50,
            term: 50,
        })
        .await
        .unwrap();

    assert_eq!(storage.last_index(), 100); // Last index should remain
    assert!(storage.entry_at(1).unwrap().is_none());
    assert!(storage.entry_at(50).unwrap().is_none());
    assert!(storage.entry_at(51).unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_truncation() {
    let (storage, _dir) = setup_storage();
    storage.persist_entries(create_entries(1..=100)).await.unwrap();

    // Truncate from index 76 onward
    storage.truncate(76).await.unwrap();

    assert_eq!(storage.last_index(), 75);
    assert!(storage.entry_at(76).unwrap().is_none());
    assert!(storage.entry_at(100).unwrap().is_none());
    assert!(storage.entry_at(75).unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_edge_cases() {
    let (storage, _dir) = setup_storage();

    // Empty persistence
    storage.persist_entries(vec![]).await.unwrap();
    assert_eq!(storage.last_index(), 0);

    // Out-of-range access
    assert!(storage.get_entries(100..=200).unwrap().is_empty());
}

#[tokio::test]
#[traced_test]
async fn test_concurrent_instances() {
    let tempdir1 = tempfile::tempdir().unwrap();
    let tempdir2 = tempfile::tempdir().unwrap();

    let storage1 = FileStorageEngine::new(tempdir1.path().to_path_buf()).unwrap();
    let storage2 = FileStorageEngine::new(tempdir2.path().to_path_buf()).unwrap();

    storage1.persist_entries(create_entries(1..=50)).await.unwrap();
    storage2.persist_entries(create_entries(1..=100)).await.unwrap();

    // Verify isolation
    assert_eq!(storage1.last_index(), 50);
    assert_eq!(storage2.last_index(), 100);
}

#[tokio::test]
#[traced_test]
async fn test_corrupted_data_handling() {
    // This test is less relevant for file storage as we don't directly manipulate the file bytes
    // But we can test error handling for invalid operations
    let (storage, _dir) = setup_storage();

    // Try to access non-existent entry
    assert!(storage.entry_at(999).unwrap().is_none());
}

#[test]
fn test_hard_state_persistence() {
    let (storage, dir) = setup_storage();
    let hard_state = HardState {
        current_term: 5,
        voted_for: Some(VotedFor {
            voted_for_id: 10,
            voted_for_term: 4,
            committed: false,
        }),
    };

    // Save and verify in-memory
    storage.save_hard_state(&hard_state).unwrap();
    let loaded = storage.load_hard_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 5);

    // Test durability after restart
    drop(storage); // Release resources

    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    let reloaded = storage2.load_hard_state().unwrap().unwrap();
    assert_eq!(reloaded.current_term, 5);
}

#[tokio::test]
#[traced_test]
async fn test_flush_persists_all_data() {
    let (storage, dir) = setup_storage();

    // Write log entries and the hard state
    storage.persist_entries(create_entries(1..=5)).await.unwrap();
    storage
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
    storage.flush().unwrap();
    drop(storage);

    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();

    // Verify the log entries and the hard state
    assert_eq!(storage2.last_index(), 5);
    assert_eq!(storage2.load_hard_state().unwrap().unwrap().current_term, 2);
}

#[test]
fn test_corrupted_meta_data() {
    let (storage, _dir) = setup_storage();

    // This test is challenging for file storage as we don't have direct access to modify the file
    // We'll verify that loading non-existent state returns None
    assert!(storage.load_hard_state().unwrap().is_none());
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
        storage.save_hard_state(&hs).unwrap();
        // No explicit flush - rely on Drop
    } // Storage dropped here

    // Reopen and verify
    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    assert_eq!(storage2.load_hard_state().unwrap().unwrap().current_term, 7);
}

// Additional tests specific to file storage

#[tokio::test]
#[traced_test]
async fn test_file_recovery_after_crash() {
    let (storage, dir) = setup_storage();

    // Write some data
    storage.persist_entries(create_entries(1..=10)).await.unwrap();

    // Simulate crash by dropping without explicit flush
    drop(storage);

    // Reopen and check if data was recovered
    let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();

    // File storage should recover all data since we flush on each write
    assert_eq!(storage2.last_index(), 10);
    assert!(storage2.entry_at(5).unwrap().is_some());
}

#[tokio::test]
#[traced_test]
async fn test_large_entry_persistence() {
    let (storage, _dir) = setup_storage();

    // Create a large entry (100KB)
    let large_entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(Bytes::from(vec![42; 1024 * 100]))),
    };

    storage.persist_entries(vec![large_entry.clone()]).await.unwrap();

    let retrieved = storage.entry_at(1).unwrap().unwrap();
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

        let result = storage_without_tempdir.persist_entries(vec![entry]).await;

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

        let result = storage_with_tempdir.persist_entries(vec![entry]).await;

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

    /// Truncate then persist survives a reopen with correct file boundaries.
    ///
    /// Verifies that after truncate(3) + persist [3,4 term=2], the file is physically
    /// truncated at the right offset so reopen does not read stale entries from the
    /// old tail. This is the key correctness invariant for the end_pos index (#350).
    #[tokio::test]
    #[traced_test]
    async fn test_truncate_then_persist_reads_correctly_after_reopen() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: write [1..5 term=1], truncate(3), write [3,4 term=2]
        {
            let storage = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();

            storage
                .persist_entries(
                    (1..=5)
                        .map(|i| Entry {
                            index: i,
                            term: 1,
                            payload: None,
                        })
                        .collect(),
                )
                .await
                .unwrap();

            storage.truncate(3).await.unwrap();

            storage
                .persist_entries(vec![
                    Entry {
                        index: 3,
                        term: 2,
                        payload: None,
                    },
                    Entry {
                        index: 4,
                        term: 2,
                        payload: None,
                    },
                ])
                .await
                .unwrap();

            storage.flush().unwrap();
        } // storage dropped here

        // Phase 2: reopen and verify — no stale entries from old tail
        let storage2 = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();

        assert_eq!(storage2.last_index(), 4);
        assert_eq!(storage2.entry_at(1).unwrap().unwrap().term, 1);
        assert_eq!(storage2.entry_at(2).unwrap().unwrap().term, 1);
        assert_eq!(storage2.entry_at(3).unwrap().unwrap().term, 2);
        assert_eq!(storage2.entry_at(4).unwrap().unwrap().term, 2);
        assert!(
            storage2.entry_at(5).unwrap().is_none(),
            "stale entry 5 must not appear after reopen"
        );
    }

    /// A term conflict replaces the tail with `truncate` then `persist_entries`:
    /// the prefix is untouched, the old tail is gone, the new entries are readable.
    #[tokio::test]
    #[traced_test]
    async fn test_truncate_then_persist_removes_stale_tail_and_stores_new_entries() {
        let (storage, _dir) = setup_storage();

        // Arrange: [1..5] all term=1
        storage
            .persist_entries(
                (1..=5)
                    .map(|i| Entry {
                        index: i,
                        term: 1,
                        payload: None,
                    })
                    .collect(),
            )
            .await
            .unwrap();

        // Act: conflict at index=3, new suffix [3(t2), 4(t2)] — entry 5 must vanish
        storage.truncate(3).await.unwrap();
        storage
            .persist_entries(vec![
                Entry {
                    index: 3,
                    term: 2,
                    payload: None,
                },
                Entry {
                    index: 4,
                    term: 2,
                    payload: None,
                },
            ])
            .await
            .unwrap();

        assert_eq!(storage.entry_at(1).unwrap().unwrap().term, 1);
        assert_eq!(storage.entry_at(2).unwrap().unwrap().term, 1);
        assert_eq!(storage.entry_at(3).unwrap().unwrap().term, 2);
        assert_eq!(storage.entry_at(4).unwrap().unwrap().term, 2);
        assert!(
            storage.entry_at(5).unwrap().is_none(),
            "entry 5 must be removed"
        );
        assert_eq!(storage.last_index(), 4);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_file_storage_performance_batch_write() {
        let (storage, _dir) = setup_storage();

        let start = std::time::Instant::now();
        let batch_size = 1000;

        // Write 1000 entries
        storage.persist_entries(create_entries(1..=batch_size)).await.unwrap();

        let duration = start.elapsed();
        let ops_per_sec = batch_size as f64 / duration.as_secs_f64();

        println!("Batch write performance:");
        println!("  - Entries: {batch_size}");
        println!("  - Duration: {duration:?}");
        println!("  - Ops/sec: {ops_per_sec:.2}");

        assert_eq!(storage.last_index(), batch_size);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_file_storage_performance_random_read() {
        let (storage, _dir) = setup_storage();

        let total_entries = 1000u64;
        storage.persist_entries(create_entries(1..=total_entries)).await.unwrap();

        let start = std::time::Instant::now();
        let read_count = 100;

        // Random reads
        for i in 1..=read_count {
            let index = (i * 7) % total_entries + 1; // Pseudo-random access
            let _ = storage.entry_at(index).unwrap();
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

        let total_entries = 10000u64;
        storage.persist_entries(create_entries(1..=total_entries)).await.unwrap();

        let start = std::time::Instant::now();

        // Range query
        let range = storage.get_entries(1000..=2000).unwrap();

        let duration = start.elapsed();

        println!("Range query performance:");
        println!("  - Range size: {}", range.len());
        println!("  - Duration: {duration:?}");

        assert_eq!(range.len(), 1001);
    }
}

/// The hard state file is replaced, never rewritten in place.
///
/// A vote must survive a crash. Rewriting `hard_state.bin` in place truncates it first, so a crash
/// midway leaves an empty or partial file; the node then starts without its term and vote and can
/// vote twice in one term. Writing a new file and renaming it over the old one leaves either the
/// old file or the new one. The inode changes only when the file was replaced, so it tells the two
/// apart without crashing anything.
#[cfg(unix)]
#[test]
fn test_save_hard_state_replaces_the_file_instead_of_rewriting_it() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let engine = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    let hard_state = |term| HardState {
        current_term: term,
        voted_for: None,
    };
    let path = dir.path().join("meta").join("hard_state.bin");

    engine.save_hard_state(&hard_state(1)).unwrap();
    let first = std::fs::metadata(&path).unwrap().ino();
    engine.save_hard_state(&hard_state(2)).unwrap();
    let second = std::fs::metadata(&path).unwrap().ino();

    assert_ne!(
        first, second,
        "the hard state file was rewritten in place: a crash midway loses the vote"
    );
}

/// Saving the hard state leaves nothing behind but `hard_state.bin`: the temporary file the new
/// content is written to must be gone, or every save would leak one.
#[test]
fn test_save_hard_state_leaves_no_temporary_file() {
    let dir = tempfile::tempdir().unwrap();
    let engine = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    engine
        .save_hard_state(&HardState {
            current_term: 3,
            voted_for: None,
        })
        .unwrap();

    let files: Vec<String> = std::fs::read_dir(dir.path().join("meta"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(files, vec!["hard_state.bin".to_string()]);
}

/// A failed hard-state save must not change what `load_hard_state` returns.
///
/// The node reads its term and vote back through `load_hard_state`. If a save reported an error
/// but the new vote still showed up there, the node would act on a vote that never reached disk,
/// and after a restart it would come back with the old one: it could then vote twice in one term.
#[test]
fn test_failed_save_hard_state_keeps_the_previous_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    let hard_state = |term| HardState {
        current_term: term,
        voted_for: None,
    };
    engine.save_hard_state(&hard_state(1)).unwrap();

    // A directory where the temporary file must go makes the next write fail.
    std::fs::create_dir(dir.path().join("meta").join("hard_state.tmp")).unwrap();
    assert!(
        engine.save_hard_state(&hard_state(2)).is_err(),
        "the save must fail when its temporary file cannot be created"
    );

    assert_eq!(
        engine.load_hard_state().unwrap().unwrap().current_term,
        1,
        "a save that failed must not be visible: the vote for term 2 never reached disk"
    );
}

/// `purge` records the boundary before it rewrites the log. A crash between the two leaves the
/// boundary on disk with the purged entries still in the log file. Reopening must drop those
/// entries and keep the boundary: they are already purged, and the term of the last purged entry
/// must survive the restart.
#[tokio::test]
async fn test_reopen_after_crash_between_boundary_and_log_rewrite_drops_purged_entries() {
    use prost::Message;

    let dir = tempfile::tempdir().unwrap();
    {
        let engine = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
        engine.persist_entries(create_entries(1..=10)).await.unwrap();
        engine.flush().unwrap();
    }

    // The state right after purge(5) wrote its boundary and before it rewrote the log.
    let boundary = LogId { index: 5, term: 1 };
    std::fs::write(
        dir.path().join("meta").join("purge_boundary.bin"),
        boundary.encode_to_vec(),
    )
    .unwrap();

    let engine = FileStorageEngine::new(dir.path().to_path_buf()).unwrap();
    assert_eq!(engine.load_purge_boundary().unwrap(), Some(boundary));
    assert!(
        engine.entry_at(5).unwrap().is_none(),
        "an entry at the purge boundary is already purged"
    );
    assert!(
        engine.entry_at(6).unwrap().is_some(),
        "entries after the boundary stay"
    );
    assert_eq!(engine.last_index(), 10);
}
