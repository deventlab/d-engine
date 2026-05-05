use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use tempfile::tempdir;
use tokio::fs::read;
use tracing_test::traced_test;

use crate::SnapshotAssembler;
use crate::SnapshotPathManager;

#[tokio::test]
#[traced_test]
async fn sequential_chunks_assembly() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    // Write 5 sequential chunks
    for i in 0..5 {
        let data = vec![i as u8; 1024];
        assembler.write_chunk(i, Bytes::from(data.clone())).await.unwrap();
    }

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: Bytes::new(),
    };
    let final_path = assembler.finalize(&snapshot_meta).await.unwrap();
    let content = read(&final_path).await.unwrap();

    assert_eq!(content.len(), 5 * 1024);
    for (i, chunk) in content.chunks(1024).enumerate() {
        assert_eq!(chunk, vec![i as u8; 1024].as_slice());
    }
}

#[tokio::test]
#[traced_test]
async fn reject_out_of_order_chunks() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    assembler.write_chunk(0, Bytes::from(vec![1])).await.unwrap();
    let result = assembler.write_chunk(2, Bytes::from(vec![3])).await;

    assert!(result.is_err());
}

#[tokio::test]
#[traced_test]
async fn detect_duplicate_chunk_index() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    assembler.write_chunk(0, Bytes::from(vec![1])).await.unwrap();
    let result = assembler.write_chunk(0, Bytes::from(vec![1])).await;

    assert!(result.is_err());
}

#[tokio::test]
#[traced_test]
async fn handle_empty_chunk() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    assembler.write_chunk(0, Bytes::new()).await.unwrap();
    assembler.write_chunk(1, Bytes::from(vec![1])).await.unwrap();

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: Bytes::new(),
    };
    let final_path = assembler.finalize(&snapshot_meta).await.unwrap();
    let content = read(final_path).await.unwrap();
    assert_eq!(content, vec![1]);
}

#[tokio::test]
#[traced_test]
async fn verify_flush_operation() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    assembler.write_chunk(0, Bytes::from(vec![0; 4096])).await.unwrap();
    assembler.flush_to_disk().await.unwrap();

    // Verify data persists before finalize
    let mid_content = read(&assembler.temp_path).await.unwrap();
    assert_eq!(mid_content.len(), 4096);
}

#[tokio::test]
#[traced_test]
async fn return_correct_final_path() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: Bytes::new(),
    };
    let final_path = assembler.finalize(&snapshot_meta).await.unwrap();
    assert!(final_path.ends_with("snapshot-1-1.tar.gz"));
    assert!(final_path.exists());
}

#[tokio::test]
#[traced_test]
async fn handle_large_data_volume() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();

    // Write 1000 chunks of 4KB each (4MB total)
    for i in 0..1000 {
        assembler.write_chunk(i, Bytes::from(vec![i as u8; 4096])).await.unwrap();
    }

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: Bytes::new(),
    };
    let final_path = assembler.finalize(&snapshot_meta).await.unwrap();
    let metadata = tokio::fs::metadata(final_path).await.unwrap();
    assert_eq!(metadata.len(), 1000 * 4096);
}

#[tokio::test]
#[traced_test]
async fn handle_existing_directory_conflict() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));

    // Create a directory with the same name as the temp file
    let temp_file_path = path_mgr.temp_assembly_file();
    tokio::fs::create_dir_all(&temp_file_path).await.unwrap();

    // Should handle directory conflict and create assembler
    let assembler = SnapshotAssembler::new(path_mgr.clone()).await;
    assert!(assembler.is_ok());

    // Verify backup directory was created
    let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
    let mut count = 0;

    // Proper async way to count entries
    while let Some(_entry) = entries.next_entry().await.unwrap() {
        count += 1;
    }

    assert!(count > 0, "Backup directory should be created");
}

/// Simulates the production retry scenario:
/// 1. First transfer writes partial chunks then fails (drop without finalize)
/// 2. Stale temp file remains on disk
/// 3. Leader retries: new SnapshotAssembler on the same path_mgr
/// 4. Final snapshot must contain ONLY the new transfer data, not old+new appended
#[tokio::test]
#[traced_test]
async fn test_assembler_on_stale_temp_file_does_not_append_old_data() {
    let dir = tempdir().unwrap();
    let path_mgr = Arc::new(SnapshotPathManager::new(
        dir.path().to_path_buf(),
        "snapshot-".to_string(),
    ));

    // First transfer: write 3 chunks then drop (simulates timeout / leader change)
    {
        let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();
        for i in 0..3u32 {
            assembler.write_chunk(i, Bytes::from(vec![0xAAu8; 1024])).await.unwrap();
        }
        // Drop without finalize — stale temp file stays on disk with 3 * 1024 bytes
    }

    let temp_path = path_mgr.temp_assembly_file();
    assert!(
        temp_path.exists(),
        "stale temp file must exist after failed transfer"
    );
    let stale_size = tokio::fs::metadata(&temp_path).await.unwrap().len();
    assert_eq!(
        stale_size,
        3 * 1024,
        "stale file should contain partial data from first transfer"
    );

    // Leader retry: new assembler on the same path_mgr, fresh complete transfer
    let mut assembler = SnapshotAssembler::new(path_mgr.clone()).await.unwrap();
    for i in 0..3u32 {
        assembler.write_chunk(i, Bytes::from(vec![(i + 1) as u8; 1024])).await.unwrap();
    }
    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: Bytes::new(),
    };
    let final_path = assembler.finalize(&snapshot_meta).await.unwrap();
    let content = tokio::fs::read(&final_path).await.unwrap();

    // Must be exactly 3 * 1024 bytes — stale 3072 bytes must NOT be prepended
    assert_eq!(
        content.len(),
        3 * 1024,
        "final snapshot must contain only new transfer data, not stale+new appended ({} bytes)",
        content.len()
    );
    for (i, chunk) in content.chunks(1024).enumerate() {
        assert_eq!(
            chunk,
            vec![(i + 1) as u8; 1024].as_slice(),
            "chunk {} must contain new transfer data, not stale bytes",
            i
        );
    }
}
