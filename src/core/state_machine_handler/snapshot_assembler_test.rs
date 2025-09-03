use std::sync::Arc;

use tempfile::tempdir;
use tokio::fs::read;
use tracing_test::traced_test;

use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
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
        assembler.write_chunk(i, data.clone()).await.unwrap();
    }

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: vec![],
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

    assembler.write_chunk(0, vec![1]).await.unwrap();
    let result = assembler.write_chunk(2, vec![3]).await;

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

    assembler.write_chunk(0, vec![1]).await.unwrap();
    let result = assembler.write_chunk(0, vec![1]).await;

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

    assembler.write_chunk(0, vec![]).await.unwrap();
    assembler.write_chunk(1, vec![1]).await.unwrap();

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: vec![],
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

    assembler.write_chunk(0, vec![0; 4096]).await.unwrap();
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
        checksum: vec![],
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
        assembler.write_chunk(i, vec![i as u8; 4096]).await.unwrap();
    }

    let snapshot_meta = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: vec![],
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
