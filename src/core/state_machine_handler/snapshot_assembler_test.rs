#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::fs::read;

    use crate::SnapshotAssembler;

    #[tokio::test]
    async fn sequential_chunks_assembly() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        // Write 5 sequential chunks
        for i in 0..5 {
            let data = vec![i as u8; 1024];
            assembler.write_chunk(i, data.clone()).await.unwrap();
        }

        let final_path = assembler.finalize().await.unwrap();
        let content = read(&final_path).await.unwrap();

        assert_eq!(content.len(), 5 * 1024);
        for (i, chunk) in content.chunks(1024).enumerate() {
            assert_eq!(chunk, vec![i as u8; 1024].as_slice());
        }
    }

    #[tokio::test]
    async fn reject_out_of_order_chunks() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        assembler.write_chunk(0, vec![1]).await.unwrap();
        let result = assembler.write_chunk(2, vec![3]).await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Snapshot error: Out-of-order chunk. Expected 1, got 2"
        );
    }

    #[tokio::test]
    async fn detect_duplicate_chunk_index() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        assembler.write_chunk(0, vec![1]).await.unwrap();
        let result = assembler.write_chunk(0, vec![1]).await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Snapshot error: Out-of-order chunk. Expected 1, got 0"
        );
    }

    #[tokio::test]
    async fn handle_empty_chunk() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        assembler.write_chunk(0, vec![]).await.unwrap();
        assembler.write_chunk(1, vec![1]).await.unwrap();

        let final_path = assembler.finalize().await.unwrap();
        let content = read(final_path).await.unwrap();
        assert_eq!(content, vec![1]);
    }

    #[tokio::test]
    async fn verify_flush_operation() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        assembler.write_chunk(0, vec![0; 4096]).await.unwrap();
        assembler.flush_to_disk().await.unwrap();

        // Verify data persists before finalize
        let mid_content = read(&assembler.temp_path).await.unwrap();
        assert_eq!(mid_content.len(), 4096);
    }

    #[tokio::test]
    async fn return_correct_final_path() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        let final_path = assembler.finalize().await.unwrap();
        assert!(final_path.ends_with("snapshot.part"));
        assert!(final_path.exists());
    }

    #[tokio::test]
    async fn handle_large_data_volume() {
        let dir = tempdir().unwrap();
        let mut assembler = SnapshotAssembler::new(dir.path()).await.unwrap();

        // Write 1000 chunks of 4KB each (4MB total)
        for i in 0..1000 {
            assembler.write_chunk(i, vec![i as u8; 4096]).await.unwrap();
        }

        let final_path = assembler.finalize().await.unwrap();
        let metadata = tokio::fs::metadata(final_path).await.unwrap();
        assert_eq!(metadata.len(), 1000 * 4096);
    }
}
