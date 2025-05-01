use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::Result;
use crate::StorageError;

pub(crate) struct SnapshotAssembler {
    temp_file: File,
    pub(crate) temp_path: PathBuf,
    expected_index: u32,
    total_size: usize,
    received_chunks: AtomicU32,
}

impl SnapshotAssembler {
    pub(crate) async fn new(temp_dir: impl AsRef<Path>) -> Result<Self> {
        let file_path = temp_dir.as_ref().join("snapshot.part");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        Ok(SnapshotAssembler {
            temp_file: file,
            temp_path: file_path,
            expected_index: 0,
            total_size: 0,
            received_chunks: AtomicU32::new(0),
        })
    }
    pub(crate) async fn write_chunk(
        &mut self,
        index: u32,
        data: Vec<u8>,
    ) -> Result<()> {
        // Check if the block index is continuous
        if index != self.expected_index {
            return Err(StorageError::Snapshot(format!(
                "Out-of-order chunk. Expected {}, got {}",
                self.expected_index, index
            ))
            .into());
        }

        // Update received chunks
        self.received_chunks.fetch_add(1, Ordering::SeqCst);

        // Write temp file
        self.temp_file.write_all(&data).await?;
        self.total_size += data.len();
        self.expected_index += 1;

        Ok(())
    }

    pub(crate) async fn flush_to_disk(&mut self) -> Result<()> {
        self.temp_file.flush().await?;
        Ok(())
    }

    pub(crate) async fn finalize(&mut self) -> Result<PathBuf> {
        self.flush_to_disk().await?;
        Ok(self.temp_path.clone())
    }

    pub(crate) fn received_chunks(&self) -> u32 {
        self.received_chunks.load(Ordering::Acquire)
    }
}
