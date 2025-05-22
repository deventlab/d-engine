use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::move_directory;
use crate::proto::SnapshotMetadata;
use crate::Result;
use crate::StorageError;

pub(crate) struct SnapshotAssembler {
    temp_file: File,
    pub(crate) temp_path: PathBuf,
    expected_index: u32,
    total_size: usize,
    received_chunks: AtomicU32,
    snapshots_dir: PathBuf,
}

impl SnapshotAssembler {
    pub(crate) async fn new(snapshots_dir: impl AsRef<Path>) -> Result<Self> {
        let snapshots_dir = snapshots_dir.as_ref().to_path_buf();

        let temp_file_path = snapshots_dir.join("snapshot.part");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&temp_file_path)
            .await
            .map_err(StorageError::IoError)?;

        Ok(SnapshotAssembler {
            temp_file: file,
            temp_path: temp_file_path,
            expected_index: 0,
            total_size: 0,
            received_chunks: AtomicU32::new(0),
            snapshots_dir,
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
        self.temp_file.write_all(&data).await.map_err(StorageError::IoError)?;
        self.total_size += data.len();
        self.expected_index += 1;

        Ok(())
    }

    pub(crate) async fn flush_to_disk(&mut self) -> Result<()> {
        self.temp_file.flush().await.map_err(StorageError::IoError)?;
        Ok(())
    }

    /// Finalizes the snapshot assembly by flushing data to disk and performing an atomic rename of
    /// the temporary snapshot directory to its final destination.
    ///
    /// # Arguments
    /// * `snapshot_meta`: Metadata for the snapshot, including the last included index and term.
    ///
    /// # Returns
    /// * `Result<PathBuf>`: The final path of the snapshot after the rename operation is
    ///   successful.
    ///
    /// # Errors
    /// This function may return an error if:
    /// - Flushing data to disk fails (`flush_to_disk()`).
    /// - The atomic rename operation fails (`move_directory()`).
    pub(crate) async fn finalize(
        &mut self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<PathBuf> {
        // 0. Validate snapshot metadata
        if let None = snapshot_metadata.last_included {
            return Err(StorageError::Snapshot(
                "snapshot_metadata is empty when install new snapshot file".to_string(),
            )
            .into());
        }
        let last_included = snapshot_metadata.last_included.unwrap();
        // 1. Flush the in-memory snapshot data to disk.
        self.flush_to_disk().await?;

        // 2. Construct the final snapshot directory path, based on snapshot metadata.
        let final_dir = self.snapshots_dir.join(format!(
            "{}{}-{}",
            SNAPSHOT_DIR_PREFIX, last_included.index, last_included.term
        ));

        debug!(
            ?self.temp_path,
            ?final_dir,
            "SnapshotAssembler: Atomic rename to final snapshot file path"
        );

        // 3. Perform an atomic rename of the temporary snapshot directory to the final directory path.
        move_directory(&self.temp_path, &final_dir).await?;

        // 4. Return the final snapshot directory path.
        Ok(final_dir)
    }

    pub(crate) fn received_chunks(&self) -> u32 {
        self.received_chunks.load(Ordering::Acquire)
    }
}
