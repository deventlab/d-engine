use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use tracing::trace;
use tracing::warn;

use crate::Result;
use crate::SnapshotError;
use crate::SnapshotPathManager;
use crate::StorageError;
use crate::file_io::create_parent_dir_if_not_exist;
use d_engine_proto::server::storage::SnapshotMetadata;

pub(crate) struct SnapshotAssembler {
    temp_file: File,
    pub(crate) temp_path: PathBuf,
    path_mgr: Arc<SnapshotPathManager>,

    expected_index: u32,
    total_size: usize,
    received_chunks: AtomicU32,
}

impl SnapshotAssembler {
    pub(crate) async fn new(path_mgr: Arc<SnapshotPathManager>) -> Result<Self> {
        let temp_file_path = path_mgr.temp_assembly_file();

        // If `snapshot.part` already exists and is a directory, remove it (or report an error)
        if temp_file_path.is_dir() {
            // Generate a new path with a timestamp (e.g. `snapshot.part_20240614_083034`)
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| {
                    StorageError::IoError(std::io::Error::other(format!(
                        "SystemTime before UNIX EPOCH: {e}"
                    )))
                })?
                .as_nanos();

            let backup_filename =
                format!("{}{}_{}", path_mgr.temp_prefix, "snapshot.part", timestamp);
            let backup_path = path_mgr.base_dir.join(backup_filename);

            trace!(?temp_file_path, ?backup_path, "Rename directory (move)");
            // Rename directory (move)
            tokio::fs::rename(&temp_file_path, &backup_path).await.map_err(|e| {
                StorageError::IoError(std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to rename directory {} to {}: {}",
                        temp_file_path.display(),
                        backup_path.display(),
                        e
                    ),
                ))
            })?;

            // Optional: Log that the old directory has been moved
            warn!(
                "Moved existing directory {} to {}",
                temp_file_path.display(),
                backup_path.display()
            );
        }

        create_parent_dir_if_not_exist(&temp_file_path)?;
        trace!("open: {:?}", &temp_file_path);

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
            path_mgr,
        })
    }
    pub(crate) async fn write_chunk(
        &mut self,
        index: u32,
        data: Bytes,
    ) -> Result<()> {
        // Check if the block index is continuous
        if index != self.expected_index {
            return Err(SnapshotError::OperationFailed(format!(
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
        if snapshot_metadata.last_included.is_none() {
            return Err(SnapshotError::OperationFailed(
                "snapshot_metadata is empty when install new snapshot file".to_string(),
            )
            .into());
        }
        let last_included = snapshot_metadata.last_included.unwrap();
        // 1. Flush the in-memory snapshot data to disk.
        self.flush_to_disk().await?;

        // 2. Construct the final snapshot directory path, based on snapshot metadata.
        let final_path = self.path_mgr.final_snapshot_path(&last_included);

        debug!(
            ?self.temp_path,
            ?final_path,
            "SnapshotAssembler: Atomic rename to final snapshot file path"
        );

        // 3. Move temporary file to final location
        tokio::fs::rename(&self.temp_path, &final_path).await.map_err(|e| {
            StorageError::IoError(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to move compressed snapshot from {} to {}: {}",
                    self.temp_path.display(),
                    final_path.display(),
                    e
                ),
            ))
        })?;

        // 4. Return the final snapshot directory path.
        Ok(final_path)
    }

    pub(crate) fn received_chunks(&self) -> u32 {
        self.received_chunks.load(Ordering::Acquire)
    }
}
