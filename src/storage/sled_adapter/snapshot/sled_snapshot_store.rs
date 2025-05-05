use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Buf;
use tokio::fs;
use tokio::fs::create_dir_all;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tracing::error;
use tracing::info;

use super::StreamingSnapshot;
use crate::file_io::delete_file;
use crate::Result;
use crate::SnapshotMetadata;
use crate::SnapshotStore;
use crate::StorageError;

pub struct SledSnapshotStore {
    dir: PathBuf,
    current_version: AtomicU64,
}

impl SnapshotStore for SledSnapshotStore {
    /// Saves snapshot data to disk with versioned file naming
    ///
    /// Implements a three-phase write process:
    /// 1. Write to temporary file
    /// 2. Atomically rename to final filename
    /// 3. Cleanup old versions
    async fn save(
        &self,
        snapshot: &dyn crate::Snapshot,
    ) -> crate::Result<PathBuf> {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst);
        let temp_path = self.dir.join(format!(".temp-{}", version));
        let final_path = self.dir.join(format!(
            "{}{}-{}-{}",
            SNAPSHOT_DIR_PREFIX,
            version,
            snapshot.metadata().last_included_index,
            snapshot.metadata().last_included_term
        ));

        // Phase 1: Stream data to temporary file
        let mut writer = BufWriter::new(File::create(&temp_path).await?);
        let mut stream = snapshot.data_stream();
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks

        loop {
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            writer.write_all(&buffer[..bytes_read]).await?;
        }
        writer.shutdown().await?;

        // Phase 2: Atomic rename
        fs::rename(&temp_path, &final_path).await?;

        // Phase 3: Async cleanup (best effort)
        let cleanup_version = version.saturating_sub(2);

        if let Err(e) = self.cleanup(cleanup_version).await {
            error!(%e, "clean up old snapshot file failed");
        }

        Ok(final_path)
    }

    /// Loads the latest valid snapshot using streaming read
    ///
    /// Implementation notes:
    /// - Never loads entire snapshot into memory
    /// - Validates file naming convention
    /// - Returns None if no valid snapshots exist
    async fn load_latest(&self) -> crate::Result<Option<Box<dyn crate::Snapshot>>> {
        let (_latest_version, latest_path) = self.find_latest_snapshot().await?;

        if let Some(path) = latest_path {
            let metadata = self.parse_metadata(&path).await?;
            let reader = BufReader::new(File::open(&path).await?);

            Ok(Some(Box::new(StreamingSnapshot {
                path,
                metadata,
                reader: Some(reader),
            })))
        } else {
            Ok(None)
        }
    }

    async fn cleanup(
        &self,
        before_version: u64,
    ) -> crate::Result<()> {
        let mut entries = fs::read_dir(self.dir.as_path()).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.is_dir() {
                continue;
            } else if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                //File name parsing logic
                if let Some((version, index, term)) = parse_filename(file_name) {
                    info!(
                        "Version: {:>4} | Index: {:>10} | Term: {:>10} | Path: {}",
                        version,
                        index,
                        term,
                        path.display()
                    );

                    if version < before_version {
                        info!(?version, "going to be deleted.");
                        if let Err(e) = delete_file(path).await {
                            error!(%e, "delete_file failed");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl SledSnapshotStore {
    pub async fn new(dir: impl AsRef<Path>) -> Result<Self> {
        create_dir_all(&dir).await?;
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            current_version: AtomicU64::new(0),
        })
    }

    fn current_version(&self) -> u64 {
        self.current_version.load(Ordering::Acquire)
    }

    /// Parse file metadata
    async fn parse_metadata(
        &self,
        path: &Path,
    ) -> Result<SnapshotMetadata> {
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(StorageError::Snapshot(format!(
                "InvalidSnapshotFile({:?})",
                path.to_path_buf()
            )))?;

        let (_, index, term) = parse_filename(file_name).ok_or(StorageError::Snapshot(format!(
            "InvalidSnapshotFile({:?})",
            path.to_path_buf()
        )))?;

        Ok(SnapshotMetadata {
            last_included_index: index,
            last_included_term: term,
        })
    }

    /// Synchronously find the latest snapshot
    async fn find_latest_snapshot(&self) -> crate::Result<(u64, Option<PathBuf>)> {
        let mut max_version = 0;
        let mut latest_path = None;

        let mut entries = fs::read_dir(&self.dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some((version, _index, _term)) = parse_filename(path.file_name().unwrap().to_str().unwrap()) {
                if version > max_version {
                    max_version = version;
                    latest_path = Some(path);
                }
            }
        }
        Ok((max_version, latest_path))
    }
}

/// Manual parsing file name format: snapshot-{version}-{index}.bin
fn parse_filename(name: &str) -> Option<(u64, u64, u64)> {
    // Check prefix and suffix
    if !name.starts_with(SNAPSHOT_DIR_PREFIX) {
        return None;
    }

    // Remove fixed parts
    let core = &name[9..name.len() - 4]; // "snapshot-".len() = 9, ".bin".len() = 4

    // Split version and index
    let parts: Vec<&str> = core.splitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }

    // Parse numbers
    match (
        parts[0].parse::<u64>(),
        parts[1].parse::<u64>(),
        parts[2].parse::<u64>(),
    ) {
        (Ok(v), Ok(i), Ok(t)) => Some((v, i, t)),
        _ => None,
    }
}

async fn read_lastest_path_by_version(
    root_dir_path: &PathBuf,
    current_version: u64,
) -> crate::Result<Option<PathBuf>> {
    let mut entries = fs::read_dir(root_dir_path.as_path()).await?;

    let mut last_included = 0;
    let mut version_path = None;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if path.is_dir() {
            continue;
        } else if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            //File name parsing logic
            if let Some((version, index, term)) = parse_filename(file_name) {
                info!(
                    "Version: {:>4} | Index: {:>10} | Path: {}",
                    version,
                    index,
                    path.display()
                );

                if version == current_version && index > last_included {
                    last_included = index;
                    version_path = Some(path.to_path_buf())
                }
            }
        }
    }
    return Ok(version_path);
}
