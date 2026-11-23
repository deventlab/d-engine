//! The engine's own record of the snapshots this node holds.
//!
//! A snapshot is an archive `snapshot-{index}-{term}.tar.gz` in the snapshots directory. Next to
//! it the engine keeps `snapshot-{index}-{term}.meta`: the snapshot's metadata (boundary and
//! checksum). The record is written only once the archive is complete and, on a follower, only
//! once the snapshot has been installed. An archive without a record is not a snapshot this node
//! can vouch for, so it is ignored.

use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use d_engine_proto::server::storage::SnapshotMetadata;
use parking_lot::RwLock;
use prost::Message;
use tracing::error;
use tracing::warn;

use crate::Result;
use crate::SnapshotError;
use crate::SnapshotPathManager;
use crate::StorageError;

const RECORD_EXTENSION: &str = "meta";

/// The newest snapshot this node holds, kept in memory and backed by the records on disk.
#[derive(Debug)]
pub(crate) struct SnapshotCatalog {
    path_mgr: Arc<SnapshotPathManager>,
    latest: RwLock<Option<SnapshotMetadata>>,
}

impl SnapshotCatalog {
    /// Reads the records on disk. Done once, when the node starts.
    pub(crate) fn open(path_mgr: Arc<SnapshotPathManager>) -> Self {
        let latest = Self::scan(&path_mgr);
        Self {
            path_mgr,
            latest: RwLock::new(latest),
        }
    }

    /// Metadata of the newest snapshot this node holds, if any.
    pub(crate) fn latest(&self) -> Option<SnapshotMetadata> {
        self.latest.read().clone()
    }

    /// Records `metadata` next to the archive of that snapshot, which must already be in place.
    ///
    /// The record is written to a temporary file, synced, then renamed, so a crash leaves the old
    /// record or the new one, never a partial file.
    pub(crate) async fn publish(
        &self,
        metadata: &SnapshotMetadata,
    ) -> Result<()> {
        let last_included = metadata.last_included.ok_or_else(|| {
            SnapshotError::OperationFailed("Missing last_included in snapshot metadata".into())
        })?;
        let record = record_path(&self.path_mgr.final_snapshot_path(&last_included));
        let bytes = metadata.encode_to_vec();
        tokio::task::spawn_blocking(move || write_record(&record, &bytes))
            .await
            .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
            .map_err(StorageError::IoError)?;

        let mut latest = self.latest.write();
        let is_newest = latest.as_ref().and_then(|m| m.last_included).is_none_or(|current| {
            (last_included.index, last_included.term) >= (current.index, current.term)
        });
        if is_newest {
            *latest = Some(metadata.clone());
        }
        Ok(())
    }

    /// Deletes the snapshots this node no longer needs.
    ///
    /// Keeps the newest `retain` recorded snapshots and deletes older recorded ones together with
    /// their records. Also deletes an archive that has no record and is not newer than the newest
    /// recorded snapshot: a snapshot is installed only if it is newer than the node's position,
    /// which is at least that of the newest recorded snapshot, so such an archive can never be
    /// installed or served. An archive newer than that may be a snapshot that is being installed
    /// right now, so it is left alone. With no recorded snapshot nothing is deleted.
    ///
    /// The newest recorded snapshot is never deleted (`retain` is at least 1), so
    /// [`latest`](Self::latest) stays valid.
    pub(crate) async fn prune(
        &self,
        retain: u64,
    ) -> Result<()> {
        let path_mgr = self.path_mgr.clone();
        tokio::task::spawn_blocking(move || prune_blocking(&path_mgr, retain))
            .await
            .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?
            .map_err(|e| StorageError::IoError(e).into())
    }

    fn scan(path_mgr: &SnapshotPathManager) -> Option<SnapshotMetadata> {
        recorded(path_mgr).pop().map(|snapshot| snapshot.metadata)
    }
}

/// A recorded snapshot: its metadata and the archive it describes.
struct RecordedSnapshot {
    metadata: SnapshotMetadata,
    boundary: (u64, u64),
    archive: PathBuf,
}

/// Every snapshot with a valid record and an archive, oldest first.
fn recorded(path_mgr: &SnapshotPathManager) -> Vec<RecordedSnapshot> {
    // A directory that does not exist yet holds no snapshots.
    let Ok(entries) = std::fs::read_dir(&path_mgr.base_dir) else {
        return Vec::new();
    };
    let mut snapshots = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != RECORD_EXTENSION) {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(?path, %e, "ignoring unreadable snapshot record");
                continue;
            }
        };
        let metadata = match SnapshotMetadata::decode(bytes.as_slice()) {
            Ok(metadata) => metadata,
            Err(e) => {
                error!(?path, %e, "ignoring undecodable snapshot record");
                continue;
            }
        };
        let Some(last_included) = metadata.last_included else {
            error!(?path, "ignoring snapshot record without a boundary");
            continue;
        };
        let archive = path_mgr.final_snapshot_path(&last_included);
        if record_path(&archive) != path {
            warn!(
                ?path,
                ?last_included,
                "ignoring snapshot record whose name disagrees with its boundary"
            );
            continue;
        }
        if !archive.exists() {
            continue;
        }
        snapshots.push(RecordedSnapshot {
            metadata,
            boundary: (last_included.index, last_included.term),
            archive,
        });
    }
    snapshots.sort_by_key(|snapshot| snapshot.boundary);
    snapshots
}

fn prune_blocking(
    path_mgr: &SnapshotPathManager,
    retain: u64,
) -> std::io::Result<()> {
    let recorded = recorded(path_mgr);
    let Some(newest) = recorded.last().map(|snapshot| snapshot.boundary) else {
        return Ok(());
    };

    let delete_from = recorded.len().saturating_sub(retain as usize);
    for snapshot in &recorded[..delete_from] {
        remove_if_present(&snapshot.archive)?;
        remove_if_present(&record_path(&snapshot.archive))?;
    }

    // Archives without a record that can no longer matter.
    let recorded_archives: Vec<&PathBuf> = recorded.iter().map(|s| &s.archive).collect();
    for entry in std::fs::read_dir(&path_mgr.base_dir)?.flatten() {
        let path = entry.path();
        if !entry.file_type()?.is_file() || recorded_archives.contains(&&path) {
            continue;
        }
        let Some(boundary) = archive_boundary(&path, &path_mgr.snapshot_prefix) else {
            continue;
        };
        if boundary <= newest {
            remove_if_present(&path)?;
        }
    }
    Ok(())
}

/// `snapshot-{index}-{term}.tar.gz` -> `(index, term)`; anything else is not an archive.
fn archive_boundary(
    path: &Path,
    prefix: &str,
) -> Option<(u64, u64)> {
    let stem = path.file_name()?.to_str()?.strip_suffix(".tar.gz")?;
    let (index, term) = stem.strip_prefix(prefix)?.split_once('-')?;
    Some((index.parse().ok()?, term.parse().ok()?))
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// `snapshot-5-1.tar.gz` is recorded in `snapshot-5-1.meta`.
fn record_path(archive: &Path) -> PathBuf {
    let name = archive.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    let stem = name.strip_suffix(".tar.gz").unwrap_or(name);
    archive.with_file_name(format!("{stem}.{RECORD_EXTENSION}"))
}

fn write_record(
    record: &Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    let temp = record.with_extension(format!("{RECORD_EXTENSION}.tmp"));
    {
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, record)?;
    if let Some(dir) = record.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_catalog_test.rs"]
mod tests;
