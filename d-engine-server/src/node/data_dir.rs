use std::path::Path;
use std::path::PathBuf;

use d_engine_core::Result;

/// Explicit runtime identity of a node: the physical directory this process
/// owns. `new()` rejects an empty path and creates the
/// directory if missing
#[derive(Debug)]
pub(crate) struct DataDir {
    path: PathBuf,
    /// Test-only: keeps `for_test()`'s backing `TempDir` alive for as long as
    /// this `DataDir` lives, so the directory is cleaned up automatically on
    /// drop instead of leaking into `/tmp` for the rest of the process.
    #[cfg(test)]
    _tempdir: Option<tempfile::TempDir>,
}

impl DataDir {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        d_engine_core::validate_directory(&path, "data_dir")?;
        Ok(Self {
            path,
            #[cfg(test)]
            _tempdir: None,
        })
    }

    pub(crate) fn as_path(&self) -> &Path {
        &self.path
    }

    /// This node's snapshot directory — always `data_dir/snapshots`, not
    /// independently configurable.
    /// Validated/created the same way `data_dir` itself is — same rule,
    /// same place, because the two are one concept, not two.
    pub(crate) fn snapshots_dir(&self) -> Result<PathBuf> {
        let dir = self.path.join("snapshots");
        d_engine_core::validate_directory(&dir, "snapshots_dir")?;
        Ok(dir)
    }

    /// Path for unified single-DB mode (`storage.unified_db = true`).
    /// Validated eagerly so a permission problem fails loudly here, at
    /// startup, instead of surfacing later as an opaque storage-engine IO
    /// error. Backend-agnostic path convention — not RocksDB-specific;
    /// any storage engine's `start_custom`/`run_custom` caller can use it.
    #[allow(dead_code)]
    pub(crate) fn unified_db_path(&self) -> Result<PathBuf> {
        let dir = self.path.join("db");
        d_engine_core::validate_directory(&dir, "db")?;
        Ok(dir)
    }

    /// Storage engine path (separate-DB mode). Same eager validation and
    /// backend-agnostic intent as `unified_db_path` — see its doc comment.
    #[allow(dead_code)]
    pub(crate) fn storage_path(&self) -> Result<PathBuf> {
        let dir = self.path.join("storage");
        d_engine_core::validate_directory(&dir, "storage")?;
        Ok(dir)
    }

    /// State machine path (separate-DB mode). Same eager validation and
    /// backend-agnostic intent as `unified_db_path` — see its doc comment.
    #[allow(dead_code)]
    pub(crate) fn state_machine_path(&self) -> Result<PathBuf> {
        let dir = self.path.join("state_machine");
        d_engine_core::validate_directory(&dir, "state_machine")?;
        Ok(dir)
    }

    /// Test-only: a fresh, uniquely-generated temp directory — for mocks that
    /// don't care about a real node identity, just need a valid `DataDir`.
    /// The backing `TempDir` is owned by this `DataDir` and cleaned up
    /// automatically when it drops.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let path = tempdir.path().to_path_buf();
        Self {
            path,
            _tempdir: Some(tempdir),
        }
    }
}
