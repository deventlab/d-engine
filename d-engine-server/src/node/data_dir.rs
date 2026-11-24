use std::path::Path;
use std::path::PathBuf;

use d_engine_core::Result;
use d_engine_core::SystemError;

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
        validate_directory(&path, "data_dir")?;
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
        validate_directory(&dir, "snapshots_dir")?;
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

/// Ensures directory path is valid and writable.
fn validate_directory(
    path: &Path,
    name: &str,
) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Err(SystemError::DataDirInvalid(format!("{name} path cannot be empty")).into());
    }

    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|e| {
            SystemError::DataDirInvalid(format!(
                "Failed to create {} directory at {}: {}",
                name,
                path.display(),
                e
            ))
        })?;
    }

    // Unique, collision-free write-permission probe: never overwrites an
    // existing file, and is removed automatically on drop.
    tempfile::Builder::new()
        .prefix(".d-engine-write-test-")
        .tempfile_in(path)
        .map_err(|e| {
            SystemError::DataDirInvalid(format!(
                "No write permission in {} directory {}: {}",
                name,
                path.display(),
                e
            ))
        })?;

    Ok(())
}
