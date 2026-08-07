// d-engine-server/src/node/dir_lock.rs

//! Exclusive OS-level lock on a node's `data_dir`, held for the life of the process.
//! Prevents two node processes from pointing at the same physical directory —
//! without this, the failure mode is a confusing mid-run error (e.g. snapshot
//! `mkdir: File exists`) instead of a clear rejection at startup.

pub(crate) struct DataDirLock {
    // Kept only to hold the OS-level lock; released automatically when this
    // `File` is dropped (flock on Unix, LockFileEx on Windows).
    _file: std::fs::File,
    // Remembered so callers that pass a pre-acquired lock into NodeBuilder
    // can be checked against the data_dir they also pass in — see `matches`.
    path: std::path::PathBuf,
    // Test-only: keeps `for_test()`'s backing `TempDir` alive for as long as
    // this lock lives, so the directory is cleaned up automatically on drop
    // instead of leaking into `/tmp` for the rest of the process.
    #[cfg(test)]
    _tempdir: Option<tempfile::TempDir>,
}

impl DataDirLock {
    /// Acquires an exclusive lock on `dir`. Fails immediately if another
    /// process already holds it.
    pub(crate) fn acquire(dir: &std::path::Path) -> d_engine_core::Result<Self> {
        let lock_path = dir.join(".d-engine.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                d_engine_core::SystemError::NodeStartFailed(format!(
                    "failed to open lock file {}: {e}",
                    lock_path.display()
                ))
            })?;
        file.try_lock().map_err(|_| {
            d_engine_core::SystemError::NodeStartFailed(format!(
                "data_dir {} is already in use by another process",
                dir.display()
            ))
        })?;
        Ok(Self {
            _file: file,
            path: dir.to_path_buf(),
            #[cfg(test)]
            _tempdir: None,
        })
    }

    /// True if this lock was acquired for `dir`. Used to catch a
    /// pre-acquired lock being paired with the wrong data_dir (e.g. via
    /// `NodeBuilder::data_dir_lock`) before it can cause silent confusion.
    pub(crate) fn matches(
        &self,
        dir: &std::path::Path,
    ) -> bool {
        self.path == dir
    }

    /// Test-only: locks a fresh, uniquely-generated temp directory instead of
    /// whatever `data_dir` the mock config carries — avoids cross-test lock
    /// collisions when many MockNodeBuilders share a default data_dir path.
    /// The backing `TempDir` is owned by the returned lock and cleaned up
    /// automatically when it drops.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut lock = Self::acquire(dir.path()).expect("acquire test lock");
        lock._tempdir = Some(dir);
        lock
    }
}

#[cfg(test)]
#[path = "dir_lock_test.rs"]
mod dir_lock_test;
