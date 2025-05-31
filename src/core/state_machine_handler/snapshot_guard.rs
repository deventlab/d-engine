//! SnapshotGuard: A simple RAII-style guard to ensure that only one snapshot operation
//! is in progress at a time.
//!
//! This module provides a concurrency-safe mechanism to guard snapshot creation
//! using an `AtomicBool`. It prevents multiple threads or async tasks from
//! concurrently creating snapshots.
//!
//! When `SnapshotGuard::new` is called, it attempts to acquire exclusive access
//! by atomically setting the flag. If another snapshot is already in progress,
//! it returns an error. When the guard is dropped, the flag is reset automatically.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::Result;
use crate::StorageError;

// Snapshot state guard (RAII mode)
pub(crate) struct SnapshotGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> SnapshotGuard<'a> {
    pub(crate) fn new(flag: &'a AtomicBool) -> Result<Self> {
        // Use compare_exchange to implement atomic state acquisition
        let already_in_progress = flag
            .compare_exchange(
                false,
                true,
                Ordering::AcqRel, // Ensure memory order
                Ordering::Relaxed,
            )
            .map_err(|e| StorageError::Snapshot(format!("SnapshotGuard::compare_exchange, {e:?}")))?;

        if already_in_progress {
            return Err(StorageError::Snapshot("Snapshot already in progress".to_string()).into());
        }

        Ok(Self { flag })
    }
}

impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}
