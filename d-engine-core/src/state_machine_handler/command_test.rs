//! Unit tests for `OwnedSnapshotDir` (PR #442 review).
//!
//! `CapturedLocalSnapshot.temp_dir` used to be a bare `PathBuf` — cleanup was
//! whichever call site remembered to call `remove_dir_all`, and two of them
//! (worker.rs's `CaptureSuperseded` branch and its dead-response-receiver
//! branch, covered by `worker_test.rs`) didn't. `OwnedSnapshotDir` makes
//! cleanup unconditional: it happens no matter which path drops the value.
//! These tests cover the type itself in isolation; the actual leak-path
//! regressions are covered where the leaks used to happen, in `worker_test.rs`.

use std::time::Duration;

use crate::OwnedSnapshotDir;

/// `OwnedSnapshotDir`'s `Drop` fallback cleans up on a detached OS thread, not
/// synchronously — poll instead of asserting once, to avoid a flaky test racing
/// that background thread.
async fn wait_until_removed(path: &std::path::Path) {
    for _ in 0..100 {
        if !path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{path:?} was not removed within 1s (Drop cleanup fallback never ran?)");
}

/// `from_existing` adopts a real, already-populated directory without touching
/// its contents, and `.path()` returns exactly the path it was given.
#[test]
fn test_from_existing_adopts_a_real_directory() {
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::write(dir.join("data.bin"), b"hello").unwrap();

    let owned = OwnedSnapshotDir::from_existing(dir.clone()).unwrap();

    assert_eq!(owned.path(), dir.as_path());
    assert!(
        dir.join("data.bin").exists(),
        "adopting must not touch contents"
    );
}

/// `OwnedSnapshotDir` never creates directories — a path that doesn't exist
/// on disk must be rejected, not silently accepted (which would make `.path()`
/// point at nothing, and `.remove()` a confusing no-op-that-looks-like-success).
#[test]
fn test_from_existing_rejects_nonexistent_path() {
    let dir = tempfile::tempdir().unwrap().keep();
    let missing = dir.join("never-created");

    let result = OwnedSnapshotDir::from_existing(missing);

    assert!(
        result.is_err(),
        "a path that was never created must be rejected"
    );
}

/// A path that exists but is a plain file (not a directory) must also be
/// rejected — this type specifically owns *directories* exported by
/// `generate_snapshot_data`, not arbitrary paths.
#[test]
fn test_from_existing_rejects_a_file_not_a_directory() {
    let dir = tempfile::tempdir().unwrap().keep();
    let file_path = dir.join("not-a-directory.txt");
    std::fs::write(&file_path, b"oops").unwrap();

    let result = OwnedSnapshotDir::from_existing(file_path);

    assert!(
        result.is_err(),
        "a plain file must be rejected — this type owns directories only"
    );
}

/// The expected/happy path: `remove().await` deletes the directory and returns
/// `Ok(())`. This also exercises the "Drop still fires after remove()" case
/// noted in the #442 review discussion — `remove(self)` consumes `self`, so its
/// own (redundant) `Drop` runs immediately afterward inside the same call; if
/// that second attempt panicked or somehow surfaced as a test failure, this
/// test would catch it (the directory is already gone by then, so the Drop
/// fallback's `remove_dir_all` gets `NotFound`, which it must ignore silently).
#[tokio::test]
async fn test_remove_deletes_the_directory_and_succeeds() {
    let dir = tempfile::tempdir().unwrap().keep();
    let owned = OwnedSnapshotDir::from_existing(dir.clone()).unwrap();

    let result = owned.remove().await;

    assert!(result.is_ok(), "remove() must succeed: {result:?}");
    assert!(
        !dir.exists(),
        "directory must be gone immediately after remove().await returns"
    );
}

/// The core guarantee this type exists for: a value that's dropped *without*
/// anyone calling `.remove()` (e.g. an error path, a superseded operation, a
/// cancelled future — see `worker_test.rs` for the real-world instances of
/// this) must still have its directory cleaned up, via the `Drop` fallback.
#[tokio::test]
async fn test_drop_without_explicit_remove_still_cleans_up_the_directory() {
    let dir = tempfile::tempdir().unwrap().keep();
    let owned = OwnedSnapshotDir::from_existing(dir.clone()).unwrap();

    drop(owned);

    wait_until_removed(&dir).await;
}
