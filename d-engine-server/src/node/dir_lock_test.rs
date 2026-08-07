use super::DataDirLock;

#[test]
fn test_acquire_succeeds_on_fresh_dir() {
    let dir = tempfile::tempdir().unwrap();
    let lock = DataDirLock::acquire(dir.path());
    assert!(lock.is_ok());
}

#[test]
fn test_second_acquire_on_same_dir_fails_while_first_held() {
    let dir = tempfile::tempdir().unwrap();
    let _first = DataDirLock::acquire(dir.path()).unwrap();
    let second = DataDirLock::acquire(dir.path());
    assert!(second.is_err());
}

#[test]
fn test_acquire_succeeds_again_after_first_lock_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let first = DataDirLock::acquire(dir.path()).unwrap();
    drop(first);
    let second = DataDirLock::acquire(dir.path());
    assert!(second.is_ok());
}

#[test]
fn test_matches_true_for_locked_dir_false_for_other_dir() {
    let dir = tempfile::tempdir().unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let lock = DataDirLock::acquire(dir.path()).unwrap();

    assert!(
        lock.matches(dir.path()),
        "lock must match the dir it was acquired for"
    );
    assert!(
        !lock.matches(other_dir.path()),
        "lock must not match an unrelated dir"
    );
}
