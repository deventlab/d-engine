use std::io::Write;
use std::os::fd::AsRawFd;

use nix::libc::flock;
use nix::libc::LOCK_EX;
use sha2::Digest;
use sha2::Sha256;
use tempfile::tempdir;
use tempfile::NamedTempFile;

use crate::file_io;
use crate::file_io::compute_checksum_from_path;
use crate::file_io::create_parent_dir_if_not_exist;
use crate::file_io::delete_file;
use crate::file_io::move_directory;
use crate::test_utils::enable_logger;
use crate::Error;
use crate::FileDeleteError;
use crate::StorageError;
use crate::SystemError;

/// Passed: "/tmp/files/data.txt"
/// Expected: "/tmp/files" created
#[tokio::test]
async fn test_create_parent_dir_for_file() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();

    // File path: create parent directory
    let file_path = temp_path.join("files").join("data.txt");
    create_parent_dir_if_not_exist(&file_path).unwrap();

    // Verify parent directory exists
    let parent_dir = file_path.parent().unwrap();
    assert!(file_io::is_dir(parent_dir).await.unwrap());
    // File itself should NOT be created
    assert!(parent_dir.exists());
    assert!(!file_path.exists());
}

/// Passed: "/tmp/dir/subdir"
/// Expected: "/tmp/dir/subdir" created
#[tokio::test]
async fn test_create_parent_dir_for_directory_without_trailing_separator() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();

    // Directory path (explicit trailing separator)
    let dir_path = temp_path.join("dir").join("subdir");
    create_parent_dir_if_not_exist(&dir_path).unwrap();

    // Verify parent directory exists
    let parent_dir = dir_path.parent().unwrap();
    assert!(parent_dir.exists());
    assert!(file_io::is_dir(parent_dir).await.unwrap());
}

/// Passed: "/tmp/dir/subdir/"
/// Expected: "/tmp/dir/subdir" created
#[tokio::test]
async fn test_create_parent_dir_for_directory_with_trailing_separator() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();

    // Directory path (explicit trailing separator)
    let dir_path = temp_path.join("dir").join("subdir").join(""); // Trailing separator
    create_parent_dir_if_not_exist(&dir_path).unwrap();

    // Verify directory itself exists
    assert!(dir_path.exists());
    assert!(file_io::is_dir(&dir_path).await.unwrap());
}

#[tokio::test]
async fn test_delete_file_success() {
    // Create temp file
    let mut file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    // Write test content
    writeln!(file, "test content").unwrap();

    // Delete the file
    let result = delete_file(&path).await;
    assert!(result.is_ok(), "Should successfully delete file");

    // Verify file no longer exists
    assert!(!path.exists(), "File should be deleted");
}

/// Test non-existent file path
#[tokio::test]
async fn test_delete_nonexistent_file() {
    let e = delete_file("nonexistent.txt").await.unwrap_err();
    assert!(
        matches!(
            e,
            Error::System(SystemError::Storage(StorageError::FileDelete(
                FileDeleteError::NotFound(_)
            )))
        ),
        "Should return NotFound error"
    );
}

/// Test directory deletion attempt
#[tokio::test]
async fn test_delete_directory() {
    // Create temp directory
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_owned();

    let e = delete_file(&dir_path).await.unwrap_err();
    assert!(
        matches!(
            e,
            Error::System(SystemError::Storage(StorageError::FileDelete(
                FileDeleteError::IsDirectory(_)
            )))
        ),
        "Should return IsDirectory error"
    );
}

/// Test busy file deletion (platform-specific)
#[tokio::test]
async fn test_delete_busy_file() {
    // Create temp file
    let temp_dir = tempfile::tempdir().unwrap();
    let dir_path = temp_dir.path().to_owned();

    // Create a test file
    let file_path = dir_path.join("test_file.txt");
    // Lock the file (platform-specific implementation)
    #[cfg(windows)]
    {
        // Keep file open to prevent deletion on Windows
        let _file_handle = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();
    }
    #[cfg(unix)]
    {
        // Open with exclusive lock on Unix-like systems
        use std::os::unix::fs::OpenOptionsExt;

        // Create a file and apply an exclusive lock
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644) // Set permissions
            .open(&file_path)
            .unwrap();

        // Apply file lock
        unsafe {
            flock(file.as_raw_fd(), LOCK_EX);
        }

        // Keep the file handle alive
        let _file_handle = file;
    }

    let e = delete_file(&file_path).await;

    #[cfg(unix)]
    assert!(e.is_ok());

    #[cfg(windows)]
    assert!(e.is_err());
}

/// Test permission error (if possible in test environment)
#[tokio::test]
#[cfg(unix)] // Unix-like systems have clearer permission semantics
async fn test_delete_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    use tokio::fs;

    // Create temp file
    let temp_dir = tempfile::tempdir().unwrap();
    let dir_path = temp_dir.path().to_owned();

    // Create a test file
    let file_path = dir_path.join("test_file.txt");
    fs::write(&file_path, b"test").await.unwrap();

    // Set read-only permissions
    let mut perms = fs::metadata(&dir_path).await.unwrap().permissions();
    perms.set_mode(0o444); // Read-only
    fs::set_permissions(&dir_path, perms.clone()).await.unwrap();

    let e = delete_file(&file_path).await.unwrap_err();
    println!("{:?}", &e);

    assert!(
        matches!(
            e,
            Error::System(SystemError::Storage(StorageError::FileDelete(
                FileDeleteError::PermissionDenied(_)
            )))
        ),
        "Should return PermissionDenied error"
    );

    // Cleanup permissions for temp file deletion
    perms.set_mode(0o700);
    fs::set_permissions(&dir_path, perms).await.unwrap();
}

#[tokio::test]
async fn test_move_directory() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();

    // Old directory path with trailing separator
    let old_path = temp_path.join("old").join("a").join("b").join(""); // Mark as directory
    create_parent_dir_if_not_exist(&old_path).unwrap();
    assert!(file_io::is_dir(&old_path).await.unwrap());

    // New directory
    let new_path = temp_path.join("new");
    move_directory(&old_path, &new_path).await.unwrap();

    // Verify new directory exists and old is removed
    assert!(file_io::is_dir(&new_path).await.unwrap());
    assert!(!old_path.exists());
}

/// Test computing checksum for an empty directory
#[tokio::test]
async fn test_compute_checksum_from_path_empty_dir() {
    let temp_dir = tempdir().unwrap();
    let checksum = compute_checksum_from_path(temp_dir.path())
        .await
        .expect("Should compute checksum for empty dir");

    // SHA-256 hash of empty data
    let hasher = Sha256::new();
    let expected: [u8; 32] = hasher.finalize().into();

    assert_eq!(
        checksum, expected,
        "Checksum for empty directory should be SHA-256 of empty data"
    );
}

/// Test computing checksum for a directory with a single file
#[tokio::test]
async fn test_compute_checksum_from_path_single_file() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    tokio::fs::write(&file_path, b"Hello, world!").await.unwrap();

    let checksum = compute_checksum_from_path(temp_dir.path())
        .await
        .expect("Should compute checksum for directory with single file");

    // Calculate expected SHA-256
    let mut hasher = Sha256::new();
    hasher.update(b"Hello, world!");
    let expected: [u8; 32] = hasher.finalize().into();

    assert_eq!(checksum, expected, "Checksum should match SHA-256 of file content");
}

/// Test computing checksum for a directory with multiple files
#[tokio::test]
async fn test_compute_checksum_from_path_multiple_files() {
    let temp_dir = tempdir().unwrap();

    // Create files with different content
    tokio::fs::write(temp_dir.path().join("file1.txt"), b"Content 1")
        .await
        .unwrap();
    tokio::fs::write(temp_dir.path().join("file2.txt"), b"Content 2")
        .await
        .unwrap();
    tokio::fs::write(temp_dir.path().join("file3.txt"), b"Content 3")
        .await
        .unwrap();

    let checksum = compute_checksum_from_path(temp_dir.path())
        .await
        .expect("Should compute checksum for directory with multiple files");

    // Calculate expected SHA-256 (concatenated content of all files)
    let mut hasher = Sha256::new();
    hasher.update(b"Content 1");
    hasher.update(b"Content 2");
    hasher.update(b"Content 3");
    let expected: [u8; 32] = hasher.finalize().into();

    assert_eq!(
        checksum, expected,
        "Checksum should match SHA-256 of concatenated file contents"
    );
}

/// Test that checksum ignores subdirectories and only processes files
#[tokio::test]
async fn test_compute_checksum_ignores_subdirectories() {
    let temp_dir = tempdir().unwrap();

    // Create a file and a subdirectory
    tokio::fs::write(temp_dir.path().join("file.txt"), b"File content")
        .await
        .unwrap();
    let sub_dir = temp_dir.path().join("subdir");
    tokio::fs::create_dir(&sub_dir).await.unwrap();
    tokio::fs::write(sub_dir.join("ignored.txt"), b"Ignored content")
        .await
        .unwrap();

    let checksum = compute_checksum_from_path(temp_dir.path())
        .await
        .expect("Should compute checksum ignoring subdirectories");

    // Calculate expected SHA-256 (only the top-level file)
    let mut hasher = Sha256::new();
    hasher.update(b"File content");
    let expected: [u8; 32] = hasher.finalize().into();

    assert_eq!(checksum, expected, "Checksum should only include top-level files");
}

/// Test error handling for non-existent directory
#[tokio::test]
async fn test_compute_checksum_nonexistent_dir() {
    let temp_dir = tempdir().unwrap();
    let non_existent_path = temp_dir.path().join("does_not_exist");

    let result = compute_checksum_from_path(&non_existent_path).await;

    assert!(result.is_err(), "Should return error for non-existent directory");
    match result.unwrap_err() {
        Error::System(SystemError::Storage(StorageError::IoError(_))) => {} // Expected
        other => panic!("Expected IoError, got {:?}", other),
    }
}

/// Test checksum consistency across multiple runs
#[tokio::test]
async fn test_compute_checksum_consistency() {
    let temp_dir = tempdir().unwrap();
    tokio::fs::write(temp_dir.path().join("data.bin"), b"Consistent data")
        .await
        .unwrap();

    // Compute checksum twice
    let checksum1 = compute_checksum_from_path(temp_dir.path())
        .await
        .expect("First computation should succeed");

    let checksum2 = compute_checksum_from_path(temp_dir.path())
        .await
        .expect("Second computation should succeed");

    assert_eq!(
        checksum1, checksum2,
        "Checksum should be consistent across multiple computations"
    );
}
