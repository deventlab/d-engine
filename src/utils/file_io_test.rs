use std::io::Write;

use tempfile::tempdir;
use tempfile::NamedTempFile;

use crate::file_io::delete_file;
use crate::Error;
use crate::FileDeleteError;
use crate::StorageError;
use crate::SystemError;

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
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    // Create temp file
    let mut file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    // Lock the file (platform-specific implementation)
    #[cfg(windows)]
    {
        // Keep file open to prevent deletion on Windows
        let _file_handle = std::fs::File::open(&path).unwrap();
    }
    #[cfg(unix)]
    {
        // Open with exclusive lock on Unix-like systems
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        let _file_handle = opts.open(&path).unwrap();
    }

    let e = delete_file(&path).await.unwrap_err();
    assert!(
        matches!(
            e,
            Error::System(SystemError::Storage(StorageError::FileDelete(FileDeleteError::Busy(_))))
        ),
        "Should return Busy error"
    );
}
/// Test permission error (if possible in test environment)
#[tokio::test]
#[cfg(unix)] // Unix-like systems have clearer permission semantics
async fn test_delete_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    use tokio::fs;

    // Create temp file
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    // Set read-only permissions
    let mut perms = fs::metadata(&path).await.unwrap().permissions();
    perms.set_mode(0o444); // Read-only
    fs::set_permissions(&path, perms.clone()).await.unwrap();

    let e = delete_file(&path).await.unwrap_err();
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
    perms.set_mode(0o600);
    fs::set_permissions(&path, perms).await.unwrap();
}
