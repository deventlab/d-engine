use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tracing::debug;
use tracing::error;

use crate::FileDeleteError;
use crate::Result;
use crate::StorageError;

pub fn crate_parent_dir_if_not_exist(path: &Path) -> Result<()> {
    if let Some(parent_dir) = path.parent() {
        if !parent_dir.exists() {
            if let Err(e) = create_dir_all(parent_dir) {
                error!("Failed to create log directory: {:?}", e);
                return Err(StorageError::IoError(e).into());
            }
        }
    }
    Ok(())
}

pub fn open_file_for_append(path: PathBuf) -> Result<File> {
    crate_parent_dir_if_not_exist(&path)?;
    let log_file = match OpenOptions::new().append(true).create(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            return Err(StorageError::IoError(e).into());
        }
    };
    Ok(log_file)
}

pub(crate) async fn write_into_file(
    path: PathBuf,
    buf: Vec<u8>,
) {
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            error!("failed to crate dir with error({})", e);
        } else {
            debug!("created successfully: {:?}", path);
        }
    }

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .unwrap();
    let mut active_file = BufWriter::new(file);
    active_file.write_all(&buf).await.unwrap();
    active_file.flush().await.unwrap();
}

/// Asynchronous function for safely deleting files
///
/// # Parameters
/// - `path`: The file path to be deleted, accepting any type that implements AsRef<Path>
///
/// # Error
/// Returns a custom FileDeleteError error type, including various possible failure scenarios
pub(crate) async fn delete_file<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    let display_path = path.display().to_string();

    // Normalized path processing
    let canonical_path = match path.canonicalize() {
        Ok(p) => p,
        Err(e) => return Err(map_canonicalize_error(e, &display_path).into()),
    };

    // Get file metadata
    let metadata = match fs::metadata(&canonical_path).await {
        Ok(m) => m,
        Err(e) => return Err(map_metadata_error(e, &display_path).into()),
    };

    // Verify file type
    if metadata.is_dir() {
        return Err(FileDeleteError::IsDirectory(display_path).into());
    }

    // Perform deletion
    match fs::remove_file(&canonical_path).await {
        Ok(_) => Ok(()),
        Err(e) => Err(map_remove_error(e, &display_path).into()),
    }
}

/// Handle path canonicalization error
fn map_canonicalize_error(
    error: std::io::Error,
    path: &str,
) -> FileDeleteError {
    match error.kind() {
        std::io::ErrorKind::NotFound => FileDeleteError::NotFound(path.to_string()),
        std::io::ErrorKind::PermissionDenied => FileDeleteError::PermissionDenied(path.to_string()),
        _ => FileDeleteError::InvalidPath(format!("{}: {}", path, error)),
    }
}

/// Handle metadata retrieval errors
fn map_metadata_error(
    error: std::io::Error,
    path: &str,
) -> FileDeleteError {
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => FileDeleteError::PermissionDenied(path.to_string()),
        std::io::ErrorKind::NotFound => FileDeleteError::NotFound(path.to_string()),
        _ => FileDeleteError::UnknownIo(format!("{}: {}", path, error)),
    }
}

/// Handle delete operation errors
fn map_remove_error(
    error: std::io::Error,
    path: &str,
) -> FileDeleteError {
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => FileDeleteError::PermissionDenied(path.to_string()),
        std::io::ErrorKind::NotFound => FileDeleteError::NotFound(path.to_string()),
        std::io::ErrorKind::Other if is_file_busy(&error) => FileDeleteError::FileBusy(path.to_string()),
        _ => FileDeleteError::UnknownIo(format!("{}: {}", path, error)),
    }
}

/// Detect file occupation status (cross-platform processing)
fn is_file_busy(error: &std::io::Error) -> bool {
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(32) // ERROR_SHARING_VIOLATION
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(16) // EBUSY
    }
    #[cfg(not(any(windows, unix)))]
    {
        false
    }
}

pub fn validate_checksum(
    data: &[u8],
    expected: &[u8],
) -> bool {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes() == expected
}
