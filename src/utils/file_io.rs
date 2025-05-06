use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::ErrorKind;
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

/// Creates parent directories for the given path.
/// e.g. path = "/tmp/a/b/c", "/tmp/a/b" will be crated
/// e.g. path = "/tmp/a/b/c/", "/tmp/a/b/c" will be crated
/// e.g. path = "/tmp/a/b/x.txt", "/tmp/a/b" will be crated
pub fn create_parent_dir_if_not_exist(path: &Path) -> Result<()> {
    /// Check if the path ends with the system separator
    fn has_trailing_separator(path: &Path) -> bool {
        let s = path.to_string_lossy();
        !s.is_empty() && s.ends_with(std::path::MAIN_SEPARATOR)
    }

    // Core logic: Prioritize detection of the end delimiter
    let dir_to_create = if has_trailing_separator(path) {
        path //Explicitly indicate the directory path and create a complete path
    } else {
        path.parent().unwrap_or(path) // file path, create parent directory
    };

    if !dir_to_create.exists() {
        if let Err(e) = create_dir_all(dir_to_create) {
            error!(?e, "create_parent_dir_if_not_exist failed.");
            return Err(StorageError::PathError {
                path: path.to_path_buf(),
                source: e,
            }
            .into());
        }
    }

    Ok(())
}

pub fn open_file_for_append(path: PathBuf) -> Result<File> {
    create_parent_dir_if_not_exist(&path)?;
    let log_file = match OpenOptions::new().append(true).create(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            return Err(StorageError::PathError { path, source: e }.into());
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

pub(crate) async fn move_directory(
    temp_dir: &PathBuf,
    final_dir: &PathBuf,
) -> Result<()> {
    // Check if final_dir already exists
    if fs::metadata(final_dir).await.is_ok() {
        return Err(StorageError::PathError {
            path: final_dir.to_path_buf(),
            source: std::io::Error::new(ErrorKind::AlreadyExists, "Target directory exists"),
        }
        .into());
    }

    // Attempt to rename/move the directory
    fs::rename(&temp_dir, &final_dir)
        .await
        .map_err(|e| StorageError::PathError {
            path: temp_dir.to_path_buf(),
            source: e,
        })?;

    Ok(())
}

pub(crate) async fn is_dir(path: &Path) -> Result<bool> {
    let metadata = fs::metadata(path).await.map_err(StorageError::IoError)?;

    // Check if it's a directory
    Ok(metadata.is_dir())
}
