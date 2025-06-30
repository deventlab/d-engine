use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use sha2::Digest;
use sha2::Sha256;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tracing::debug;
use tracing::error;

use crate::init_sled_state_machine_db;
use crate::ConvertError;
use crate::FileError;
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

pub fn open_file_for_append(path: PathBuf) -> Result<std::fs::File> {
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
/// Returns a custom FileError error type, including various possible failure scenarios
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
        return Err(FileError::IsDirectory(display_path).into());
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
) -> FileError {
    match error.kind() {
        std::io::ErrorKind::NotFound => FileError::NotFound(path.to_string()),
        std::io::ErrorKind::PermissionDenied => FileError::PermissionDenied(path.to_string()),
        _ => FileError::InvalidPath(format!("{path}: {error}")),
    }
}

/// Handle metadata retrieval errors
fn map_metadata_error(
    error: std::io::Error,
    path: &str,
) -> FileError {
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => FileError::PermissionDenied(path.to_string()),
        std::io::ErrorKind::NotFound => FileError::NotFound(path.to_string()),
        _ => FileError::UnknownIo(format!("{path}: {error}")),
    }
}

/// Handle delete operation errors
fn map_remove_error(
    error: std::io::Error,
    path: &str,
) -> FileError {
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => FileError::PermissionDenied(path.to_string()),
        std::io::ErrorKind::NotFound => FileError::NotFound(path.to_string()),
        std::io::ErrorKind::Other if is_file_busy(&error) => FileError::FileBusy(path.to_string()),
        _ => FileError::UnknownIo(format!("{path}: {error}")),
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

/// Converts a byte vector into a fixed-size 32-byte checksum array
///
/// # Parameters
/// - `checksum`: Input byte vector representing a checksum value
///
/// # Returns
/// - `Ok([u8; 32])`: Successfully converted checksum array
/// - `Err(String)`: Error with detailed reason if conversion fails
///
/// # Errors
/// Returns an error in the following cases:
/// 1. Input vector length is not exactly 32 bytes
/// 2. Input vector is empty (special case handling)
///
/// # Example
/// ```rust
/// use d_engine::file_io::convert_vec_checksum;
///
/// let vec_checksum = vec![0u8; 32];
/// let array_checksum = convert_vec_checksum(vec_checksum).unwrap();
/// assert_eq!(array_checksum, [0u8; 32]);
/// ```
pub fn convert_vec_checksum(checksum: Vec<u8>) -> Result<[u8; 32]> {
    match checksum.len() {
        // Exact size match - use efficient conversion
        32 => checksum.try_into().map_err(|_| {
            ConvertError::ConversionFailure("Conversion failed despite correct length".to_string()).into()
        }),

        // Handle empty vector as special case
        0 => Err(ConvertError::ConversionFailure("Empty checksum vector provided".to_string()).into()),

        // All other length mismatches
        len => Err(ConvertError::ConversionFailure(format!(
            "Invalid checksum length: expected 32 bytes, got {len} bytes"
        ))
        .into()),
    }
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

/// Computes a SHA-256 checksum for a directory by hashing the contents of its files.
///
/// IMPORTANT: Files are processed in lexicographical order by filename. This is critical because:
/// 1. File system enumeration order (via `read_dir`) is implementation-defined and
///    non-deterministic
/// 2. SHA-256 is order-sensitive: `hash(fileA + fileB) ≠ hash(fileB + fileA)`
///
/// The algorithm:
/// 1. Collects all top-level files in the directory (ignores subdirectories and symlinks)
/// 2. Sorts files by filename to ensure consistent processing order
/// 3. Reads each file's content in sorted order
/// 4. Updates hasher with each file's bytes sequentially
/// 5. Finalizes and returns the SHA-256 hash
///
/// Notes:
/// - Non-files (directories/symlinks) are silently ignored
/// - Only top-level files are processed (no recursion into subdirectories)
/// - File read order is determined by filename sort, not creation time or modification time
/// - Empty directories will return the SHA-256 hash of empty data
pub(crate) async fn compute_checksum_from_folder_path(folder_path: &Path) -> Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    let mut entries = fs::read_dir(folder_path).await.map_err(StorageError::IoError)?;

    // Collect files while preserving DirEntry information
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(StorageError::IoError)? {
        if entry.file_type().await.map_err(StorageError::IoError)?.is_file() {
            files.push(entry);
        }
    }

    // Critical sorting: Filenames compared as OsString but sorted by simple byte order.
    // This matches lexical order for valid UTF-8 names, and provides consistent ordering
    // for non-UTF names across platforms.
    files.sort_by_key(|entry| entry.file_name());

    // Process files in deterministic sorted order
    for entry in files {
        let data = fs::read(entry.path()).await.map_err(StorageError::IoError)?;
        hasher.update(data);
    }

    Ok(hasher.finalize().into())
}

/// Computes a SHA-256 checksum for a file by hashing its contents.
///
/// The algorithm:
/// 1. Opens the file at the specified path
/// 2. Reads the file content in chunks to handle large files efficiently
/// 3. Updates the hasher with each chunk of bytes
/// 4. Finalizes and returns the SHA-256 hash
///
/// Notes:
/// - Processes the entire file content sequentially
/// - Uses buffered reading to handle large files efficiently
/// - Consistent across platforms and file systems
pub(crate) async fn compute_checksum_from_file_path(file_path: &Path) -> Result<[u8; 32]> {
    let mut file = tokio::fs::File::open(file_path).await.map_err(StorageError::IoError)?;

    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 4096]; // 4KB buffer

    loop {
        let bytes_read = file.read(&mut buffer).await.map_err(StorageError::IoError)?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hasher.finalize().into())
}

/// Validates compressed file format using magic numbers and extensions
/// Referenced  flate2 header validation principles and  security practices
pub(crate) fn validate_compressed_format(path: &Path) -> Result<()> {
    // 1. Check if the file exists
    if !path.exists() {
        return Err(FileError::NotFound(path.display().to_string()).into());
    }
    // 2. Check file size
    if let Ok(metadata) = std::fs::metadata(path) {
        if metadata.len() < 10 {
            return Err(FileError::TooSmall(metadata.len()).into());
        }
    }

    // 3. Check the file extension
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| FileError::InvalidExt("Invalid file extension".into()))?;

    if !matches!(ext.to_lowercase().as_str(), "gz" | "tgz" | "snap") {
        return Err(FileError::InvalidExt(format!("Invalid compression extension: {ext}")).into());
    }

    // 4. Verify magic numbers (GZIP header checks)
    let mut file = std::fs::File::open(path).map_err(StorageError::IoError)?;
    let mut header = [0u8; 2];
    file.read_exact(&mut header).map_err(StorageError::IoError)?;

    // GZIP magic numbers: 0x1f 0x8b
    if header != [0x1f, 0x8b] {
        return Err(FileError::InvalidGzipHeader("Invalid GZIP header".to_string()).into());
    }

    Ok(())
}

pub(crate) async fn create_valid_snapshot<F>(
    path: &Path,
    data_setup: F,
) -> [u8; 32]
where
    F: FnOnce(&sled::Db),
{
    let temp_data_dir = tempfile::tempdir().unwrap();
    let db = init_sled_state_machine_db(temp_data_dir.path()).unwrap();
    data_setup(&db);
    let checksum = compute_checksum_from_folder_path(temp_data_dir.path()).await.unwrap();

    // Ensure parent directory exists before file creation
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }

    let file = File::create(path).await.unwrap();
    let gzip_encoder = async_compression::tokio::write::GzipEncoder::new(file);
    let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);
    tar_builder.append_dir_all(".", temp_data_dir.path()).await.unwrap();

    tar_builder.finish().await.unwrap();

    let mut gzip_encoder = tar_builder.into_inner().await.unwrap();
    gzip_encoder.shutdown().await.unwrap();

    checksum
}
