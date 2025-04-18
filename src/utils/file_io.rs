use crate::Result;
use crate::StorageError;
use log::debug;
use log::error;
use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

pub fn crate_parent_dir_if_not_exist(path: &PathBuf) -> Result<()> {
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
