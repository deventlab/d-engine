mod converters;
mod sled_state_machine;
mod sled_storage_engine;
pub(crate) use converters::{safe_kv, safe_vk, safe_vk_ivec};
pub use sled_state_machine::*;
pub use sled_storage_engine::*;

use sha2::Digest;
use sha2::Sha256;
use std::path::Path;
use tokio::fs;

#[cfg(test)]
mod sled_engine_test;

pub(crate) async fn compute_checksum_from_folder_path(
    folder_path: &Path
) -> Result<[u8; 32], std::io::Error> {
    let mut hasher = Sha256::new();
    let mut entries = fs::read_dir(folder_path).await?;

    // Collect files while preserving DirEntry information
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            files.push(entry);
        }
    }

    // Critical sorting: Filenames compared as OsString but sorted by simple byte order.
    // This matches lexical order for valid UTF-8 names, and provides consistent ordering
    // for non-UTF names across platforms.
    files.sort_by_key(|entry| entry.file_name());

    // Process files in deterministic sorted order
    for entry in files {
        let data = fs::read(entry.path()).await?;
        hasher.update(data);
    }

    Ok(hasher.finalize().into())
}
