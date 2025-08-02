mod buffered_raft_log;
mod raft_log;
mod sled_adapter;
mod snapshot_path_manager;
mod state_machine;
mod storage_engine;

#[doc(hidden)]
pub use buffered_raft_log::*;
#[doc(hidden)]
pub use raft_log::*;
#[doc(hidden)]
pub use sled_adapter::*;
#[doc(hidden)]
pub(crate) use snapshot_path_manager::*;
#[doc(hidden)]
pub use state_machine::*;
#[doc(hidden)]
pub use storage_engine::*;

#[cfg(test)]
mod buffered_raft_log_test;
#[cfg(test)]
mod storage_test;

use std::path::Path;

use tracing::debug;
use tracing::warn;

// -----------------------------------------------------------------------------
// Database namespaces
/// Sled database tree namespaces
const RAFT_LOG_NAMESPACE: &str = "raft_log";
const RAFT_META_NAMESPACE: &str = "raft_meta";

/// raft logs storage
/// (raft_log_db, state_machine_db, state_storage_db, snapshot_storge_db)
pub fn init_sled_storages(
    sled_db_root_path: impl AsRef<Path> + std::fmt::Debug
) -> std::result::Result<(sled::Db, sled::Db, sled::Db, sled::Db), std::io::Error> {
    debug!("init_sled_storages from path: {:?}", &sled_db_root_path);

    Ok((
        init_sled_raft_log_db(&sled_db_root_path)?,
        init_sled_state_machine_db(&sled_db_root_path)?,
        init_sled_state_storage_db(&sled_db_root_path)?,
        init_sled_snapshot_storage_db(&sled_db_root_path)?,
    ))
}

pub fn init_sled_raft_log_db(
    sled_db_root_path: impl AsRef<Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, std::io::Error> {
    debug!("init_sled_raft_log_db from path: {:?}", &sled_db_root_path);

    let path = sled_db_root_path.as_ref();
    let raft_log_db_path = path.join("raft_log");

    sled::Config::default()
        .path(&raft_log_db_path)
        .cache_capacity(4 * 1024 * 1024 * 1024) //4GB
        // .flush_every_ms(Some(1000))
        .use_compression(true)
        .compression_factor(1)
        .mode(sled::Mode::HighThroughput)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                raft_log_db_path, e
            );
            std::io::Error::other(e)
        })
}

pub fn init_sled_storage_engine_db(
    sled_db_root_path: impl AsRef<Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, std::io::Error> {
    debug!(
        "init_sled_storage_engine_db from path: {:?}",
        &sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let raft_log_db_path = path.join("raft_log");

    sled::Config::default()
        .path(&raft_log_db_path)
        .cache_capacity(4 * 1024 * 1024 * 1024) //4GB
        // .flush_every_ms(Some(1000))
        .use_compression(true)
        .compression_factor(1)
        .mode(sled::Mode::HighThroughput)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                raft_log_db_path, e
            );
            std::io::Error::other(e)
        })
}

pub fn init_sled_state_machine_db(
    sled_db_root_path: impl AsRef<Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, std::io::Error> {
    debug!(
        "init_sled_state_machine_db from path: {:?}",
        sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let state_machine_db_path = path.join("state_machine");

    sled::Config::default()
        .path(&state_machine_db_path)
        .cache_capacity(10 * 1024 * 1024) //10MB
        .flush_every_ms(Some(3))
        .use_compression(true)
        .compression_factor(1)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                state_machine_db_path, e
            );
            std::io::Error::other(e)
        })
}
pub fn init_sled_state_storage_db(
    sled_db_root_path: impl AsRef<Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, std::io::Error> {
    debug!(
        "init_sled_state_storage_db from path: {:?}",
        &sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let state_storage_db_path = path.join("state_storage");

    sled::Config::default()
        .path(&state_storage_db_path)
        .cache_capacity(10 * 1024 * 1024) //10MB
        .flush_every_ms(Some(3))
        .use_compression(true)
        .compression_factor(1)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                state_storage_db_path, e
            );
            std::io::Error::other(e)
        })
}

pub fn init_sled_snapshot_storage_db(
    sled_db_root_path: impl AsRef<Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, std::io::Error> {
    debug!(
        "init_sled_snapshot_storage_db from path: {:?}",
        &sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let snapshot_storage_db_path = path.join("snapshot_storage");

    sled::Config::default()
        .path(&snapshot_storage_db_path)
        .cache_capacity(10 * 1024 * 1024) //10MB
        .flush_every_ms(Some(3))
        .use_compression(true)
        .compression_factor(1)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                snapshot_storage_db_path, e
            );
            std::io::Error::other(e)
        })
}
