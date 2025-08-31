mod state_machine;
mod storage_engine;

pub use state_machine::*;
pub use storage_engine::*;

use crate::Error;

//---
// Database namespaces
/// Sled database tree namespaces
const RAFT_LOG_NAMESPACE: &str = "raft_log";
const RAFT_META_NAMESPACE: &str = "raft_meta";

/// raft logs storage
/// (raft_log_db, state_machine_db, state_storage_db, snapshot_storge_db)
#[doc(hidden)]
pub fn init_sled_storages(
    sled_db_root_path: impl AsRef<std::path::Path> + std::fmt::Debug
) -> Result<(sled::Db, sled::Db), Error> {
    tracing::debug!("init_sled_storages from path: {:?}", &sled_db_root_path);

    Ok((
        init_sled_storage_engine_db(&sled_db_root_path)?,
        init_sled_state_machine_db(&sled_db_root_path)?,
    ))
}

pub fn init_sled_log_tree_and_meta_tree(
    sled_db_root_path: impl AsRef<std::path::Path> + std::fmt::Debug,
    node_id: u32,
) -> Result<(sled::Tree, sled::Tree), Error> {
    let db = init_sled_storage_engine_db(&sled_db_root_path)?;
    let log_tree_name = format!("raft_log_{RAFT_LOG_NAMESPACE}_{node_id}");
    let meta_tree_name = format!("raft_meta_{RAFT_META_NAMESPACE}_{node_id}");
    let log_tree = db.open_tree(&log_tree_name)?;
    let meta_tree = db.open_tree(&meta_tree_name)?;

    Ok((log_tree, meta_tree))
}

#[doc(hidden)]
pub fn init_sled_storage_engine_db(
    sled_db_root_path: impl AsRef<std::path::Path> + std::fmt::Debug
) -> Result<sled::Db, Error> {
    tracing::debug!(
        "init_sled_storage_engine_db from path: {:?}",
        &sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let raft_log_db_path = path.join("storage_engine");

    sled::Config::default()
        .path(&raft_log_db_path)
        .cache_capacity(1024 * 1024 * 1024) //1GB
        .flush_every_ms(Some(10))
        .use_compression(true)
        .compression_factor(1)
        .mode(sled::Mode::HighThroughput)
        .segment_size(16_777_216) // 16MB
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            tracing::warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                raft_log_db_path,
                e
            );
            e.into()
        })
}
#[doc(hidden)]
pub fn init_sled_state_machine_db(
    sled_db_root_path: impl AsRef<std::path::Path> + std::fmt::Debug
) -> Result<sled::Db, Error> {
    tracing::debug!(
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
            tracing::warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                state_machine_db_path,
                e
            );
            e.into()
        })
}
