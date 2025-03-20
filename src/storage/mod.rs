mod raft_log;
mod sled_adapter;
mod state_machine;
mod state_storage;

pub use raft_log::*;
pub use sled_adapter::*;
pub use state_machine::*;
pub use state_storage::*;

// Module level utils
// -----------------------------------------------------------------------------

/// raft logs storage
/// (raft_log_db, state_machine_db, state_storage_db, snapshot_storge_db)
pub fn init_sled_storages(
    sled_db_root_path: String,
) -> std::result::Result<(sled::Db, sled::Db, sled::Db, sled::Db), std::io::Error> {
    use log::{debug, warn};
    use std::{io::ErrorKind, path::Path};

    debug!("init_sled_storages from path: {:?}", &sled_db_root_path);

    let path = &Path::new(&sled_db_root_path);
    let raft_log_db_path = path.join("raft_log");
    let state_machine_db_path = path.join("state_machine");
    let state_storage_db_path = path.join("state_storage");
    let snapshot_storage_db_path = path.join("snapshot_storage");

    let raft_log_db = sled::Config::default()
        .path(&raft_log_db_path)
        .cache_capacity(4 * 1024 * 1024 * 1024) //4GB
        // .flush_every_ms(Some(1))
        .use_compression(true)
        .compression_factor(1)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
        .map_err(|e| {
            warn!(
                "Try to open DB at this location: {:?} and failed: {:?}",
                raft_log_db_path, e
            );
            std::io::Error::new(ErrorKind::Other, e)
        })?;

    let state_machine_db = sled::Config::default()
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
            std::io::Error::new(ErrorKind::Other, e)
        })?;

    let state_storage_db = sled::Config::default()
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
            std::io::Error::new(ErrorKind::Other, e)
        })?;

    let snapshot_storage_db = sled::Config::default()
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
            std::io::Error::new(ErrorKind::Other, e)
        })?;

    Ok((
        raft_log_db,
        state_machine_db,
        state_storage_db,
        snapshot_storage_db,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        grpc::rpc_service::{ClusterMembership, NodeMeta},
        test_utils,
        util::{kv, skv},
        RaftRole, FOLLOWER, LEARNER,
    };

    /// # Case 1: restart
    ///
    /// ## Setup:
    /// 1. there was existing entry in local log db
    ///     key:1 - value:2
    /// 2. renew the db from file path
    ///
    /// ## Criterias:
    /// 1. find the same key value from local log db
    ///
    #[test]
    fn test_init_storages_case1() {
        test_utils::enable_logger();

        use prost::Message;
        let path = "/tmp/test_init_storages_case1".to_string();
        let state_key = skv("state_key".to_string());
        {
            let (raft_log_db, state_machine_db, state_storage_db, _snapshot_storage_db) =
                init_sled_storages(path.to_string()).unwrap();

            raft_log_db.insert(kv(1), kv(2)).expect("should succeed");
            state_machine_db
                .insert(state_key.clone(), kv(17))
                .expect("should succeed");

            //prepare a formal membership conf
            let cluster_membership = ClusterMembership {
                nodes: vec![
                    NodeMeta {
                        id: 2,
                        ip: "127.0.0.1".to_string(),
                        port: 10000,
                        role: FOLLOWER,
                    },
                    NodeMeta {
                        id: 3,
                        ip: "127.0.0.1".to_string(),
                        port: 10000,
                        role: FOLLOWER,
                    },
                    NodeMeta {
                        id: 4,
                        ip: "127.0.0.1".to_string(),
                        port: 10000,
                        role: LEARNER,
                    },
                    NodeMeta {
                        id: 5,
                        ip: "127.0.0.1".to_string(),
                        port: 10000,
                        role: LEARNER,
                    },
                ],
            };
            state_storage_db
                .insert(kv(11), cluster_membership.encode_to_vec())
                .expect("should succeed");
        }

        {
            let (raft_log_db, state_machine_db, state_storage_db, _snapshot_storage_db) =
                init_sled_storages(path.to_string()).unwrap();
            assert_eq!(
                Some(kv(2)),
                raft_log_db
                    .get(kv(1))
                    .expect("should succeed")
                    .map(|v| v.to_vec())
            );
            assert_eq!(
                Some(kv(17)),
                state_machine_db
                    .get(state_key)
                    .expect("should succeed")
                    .map(|v| v.to_vec())
            );

            if let Ok(Some(v)) = state_storage_db.get(kv(11)) {
                let v = v.to_vec();
                match ClusterMembership::decode(&v[..]) {
                    Err(_e) => {
                        assert!(false);
                    }
                    Ok(m) => {
                        assert_eq!(4, m.nodes.len());
                    }
                }
            } else {
                assert!(false);
            }
        }
    }
}
