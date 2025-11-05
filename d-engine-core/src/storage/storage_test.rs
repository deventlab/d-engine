use super::*;
use crate::convert::safe_kv;
use crate::convert::skv;
use d_engine_proto::cluster::ClusterMembership;
use d_engine_proto::cluster::NodeMeta;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::common::NodeStatus;

/// # Case 1: restart
///
/// ## Setup:
/// 1. there was existing entry in local log db key:1 - value:2
/// 2. renew the db from file path
///
/// ## Criterias:
/// 1. find the same key value from local log db
#[test]
fn test_init_storages_case1() {
    use prost::Message;
    let path = "/tmp/test_init_storages_case1".to_string();
    std::fs::remove_dir_all(&path).ok();
    let state_key = skv("state_key".to_string());
    {
        let (storage_engine_db, state_machine_db) = init_sled_storages(path.to_string()).unwrap();

        storage_engine_db.insert(safe_kv(1), &safe_kv(2)).expect("should succeed");
        state_machine_db
            .insert(state_key.clone(), &safe_kv(17))
            .expect("should succeed");

        //prepare a formal membership conf
        let cluster_membership = ClusterMembership {
            version: 1,
            nodes: vec![
                NodeMeta {
                    id: 2,
                    address: "127.0.0.1:10000".to_string(),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: 3,
                    address: "127.0.0.1:10000".to_string(),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: 4,
                    address: "127.0.0.1:10000".to_string(),
                    role: LEARNER,
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: 5,
                    address: "127.0.0.1:10000".to_string(),
                    role: LEARNER,
                    status: NodeStatus::Active.into(),
                },
            ],
        };
        storage_engine_db
            .insert(safe_kv(11), cluster_membership.encode_to_vec())
            .expect("should succeed");
    }

    {
        let (storage_engine_db, state_machine_db) = init_sled_storages(path.to_string()).unwrap();
        assert_eq!(
            Some(safe_kv(2).to_vec()),
            storage_engine_db.get(safe_kv(1)).expect("should succeed").map(|v| v.to_vec())
        );
        assert_eq!(
            Some(safe_kv(17).to_vec()),
            state_machine_db.get(&state_key).expect("should succeed").map(|v| v.to_vec())
        );

        let v = storage_engine_db.get(safe_kv(11)).unwrap().unwrap();
        let v = v.to_vec();
        let m = ClusterMembership::decode(&v[..]).unwrap();
        assert_eq!(4, m.nodes.len());
    }
}
