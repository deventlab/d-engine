use std::sync::Arc;

use sled::Batch;

use super::*;
use crate::{
    init_sled_storages,
    test_utils::{self, setup_raft_components},
    util::kv,
    StateMachine,
};

#[test]
fn test_apply_committed_raft_logs_in_batch() {
    let root_path = "/tmp/test_apply_committed_raft_logs_in_batch";
    let context = setup_raft_components(root_path, None, false);
    let leader_id = 1;
    //step1: prepare some entries inside state machine
    let mut batch = Batch::default();
    for i in 1..=3 {
        batch.insert(kv(i), kv(i));
    }
    context.state_machine.apply_batch(batch).expect("should succeed");
    assert_eq!(context.state_machine.last_entry_index(), Some(3));
}

fn init(path: &str) -> Arc<sled::Db> {
    let (_raft_log_db, state_machine_db, _state_storage_db, _snapshot_storage_db) =
        init_sled_storages(path.to_string()).unwrap();
    Arc::new(state_machine_db)
}

/// # Case 1: test if node restart, the state machine entries should load from disk
///
#[test]
fn test_state_machine_flush() {
    test_utils::enable_logger();

    let p = "/tmp/test_state_machine_flush";
    {
        let _ = std::fs::remove_dir_all(p);
        println!("Test setup ...");
        let state_machine_db = init(p);
        let state_machine = Arc::new(RaftStateMachine::new(1, state_machine_db));
        let mut batch = Batch::default();
        batch.insert(kv(1), kv(1));
        batch.insert(kv(2), kv(2));
        state_machine.apply_batch(batch).expect("should succeed");
        state_machine.flush().expect("should succeed");
        println!(">>state_machine disk length: {:?}", state_machine.len());
    }

    {
        let state_machine_db = init(p);
        let state_machine = RaftStateMachine::new(1, state_machine_db);
        assert_eq!(state_machine.len(), 2);
        assert_eq!(
            state_machine.get(&kv(2)).unwrap_or(Some(kv(0))),
            Some(kv(2))
        );
    }
}
