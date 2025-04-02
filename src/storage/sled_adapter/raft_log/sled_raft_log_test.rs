use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use futures::future::join_all;
use log::debug;
use tokio::time::Instant;

use super::*;
use crate::alias::ROF;
use crate::convert::kv;
use crate::grpc::rpc_service::Entry;
use crate::init_sled_storages;
use crate::test_utils::reset_dbs;
use crate::test_utils::{self};
use crate::RaftLog;
use crate::RaftTypeConfig;
use crate::SledStateStorage;

struct TestContext {
    // s: Arc<RaftState>,
    raft_log: Arc<ROF<RaftTypeConfig>>,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        println!("Test teardown ...");
        // self.s.db.drop_tree("logs").expect("drop successfully!");
        let _ = std::fs::remove_dir_all("/tmp/test_db");
    }
}

fn setup(path: &str) -> TestContext {
    println!("Test setup ...");
    let (raft_log_db, _, state_storage_db, _) = reset_dbs(path);
    let arc_raft_log_db = Arc::new(raft_log_db);

    let sled_state_storage = Arc::new(SledStateStorage::new(Arc::new(state_storage_db)));
    let sled_raft_log = Arc::new(SledRaftLog::new(arc_raft_log_db, None));

    TestContext {
        // s: Arc::new(RaftState::new(
        //     1,
        //     // sled_raft_log.clone(),
        //     sled_state_storage,
        //     None,
        // )),
        raft_log: sled_raft_log,
    }
}

fn init(
    path: &str,
    commit_index: u64,
) -> Arc<ROF<RaftTypeConfig>> {
    let (raft_log_db, _state_machine_db, _state_storage_db, _snapshot_storage_db) =
        init_sled_storages(path.to_string()).unwrap();
    Arc::new(SledRaftLog::new(Arc::new(raft_log_db), Some(commit_index)))
}

#[test]
fn test_get_as_vec() {
    let c = setup("/tmp/test_get_as_vec");

    let value = Entry {
        index: 1,
        term: 1,
        command: vec![1; 8],
    };

    c.raft_log.insert_batch(vec![value.clone()]).expect("should succeed");

    if let Some(entry) = c.raft_log.get_entry_by_index(1) {
        assert_eq!(value, entry);
    } else {
        assert!(false);
    }
}

fn insert(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    key: u64,
) {
    let log = Entry {
        index: key,
        term: 7,
        command: Vec::new(),
    };
    raft_log.insert_batch(vec![log]).expect("should succeed");
}

#[test]
fn test_get_range1() {
    let context = setup("/tmp/test_get_range1");
    let i1: u64 = 11;
    let i2: u64 = 12;
    let i3: u64 = 13;
    let i4: u64 = 14;
    test_utils::simulate_insert_proposal(&context.raft_log, vec![i1, i2, i3, i4], 4);
    let list = context.raft_log.get_entries_between(1..=2);

    // let mut list = Vec::<Entry>::new();
    // for i in r {
    //     let mut l = Entry::default();
    //     l.unpack(&mut Cursor::new(i)).expect("unpack failed.");
    //     list.push(l);
    // }
    assert_eq!(2, list.len());
    assert_eq!(list[0].index, 1);
    assert_eq!(list[1].index, 2);
}

#[test]
fn test_get_range2() {
    let context = setup("/tmp/test_get_range2");

    for i in 1..300 {
        insert(&context.raft_log, i);
    }

    let list = context.raft_log.get_entries_between(255..=255);

    // let mut list = Vec::<Entry>::new();
    // for i in r {
    //     let mut l = Entry::default();
    //     l.unpack(&mut Cursor::new(i)).expect("unpack failed.");
    //     list.push(l);
    // }
    assert_eq!(1, list.len());
    assert_eq!(list[0].index, 255);
}

// # Case 1: test follower f delete conflicts (Figure 7 in raft paper)
///
// ## Setup:
// 1.
//     leader:     log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6),
// log10(6)     follower_f: log1(1), log2(1), log3(1), log4(2), log5(2), log6(2), log7(3), log8(3),
// log9(3), log10(3), log11(3)
///
// ## Criterias:
// 1. next_id been updated to:
// Follower	lastIndex	nextIndex
// follower_a	    9	    10
// follower_b	    4	    5
// follower_c	    10	    11
// follower_d	    12	    11
// follower_e	    7	    6
// follower_f	    11  	4
///
#[test]
fn test_filter_out_conflicts_and_append_case1() {
    let context = setup("/tmp/test_filter_out_conflicts_and_append_case1");
    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![2, 3], 2);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![4], 4);

    //case 1
    let l1 = Entry {
        index: 2,
        term: 2,
        command: Vec::new(),
    };
    let l2 = Entry {
        index: 3,
        term: 3,
        command: Vec::new(),
    };
    let new_ones = vec![l1, l2];
    let prev_log_index = 1;
    let last_applied = context
        .raft_log
        .filter_out_conflicts_and_append(prev_log_index, new_ones);
    assert_eq!(last_applied, 3);

    if let Some(e) = context.raft_log.get_entry_by_index(3) {
        assert_eq!(e.term, 3);
    } else {
        assert!(false);
    }
}
#[test]
fn test_filter_out_conflicts_and_append() {
    let context = setup("/tmp/test_filter_out_conflicts_and_append");
    // let state = &context.s;
    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![2, 3], 2);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![4], 4);

    //case 1
    let l1 = Entry {
        index: 2,
        term: 2,
        command: Vec::new(),
    };
    let l2 = Entry {
        index: 3,
        term: 3,
        command: Vec::new(),
    };
    let new_ones = vec![l1, l2];
    let prev_log_index = 1;
    let last_applied = context
        .raft_log
        .filter_out_conflicts_and_append(prev_log_index, new_ones);
    assert_eq!(last_applied, 3);

    if let Some(e) = context.raft_log.get_entry_by_index(3) {
        assert_eq!(e.term, 3);
    } else {
        assert!(false);
    }

    //case 2
    let l3 = Entry {
        index: 4,
        term: 2,
        command: Vec::new(),
    };
    let new_ones = vec![l3];
    let prev_log_index = 1;
    let last_applied = context
        .raft_log
        .filter_out_conflicts_and_append(prev_log_index, new_ones);
    assert_eq!(last_applied, 4);
    if let Some(e) = context.raft_log.get_entry_by_index(4) {
        assert_eq!(e.term, 2);
    } else {
        assert!(false);
    }
}

#[test]
fn test_retrieve_one_entry_for_this_follower_case1() {
    let context = setup("/tmp/test_retrieve_one_entry_for_this_follower_case1");
    let follower_id = 1;
    let next_id = 1;
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1], 1);

    let entries = context
        .raft_log
        .retrieve_one_entry_for_this_follower(follower_id, next_id);
    assert!(entries != vec![0, 0]);
}

#[test]
fn test_retrieve_one_entry_for_this_follower_case2() {
    let context = setup("/tmp/test_retrieve_one_entry_for_this_follower_case2");

    let follower_id = 1;
    let next_id = 2;

    test_utils::simulate_insert_proposal(&context.raft_log, vec![1], 1);

    let entries = context
        .raft_log
        .retrieve_one_entry_for_this_follower(follower_id, next_id);
    assert!(entries.is_empty());
}

#[test]
fn test_get_last_raft_log() {
    let context = setup("/tmp/test_get_last_raft_log");

    context.raft_log.reset().expect("should succeed");
    let mut vec_1 = Vec::new();
    for id in 1..300 {
        vec_1.push(id);
    }
    test_utils::simulate_insert_proposal(&context.raft_log, vec_1, 1);

    let mut vec_2 = Vec::new();
    for id in 1..3 {
        vec_2.push(id);
    }
    test_utils::simulate_insert_proposal(&context.raft_log, vec_2, 1);
    if let Some(last) = context.raft_log.last() {
        assert_eq!(last.index, 301);
    }
}

#[test]
fn test_sled_last() {
    let context = setup("/tmp/test_sled_last");

    test_utils::simulate_insert_proposal(&context.raft_log, (1..=300).collect(), 1);
    // for i in 1..300 {
    //     debug!("key:{:?}, buffer:{:?}", i, kv(i));
    //     if let Err(e) = context.raft_log.db.insert(kv(i), kv(i)) {
    //         eprint!("error: {:?}", e);
    //         assert!(false);
    //     }
    // }
    // context.raft_log.db.flush().expect("flush failed!");

    if let Some(l) = context.raft_log.last() {
        let last = l.index;
        let len = context.raft_log.len();
        debug!("last: {:?}, while len: {:?}", last, len);
        assert_eq!(last, len as u64);
    } else {
        assert!(false);
    }
}
#[test]
fn test_sled_last_max() {
    let context = setup("/tmp/test_sled_last_max");

    let max = std::u64::MAX; //max of u64
    test_utils::simulate_insert_proposal(&context.raft_log, ((max - 1)..max).collect(), 1);
    // for i in max - 1..max {
    //     debug!("key:{:?}, buffer:{:?}", i, kv(i));
    //     if let Err(e) = context.raft_log.db.insert(kv(i), kv(i)) {
    //         eprint!("error: {:?}", e);
    //         assert!(false);
    //     }
    // }
    // context.raft_log.db.flush().expect("flush failed!");

    if let Some(l) = context.raft_log.last() {
        assert_eq!(l.index, 1);
    } else {
        assert!(false);
    }
}

///3 nodes, leader append 1000 logs, the other two logs can not get 1000
/// entries
// to test insert_one_client_command and get_entries_between two functions
#[test]
fn test_insert_one_client_command() {
    let context = setup("/tmp/test_insert_one_client_command");

    context.raft_log.reset().expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..2001 {
        v.push(i);
    }
    test_utils::simulate_insert_proposal(&context.raft_log, v, 1);

    let mut count = 900;
    for entry in context.raft_log.get_entries_between(900..=2000) {
        assert_eq!(entry.index, count);
        count += 1;
    }
}

#[test]
fn test_get_raft_log_entry_between() {
    let context = setup("/tmp/test_get_raft_log_entry_between");

    context.raft_log.reset().expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..2001 {
        v.push(i);
    }
    test_utils::simulate_insert_proposal(&context.raft_log, v, 1);

    let log_len = context.raft_log.last_entry_id();
    let result = context.raft_log.get_entries_between(900..=log_len);
    let mut count = 900;
    for entry in &result {
        assert_eq!(entry.index, count);
        count += 1;
    }
    assert_eq!(result.len(), 1101);

    let result = context.raft_log.get_entries_between(900..=log_len - 1);
    assert_eq!(result.len(), 1100);

    //end index will not be returned as result
    let result = context.raft_log.get_entries_between(0..=log_len);
    assert_eq!(result.len(), log_len as usize);

    //index starts from 1
    let result = context.raft_log.get_entries_between(0..=0);
    assert_eq!(result.len(), 0);

    //index starts from 1
    let result = context.raft_log.get_entries_between(0..=1);
    assert_eq!(result.len(), 1);
}
// Test duplicated insert
// Leader might received duplicated commands from clients in several RPC
// request. But, leader should not treat them as duplicated entries. They are
// just sequence events.
///
#[test]
fn test_insert_one_client_command_dup_case() {
    let context = setup("/tmp/test_insert_one_client_command_dup_case");

    context.raft_log.reset().expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..10 {
        v.push(i);
    }
    test_utils::simulate_insert_proposal(&context.raft_log, v, 1);

    let len = context.raft_log.last_entry_id();
    let mut v = Vec::new();
    for i in 1..10 {
        v.push(i + len);
    }
    test_utils::simulate_insert_proposal(&context.raft_log, v, 2);

    assert_eq!(context.raft_log.last_entry_id(), 18);
    if let Some(last) = context.raft_log.last() {
        assert_eq!(last.index, 18);
    } else {
        assert!(false);
    }
}

// # Test client propose commands which contains insert and delete
///
// ## Criterias:
// 1. all client proposal should be recorded in RaftLog without lose
///
#[test]
fn test_client_proposal_insert_delete() {
    let context = setup("/tmp/test_apply");

    context.raft_log.reset().expect("reset successfully!");

    test_utils::simulate_insert_proposal(&context.raft_log, (1..=10).collect(), 1);
    assert_eq!(context.raft_log.last_entry_id(), 10);

    test_utils::simulate_delete_proposal(&context.raft_log, 1..=3, 1);
    assert_eq!(context.raft_log.last_entry_id(), 13);
    assert_eq!(context.raft_log.len(), 13);

    test_utils::simulate_insert_proposal(&context.raft_log, (11..=20).collect(), 1);

    assert_eq!(context.raft_log.last_entry_id(), 23);
    assert_eq!(context.raft_log.len(), 23);
}

#[test]
fn test_load_uncommitted_from_db_to_cache() {
    let context = setup("/tmp/test_load_uncommitted_from_db_to_cache");

    context.raft_log.reset().expect("reset successfully!");

    let mut entries = Vec::new();
    for i in 1..10 {
        entries.push(Entry {
            index: i,
            term: 1,
            command: vec![],
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    let len = context.raft_log.last_entry_id();
    context.raft_log.load_uncommitted_from_db_to_cache(8, len);
    for j in 8..len + 1 {
        if context.raft_log.get_from_cache(&kv(j)).is_none() {
            assert!(false);
        }
    }
}

///Bug: setup logger with error: SetGlobalDefaultError("a global default trace
/// dispatcher has already been set")
// #[traced_test]
#[test]
fn test_delete_entries_before() {
    let context = setup("/tmp/test_delete_entries_before");

    context.raft_log.reset().expect("reset successfully!");

    let mut entries = Vec::new();
    for i in 1..10 {
        entries.push(Entry {
            index: i,
            term: 1,
            command: vec![],
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    //..
    // assume we have generated snapshot until $last_applied,
    // now we can clean the local logs
    let last_applied = 3;
    context
        .raft_log
        .delete_entries_before(last_applied)
        .expect("should succeed");

    assert_eq!(9, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.get_entry_by_index(3));
    assert_eq!(None, context.raft_log.get_entry_by_index(2));
    assert_eq!(None, context.raft_log.get_entry_by_index(1));
}

#[test]
fn test_get_first_raft_log_entry_id_after_delete_entries() {
    let context = setup("/tmp/test_get_first_raft_log_entry_id_after_delete_entries");

    context.raft_log.reset().expect("reset successfully!");
    let mut entries = Vec::new();
    for i in 1..=10 {
        entries.push(Entry {
            index: i,
            term: 1,
            command: vec![],
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    let last_applied = 4;
    context
        .raft_log
        .delete_entries_before(last_applied)
        .expect("should succeed");
    assert_eq!(10, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.get_entry_by_index(3));
    assert_eq!(None, context.raft_log.get_entry_by_index(2));
    assert_eq!(None, context.raft_log.get_entry_by_index(1));

    assert_eq!(context.raft_log.first_entry_id(), 5);
}

#[test]
fn test_get_span_between_first_entry_and_last_entry_after_deleting() {
    let context = setup("/tmp/test_get_span_between_first_entry_and_last_entry_after_deleting");

    context.raft_log.reset().expect("reset successfully!");
    let mut entries = Vec::new();
    for i in 1..=10 {
        entries.push(Entry {
            index: i,
            term: 1,
            command: vec![],
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    let last_applied = 4;
    context
        .raft_log
        .delete_entries_before(last_applied)
        .expect("should succeed");
    assert_eq!(10, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.get_entry_by_index(3));
    assert_eq!(None, context.raft_log.get_entry_by_index(2));
    assert_eq!(None, context.raft_log.get_entry_by_index(1));

    assert_eq!(
        context.raft_log.span_between_first_entry_and_last_entry(),
        10 - last_applied
    );
}

// Case 1: we have multi thread working concurrently
//     each thread should get unique pre allocated index for pending apply
// entries     At last when we retrieve last entry of the locallog, we should
// see the last pre allocated index
///
#[tokio::test]
async fn test_pre_allocate_raft_logs_next_index_case1() {
    let context = setup("/tmp/test_pre_allocate_raft_logs_next_index_case1");

    context.raft_log.reset().expect("reset successfully!");
    let start = context.raft_log.last_entry_id();
    println!("start: {}", start);

    let mut handles = Vec::new();

    let i = 10;
    let j = 10;
    for _ in 1..=i {
        let cloned_raft_log = context.raft_log.clone();
        let handle = tokio::spawn(async move {
            let mut entries: Vec<Entry> = Vec::new();
            for i in 1..=j {
                entries.push(Entry {
                    index: cloned_raft_log.pre_allocate_raft_logs_next_index(),
                    term: 1,
                    command: kv(i),
                });
            }
            cloned_raft_log.insert_batch(entries.clone()).expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{}", end);

    assert_eq!(start + i * j, end);
}

// Case 2: we have mutil thread working concurrently
//     there might be one thread failed to apply the local log entries
//     which cases some log entry index might not really exists inside local
// logs     we still want to validate the last log entry id is the same one as
// we expected #[ignore = "architecture changes, this case will not exist"]
#[tokio::test]
async fn test_pre_allocate_raft_logs_next_index_case2() {
    let context = setup("/tmp/test_pre_allocate_raft_logs_next_index_case2");

    context.raft_log.reset().expect("reset successfully!");
    let start = context.raft_log.last_entry_id();
    println!("start: {}", start);

    let mut handles = Vec::new();

    let i = 10;
    let k = 3; //make sure k<i
    let j = 10;

    // simulated failed apply thread
    for _ in 1..=k {
        let cloned_raft_log = context.raft_log.clone();
        let handle = tokio::spawn(async move {
            let mut entries: Vec<Entry> = Vec::new();
            for i in 1..=j {
                entries.push(Entry {
                    index: cloned_raft_log.pre_allocate_raft_logs_next_index(),
                    term: 1,
                    command: kv(i),
                });
            }
        });
        handles.push(handle);
    }

    for _ in k + 1..=i {
        let cloned_raft_log = context.raft_log.clone();
        let handle = tokio::spawn(async move {
            let mut entries: Vec<Entry> = Vec::new();
            for i in 1..=j {
                entries.push(Entry {
                    index: cloned_raft_log.pre_allocate_raft_logs_next_index(),
                    term: 1,
                    command: kv(i),
                });
            }
            cloned_raft_log.insert_batch(entries.clone()).expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{}", end);

    assert_eq!(start + i * j, end);
    assert_eq!(
        context.raft_log.span_between_first_entry_and_last_entry(),
        i * j - (k * j + 1) + 1
    );
}

// Case 1: stress testing with 100K entries,
//     the entries was insert successfully and last log entry id is as expected
#[ignore = "architecture changes, this case will not exist"]
#[tokio::test]
async fn test_insert_batch_logs_case1() {
    let context = setup("/tmp/test_insert_batch_logs_case1");

    context.raft_log.reset().expect("reset successfully!");
    let start = context.raft_log.last_entry_id();
    println!("start: {}", start);

    let mut handles = Vec::new();

    let i = 1000;
    let j = 100;

    for _ in 1..=i {
        let cloned_raft_log = context.raft_log.clone();
        let handle = tokio::spawn(async move {
            let mut entries: Vec<Entry> = Vec::new();
            for i in 1..=j {
                entries.push(Entry {
                    index: cloned_raft_log.pre_allocate_raft_logs_next_index(),
                    term: 1,
                    command: kv(i),
                });
            }
            cloned_raft_log.insert_batch(entries.clone()).expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{}", end);

    assert_eq!(start + i * j, end);
}

// Case 2: combine 'filter_out_conflicts_and_append', 'delete_entries_before'
//     and 'insert_batch' actions
// step1: current node is Leader at T1, so insert_batch is invoked, contains
// log_1-log_10(with term: 1). step2: current node starts generating snapshots,
// so delete_entries_before is invoked step3: fake the current Leader restarts
// step4: new Leader elected(contains log_1-log_8(with term: 1)), current node
// is Follower step5: new Leader insert log_9, log_10 with term:2.
// step6: current node receives new Leader's append request
//     so it starts filter_out_conflicts_and_append, but new entries' term is 2
///
// Check the result:
//     1/ current node's last entry id is expected: 10 with term 2
#[tokio::test]
async fn test_insert_batch_logs_case2() {
    //preparation
    let ex_leader_id = 1;
    let new_leader_id = 2;
    let (raft_log_db, _, state_storage_db, _) = reset_dbs("/tmp/test_insert_batch_logs_case2_node1");

    let sled_state_storage = Arc::new(SledStateStorage::new(Arc::new(state_storage_db)));
    let ex_leader_sled_raft_log = Arc::new(SledRaftLog::new(Arc::new(raft_log_db), None));

    let (raft_log_db, _, state_storage_db, _) = reset_dbs("/tmp/test_insert_batch_logs_case2_node2");
    let sled_state_storage = Arc::new(SledStateStorage::new(Arc::new(state_storage_db)));
    let new_leader_sled_raft_log = Arc::new(SledRaftLog::new(Arc::new(raft_log_db), None));

    let mut entries: Vec<Entry> = Vec::new();
    for i in 1..=7 {
        entries.push(Entry {
            index: ex_leader_sled_raft_log.pre_allocate_raft_logs_next_index(),
            term: 1,
            command: kv(i),
        });
    }
    ex_leader_sled_raft_log
        .insert_batch(entries.clone())
        .expect("should succeed");
    new_leader_sled_raft_log
        .insert_batch(entries.clone())
        .expect("should succeed");

    //ex_leader has one more entry inside local log but not commited yet.
    ex_leader_sled_raft_log
        .insert_batch(vec![Entry {
            index: ex_leader_sled_raft_log.pre_allocate_raft_logs_next_index(),
            term: 1,
            command: kv(8),
        }])
        .expect("should succeed");
    //simulating step2: ex_leader generate snapshots:
    let last_applied = 3;
    ex_leader_sled_raft_log
        .delete_entries_before(last_applied)
        .expect("should succeed");

    //simulating step4,step5
    let mut handles = Vec::new();
    let mut new_leader_entries: Vec<Entry> = Vec::new();
    for i in 8..=10 {
        new_leader_entries.push(Entry {
            index: new_leader_sled_raft_log.pre_allocate_raft_logs_next_index(),
            term: 2,
            command: kv(i),
        });
    }
    let cloned_new_leader_entries = new_leader_entries.clone();
    let handle = tokio::spawn(async move {
        new_leader_sled_raft_log
            .insert_batch(cloned_new_leader_entries.clone())
            .expect("should succeed");
    });
    handles.push(handle);
    let _results = join_all(handles).await;

    println!("new_leader_entries: {:?}", new_leader_entries.clone());
    // simulating step6:
    let ex_leader_last_appled_id =
        ex_leader_sled_raft_log.filter_out_conflicts_and_append(0, new_leader_entries.clone());
    assert_eq!(ex_leader_last_appled_id, 10);

    //validations:
    if let Some(ex_leader_last_entry) = ex_leader_sled_raft_log.last() {
        assert_eq!(ex_leader_last_entry.index, 10);
        assert_eq!(ex_leader_last_entry.term, 2);
    }

    if let Some(entry) = ex_leader_sled_raft_log.get_entry_by_index(7) {
        assert_eq!(entry.term, 1);
    }
    if let Some(entry) = ex_leader_sled_raft_log.get_entry_by_index(8) {
        assert_eq!(entry.term, 2);
    }
}

// # Case 1.1: cluster just start
// 1. node's locallog is empty
// 2. prev_log_index is 0
///
// ## Criterias:
// 1. prev_log_ok return true
#[test]
fn test_prev_log_ok_case_1_1() {
    let context = setup("/tmp/test_prev_log_ok_case_1_1");

    context.raft_log.reset().expect("reset successfully!");
    assert!(context.raft_log.prev_log_ok(0, 0, 0));
}

// # Case 1.2: normal case
// 1. node's locallog is not empty, which contains log1(1), log2(1), log3(2)
// 2. prev_log_index: 2, prev_log_term: 1
///
// ## Criterias:
// 1. prev_log_ok return true
#[test]
fn test_prev_log_ok_case_1_2() {
    let context = setup("/tmp/test_prev_log_ok_case_1_2");

    context.raft_log.reset().expect("reset successfully!");
    // so its locallog is not empty, it contains some log (log-1,log-2)
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1, 2], 1);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![3], 2);
    // prev_log index > 0
    let prev_log_index = 2;
    let prev_log_term = 1;
    assert!(context.raft_log.prev_log_ok(prev_log_index, prev_log_term, 0));
}

// # Case 1.3: conflict case
// 1. node's locallog is not empty, which contains log1(1), log2(1), log3(2)
// 2. prev_log_index: 3, prev_log_term: 1
///
// ## Criterias:
// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_1_3() {
    let context = setup("/tmp/test_prev_log_ok_case_1_3");

    context.raft_log.reset().expect("reset successfully!");
    // so its locallog is not empty, it contains some log (log-1,log-2)
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1, 2], 1);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![3], 2);
    // prev_log index > 0
    let prev_log_index = 3;
    let prev_log_term = 1;
    assert!(!context.raft_log.prev_log_ok(prev_log_index, prev_log_term, 0));
}

// # Case 1.4: conflict case
// 1. node's locallog is not empty, which contains log1(1), log2(1), log3(2)
// 2. prev_log_index: 3, prev_log_term: 3
///
// ## Criterias:
// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_1_4() {
    let context = setup("/tmp/test_prev_log_ok_case_1_4");

    context.raft_log.reset().expect("reset successfully!");
    // so its locallog is not empty, it contains some log (log-1,log-2)
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1, 2], 1);
    test_utils::simulate_insert_proposal(&context.raft_log, vec![3], 2);
    // prev_log index > 0
    let prev_log_index = 3;
    let prev_log_term = 3;
    assert!(!context.raft_log.prev_log_ok(prev_log_index, prev_log_term, 0));
}

// # Case 1.5: conflict case
// 1. node's locallog is not empty, which contains log1(1), log2(2), log3(3), log4(3)
// 2. prev_log_index: 4, prev_log_term: 6
///
// ## Criterias:
// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_1_5() {}

// # Case 2.4: snapshot related test case
// 1. the current node is learner, it just installed Leader's snapshot
// 2. so its locallog is not empty, it contains some log (log-1, 1), (log-2,1)
// 3. while its last_applied is 10 which means log-10 has been committed
// 4. prev_log_index: 11, prev_log_term: 1
///
// ## Criterias:
// 1. prev_log_ok return false
///
#[test]
fn test_prev_log_ok_case_2_4() {
    let context = setup("/tmp/test_prev_log_ok_case_2_4");

    let last_applied = 10;
    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1, 2], 1);
    let prev_log_index = 11;
    let prev_log_term = 1;
    assert!(
        !context
            .raft_log
            .prev_log_ok(prev_log_index, prev_log_term, last_applied)
    );
}
#[test]
fn test_apply_and_then_get_last() {
    let context = setup("/tmp/test_apply_and_then_get_last");

    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_proposal(&context.raft_log, vec![1, 2], 1);
    assert_eq!(2, context.raft_log.last_entry_id());
}

// # Case 1: test if node restart, the local log cache should load from disk
///
#[test]
fn test_new_case1() {
    test_utils::enable_logger();

    let p = "/tmp/test_new_case1";
    {
        let _ = std::fs::remove_dir_all(p);
        println!("Test setup ...");
        let commit_index = 0;
        let raft_log = init(p, commit_index);
        test_utils::simulate_insert_proposal(&raft_log, vec![1, 2], 1);

        assert_eq!(raft_log.cached_mapped_entries_len(), 2);
        assert_eq!(raft_log.cached_length(), 2);
        assert_eq!(raft_log.cached_next_id(), 3);
        raft_log.flush().expect("should succeed");
        println!(">>RaftLog disk length: {:?}", raft_log.len());
    }

    {
        let raft_log = init(p, 1);
        assert_eq!(raft_log.cached_mapped_entries_len(), 1);
        assert_eq!(raft_log.cached_length(), 2);
        assert_eq!(raft_log.cached_next_id(), 3);
    }

    // Test if we try to pass commit_index > len, it should not panic
    {
        let raft_log = init(p, 3);
        assert_eq!(raft_log.cached_mapped_entries_len(), 0);
    }
}
#[test]
fn test_pre_allocate_raft_logs_next_index() {
    let context = setup("/tmp/test_pre_allocate_raft_logs_next_index");

    assert_eq!(1, context.raft_log.pre_allocate_raft_logs_next_index());
    assert_eq!(2, context.raft_log.pre_allocate_raft_logs_next_index());
}

#[test]
fn test_calculate_majority_matched_index_case0() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case0");
    let raft_log = c.raft_log.clone();
    raft_log.reset().expect("reset successfully!");

    let current_term = 2;
    let commit_index = 2;
    let peer_ids = [2, 3];
    let map = HashMap::from([(3, 2), (1, 3), (2, 12)]);
    // for (id, mid) in map.iter() {
    //     state.update_match_index(*id, *mid);
    // }
    // state.update_current_term(current_term);
    // state.update_commit_index(commit_index);
    test_utils::simulate_insert_proposal(&raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&raft_log, vec![2, 3], 2);

    // let matched_ids: Vec<u64> = peer_ids
    //     .iter()
    //     .map(|&id| state.match_index(id).unwrap_or(0))
    //     .collect();

    assert_eq!(
        Some(3),
        raft_log.calculate_majority_matched_index(current_term, commit_index, vec![2, 12])
    );
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
// (§5.3, §5.4).
#[test]
fn test_calculate_majority_matched_index_case1() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case1");
    let raft_log = c.raft_log.clone();
    let _ = c.raft_log.reset();
    let peer_ids = [2, 3];
    //case 1: majority matched index is 2, commit_index: 4, current_term is 3,
    // while log(2) term is 2, return None
    let ct = 3;
    let ci = 4;
    // state.update_current_term(ct);
    // state.update_commit_index(ci);
    // // state.update_match_index(1, 3);
    // state.update_match_index(2, 1);
    // state.update_match_index(3, 2);

    test_utils::simulate_insert_proposal(&raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&raft_log, vec![2], 2);
    // let matched_ids: Vec<u64> = peer_ids
    //     .iter()
    //     .map(|&id| state.match_index(id).unwrap_or(0))
    //     .collect();
    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![1, 2]));
}

#[test]
fn test_calculate_majority_matched_index_case2() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case2");
    // let state = c.s.clone();
    let raft_log = c.raft_log.clone();
    let _ = c.raft_log.reset();
    let peer_ids = [2, 3];

    //case 2: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 3, return Some(3)
    let ct = 3;
    let ci = 2;
    // state.update_current_term(ct);
    // let _ct = state.current_term();
    // state.update_commit_index(ci);
    // // state.update_match_index(1, 3);
    // state.update_match_index(2, 4);
    // state.update_match_index(3, 2);

    test_utils::simulate_insert_proposal(&raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&raft_log, vec![2], 2);
    test_utils::simulate_insert_proposal(&raft_log, vec![3], 3);
    // let matched_ids: Vec<u64> = peer_ids
    //     .iter()
    //     .map(|&id| state.match_index(id).unwrap_or(0))
    //     .collect();
    assert_eq!(Some(3), raft_log.calculate_majority_matched_index(ct, ci, vec![4, 2]));
}

#[test]
fn test_calculate_majority_matched_index_case3() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case3");
    // let state = c.s.clone();
    let raft_log = c.raft_log.clone();

    let peer_ids = [2, 3];
    //case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;
    // state.update_current_term(ct);
    // state.update_commit_index(ci);
    // // state.update_match_index(1, 4);
    // state.update_match_index(2, 3);
    // state.update_match_index(3, 2);

    test_utils::simulate_insert_proposal(&raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&raft_log, vec![2], 2);
    // let matched_ids: Vec<u64> = peer_ids
    //     .iter()
    //     .map(|&id| state.match_index(id).unwrap_or(0))
    //     .collect();
    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![3, 2]));
}

#[test]
fn test_calculate_majority_matched_index_case4() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case4");
    // let state = c.s.clone();
    let raft_log = c.raft_log.clone();
    let peer_ids = [2, 3];
    //case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;
    // state.update_current_term(ct);
    // state.update_commit_index(ci);
    // // state.update_match_index(1, 2);
    // state.update_match_index(2, 2);
    // state.update_match_index(3, 2);
    test_utils::simulate_insert_proposal(&raft_log, vec![1], 1);
    test_utils::simulate_insert_proposal(&raft_log, vec![2], 2);
    // let matched_ids: Vec<u64> = peer_ids
    //     .iter()
    //     .map(|&id| state.match_index(id).unwrap_or(0))
    //     .collect();
    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![2, 2]));
}

// # Case 5: stress testing
///
// ## Setup
// 1. Simulate 100,000 local log entries
// 2. peer1's match: 90,000, peer2's match 98,000
///
// ## Ceriteria:
// 1. compare calculate_majority_matched_index as calculate_majority_matched_index2's performance
///
#[test]
fn test_calculate_majority_matched_index_case5() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case5");
    let raft_log = c.raft_log.clone();

    let term = 1;
    let raft_log_length = 100000;
    let commit = 90000;
    let peer1_id = 2;
    let peer2_id = 3;
    let peer1_match = 97000;
    let peer2_match = 98000;
    // state.update_current_term(term);
    // state.update_commit_index(commit);
    // state.update_match_index(2, peer1_match);
    // state.update_match_index(3, peer2_match);
    let raft_log_entry_ids: Vec<u64> = (1..=raft_log_length).collect();
    test_utils::simulate_insert_proposal(&raft_log, raft_log_entry_ids, 1);
    // let matched_ids: Vec<u64> = vec![peer1_id, peer2_id]
    //     .iter()
    //     .map(|&id| state.match_index(id).unwrap_or(0))
    //     .collect();
    let now = Instant::now();
    assert_eq!(
        Some(peer2_match),
        raft_log.calculate_majority_matched_index(term, commit, vec![peer1_match, peer2_match])
    );
    println!(
        "calculate_majority_matched_index takes: {} micros.",
        now.elapsed().as_micros()
    );
}

#[test]
fn test_raft_log_insert() {
    let c = setup("/tmp/test_raft_log_insert");
    let raft_log = c.raft_log.clone();

    test_utils::simulate_insert_proposal(&raft_log, vec![1], 1);
    assert_eq!(1, raft_log.last_entry_id());
}
