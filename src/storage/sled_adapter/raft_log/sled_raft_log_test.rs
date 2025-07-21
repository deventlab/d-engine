use std::sync::Arc;
use std::vec;

use futures::future::join_all;
use tokio::time::Instant;
use tracing::debug;

use super::*;
use crate::alias::ROF;
use crate::init_sled_storages;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::storage::sled_adapter::RAFT_LOG_NAMESPACE;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::reset_dbs;
use crate::test_utils::{self};
use crate::RaftLog;
use crate::RaftTypeConfig;

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
    let (raft_log_db, _, _state_storage_db, _) = reset_dbs(path);
    let arc_raft_log_db = Arc::new(raft_log_db);

    let sled_raft_log = Arc::new(SledRaftLog::new(1, arc_raft_log_db, 0));

    TestContext {
        raft_log: sled_raft_log,
    }
}

fn init(
    path: &str,
    commit_index: u64,
) -> Arc<ROF<RaftTypeConfig>> {
    let (raft_log_db, _state_machine_db, _state_storage_db, _snapshot_storage_db) =
        init_sled_storages(path.to_string()).unwrap();
    Arc::new(SledRaftLog::new(1, Arc::new(raft_log_db), commit_index))
}

#[test]
fn test_get_as_vec() {
    let c = setup("/tmp/test_get_as_vec");

    let value = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    };

    c.raft_log.insert_batch(vec![value.clone()]).expect("should succeed");

    let entry = c.raft_log.get_entry_by_index(1).unwrap();
    assert_eq!(value, entry);
}

fn insert(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    key: u64,
) {
    let log = Entry {
        index: key,
        term: 7,
        payload: None,
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
    test_utils::simulate_insert_command(&context.raft_log, vec![i1, i2, i3, i4], 4);
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

/// # Case 1: test follower f delete conflicts (Figure 7 in raft paper)
///
/// ## Setup:
/// 1. leader:log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6),
///    log10(6)
///
/// follower_f: log1(1), log2(1), log3(1), log4(2), log5(2), log6(2), log7(3), log8(3),
///    log9(3), log10(3), log11(3)
///
/// ## Criterias:
/// 1. next_id been updated to:
///
///   Follower    lastIndex    nextIndex
///   follower_a      9      10
///   follower_b      4      5
///   follower_c      10      11
///   follower_d      12      11
///   follower_e      7      6
///   follower_f      11    4
#[test]
fn test_filter_out_conflicts_and_append_case1() {
    let context = setup("/tmp/test_filter_out_conflicts_and_append_case1");
    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&context.raft_log, vec![2, 3], 2);
    test_utils::simulate_insert_command(&context.raft_log, vec![4], 4);

    //case 1
    let l1 = Entry {
        index: 2,
        term: 2,
        payload: None,
    };
    let l2 = Entry {
        index: 3,
        term: 3,
        payload: None,
    };
    let new_ones = vec![l1, l2];
    let prev_log_index = 1;
    let prev_log_term = 1;
    let last_applied = context
        .raft_log
        .filter_out_conflicts_and_append(prev_log_index, prev_log_term, new_ones)
        .unwrap()
        .unwrap();
    assert_eq!(last_applied.index, 3);

    let e = context.raft_log.get_entry_by_index(3).unwrap();
    assert_eq!(e.term, 3);
}
#[test]
fn test_filter_out_conflicts_and_append() {
    let context = setup("/tmp/test_filter_out_conflicts_and_append");
    // let state = &context.s;
    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&context.raft_log, vec![2, 3], 2);
    test_utils::simulate_insert_command(&context.raft_log, vec![4], 4);

    //case 1
    let l1 = Entry {
        index: 2,
        term: 2,
        payload: None,
    };
    let l2 = Entry {
        index: 3,
        term: 3,
        payload: None,
    };
    let new_ones = vec![l1, l2];
    let prev_log_index = 1;
    let prev_log_term = 1;
    let last_applied = context
        .raft_log
        .filter_out_conflicts_and_append(prev_log_index, prev_log_term, new_ones)
        .unwrap()
        .unwrap();
    assert_eq!(last_applied.index, 3);

    let e = context.raft_log.get_entry_by_index(3).unwrap();
    assert_eq!(e.term, 3);

    //case 2
    let l3 = Entry {
        index: 4,
        term: 2,
        payload: None,
    };
    let new_ones = vec![l3];
    let prev_log_index = 1;
    let prev_log_term = 1;
    let last_applied = context
        .raft_log
        .filter_out_conflicts_and_append(prev_log_index, prev_log_term, new_ones)
        .unwrap()
        .unwrap();
    assert_eq!(last_applied.index, 4);
    let e = context.raft_log.get_entry_by_index(4).unwrap();
    assert_eq!(e.term, 2);
}

#[test]
fn test_retrieve_one_entry_for_this_follower_case1() {
    let context = setup("/tmp/test_retrieve_one_entry_for_this_follower_case1");
    let follower_id = 1;
    let next_id = 1;
    test_utils::simulate_insert_command(&context.raft_log, vec![1], 1);

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

    test_utils::simulate_insert_command(&context.raft_log, vec![1], 1);

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
    test_utils::simulate_insert_command(&context.raft_log, vec_1, 1);

    let mut vec_2 = Vec::new();
    for id in 1..3 {
        vec_2.push(id);
    }
    test_utils::simulate_insert_command(&context.raft_log, vec_2, 1);
    if let Some(last) = context.raft_log.last() {
        assert_eq!(last.index, 301);
    }
}

#[test]
fn test_sled_last() {
    let context = setup("/tmp/test_sled_last");

    test_utils::simulate_insert_command(&context.raft_log, (1..=300).collect(), 1);

    let l = context.raft_log.last().unwrap();
    let last = l.index;
    let len = context.raft_log.len();
    debug!("last: {:?}, while len: {:?}", last, len);
    assert_eq!(last, len as u64);
}
#[test]
fn test_sled_last_max() {
    let context = setup("/tmp/test_sled_last_max");

    let max = u64::MAX; //max of u64
    test_utils::simulate_insert_command(&context.raft_log, ((max - 1)..max).collect(), 1);

    let l = context.raft_log.last().unwrap();
    assert_eq!(l.index, 1);
}

///3 nodes, leader append 1000 logs, the other two logs can not get 1000
/// entries
/// to test insert_one_client_command and get_entries_between two functions
#[test]
fn test_insert_one_client_command() {
    let context = setup("/tmp/test_insert_one_client_command");

    context.raft_log.reset().expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..2001 {
        v.push(i);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 1);

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
    test_utils::simulate_insert_command(&context.raft_log, v, 1);

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
/// Test duplicated insert
/// Leader might received duplicated commands from clients in several RPC
/// request. But, leader should not treat them as duplicated entries. They are
/// just sequence events.
#[test]
fn test_insert_one_client_command_dup_case() {
    let context = setup("/tmp/test_insert_one_client_command_dup_case");

    context.raft_log.reset().expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..10 {
        v.push(i);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 1);

    let len = context.raft_log.last_entry_id();
    let mut v = Vec::new();
    for i in 1..10 {
        v.push(i + len);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 2);

    assert_eq!(context.raft_log.last_entry_id(), 18);
    let last = context.raft_log.last().unwrap();
    assert_eq!(last.index, 18);
}

/// # Test client propose commands which contains insert and delete
///
/// ## Criterias:
/// 1. all client proposal should be recorded in RaftLog without lose
#[test]
fn test_client_proposal_insert_delete() {
    let context = setup("/tmp/test_apply");

    context.raft_log.reset().expect("reset successfully!");

    test_utils::simulate_insert_command(&context.raft_log, (1..=10).collect(), 1);
    assert_eq!(context.raft_log.last_entry_id(), 10);

    test_utils::simulate_delete_command(&context.raft_log, 1..=3, 1);
    assert_eq!(context.raft_log.last_entry_id(), 13);
    assert_eq!(context.raft_log.len(), 13);

    test_utils::simulate_insert_command(&context.raft_log, (11..=20).collect(), 1);

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
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    let len = context.raft_log.last_entry_id();
    context.raft_log.load_uncommitted_from_db_to_cache(8, len);
    for j in 8..len + 1 {
        assert!(context.raft_log.get_from_cache(j).is_some());
    }
}

///Bug: setup logger with error: SetGlobalDefaultError("a global default trace
/// dispatcher has already been set")
/// #[traced_test]
#[test]
fn test_purge_logs_up_to() {
    let context = setup("/tmp/test_purge_logs_up_to");

    context.raft_log.reset().expect("reset successfully!");

    let mut entries = Vec::new();
    for i in 1..10 {
        entries.push(Entry {
            index: i,
            term: 1,
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    //..
    // assume we have generated snapshot until $last_applied,
    // now we can clean the local logs
    let last_applied = LogId { index: 3, term: 1 };
    context.raft_log.purge_logs_up_to(last_applied).expect("should succeed");

    assert_eq!(9, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.get_entry_by_index(3));
    assert_eq!(None, context.raft_log.get_entry_by_index(2));
    assert_eq!(None, context.raft_log.get_entry_by_index(1));
}

#[test]
fn test_purge_logs_up_to_concurrent_purge() {
    let context = setup("/tmp/test_purge_logs_up_to_concurrent_purge");
    context.raft_log.reset().expect("reset failed");

    // Insert test logs
    let entries: Vec<_> = (1..=10)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    context.raft_log.insert_batch(entries).unwrap();

    // Concurrent purge calls
    let handles: Vec<_> = vec![1, 2, 3, 4, 5, 6, 7]
        .into_iter()
        .map(|cutoff| {
            let raft_log = context.raft_log.clone();
            std::thread::spawn(move || {
                raft_log.purge_logs_up_to(LogId { index: cutoff, term: 1 }).unwrap();
            })
        })
        .collect();

    // Wait for all threads
    for h in handles {
        h.join().unwrap();
    }

    // Verify final state
    assert_eq!(10, context.raft_log.last_entry_id());

    // The highest successful purge should win
    assert!(context.raft_log.get_entry_by_index(7).is_none());
    assert!(context.raft_log.get_entry_by_index(8).is_some());
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
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    let last_applied = LogId { index: 4, term: 1 };
    context.raft_log.purge_logs_up_to(last_applied).expect("should succeed");
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
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).expect("should succeed");

    let last_applied = LogId { index: 4, term: 1 };
    context.raft_log.purge_logs_up_to(last_applied).expect("should succeed");
    assert_eq!(10, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.get_entry_by_index(3));
    assert_eq!(None, context.raft_log.get_entry_by_index(2));
    assert_eq!(None, context.raft_log.get_entry_by_index(1));

    assert_eq!(
        context.raft_log.span_between_first_entry_and_last_entry(),
        10 - last_applied.index
    );
}

/// Case 1: we have multi thread working concurrently
///     each thread should get unique pre allocated index for pending apply
/// entries     At last when we retrieve last entry of the locallog, we should
/// see the last pre allocated index
#[tokio::test]
async fn test_pre_allocate_raft_logs_next_index_case1() {
    let context = setup("/tmp/test_pre_allocate_raft_logs_next_index_case1");

    context.raft_log.reset().expect("reset successfully!");
    let start = context.raft_log.last_entry_id();
    println!("start: {start}",);

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
                    payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
                });
            }
            cloned_raft_log.insert_batch(entries.clone()).expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{end}",);

    assert_eq!(start + i * j, end);
}

/// Case 2: we have mutil thread working concurrently
///     there might be one thread failed to apply the local log entries
///     which cases some log entry index might not really exists inside local
/// logs     we still want to validate the last log entry id is the same one as
/// we expected #[ignore = "architecture changes, this case will not exist"]
#[tokio::test]
async fn test_pre_allocate_raft_logs_next_index_case2() {
    let context = setup("/tmp/test_pre_allocate_raft_logs_next_index_case2");

    context.raft_log.reset().expect("reset successfully!");
    let start = context.raft_log.last_entry_id();
    println!("start: {start}",);

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
                    payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
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
                    payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
                });
            }
            cloned_raft_log.insert_batch(entries.clone()).expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{end}",);

    assert_eq!(start + i * j, end);
    assert_eq!(
        context.raft_log.span_between_first_entry_and_last_entry(),
        i * j - (k * j + 1) + 1
    );
}

/// Case 1: stress testing with 100K entries,
///     the entries was insert successfully and last log entry id is as expected
#[ignore = "architecture changes, this case will not exist"]
#[tokio::test]
async fn test_insert_batch_logs_case1() {
    let context = setup("/tmp/test_insert_batch_logs_case1");

    context.raft_log.reset().expect("reset successfully!");
    let start = context.raft_log.last_entry_id();
    println!("start: {start}",);

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
                    payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
                });
            }
            cloned_raft_log.insert_batch(entries.clone()).expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{end}",);

    assert_eq!(start + i * j, end);
}

/// # Case 2: Test scenario for log replication conflict handling when combining:
/// 1. `filter_out_conflicts_and_append` (follower's perspective)
/// 2. `purge_logs_up_to` (snapshot compaction)
/// 3. `insert_batch` (leader's log appending)
///
/// # Test Scenario Flow
///
/// 1. **Setup Phase**:
///    - Old leader (T1) appends logs 1-10 (term 1)
///    - Generates snapshot up to index 3 (compaction)
///
/// 2. **Leadership Change**:
///    - Old leader restarts and becomes follower
///    - New leader (T2) elected with logs 1-8 (term 1)
///
/// 3. **Conflict Handling**:
///    - New leader appends logs 9-10 (term 2)
///    - Old follower processes AppendEntries RPC
///
/// # Validation Points
/// - Final log state must have last entry index=10, term=2
/// - Log continuity: index 7 (term 1) and 8 (term 2) must coexist
#[tokio::test]
async fn test_insert_batch_logs_case2() {
    // 1. Initialize two nodes (old_leader and new_leader)
    let (raft_log_db, _, _state_storage_db, _) = reset_dbs("/tmp/test_insert_batch_logs_case2_node1");
    let old_leader = Arc::new(SledRaftLog::new(1, Arc::new(raft_log_db), 0));

    let (raft_log_db, _, _state_storage_db, _) = reset_dbs("/tmp/test_insert_batch_logs_case2_node2");
    let new_leader = Arc::new(SledRaftLog::new(1, Arc::new(raft_log_db), 0));

    let mut entries: Vec<Entry> = Vec::new();
    for i in 1..=7 {
        entries.push(Entry {
            index: old_leader.pre_allocate_raft_logs_next_index(),
            term: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
        });
    }

    // 2. Old leader initial logs (term 1)
    old_leader.insert_batch(entries.clone()).expect("should succeed");
    new_leader.insert_batch(entries.clone()).expect("should succeed");

    // 3. Add uncommitted entry on old leader
    old_leader
        .insert_batch(vec![Entry {
            index: old_leader.pre_allocate_raft_logs_next_index(),
            term: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![8]))),
        }])
        .expect("should succeed");

    // 4. Simulate snapshot compaction
    let last_applied = LogId { index: 3, term: 1 };
    old_leader.purge_logs_up_to(last_applied).expect("should succeed");

    // 5. New leader appends higher term logs
    let mut handles = Vec::new();
    let mut new_leader_entries: Vec<Entry> = Vec::new();
    for i in 8..=10 {
        new_leader_entries.push(Entry {
            index: new_leader.pre_allocate_raft_logs_next_index(),
            term: 2,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
        });
    }
    let cloned_new_leader_entries = new_leader_entries.clone();
    let handle = tokio::spawn(async move {
        new_leader
            .insert_batch(cloned_new_leader_entries.clone())
            .expect("should succeed");
    });
    handles.push(handle);
    let _results = join_all(handles).await;

    debug!("new_leader_entries: {:?}", new_leader_entries.clone());

    // 6. Follower processes conflicting entries
    let ex_leader_last_appled_id = old_leader
        .filter_out_conflicts_and_append(8, 1, new_leader_entries.clone())
        .unwrap()
        .unwrap();
    assert_eq!(ex_leader_last_appled_id.index, 10);

    // // 7. Validate final log state
    // if let Some(ex_leader_last_entry) = old_leader.last() {
    //     assert_eq!(ex_leader_last_entry.index, 10);
    //     assert_eq!(ex_leader_last_entry.term, 2);
    // }

    // if let Some(entry) = old_leader.get_entry_by_index(7) {
    //     assert_eq!(entry.term, 1);
    // }
    // if let Some(entry) = old_leader.get_entry_by_index(8) {
    //     assert_eq!(entry.term, 2);
    // }

    // 7. Validate final log state
    validate_log_continuity(
        &old_leader,
        &[
            (7, 1),  // Original term 1 entry
            (8, 2),  // Overwritten entry
            (10, 2), // New highest entry
        ],
    )
    .await;
}

/// Helper function: Validate log entries' term continuity
async fn validate_log_continuity(
    node: &Arc<SledRaftLog>,
    expected: &[(u64, u64)],
) {
    for (index, term) in expected {
        let entry = node
            .get_entry_by_index(*index)
            .unwrap_or_else(|| panic!("Missing entry at index {index}",));
        assert_eq!(
            entry.term, *term,
            "Term mismatch at index {}: expected {}, got {}",
            index, term, entry.term
        );
    }
}

/// # Case 1.1: cluster just start
/// 1. node's locallog is empty
/// 2. prev_log_index is 0
///
/// ## Criterias:
/// 1. prev_log_ok return true
#[test]
fn test_prev_log_ok_case_1_1() {
    let context = setup("/tmp/test_prev_log_ok_case_1_1");

    context.raft_log.reset().expect("reset successfully!");
    assert!(context.raft_log.prev_log_ok(0, 0, 0));
}

/// # Case 1.2: normal case
/// 1. node's locallog is not empty, which contains log1(1), log2(1), log3(2)
/// 2. prev_log_index: 2, prev_log_term: 1
///
/// ## Criterias:
/// 1. prev_log_ok return true
#[test]
fn test_prev_log_ok_case_1_2() {
    let context = setup("/tmp/test_prev_log_ok_case_1_2");

    context.raft_log.reset().expect("reset successfully!");
    // so its locallog is not empty, it contains some log (log-1,log-2)
    test_utils::simulate_insert_command(&context.raft_log, vec![1, 2], 1);
    test_utils::simulate_insert_command(&context.raft_log, vec![3], 2);
    // prev_log index > 0
    let prev_log_index = 2;
    let prev_log_term = 1;
    assert!(context.raft_log.prev_log_ok(prev_log_index, prev_log_term, 0));
}

/// # Case 1.3: conflict case
/// 1. node's locallog is not empty, which contains log1(1), log2(1), log3(2)
/// 2. prev_log_index: 3, prev_log_term: 1
///
/// ## Criterias:
/// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_1_3() {
    let context = setup("/tmp/test_prev_log_ok_case_1_3");

    context.raft_log.reset().expect("reset successfully!");
    // so its locallog is not empty, it contains some log (log-1,log-2)
    test_utils::simulate_insert_command(&context.raft_log, vec![1, 2], 1);
    test_utils::simulate_insert_command(&context.raft_log, vec![3], 2);
    // prev_log index > 0
    let prev_log_index = 3;
    let prev_log_term = 1;
    assert!(!context.raft_log.prev_log_ok(prev_log_index, prev_log_term, 0));
}

/// # Case 1.4: conflict case
/// 1. node's locallog is not empty, which contains log1(1), log2(1), log3(2)
/// 2. prev_log_index: 3, prev_log_term: 3
///
/// ## Criterias:
/// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_1_4() {
    let context = setup("/tmp/test_prev_log_ok_case_1_4");

    context.raft_log.reset().expect("reset successfully!");
    // so its locallog is not empty, it contains some log (log-1,log-2)
    test_utils::simulate_insert_command(&context.raft_log, vec![1, 2], 1);
    test_utils::simulate_insert_command(&context.raft_log, vec![3], 2);
    // prev_log index > 0
    let prev_log_index = 3;
    let prev_log_term = 3;
    assert!(!context.raft_log.prev_log_ok(prev_log_index, prev_log_term, 0));
}

/// # Case 1.5: conflict case
/// 1. node's locallog is not empty, which contains log1(1), log2(2), log3(3), log4(3)
/// 2. prev_log_index: 4, prev_log_term: 6
///
/// ## Criterias:
/// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_1_5() {}

/// # Case 2.4: snapshot related test case
/// 1. the current node is learner, it just installed Leader's snapshot
/// 2. so its locallog is not empty, it contains some log (log-1, 1), (log-2,1)
/// 3. while its last_applied is 10 which means log-10 has been committed
/// 4. prev_log_index: 11, prev_log_term: 1
///
/// ## Criterias:
/// 1. prev_log_ok return false
#[test]
fn test_prev_log_ok_case_2_4() {
    let context = setup("/tmp/test_prev_log_ok_case_2_4");

    let last_applied = 10;
    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1, 2], 1);
    let prev_log_index = 11;
    let prev_log_term = 1;
    assert!(!context
        .raft_log
        .prev_log_ok(prev_log_index, prev_log_term, last_applied));
}
#[test]
fn test_apply_and_then_get_last() {
    let context = setup("/tmp/test_apply_and_then_get_last");

    context.raft_log.reset().expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1, 2], 1);
    assert_eq!(2, context.raft_log.last_entry_id());
}

/// # Case 1: test if node restart, the local log cache should load from disk
#[test]
fn test_new_case1() {
    test_utils::enable_logger();

    let p = "/tmp/test_new_case1";
    {
        let _ = std::fs::remove_dir_all(p);
        println!("Test setup ...");
        let commit_index = 0;
        let raft_log = init(p, commit_index);
        test_utils::simulate_insert_command(&raft_log, vec![1, 2], 1);

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

    test_utils::simulate_insert_command(&raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&raft_log, vec![2, 3], 2);

    assert_eq!(
        Some(3),
        raft_log.calculate_majority_matched_index(current_term, commit_index, vec![2, 12])
    );
}

/// If there exists an N such that N > commitIndex, a majority
/// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
/// (§5.3, §5.4).
#[test]
fn test_calculate_majority_matched_index_case1() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case1");
    let raft_log = c.raft_log.clone();
    let _ = c.raft_log.reset();
    //case 1: majority matched index is 2, commit_index: 4, current_term is 3,
    // while log(2) term is 2, return None
    let ct = 3;
    let ci = 4;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&raft_log, vec![2], 2);
    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![1, 2]));
}

#[test]
fn test_calculate_majority_matched_index_case2() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case2");
    let raft_log = c.raft_log.clone();
    let _ = c.raft_log.reset();

    //case 2: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 3, return Some(3)
    let ct = 3;
    let ci = 2;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&raft_log, vec![2], 2);
    test_utils::simulate_insert_command(&raft_log, vec![3], 3);
    assert_eq!(Some(3), raft_log.calculate_majority_matched_index(ct, ci, vec![4, 2]));
}

#[test]
fn test_calculate_majority_matched_index_case3() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case3");
    let raft_log = c.raft_log.clone();

    //case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;
    test_utils::simulate_insert_command(&raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&raft_log, vec![2], 2);

    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![3, 2]));
}

#[test]
fn test_calculate_majority_matched_index_case4() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case4");

    let raft_log = c.raft_log.clone();

    //case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1);
    test_utils::simulate_insert_command(&raft_log, vec![2], 2);

    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![2, 2]));
}

/// # Case 5: stress testing
///
/// ## Setup
/// 1. Simulate 100,000 local log entries
/// 2. peer1's match: 90,000, peer2's match 98,000
///
/// ## Ceriteria:
/// 1. compare calculate_majority_matched_index as calculate_majority_matched_index2's performance
#[test]
fn test_calculate_majority_matched_index_case5() {
    let c = setup("/tmp/test_calculate_majority_matched_index_case5");
    let raft_log = c.raft_log.clone();

    let term = 1;
    let raft_log_length = 100000;
    let commit = 90000;

    let peer1_match = 97000;
    let peer2_match = 98000;

    let raft_log_entry_ids: Vec<u64> = (1..=raft_log_length).collect();
    test_utils::simulate_insert_command(&raft_log, raft_log_entry_ids, 1);

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

    test_utils::simulate_insert_command(&raft_log, vec![1], 1);
    assert_eq!(1, raft_log.last_entry_id());
}

/// #Case 1: local raft log is empty
#[test]
fn test_has_log_at_case1() {
    let c = setup("/tmp/test_has_log_at_case1");
    let raft_log = c.raft_log.clone();

    assert!(raft_log.has_log_at(0, 0));
}

/// #Case 2: local raft log is not empty
#[test]
fn test_has_log_at_case2() {
    let c = setup("/tmp/test_has_log_at_case2");
    let raft_log = c.raft_log.clone();
    test_utils::simulate_insert_command(&raft_log, vec![1], 1);

    assert!(!raft_log.has_log_at(0, 0));
}

/// #Case 1: local raft log is empty
#[test]
fn test_is_empty_case1() {
    let c = setup("/tmp/test_is_empty_case1");
    let raft_log = c.raft_log.clone();

    assert!(raft_log.is_empty());
}

/// #Case 2: local raft log is not empty
#[test]
fn test_is_empty_case2() {
    let c = setup("/tmp/test_is_empty_case2");
    let raft_log = c.raft_log.clone();
    test_utils::simulate_insert_command(&raft_log, vec![1], 1);

    assert!(!raft_log.is_empty());
}

/// # Case1: No last log in raft_log, should returns (0,0)
#[test]
fn test_get_last_entry_metadata_case1() {
    let c = setup("/tmp/test_get_last_entry_metadata_case1");
    let raft_log = c.raft_log.clone();

    assert_eq!((0, 0), raft_log.get_last_entry_metadata());
}
/// # Case2: There is last log in raft_log, should returns last log metadata
#[test]
fn test_get_last_entry_metadata_case2() {
    let c = setup("/tmp/test_get_last_entry_metadata_case2");
    let raft_log = c.raft_log.clone();
    test_utils::simulate_insert_command(&raft_log, vec![1], 11);

    assert_eq!((1, 11), raft_log.get_last_entry_metadata());
}

#[tokio::test]
async fn test_raft_log_drop() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_raft_log_drop");

    {
        let db = Arc::new(sled::open(case_path.clone()).unwrap());
        // Create real instance instead of mock
        let raft_log = Arc::new(SledRaftLog::new(1, db.clone(), 0));

        // Insert test data
        test_utils::simulate_insert_command(&raft_log, vec![1], 1);

        // Explicitly drop to trigger flush
        drop(raft_log);
    }

    // Verify flush occurred by checking persistence
    let reloaded_db = sled::open(case_path).unwrap();
    assert!(!reloaded_db.open_tree(RAFT_LOG_NAMESPACE).unwrap().is_empty());
}

#[test]
fn test_first_index_for_term() {
    let context = setup("/tmp/test_first_index_for_term");
    context.raft_log.reset().expect("reset successfully!");

    // Insert test logs with various terms
    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: None,
        },
        Entry {
            index: 2,
            term: 1,
            payload: None,
        },
        Entry {
            index: 3,
            term: 2,
            payload: None,
        },
        Entry {
            index: 4,
            term: 2,
            payload: None,
        },
        Entry {
            index: 5,
            term: 3,
            payload: None,
        },
        Entry {
            index: 6,
            term: 3,
            payload: None,
        },
        Entry {
            index: 7,
            term: 3,
            payload: None,
        },
    ];
    context.raft_log.insert_batch(entries).expect("insert should succeed");

    // Test various cases
    assert_eq!(context.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(context.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(context.raft_log.first_index_for_term(3), Some(5));
    assert_eq!(context.raft_log.first_index_for_term(4), None); // Term not found

    // Test with partial data
    let entries = vec![
        Entry {
            index: 8,
            term: 4,
            payload: None,
        },
        Entry {
            index: 9,
            term: 5,
            payload: None,
        },
    ];
    context.raft_log.insert_batch(entries).expect("insert should succeed");
    assert_eq!(context.raft_log.first_index_for_term(4), Some(8));
    assert_eq!(context.raft_log.first_index_for_term(5), Some(9));

    // Test with empty log
    context.raft_log.reset().expect("reset should succeed");
    assert_eq!(context.raft_log.first_index_for_term(1), None);
}

#[test]
fn test_last_index_for_term() {
    let context = setup("/tmp/test_last_index_for_term");
    context.raft_log.reset().expect("reset successfully!");

    // Insert test logs with various terms
    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: None,
        },
        Entry {
            index: 2,
            term: 1,
            payload: None,
        },
        Entry {
            index: 3,
            term: 2,
            payload: None,
        },
        Entry {
            index: 4,
            term: 2,
            payload: None,
        },
        Entry {
            index: 5,
            term: 3,
            payload: None,
        },
        Entry {
            index: 6,
            term: 3,
            payload: None,
        },
        Entry {
            index: 7,
            term: 3,
            payload: None,
        },
    ];
    context.raft_log.insert_batch(entries).expect("insert should succeed");

    // Test various cases
    assert_eq!(context.raft_log.last_index_for_term(1), Some(2));
    assert_eq!(context.raft_log.last_index_for_term(2), Some(4));
    assert_eq!(context.raft_log.last_index_for_term(3), Some(7));
    assert_eq!(context.raft_log.last_index_for_term(4), None); // Term not found

    // Test with gaps in terms
    let entries = vec![
        Entry {
            index: 8,
            term: 5,
            payload: None,
        },
        Entry {
            index: 9,
            term: 5,
            payload: None,
        },
    ];
    context.raft_log.insert_batch(entries).expect("insert should succeed");
    assert_eq!(context.raft_log.last_index_for_term(5), Some(9));

    // Test with empty log
    context.raft_log.reset().expect("reset should succeed");
    assert_eq!(context.raft_log.last_index_for_term(1), None);
}

#[test]
fn test_term_index_functions_with_purged_logs() {
    let context = setup("/tmp/test_term_index_with_purged");
    context.raft_log.reset().expect("reset successfully!");

    // Insert initial logs
    let entries: Vec<_> = (1..=10)
        .map(|i| Entry {
            index: i,
            term: if i <= 3 { 1 } else { 2 },
            payload: None,
        })
        .collect();
    context.raft_log.insert_batch(entries).expect("insert should succeed");

    // Purge first 5 logs
    context
        .raft_log
        .purge_logs_up_to(LogId { index: 5, term: 2 })
        .expect("purge should succeed");

    // Verify after purge
    assert_eq!(context.raft_log.first_index_for_term(1), None); // Term 1 purged
    assert_eq!(context.raft_log.last_index_for_term(1), None); // Term 1 purged
    assert_eq!(context.raft_log.first_index_for_term(2), Some(6));
    assert_eq!(context.raft_log.last_index_for_term(2), Some(10));
}

#[test]
fn test_term_index_functions_with_concurrent_writes() {
    let context = setup("/tmp/test_term_index_concurrent");
    context.raft_log.reset().expect("reset successfully!");

    let raft_log = context.raft_log.clone();
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let cloned_raft_log = raft_log.clone();
            std::thread::spawn(move || {
                let term = i + 1;
                for j in 1..=100 {
                    let index = (i * 100) + j;
                    let entry = Entry {
                        index,
                        term,
                        payload: None,
                    };
                    cloned_raft_log
                        .insert_batch(vec![entry])
                        .expect("insert should succeed");
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify term indices
    for term in 1..=10 {
        assert_eq!(context.raft_log.first_index_for_term(term), Some((term - 1) * 100 + 1));
        assert_eq!(context.raft_log.last_index_for_term(term), Some(term * 100));
    }
}

#[test]
fn test_term_index_performance_large_dataset() {
    let context = setup("/tmp/test_term_index_performance");
    context.raft_log.reset().expect("reset successfully!");

    // Insert 100,000 logs with terms cycling every 100 logs
    let mut entries = Vec::new();
    for i in 1..=100_000 {
        let term = i / 100 + 1;
        entries.push(Entry {
            index: i,
            term,
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).expect("insert should succeed");

    // Measure performance for first_index_for_term
    let start = std::time::Instant::now();
    assert_eq!(context.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(context.raft_log.first_index_for_term(500), Some(49900));
    assert_eq!(context.raft_log.first_index_for_term(1000), Some(99900));
    println!("first_index_for_term took: {:?}", start.elapsed());

    // Measure performance for last_index_for_term
    let start = std::time::Instant::now();
    assert_eq!(context.raft_log.last_index_for_term(1), Some(99));
    assert_eq!(context.raft_log.last_index_for_term(500), Some(49999));
    assert_eq!(context.raft_log.last_index_for_term(1000), Some(99999));
    println!("last_index_for_term took: {:?}", start.elapsed());
}

#[cfg(test)]
mod id_allocation_tests {
    use super::*;
    use std::sync::{atomic::Ordering, Arc};

    // In-memory test setup
    fn setup_memory() -> Arc<SledRaftLog> {
        let config = sled::Config::new().temporary(true);
        let db = Arc::new(config.open().unwrap());

        Arc::new(SledRaftLog::new(1, db, 0))
    }

    #[test]
    fn test_pre_allocate_next_index_sequential() {
        let raft_log = setup_memory();

        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 1);
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 2);
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 3);

        // Verify cache state
        assert_eq!(raft_log.cache.next_id.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn test_pre_allocate_next_index_concurrent() {
        let raft_log = setup_memory();
        let raft_log_clone = Arc::clone(&raft_log);

        let handle = std::thread::spawn(move || raft_log_clone.pre_allocate_raft_logs_next_index());

        let main_id = raft_log.pre_allocate_raft_logs_next_index();
        let thread_id = handle.join().unwrap();

        // Verify IDs are unique and sequential
        let ids = [main_id, thread_id];
        assert!(ids.contains(&1) && ids.contains(&2));
        assert_eq!(raft_log.cache.next_id.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_pre_allocate_id_range_exact_batch() {
        let raft_log = setup_memory();

        let range = raft_log.pre_allocate_id_range(100);
        assert_eq!(*range.start(), 1);
        assert_eq!(*range.end(), 100);

        // Next allocation should start after the range
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 101);
    }

    #[test]
    fn test_pre_allocate_id_range_partial_batch() {
        let raft_log = setup_memory();

        let range = raft_log.pre_allocate_id_range(50);
        assert_eq!(*range.start(), 1);
        assert_eq!(*range.end(), 100); // Still allocates full batch

        // Verify we can use the full range
        for i in 1..=100 {
            assert!(range.contains(&i));
        }
    }

    #[test]
    fn test_pre_allocate_id_range_multiple_batches() {
        let raft_log = setup_memory();

        // First batch
        let range1 = raft_log.pre_allocate_id_range(150);
        assert_eq!(*range1.start(), 1);
        assert_eq!(*range1.end(), 200); // 2 batches (200 IDs)

        // Second batch
        let range2 = raft_log.pre_allocate_id_range(50);
        assert_eq!(*range2.start(), 201);
        assert_eq!(*range2.end(), 300); // Another 2 batches (100 IDs requested -> 100 allocated)
    }

    #[test]
    fn test_pre_allocate_id_range_edge_cases() {
        let raft_log = setup_memory();

        // Zero count (should allocate minimum batch)
        let range = raft_log.pre_allocate_id_range(0);
        assert_eq!(*range.start(), 1);
        assert_eq!(*range.end(), 100);

        // Exact batch size
        let range = raft_log.pre_allocate_id_range(100);
        assert_eq!(*range.start(), 101);
        assert_eq!(*range.end(), 200);

        // Single ID over batch
        let range = raft_log.pre_allocate_id_range(101);
        assert_eq!(*range.start(), 201);
        assert_eq!(*range.end(), 400); // 4 batches (400 IDs)
    }

    #[test]
    fn test_mixed_allocation_strategies() {
        let raft_log = setup_memory();

        // Single allocation
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 1);

        // Range allocation
        let range = raft_log.pre_allocate_id_range(5);
        assert_eq!(*range.start(), 2);
        assert_eq!(*range.end(), 101); // Full batch

        // More single allocations
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 102);
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 103);

        // Large range allocation
        let range = raft_log.pre_allocate_id_range(250);
        assert_eq!(*range.start(), 104);
        assert_eq!(*range.end(), 403); // 3 batches (300 IDs)

        // Final check
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 404);
    }
}
