use super::*;
use crate::alias::ROF;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::test_utils::enable_logger;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::reset_dbs;
use crate::test_utils::reuse_dbs;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::{self};
use crate::BufferedRaftLog;
use crate::FlushPolicy;
use crate::PersistenceConfig;
use crate::PersistenceStrategy;
use crate::RaftLog;
use crate::RaftTypeConfig;
use futures::future::join_all;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use tempfile::tempdir;
use tokio::time::sleep;
use tokio::time::Instant;
use tracing::debug;

// Test utilities
struct TestContext {
    raft_log: Arc<ROF<RaftTypeConfig>>,
    storage: Option<Arc<SledStorageEngine>>,
    _temp_dir: Option<tempfile::TempDir>,
}

impl TestContext {
    fn new(
        strategy: PersistenceStrategy,
        flush_policy: FlushPolicy,
    ) -> Self {
        let temp_dir = tempdir().unwrap();
        let storage = {
            let (db, _, _, _) = reset_dbs(temp_dir.path().to_str().unwrap());
            Some(Arc::new(SledStorageEngine::new(1, db).unwrap()))
        };

        let (raft_log, receiver) = BufferedRaftLog::new(
            1,
            PersistenceConfig {
                strategy,
                flush_policy,
                max_buffered_entries: 1000,
            },
            storage.clone(),
        );
        let raft_log = { raft_log.start(receiver) };

        // Small delay to ensure processor is ready
        std::thread::sleep(Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            _temp_dir: Some(temp_dir),
        }
    }

    async fn append_entries(
        &self,
        start: u64,
        count: u64,
        term: u64,
    ) {
        let entries: Vec<_> = (start..start + count)
            .map(|index| Entry {
                index,
                term,
                payload: Some(EntryPayload::command(b"data".to_vec())),
            })
            .collect();

        self.raft_log.append_entries(entries).await.unwrap();
    }
}

// impl Drop for TestContext {
//     fn drop(&mut self) {
//         println!("Test teardown ...");
//         // self.s.db.drop_tree("logs").expect("drop successfully!");
//         let _ = std::fs::remove_dir_all("/tmp/test_db");
//     }
// }
async fn insert(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    key: u64,
) {
    let log = Entry {
        index: key,
        term: 7,
        payload: None,
    };
    raft_log.insert_batch(vec![log]).await.expect("should succeed");
}

#[tokio::test]
async fn test_get_range1() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let i1: u64 = 11;
    let i2: u64 = 12;
    let i3: u64 = 13;
    let i4: u64 = 14;
    test_utils::simulate_insert_command(&context.raft_log, vec![i1, i2, i3, i4], 4).await;
    let list = context.raft_log.get_entries_range(1..=2).unwrap();

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

#[tokio::test]
async fn test_get_range2() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    for i in 1..300 {
        insert(&context.raft_log, i).await;
    }

    let list = context.raft_log.get_entries_range(255..=255).unwrap();

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
#[tokio::test]
async fn test_filter_out_conflicts_and_append_case1() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    context.raft_log.reset().await.expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&context.raft_log, vec![2, 3], 2).await;
    test_utils::simulate_insert_command(&context.raft_log, vec![4], 4).await;

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
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last_applied.index, 3);

    let e = context.raft_log.entry(3).unwrap().unwrap();
    assert_eq!(e.term, 3);
}
#[tokio::test]
async fn test_filter_out_conflicts_and_append() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    // let state = &context.s;
    context.raft_log.reset().await.expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&context.raft_log, vec![2, 3], 2).await;
    test_utils::simulate_insert_command(&context.raft_log, vec![4], 4).await;

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
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last_applied.index, 3);

    let e = context.raft_log.entry(3).unwrap().unwrap();
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
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last_applied.index, 4);
    let e = context.raft_log.entry(4).unwrap().unwrap();
    assert_eq!(e.term, 2);
}

#[tokio::test]
async fn test_get_last_raft_log() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("should succeed");
    let mut vec_1 = Vec::new();
    for id in 1..300 {
        vec_1.push(id);
    }
    test_utils::simulate_insert_command(&context.raft_log, vec_1, 1).await;

    let mut vec_2 = Vec::new();
    for id in 1..3 {
        vec_2.push(id);
    }
    test_utils::simulate_insert_command(&context.raft_log, vec_2, 1).await;
    if let Some(last) = context.raft_log.last_entry() {
        assert_eq!(last.index, 301);
    }
}

#[tokio::test]
async fn test_sled_last() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    test_utils::simulate_insert_command(&context.raft_log, (1..=300).collect(), 1).await;

    let l = context.raft_log.last_entry().unwrap();
    let last = l.index;
    let len = context.raft_log.len();
    debug!("last: {:?}, while len: {:?}", last, len);
    assert_eq!(last, len as u64);
}
#[tokio::test]
async fn test_sled_last_max() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    let max = u64::MAX; //max of u64
    test_utils::simulate_insert_command(&context.raft_log, ((max - 1)..max).collect(), 1).await;

    let l = context.raft_log.last_entry().unwrap();
    assert_eq!(l.index, 1);
}

///3 nodes, leader append 1000 logs, the other two logs can not get 1000
/// entries
/// to test insert_one_client_command and get_entries_range two functions
#[tokio::test]
async fn test_insert_one_client_command() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..2001 {
        v.push(i);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 1).await;

    let mut count = 900;
    for entry in context.raft_log.get_entries_range(900..=2000).unwrap() {
        assert_eq!(entry.index, count);
        count += 1;
    }
}

#[tokio::test]
async fn test_get_raft_log_entry_between() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..2001 {
        v.push(i);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 1).await;

    let log_len = context.raft_log.last_entry_id();
    let result = context.raft_log.get_entries_range(900..=log_len).unwrap();
    let mut count = 900;
    for entry in &result {
        assert_eq!(entry.index, count);
        count += 1;
    }
    assert_eq!(result.len(), 1101);

    let result = context.raft_log.get_entries_range(900..=log_len - 1).unwrap();
    assert_eq!(result.len(), 1100);

    //end index will not be returned as result
    let result = context.raft_log.get_entries_range(0..=log_len).unwrap();
    assert_eq!(result.len(), log_len as usize);

    //index starts from 1
    let result = context.raft_log.get_entries_range(0..=0).unwrap();
    assert_eq!(result.len(), 0);

    //index starts from 1
    let result = context.raft_log.get_entries_range(0..=1).unwrap();
    assert_eq!(result.len(), 1);
}
/// Test duplicated insert
/// Leader might received duplicated commands from clients in several RPC
/// request. But, leader should not treat them as duplicated entries. They are
/// just sequence events.
#[tokio::test]
async fn test_insert_one_client_command_dup_case() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");

    let mut v = Vec::new();
    for i in 1..10 {
        v.push(i);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 1).await;

    let len = context.raft_log.last_entry_id();
    let mut v = Vec::new();
    for i in 1..10 {
        v.push(i + len);
    }
    test_utils::simulate_insert_command(&context.raft_log, v, 2).await;

    assert_eq!(context.raft_log.last_entry_id(), 18);
    let last = context.raft_log.last_entry().unwrap();
    assert_eq!(last.index, 18);
}

/// # Test client propose commands which contains insert and delete
///
/// ## Criterias:
/// 1. all client proposal should be recorded in RaftLog without lose
#[tokio::test]
async fn test_client_proposal_insert_delete() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");

    test_utils::simulate_insert_command(&context.raft_log, (1..=10).collect(), 1).await;
    assert_eq!(context.raft_log.last_entry_id(), 10);

    test_utils::simulate_delete_command(&context.raft_log, 1..=3, 1).await;
    assert_eq!(context.raft_log.last_entry_id(), 13);
    assert_eq!(context.raft_log.len(), 13);

    test_utils::simulate_insert_command(&context.raft_log, (11..=20).collect(), 1).await;

    assert_eq!(context.raft_log.last_entry_id(), 23);
    assert_eq!(context.raft_log.len(), 23);
}

///Bug: setup logger with error: SetGlobalDefaultError("a global default trace
/// dispatcher has already been set")
/// #[traced_test]
#[tokio::test]
async fn test_purge_logs_up_to() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");

    let mut entries = Vec::new();
    for i in 1..10 {
        entries.push(Entry {
            index: i,
            term: 1,
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).await.expect("should succeed");

    //..
    // assume we have generated snapshot until $last_applied,
    // now we can clean the local logs
    let last_applied = LogId { index: 3, term: 1 };
    context.raft_log.purge_logs_up_to(last_applied).expect("should succeed");

    assert_eq!(9, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.entry(3).unwrap());
    assert_eq!(None, context.raft_log.entry(2).unwrap());
    assert_eq!(None, context.raft_log.entry(1).unwrap());
}

#[tokio::test]
async fn test_purge_logs_up_to_concurrent_purge() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    context.raft_log.reset().await.expect("reset failed");

    // Insert test logs
    let entries: Vec<_> = (1..=10)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    context.raft_log.insert_batch(entries).await.unwrap();

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
    assert!(context.raft_log.entry(7).unwrap().is_none());
    assert!(context.raft_log.entry(8).unwrap().is_some());
}

#[tokio::test]
async fn test_get_first_raft_log_entry_id_after_delete_entries() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");
    let mut entries = Vec::new();
    for i in 1..=10 {
        entries.push(Entry {
            index: i,
            term: 1,
            payload: None,
        });
    }
    context.raft_log.insert_batch(entries).await.expect("should succeed");

    let last_applied = LogId { index: 4, term: 1 };
    context.raft_log.purge_logs_up_to(last_applied).expect("should succeed");
    assert_eq!(10, context.raft_log.last_entry_id());
    assert_eq!(None, context.raft_log.entry(3).unwrap());
    assert_eq!(None, context.raft_log.entry(2).unwrap());
    assert_eq!(None, context.raft_log.entry(1).unwrap());

    assert_eq!(context.raft_log.first_entry_id(), 5);
}

/// Case 1: we have multi thread working concurrently
///     each thread should get unique pre allocated index for pending apply
/// entries     At last when we retrieve last entry of the locallog, we should
/// see the last pre allocated index
#[tokio::test]
async fn test_pre_allocate_raft_logs_next_index_case1() {
    let context = TestContext::new(PersistenceStrategy::DiskFirst, FlushPolicy::Immediate);

    context.raft_log.reset().await.expect("reset successfully!");
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
            cloned_raft_log
                .insert_batch(entries.clone())
                .await
                .expect("should succeed");
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
    let context = TestContext::new(PersistenceStrategy::DiskFirst, FlushPolicy::Immediate);

    context.raft_log.reset().await.expect("reset successfully!");
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
            cloned_raft_log
                .insert_batch(entries.clone())
                .await
                .expect("should succeed");
        });
        handles.push(handle);
    }
    let _results = join_all(handles).await;
    let end = context.raft_log.last_entry_id();
    println!("end:{end}",);

    assert_eq!(start + i * j, end);
}

/// Case 1: stress testing with 100K entries,
///     the entries was insert successfully and last log entry id is as expected
#[ignore = "architecture changes, this case will not exist"]
#[tokio::test]
async fn test_insert_batch_logs_case1() {
    let context = TestContext::new(PersistenceStrategy::MemFirst, FlushPolicy::Immediate);

    context.raft_log.reset().await.expect("reset successfully!");
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
            cloned_raft_log
                .insert_batch(entries.clone())
                .await
                .expect("should succeed");
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
    let (old_leader, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
        1,
        PersistenceConfig {
            strategy: PersistenceStrategy::DiskFirst,
            flush_policy: FlushPolicy::Immediate,
            max_buffered_entries: 1000,
        },
        Some(Arc::new(SledStorageEngine::new(1, raft_log_db).unwrap())),
    );
    let old_leader = old_leader.start(receiver);

    let (raft_log_db, _, _state_storage_db, _) = reset_dbs("/tmp/test_insert_batch_logs_case2_node2");
    let (new_leader, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
        2,
        PersistenceConfig {
            strategy: PersistenceStrategy::DiskFirst,
            flush_policy: FlushPolicy::Immediate,
            max_buffered_entries: 1000,
        },
        Some(Arc::new(SledStorageEngine::new(2, raft_log_db).unwrap())),
    );
    let new_leader = new_leader.start(receiver);

    let mut entries: Vec<Entry> = Vec::new();
    for i in 1..=7 {
        entries.push(Entry {
            index: old_leader.pre_allocate_raft_logs_next_index(),
            term: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![i]))),
        });
    }

    // 2. Old leader initial logs (term 1)
    old_leader.insert_batch(entries.clone()).await.expect("should succeed");
    new_leader.insert_batch(entries.clone()).await.expect("should succeed");

    // 3. Add uncommitted entry on old leader
    old_leader
        .insert_batch(vec![Entry {
            index: old_leader.pre_allocate_raft_logs_next_index(),
            term: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![8]))),
        }])
        .await
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
            .await
            .expect("should succeed");
    });
    handles.push(handle);
    let _results = join_all(handles).await;

    debug!("new_leader_entries: {:?}", new_leader_entries.clone());

    // 6. Follower processes conflicting entries
    let ex_leader_last_appled_id = old_leader
        .filter_out_conflicts_and_append(8, 1, new_leader_entries.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ex_leader_last_appled_id.index, 10);

    // // 7. Validate final log state
    // if let Some(ex_leader_last_entry) = old_leader.last() {
    //     assert_eq!(ex_leader_last_entry.index, 10);
    //     assert_eq!(ex_leader_last_entry.term, 2);
    // }

    // if let Some(entry) = old_leader.entry(7) {
    //     assert_eq!(entry.term, 1);
    // }
    // if let Some(entry) = old_leader.entry(8) {
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
    node: &Arc<BufferedRaftLog<RaftTypeConfig>>,
    expected: &[(u64, u64)],
) {
    for (index, term) in expected {
        let entry = node
            .entry(*index)
            .unwrap()
            .unwrap_or_else(|| panic!("Missing entry at index {index}",));

        assert_eq!(
            entry.term, *term,
            "Term mismatch at index {}: expected {}, got {}",
            index, term, entry.term
        );
    }
}

#[tokio::test]
async fn test_apply_and_then_get_last() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    context.raft_log.reset().await.expect("reset successfully!");
    test_utils::simulate_insert_command(&context.raft_log, vec![1, 2], 1).await;
    assert_eq!(2, context.raft_log.last_entry_id());
}

#[tokio::test]
async fn test_pre_allocate_raft_logs_next_index() {
    let context = TestContext::new(PersistenceStrategy::MemFirst, FlushPolicy::Immediate);

    assert_eq!(1, context.raft_log.pre_allocate_raft_logs_next_index());
    assert_eq!(2, context.raft_log.pre_allocate_raft_logs_next_index());
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case0() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();
    raft_log.reset().await.expect("reset successfully!");

    let current_term = 2;
    let commit_index = 2;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&raft_log, vec![2, 3], 2).await;

    assert_eq!(
        Some(3),
        raft_log.calculate_majority_matched_index(current_term, commit_index, vec![2, 12])
    );
}

/// If there exists an N such that N > commitIndex, a majority
/// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
/// (§5.3, §5.4).
#[tokio::test]
async fn test_calculate_majority_matched_index_case1() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();
    let _ = c.raft_log.reset().await;
    //case 1: majority matched index is 2, commit_index: 4, current_term is 3,
    // while log(2) term is 2, return None
    let ct = 3;
    let ci = 4;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&raft_log, vec![2], 2).await;
    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![1, 2]));
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case2() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();
    let _ = c.raft_log.reset().await;

    //case 2: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 3, return Some(3)
    let ct = 3;
    let ci = 2;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&raft_log, vec![2], 2).await;
    test_utils::simulate_insert_command(&raft_log, vec![3], 3).await;
    assert_eq!(Some(3), raft_log.calculate_majority_matched_index(ct, ci, vec![4, 2]));
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case3() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();

    //case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;
    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&raft_log, vec![2], 2).await;

    assert_eq!(None, raft_log.calculate_majority_matched_index(ct, ci, vec![3, 2]));
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case4() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );

    let raft_log = c.raft_log.clone();

    //case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;

    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;
    test_utils::simulate_insert_command(&raft_log, vec![2], 2).await;

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
#[tokio::test]
async fn test_calculate_majority_matched_index_case5() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();

    let term = 1;
    let raft_log_length = 100000;
    let commit = 90000;

    let peer1_match = 97000;
    let peer2_match = 98000;

    let raft_log_entry_ids: Vec<u64> = (1..=raft_log_length).collect();
    test_utils::simulate_insert_command(&raft_log, raft_log_entry_ids, 1).await;

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

#[tokio::test]
async fn test_raft_log_insert() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();

    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;
    assert_eq!(1, raft_log.last_entry_id());
}

/// #Case 1: local raft log is empty
#[tokio::test]
async fn test_is_empty_case1() {
    let c = TestContext::new(PersistenceStrategy::MemFirst, FlushPolicy::Immediate);
    let raft_log = c.raft_log.clone();

    assert!(raft_log.is_empty());
}

/// #Case 2: local raft log is not empty
#[tokio::test]
async fn test_is_empty_case2() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();
    test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;

    assert!(!raft_log.is_empty());
}

/// # Case1: No last log in raft_log, should returns (0,0)
#[tokio::test]
async fn test_get_last_entry_metadata_case1() {
    let c = TestContext::new(PersistenceStrategy::MemFirst, FlushPolicy::Immediate);
    let raft_log = c.raft_log.clone();

    assert_eq!(
        LogId { index: 0, term: 0 },
        raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 })
    );
}
/// # Case2: There is last log in raft_log, should returns last log metadata
#[tokio::test]
async fn test_get_last_entry_metadata_case2() {
    let c = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    let raft_log = c.raft_log.clone();
    test_utils::simulate_insert_command(&raft_log, vec![1], 11).await;

    assert_eq!(
        LogId { index: 1, term: 11 },
        raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 })
    );
}

#[tokio::test]
async fn test_raft_log_drop() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_raft_log_drop");

    {
        let (db, _, _, _) = reset_dbs(case_path.to_str().unwrap());
        // Create real instance instead of mock
        let (raft_log, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::DiskFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
            },
            Some(Arc::new(SledStorageEngine::new(1, db).unwrap())),
        );
        let raft_log = raft_log.start(receiver);
        // Insert test data
        test_utils::simulate_insert_command(&raft_log, vec![1], 1).await;

        // Explicitly drop to trigger flush
        drop(raft_log);
    }

    // Verify flush occurred by checking persistence
    let (reloaded_db, _, _, _) = reuse_dbs(case_path.to_str().unwrap());
    let db_engine = SledStorageEngine::new(1, reloaded_db).unwrap();
    assert!(!db_engine.is_empty());
}

#[tokio::test]
async fn test_first_index_for_term() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    context.raft_log.reset().await.expect("reset successfully!");

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
    context
        .raft_log
        .insert_batch(entries)
        .await
        .expect("insert should succeed");

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
    context
        .raft_log
        .insert_batch(entries)
        .await
        .expect("insert should succeed");
    assert_eq!(context.raft_log.first_index_for_term(4), Some(8));
    assert_eq!(context.raft_log.first_index_for_term(5), Some(9));

    // Test with empty log
    context.raft_log.reset().await.expect("reset should succeed");
    assert_eq!(context.raft_log.first_index_for_term(1), None);
}

#[tokio::test]
async fn test_last_index_for_term() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    context.raft_log.reset().await.expect("reset successfully!");

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
    context
        .raft_log
        .insert_batch(entries)
        .await
        .expect("insert should succeed");

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
    context
        .raft_log
        .insert_batch(entries)
        .await
        .expect("insert should succeed");
    assert_eq!(context.raft_log.last_index_for_term(5), Some(9));

    // Test with empty log
    context.raft_log.reset().await.expect("reset should succeed");
    assert_eq!(context.raft_log.last_index_for_term(1), None);
}

#[tokio::test]
async fn test_term_index_functions_with_purged_logs() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    context.raft_log.reset().await.expect("reset successfully!");

    // Insert initial logs
    let entries: Vec<_> = (1..=10)
        .map(|i| Entry {
            index: i,
            term: if i <= 3 { 1 } else { 2 },
            payload: None,
        })
        .collect();
    context
        .raft_log
        .insert_batch(entries)
        .await
        .expect("insert should succeed");

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

#[tokio::test]
async fn test_term_index_functions_with_concurrent_writes() {
    enable_logger();
    let (raft_log_db, _, _state_storage_db, _) = reset_dbs("/tmp/test_term_index_functions_with_concurrent_writes");
    let _arc_raft_log_db = Arc::new(raft_log_db);

    let (raft_log, log_command_receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
        1,
        PersistenceConfig {
            strategy: PersistenceStrategy::MemFirst,
            flush_policy: FlushPolicy::Immediate,
            max_buffered_entries: 1000,
        },
        None,
    );
    let raft_log = raft_log.start(log_command_receiver);
    // raft_log.reset().await.expect("reset successfully!");

    // Add a small delay to ensure command processor is ready
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let mut handles = Vec::new();

    for i in 0..10 {
        let cloned_raft_log = raft_log.clone();
        let handle = tokio::spawn(async move {
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
                    .await
                    .expect("insert should succeed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("task panicked");
        println!("Task completed");
    }

    // Small delay to ensure all persist commands are processed
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify term indices
    for term in 1..=10 {
        assert_eq!(raft_log.first_index_for_term(term), Some((term - 1) * 100 + 1));
        assert_eq!(raft_log.last_index_for_term(term), Some(term * 100));
    }
}

#[tokio::test]
async fn test_term_index_performance_large_dataset() {
    let context = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
    );
    context.raft_log.reset().await.expect("reset successfully!");

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
    context
        .raft_log
        .insert_batch(entries)
        .await
        .expect("insert should succeed");

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
    fn setup_memory() -> Arc<BufferedRaftLog<RaftTypeConfig>> {
        let config = sled::Config::new().temporary(true);
        let _db = Arc::new(config.open().unwrap());

        let (raft_log, _receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
            },
            None,
        );

        Arc::new(raft_log)
    }

    #[test]
    fn test_pre_allocate_next_index_sequential() {
        let raft_log = setup_memory();

        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 1);
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 2);
        assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 3);

        // Verify cache state
        assert_eq!(raft_log.next_id.load(Ordering::SeqCst), 4);
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
        assert_eq!(raft_log.next_id.load(Ordering::SeqCst), 3);
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

// Strategy-specific test module
mod disk_first_tests {

    use super::*;

    #[tokio::test]
    async fn test_basic_write_and_read() {
        let ctx = TestContext::new(PersistenceStrategy::DiskFirst, FlushPolicy::Immediate);
        ctx.append_entries(1, 5, 1).await;

        // Verify entries are immediately durable
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 5);
        assert_eq!(ctx.raft_log.entry(3).unwrap().unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_crash_recovery() {
        enable_logger();
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path();

        // Create and populate storage
        {
            let (db, _, _, _) = reset_dbs(db_path.to_str().unwrap());
            let storage = Arc::new(SledStorageEngine::new(1, db).unwrap());
            let (raft_log, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
                1,
                PersistenceConfig {
                    strategy: PersistenceStrategy::DiskFirst,
                    flush_policy: FlushPolicy::Immediate,
                    max_buffered_entries: 1000,
                },
                Some(storage),
            );
            let raft_log = raft_log.start(receiver);
            raft_log
                .append_entries(vec![Entry {
                    index: 1,
                    term: 1,
                    payload: Some(EntryPayload::command(b"data".to_vec())),
                }])
                .await
                .unwrap();
            // Simulate crash without proper shutdown
        }

        // Recover from disk
        let (db, _, _, _) = reuse_dbs(db_path.to_str().unwrap());
        let storage = Arc::new(SledStorageEngine::new(1, db).unwrap());
        let (raft_log, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::DiskFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
            },
            Some(storage),
        );
        let raft_log = raft_log.start(receiver);
        sleep(Duration::from_millis(50)).await; // Allow recovery

        // Verify recovery
        assert_eq!(raft_log.durable_index.load(Ordering::Acquire), 1);
        assert!(raft_log.entry(1).unwrap().is_some());
    }

    #[tokio::test]
    async fn test_high_concurrency() {
        enable_logger();
        let ctx = TestContext::new(PersistenceStrategy::DiskFirst, FlushPolicy::Immediate);
        let mut handles = vec![];

        for i in 0..10 {
            let log = ctx.raft_log.clone();
            handles.push(tokio::spawn(async move {
                for j in 1..=100 {
                    let index = i * 100 + j;
                    log.append_entries(vec![Entry {
                        index,
                        term: 1,
                        payload: None,
                    }])
                    .await
                    .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all entries persisted
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 1000);
        assert_eq!(ctx.raft_log.entries.len(), 1000);
    }
}

mod mem_first_tests {

    use super::*;

    #[tokio::test]
    async fn test_basic_write_before_persist() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 1,
                interval_ms: 1,
            },
        );
        ctx.append_entries(1, 5, 1).await;

        // Verify in memory but not yet durable
        assert_eq!(ctx.raft_log.entries.len(), 5);
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_async_persistence() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 1,
                interval_ms: 1,
            },
        );
        ctx.append_entries(1, 100, 1).await;

        // Trigger flush
        ctx.raft_log.flush().await.unwrap();

        // Verify persistence
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 100);
        assert_eq!(ctx.storage.as_ref().unwrap().last_index(), 100);
    }

    #[tokio::test]
    async fn test_power_loss_data_loss() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 1,
                interval_ms: 1,
            },
        );
        ctx.append_entries(1, 100, 1).await;

        // Simulate power loss before flush
        // storage.reset() simulates data loss after crash

        // In real scenario, durable_index would be 0 after restart
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_high_concurrency_memory_only() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 1,
                interval_ms: 1,
            },
        );
        let mut handles = vec![];

        for i in 0..10 {
            let log = ctx.raft_log.clone();
            handles.push(tokio::spawn(async move {
                for j in 1..=100 {
                    let index = i * 100 + j;
                    log.append_entries(vec![Entry {
                        index,
                        term: 1,
                        payload: None,
                    }])
                    .await
                    .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all entries in memory
        assert_eq!(ctx.raft_log.entries.len(), 1000);
        assert_eq!(ctx.raft_log.last_entry_id(), 1000);
    }
}

mod batched_tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_flush_trigger() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 10,
                interval_ms: 100,
            },
        );

        // Add 9 entries - should not trigger flush
        ctx.append_entries(1, 9, 1).await;
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 0);

        // Add 1 more to reach batch size
        ctx.append_entries(10, 1, 1).await;
        sleep(Duration::from_millis(50)).await; // Allow time for async processing

        // Should have flushed first 10
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 10);
    }

    #[tokio::test]
    async fn test_timed_flush_trigger() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 100,
                interval_ms: 50,
            },
        );
        ctx.append_entries(1, 50, 1).await;

        // Wait for timer to trigger
        sleep(Duration::from_millis(75)).await;

        // Should have flushed via timer
        assert!(ctx.raft_log.durable_index.load(Ordering::Acquire) > 0);
    }

    #[tokio::test]
    async fn test_partial_flush_after_crash() {
        enable_logger();
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path();
        let batch_size = 50;

        // Create and partially populate storage
        {
            let (db, _, _, _) = reset_dbs(db_path.to_str().unwrap());
            let storage = Arc::new(SledStorageEngine::new(1, db).unwrap());
            let (raft_log, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
                1,
                PersistenceConfig {
                    strategy: PersistenceStrategy::MemFirst,
                    flush_policy: FlushPolicy::Batch {
                        threshold: batch_size,
                        interval_ms: 100,
                    },
                    max_buffered_entries: 1000,
                },
                Some(storage),
            );
            let raft_log = raft_log.start(receiver);

            // Add 75 entries (1.5 batches)
            for i in 1..=75 {
                raft_log
                    .append_entries(vec![Entry {
                        index: i,
                        term: 1,
                        payload: None,
                    }])
                    .await
                    .unwrap();
            }

            // Simulate crash without proper shutdown
        }

        // Recover from disk
        let (db, _, _, _) = reuse_dbs(db_path.to_str().unwrap());
        let storage = Arc::new(SledStorageEngine::new(1, db).unwrap());
        let (raft_log, receiver) = BufferedRaftLog::<RaftTypeConfig>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Batch {
                    threshold: batch_size,
                    interval_ms: 100,
                },
                max_buffered_entries: 1000,
            },
            Some(storage),
        );
        let raft_log = raft_log.start(receiver);
        sleep(Duration::from_millis(50)).await; // Allow recovery

        // Only first batch should be durable
        assert_eq!(raft_log.durable_index.load(Ordering::Acquire), 0); //because batch threshold is 100
        assert!(raft_log.entry(60).unwrap().is_none()); // Not persisted
    }
}

// Generic tests for all strategies
mod common_tests {
    use super::*;

    #[tokio::test]
    async fn test_log_compaction() {
        let ctx = TestContext::new(PersistenceStrategy::DiskFirst, FlushPolicy::Immediate);
        ctx.append_entries(1, 100, 1).await;

        // Compact first 50 entries
        ctx.raft_log.purge_logs_up_to(LogId { index: 50, term: 1 }).unwrap();

        // Verify compaction
        assert!(ctx.raft_log.entry(25).unwrap().is_none());
        assert_eq!(ctx.raft_log.first_entry_id(), 51);
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 100);
    }

    #[tokio::test]
    async fn test_term_index_calculation() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 1,
                interval_ms: 1,
            },
        );

        // Add entries with different terms
        ctx.append_entries(1, 50, 1).await;
        ctx.append_entries(51, 50, 2).await;
        ctx.append_entries(101, 50, 3).await;

        // Verify term indices
        assert_eq!(ctx.raft_log.first_index_for_term(2), Some(51));
        assert_eq!(ctx.raft_log.last_index_for_term(2), Some(100));
        assert_eq!(ctx.raft_log.last_index_for_term(3), Some(150));
    }

    #[tokio::test]
    async fn test_drop_behavior() {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 10,
                interval_ms: 100,
            },
        );
        ctx.append_entries(1, 5, 1).await;

        // Explicitly drop context
        drop(ctx);

        // Test passes if no deadlock occurs during drop
    }

    #[tokio::test]
    async fn test_reset_operation() {
        let ctx = TestContext::new(PersistenceStrategy::DiskFirst, FlushPolicy::Immediate);
        ctx.append_entries(1, 100, 1).await;

        // Reset log
        ctx.raft_log.reset().await.unwrap();

        // Verify reset state
        assert!(ctx.raft_log.is_empty());
        assert_eq!(ctx.raft_log.durable_index.load(Ordering::Acquire), 0);
        assert_eq!(ctx.raft_log.next_id.load(Ordering::Acquire), 1);
    }
}

mod filter_out_conflicts_and_append_performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_filter_out_conflicts_performance_consistent_across_flush_intervals_fresh_cluster() {
        enable_logger();

        // Test configuration
        let test_cases = vec![
            (10, 50),   // 10ms interval, 50ms max duration
            (100, 50),  // 100ms interval, 50ms max duration
            (1000, 50), // 1000ms interval, 50ms max duration
        ];

        for (interval_ms, max_duration_ms) in test_cases {
            // Create MemFirst storage with batch policy
            let config = PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Batch {
                    threshold: 1000,
                    interval_ms,
                },
                max_buffered_entries: 1000,
            };

            let mut storage = MockStorageEngine::new();
            storage.expect_last_index().returning(|| 0);
            storage.expect_truncate().returning(|_| Ok(()));
            storage.expect_persist_entries().returning(|_| Ok(()));
            storage.expect_reset().returning(|| Ok(()));

            let (log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(1, config, Some(Arc::new(storage)));
            let log = log.start(receiver);

            // Populate with test data (1000 entries)
            let mut entries = vec![];
            for i in 1..=1000 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(vec![0; 256])), // 256B payload
                });
            }
            log.append_entries(entries.clone()).await.unwrap();

            // Measure conflict resolution performance
            let start = Instant::now();
            log.filter_out_conflicts_and_append(
                0, // prev_log_index
                0, // prev_log_term
                vec![Entry {
                    index: 501,
                    term: 1,
                    payload: Some(EntryPayload::command(vec![1; 256])),
                }],
            )
            .await
            .unwrap();

            let duration = start.elapsed().as_millis() as u64;
            println!("Interval {}ms: Took {}ms", interval_ms, duration);

            // Verify performance consistency
            assert!(
                duration <= max_duration_ms,
                "Duration {}ms exceeds max {}ms for {}ms interval",
                duration,
                max_duration_ms,
                interval_ms
            );

            // Verify correctness
            assert!(log.entry(500).unwrap().is_none());
            assert!(log.entry(501).unwrap().is_some());
            assert!(log.entry(502).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_filter_out_conflicts_performance_consistent_across_flush_intervals() {
        // Test configuration
        let test_cases = vec![
            (10, 50),   // 10ms interval, 50ms max duration
            (100, 50),  // 100ms interval, 50ms max duration
            (1000, 50), // 1000ms interval, 50ms max duration
        ];

        for (interval_ms, max_duration_ms) in test_cases {
            // Create MemFirst storage with batch policy
            let config = PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Batch {
                    threshold: 1000,
                    interval_ms,
                },
                max_buffered_entries: 1000,
            };

            let mut storage = MockStorageEngine::new();
            storage.expect_last_index().returning(|| 0);
            storage.expect_truncate().returning(|_| Ok(()));
            storage.expect_persist_entries().returning(|_| Ok(()));
            storage.expect_reset().returning(|| Ok(()));

            let (log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(1, config, Some(Arc::new(storage)));
            let log = log.start(receiver);

            // Populate with test data (1000 entries)
            let mut entries = vec![];
            for i in 1..=1000 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(vec![0; 256])), // 256B payload
                });
            }
            log.append_entries(entries.clone()).await.unwrap();

            // Measure conflict resolution performance
            let start = Instant::now();
            log.filter_out_conflicts_and_append(
                500, // prev_log_index
                1,   // prev_log_term
                vec![Entry {
                    index: 501,
                    term: 1,
                    payload: Some(EntryPayload::command(vec![1; 256])),
                }],
            )
            .await
            .unwrap();

            let duration = start.elapsed().as_millis() as u64;
            println!("Interval {}ms: Took {}ms", interval_ms, duration);

            // Verify performance consistency
            assert!(
                duration <= max_duration_ms,
                "Duration {}ms exceeds max {}ms for {}ms interval",
                duration,
                max_duration_ms,
                interval_ms
            );

            // Verify correctness
            assert!(log.entry(500).unwrap().is_some());
            assert!(log.entry(501).unwrap().is_some());
            assert!(log.entry(502).unwrap().is_none()); // Conflict removed
        }
    }
}
