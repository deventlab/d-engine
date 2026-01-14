use std::time::Duration;

use futures::future::join_all;
use tokio;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::{BufferedRaftLogTestContext, simulate_insert_command};
use crate::{FlushPolicy, PersistenceStrategy};
use d_engine_proto::common::{Entry, LogId};

#[tokio::test]
async fn test_remove_range_with_concurrent_reads() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_range_with_concurrent_reads",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 1000 entries
    simulate_insert_command(&ctx.raft_log, (1..=1000).collect(), 1).await;

    // Spawn concurrent readers
    let readers: Vec<_> = (0..10)
        .map(|_| {
            let log = ctx.raft_log.clone();
            tokio::spawn(async move {
                for i in 1..=1000 {
                    // This shouldn't panic even during removal
                    let _ = log.entry(i);
                }
            })
        })
        .collect();

    // Remove a large range while reads are happening
    ctx.raft_log.remove_range(300..=700);

    // Wait for readers to finish
    for handle in readers {
        handle.await.expect("reader task failed");
    }

    // Verify final state
    assert_eq!(ctx.raft_log.len(), 599); // 299 before + 300 after
    assert!(ctx.raft_log.entry(299).unwrap().is_some());
    assert!(ctx.raft_log.entry(300).unwrap().is_none());
    assert!(ctx.raft_log.entry(700).unwrap().is_none());
    assert!(ctx.raft_log.entry(701).unwrap().is_some());
}

#[tokio::test]
async fn test_concurrent_append_and_purge() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 100,
            interval_ms: 50,
        },
        "test_concurrent_append_purge",
    );

    // Pre-populate
    simulate_insert_command(&ctx.raft_log, (1..=1000).collect(), 1).await;

    let mut handles = vec![];

    // Concurrent appends
    for i in 0..5 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            let start = 1000 + i * 100 + 1;
            for j in 0..100 {
                log.append_entries(vec![Entry {
                    index: start + j,
                    term: 2,
                    payload: None,
                }])
                .await
                .unwrap();
            }
        }));
    }

    // Concurrent purges
    for cutoff in [100, 200, 300, 400, 500] {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            log.purge_logs_up_to(LogId {
                index: cutoff,
                term: 1,
            })
            .await
            .unwrap();
        }));
    }

    join_all(handles).await;

    // Verify consistency
    assert!(ctx.raft_log.first_entry_id() > 500);
    assert_eq!(ctx.raft_log.last_entry_id(), 1500);
}
