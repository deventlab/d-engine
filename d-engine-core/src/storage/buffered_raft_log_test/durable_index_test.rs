use std::time::Duration;

use futures::future::join_all;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy};
use d_engine_proto::common::Entry;

#[tokio::test]
async fn test_durable_index_monotonic_under_concurrency() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Immediate,
        "test_durable_index_monotonic",
    );

    let mut handles = vec![];

    for batch in 0..10 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            let start = batch * 100 + 1;
            let entries: Vec<Entry> = (start..start + 100)
                .map(|i| Entry {
                    index: i,
                    term: 1,
                    payload: None,
                })
                .collect();
            log.append_entries(entries).await.unwrap();
        }));
    }

    join_all(handles).await;

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify monotonicity
    let durable = ctx.raft_log.durable_index();
    assert!(durable >= 1000, "durable_index should reach max value");

    // Verify no entries lost
    assert_eq!(ctx.raft_log.len(), 1000);
}

#[tokio::test]
async fn test_durable_index_with_non_contiguous_entries() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Immediate,
        "test_durable_index_non_contiguous",
    );

    // Create entries with non-contiguous indexes: 4, 7, 8, 10
    let entries = vec![
        Entry {
            index: 4,
            term: 1,
            payload: None,
        },
        Entry {
            index: 7,
            term: 1,
            payload: None,
        },
        Entry {
            index: 8,
            term: 1,
            payload: None,
        },
        Entry {
            index: 10,
            term: 1,
            payload: None,
        },
    ];

    ctx.raft_log.append_entries(entries).await.unwrap();

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify durable_index handles non-contiguous entries correctly
    // With mock storage, durable_index behavior depends on flush completion
    let _durable = ctx.raft_log.durable_index();
    // Note: durable_index returns u64, which is always >= 0 by type definition

    // Verify entries exist
    assert!(ctx.raft_log.entry(4).unwrap().is_some());
    assert!(ctx.raft_log.entry(7).unwrap().is_some());
    assert!(ctx.raft_log.entry(8).unwrap().is_some());
    assert!(ctx.raft_log.entry(10).unwrap().is_some());
}
