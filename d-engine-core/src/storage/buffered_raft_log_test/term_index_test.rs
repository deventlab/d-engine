//! Term index calculation and tracking tests
//!
//! Tests verify term boundary tracking:
//! - First/last index for term queries
//! - Updates after purge/compaction
//! - Correctness under concurrent writes

use d_engine_proto::common::Entry;

use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy, RaftLog};

#[tokio::test]
async fn test_first_index_for_term() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_first_index_for_term",
    );
    ctx.raft_log.reset().await.unwrap();

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
    ctx.raft_log.insert_batch(entries).await.unwrap();

    assert_eq!(ctx.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(5));
    assert_eq!(ctx.raft_log.first_index_for_term(4), None);

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
    ctx.raft_log.insert_batch(entries).await.unwrap();
    assert_eq!(ctx.raft_log.first_index_for_term(4), Some(8));
    assert_eq!(ctx.raft_log.first_index_for_term(5), Some(9));

    ctx.raft_log.reset().await.unwrap();
    assert_eq!(ctx.raft_log.first_index_for_term(1), None);
}

#[tokio::test]
async fn test_last_index_for_term() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_last_index_for_term",
    );
    ctx.raft_log.reset().await.unwrap();

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
    ctx.raft_log.insert_batch(entries).await.unwrap();

    assert_eq!(ctx.raft_log.last_index_for_term(1), Some(2));
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(4));
    assert_eq!(ctx.raft_log.last_index_for_term(3), Some(7));
    assert_eq!(ctx.raft_log.last_index_for_term(4), None);

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
    ctx.raft_log.insert_batch(entries).await.unwrap();
    assert_eq!(ctx.raft_log.last_index_for_term(5), Some(9));

    ctx.raft_log.reset().await.unwrap();
    assert_eq!(ctx.raft_log.last_index_for_term(1), None);
}

#[tokio::test]
async fn test_term_index_functions_with_purged_logs() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_term_index_with_purged",
    );
    ctx.raft_log.reset().await.unwrap();

    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: None,
        },
        Entry {
            index: 2,
            term: 2,
            payload: None,
        },
        Entry {
            index: 3,
            term: 2,
            payload: None,
        },
        Entry {
            index: 4,
            term: 3,
            payload: None,
        },
    ];
    ctx.raft_log.insert_batch(entries).await.unwrap();

    ctx.raft_log
        .purge_logs_up_to(d_engine_proto::common::LogId { index: 2, term: 2 })
        .await
        .unwrap();

    assert_eq!(ctx.raft_log.first_index_for_term(1), None);
    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(4));
}

#[tokio::test]
async fn test_term_index_functions_with_concurrent_writes() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 100,
            interval_ms: 1000,
        },
        "test_term_index_concurrent",
    );
    ctx.raft_log.reset().await.unwrap();

    let mut handles = vec![];
    for term in 1..=5 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for i in 1..=10 {
                let index = (term - 1) * 10 + i;
                log.insert_batch(vec![Entry {
                    index,
                    term,
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

    assert_eq!(ctx.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(ctx.raft_log.last_index_for_term(1), Some(10));
    assert_eq!(ctx.raft_log.first_index_for_term(5), Some(41));
    assert_eq!(ctx.raft_log.last_index_for_term(5), Some(50));
}

#[tokio::test]
async fn test_term_index_performance_large_dataset() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1000,
            interval_ms: 5000,
        },
        "test_term_index_performance",
    );
    ctx.raft_log.reset().await.unwrap();

    let mut entries = vec![];
    for i in 1..=1000 {
        entries.push(Entry {
            index: i,
            term: (i / 100) + 1,
            payload: None,
        });
    }
    ctx.raft_log.insert_batch(entries).await.unwrap();

    let start = tokio::time::Instant::now();
    for term in 1..=10 {
        ctx.raft_log.first_index_for_term(term);
        ctx.raft_log.last_index_for_term(term);
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 100,
        "Term index lookup should be fast (took {:?})",
        elapsed
    );
}
