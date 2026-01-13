use crate::storage::raft_log::RaftLog;
use crate::test_utils::{BufferedRaftLogTestContext, simulate_insert_command};
use crate::{FlushPolicy, PersistenceStrategy};

#[tokio::test]
async fn test_remove_middle_range() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_middle_range",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;
    assert_eq!(ctx.raft_log.len(), 100);
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 100);

    // Remove middle range
    ctx.raft_log.remove_range(40..=60);

    // Verify removal
    assert_eq!(ctx.raft_log.len(), 79);
    assert_eq!(ctx.raft_log.first_entry_id(), 1); // Min unchanged
    assert_eq!(ctx.raft_log.last_entry_id(), 100); // Max unchanged

    // Verify specific entries
    assert!(ctx.raft_log.entry(39).unwrap().is_some());
    assert!(ctx.raft_log.entry(40).unwrap().is_none());
    assert!(ctx.raft_log.entry(60).unwrap().is_none());
    assert!(ctx.raft_log.entry(61).unwrap().is_some());
}

#[tokio::test]
async fn test_remove_from_start() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_from_start",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Remove first 50 entries
    ctx.raft_log.remove_range(1..=50);

    // Verify state
    assert_eq!(ctx.raft_log.len(), 50);
    assert_eq!(ctx.raft_log.first_entry_id(), 51); // Min updated
    assert_eq!(ctx.raft_log.last_entry_id(), 100); // Max unchanged

    // Boundary checks
    assert!(ctx.raft_log.entry(50).unwrap().is_none());
    assert!(ctx.raft_log.entry(51).unwrap().is_some());
}

#[tokio::test]
async fn test_remove_to_end() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_to_end",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Remove from 90 to end
    ctx.raft_log.remove_range(90..=u64::MAX);

    // Verify state
    assert_eq!(ctx.raft_log.len(), 89);
    assert_eq!(ctx.raft_log.first_entry_id(), 1); // Min unchanged
    assert_eq!(ctx.raft_log.last_entry_id(), 89); // Max updated

    // Boundary checks
    assert!(ctx.raft_log.entry(89).unwrap().is_some());
    assert!(ctx.raft_log.entry(90).unwrap().is_none());
    assert!(ctx.raft_log.entry(100).unwrap().is_none());
}

#[tokio::test]
async fn test_remove_empty_range() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_empty_range",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    simulate_insert_command(&ctx.raft_log, vec![1, 2, 3], 1).await;

    // Remove nothing
    ctx.raft_log.remove_range(5..=10);

    assert_eq!(ctx.raft_log.len(), 3);
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 3);
}

#[tokio::test]
async fn test_remove_entire_log() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_entire_log",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Remove entire log
    ctx.raft_log.remove_range(1..=u64::MAX);

    // Verify state
    assert_eq!(ctx.raft_log.len(), 0);
    assert_eq!(ctx.raft_log.first_entry_id(), 0);
    assert_eq!(ctx.raft_log.last_entry_id(), 0);
    assert!(ctx.raft_log.is_empty());
}

#[tokio::test]
async fn test_remove_single_entry() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1,
            interval_ms: 1,
        },
        "test_remove_single_entry",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    simulate_insert_command(&ctx.raft_log, vec![1, 2, 3], 1).await;

    // Remove middle entry
    ctx.raft_log.remove_range(2..=2);

    assert_eq!(ctx.raft_log.len(), 2);
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 3);
    assert!(ctx.raft_log.entry(2).unwrap().is_none());
}
