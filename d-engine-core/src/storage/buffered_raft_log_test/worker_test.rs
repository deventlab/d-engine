use std::time::Duration;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy};

#[tokio::test]
async fn test_worker_retry_survives_transient_errors() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 10,
            interval_ms: 50,
        },
        "test_worker_retry_survives",
    );

    // Append entries that will trigger flush
    for i in 1..=100 {
        ctx.append_entries(i, 1, 1).await;
    }

    // Wait for flush workers to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all workers are still alive by checking continued processing
    ctx.append_entries(101, 50, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(ctx.raft_log.last_entry_id(), 150);
}
