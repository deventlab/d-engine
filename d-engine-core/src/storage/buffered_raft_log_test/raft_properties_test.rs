use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy};
use d_engine_proto::common::Entry;

#[tokio::test]
async fn test_log_matching_property() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::DiskFirst,
        FlushPolicy::Immediate,
        "test_log_matching",
    );

    // Build log: [1,1] [2,1] [3,2] [4,2] [5,3]
    ctx.append_entries(1, 2, 1).await;
    ctx.append_entries(3, 2, 2).await;
    ctx.append_entries(5, 1, 3).await;

    // Verify consistency
    assert_eq!(ctx.raft_log.entry_term(3), Some(2));
    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(4));

    // Conflict resolution: new leader with [1,1] [2,1] [3,2] [4,3]
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(
            3,
            2,
            vec![Entry {
                index: 4,
                term: 3,
                payload: None,
            }],
        )
        .await
        .unwrap();

    assert_eq!(result.unwrap().index, 4);
    assert_eq!(ctx.raft_log.entry_term(4), Some(3));
    assert!(ctx.raft_log.entry(5).unwrap().is_none());
}

#[tokio::test]
async fn test_leader_completeness_property() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::DiskFirst,
        FlushPolicy::Immediate,
        "test_leader_completeness",
    );

    // Leader writes entries
    ctx.append_entries(1, 10, 1).await;

    // Simulate majority replication scenario:
    // Leader has: [1,2,3,4,5,6,7,8,9,10]
    // Peer1 matched: 7, Peer2 matched: 6, Peer3 matched: 5
    // With leader's last_entry_id (10), the sorted array is: [10, 7, 6, 5]
    // Majority index (median) at position 2 is: 6
    let commit_index = ctx.raft_log.calculate_majority_matched_index(
        1,             // current_term
        0,             // commit_index (previous)
        vec![7, 6, 5], // peer match indexes
    );

    // Expected result is 6, not 7
    // Because: sorted [10, 7, 6, 5], majority_index = peers[len/2] = peers[2] = 6
    assert_eq!(commit_index, Some(6), "Majority commit index should be 6");

    // New leader must have all committed entries
    for i in 1..=6 {
        assert!(
            ctx.raft_log.entry(i).unwrap().is_some(),
            "Entry {i} should exist",
        );
    }
}
