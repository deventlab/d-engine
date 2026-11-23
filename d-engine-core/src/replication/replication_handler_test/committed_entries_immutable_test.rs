//! A follower must never let an AppendEntries request change an entry that is already committed.
//!
//! A correct leader never sends such a request: it holds every committed entry, so its log
//! agrees with the follower's at every committed index. These tests cover the case where that
//! assumption breaks (a buggy or illegitimate sender). The follower still must not rewrite
//! committed history.
//!
//! Scenario used throughout: the follower holds entries 1..=50 from term 1, and entries
//! 1..=30 are committed. A sender in term 2 asks it to replace entries after index 10.

use std::sync::Arc;

use d_engine_proto::common::Entry;
use d_engine_proto::server::replication::AppendEntriesRequest;

use crate::MockRaftLog;
use crate::MockTypeConfig;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::ReplicationHandler;
use crate::StateSnapshot;
use crate::test_utils::RaftLogCoreTestContext;
use crate::test_utils::mock_entries;

const OLD_TERM: u64 = 1;
const NEW_TERM: u64 = 2;
const COMMIT_INDEX: u64 = 30;

fn follower_state() -> StateSnapshot {
    StateSnapshot {
        role: d_engine_proto::common::NodeRole::Follower as i32,
        current_term: NEW_TERM,
        voted_for: None,
        commit_index: COMMIT_INDEX,
    }
}

fn request(
    prev_log_index: u64,
    entries: Vec<Entry>,
) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term: NEW_TERM,
        leader_id: 1,
        prev_log_index,
        prev_log_term: OLD_TERM,
        entries,
        leader_commit_index: COMMIT_INDEX,
    }
}

/// The handler is generic over a mock log type; this mock forwards every call the follower
/// path makes to the real in-memory log, so the behavior under test is the real one.
fn log_backed_by(follower: &RaftLogCoreTestContext) -> Arc<MockRaftLog> {
    let mut mock = MockRaftLog::new();

    let real = follower.raft_log.clone();
    mock.expect_last_log_id().returning(move || real.last_log_id());
    let real = follower.raft_log.clone();
    mock.expect_last_entry_id().returning(move || real.last_entry_id());
    let real = follower.raft_log.clone();
    mock.expect_entry_term().returning(move |index| real.entry_term(index));
    let real = follower.raft_log.clone();
    mock.expect_first_index_for_term()
        .returning(move |term| real.first_index_for_term(term));
    let real = follower.raft_log.clone();
    mock.expect_filter_out_conflicts_and_append().returning(
        move |prev_index, prev_term, entries, commit_index| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(real.filter_out_conflicts_and_append(
                    prev_index,
                    prev_term,
                    entries,
                    commit_index,
                ))
            })
        },
    );
    Arc::new(mock)
}

async fn follower_with_committed_prefix(name: &str) -> RaftLogCoreTestContext {
    let ctx = RaftLogCoreTestContext::new(name);
    ctx.append_entries(1, 50, OLD_TERM).await;
    ctx
}

fn term_at(
    follower: &RaftLogCoreTestContext,
    index: u64,
) -> Option<u64> {
    follower.raft_log.entry(index).unwrap().map(|entry| entry.term)
}

/// The sender's entry 11 has another term than the follower's committed entry 11.
/// Whatever the follower answers, entries 11..=30 must still be the ones it committed.
/// The outcome of the call itself is not asserted: rejecting is fine, so is returning an
/// error; rewriting committed history is not.
#[tokio::test(flavor = "multi_thread")]
async fn test_append_entries_conflict_at_committed_index_leaves_committed_entries_unchanged() {
    let follower = follower_with_committed_prefix("conflict_at_committed_index").await;
    let handler = ReplicationHandler::<MockTypeConfig>::new(2);
    let conflicting = mock_entries(11, 1, NEW_TERM);

    let _ = handler
        .handle_append_entries(
            request(10, conflicting),
            &follower_state(),
            &log_backed_by(&follower),
        )
        .await;

    for index in 1..=COMMIT_INDEX {
        assert_eq!(
            term_at(&follower, index),
            Some(OLD_TERM),
            "entry {index} is committed and must be unchanged"
        );
    }
}

/// Control for the test above: a conflict strictly after the committed prefix is the normal
/// election-recovery case, and the stale uncommitted tail must still be replaced. Without
/// this, a handler that rejected every conflicting request would pass the test above.
#[tokio::test(flavor = "multi_thread")]
async fn test_append_entries_conflict_after_committed_index_replaces_uncommitted_tail() {
    let follower = follower_with_committed_prefix("conflict_after_committed_index").await;
    let handler = ReplicationHandler::<MockTypeConfig>::new(2);
    let conflicting = mock_entries(COMMIT_INDEX + 1, 1, NEW_TERM);

    handler
        .handle_append_entries(
            request(COMMIT_INDEX, conflicting),
            &follower_state(),
            &log_backed_by(&follower),
        )
        .await
        .expect("handle_append_entries");

    assert_eq!(term_at(&follower, COMMIT_INDEX + 1), Some(NEW_TERM));
    assert_eq!(
        follower.raft_log.last_entry_id(),
        COMMIT_INDEX + 1,
        "entries after the replaced one belonged to the earlier leader and must be gone"
    );
    for index in 1..=COMMIT_INDEX {
        assert_eq!(term_at(&follower, index), Some(OLD_TERM));
    }
}
