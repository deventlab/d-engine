//! What a follower reports as `last_match`, and why it must never exceed what the request verified.
//!
//! A leader turns `last_match` into that follower's `match_index`, and counts it toward the
//! commit quorum. The follower has only verified its log against the leader's up to
//! `prev_log_index + entries.len()` of the request it just handled. Anything past that point
//! may be a tail left by an earlier leader that differs from the leader's log, so reporting
//! it would let unreplicated entries be counted as replicated.
//!
//! Scenario used throughout: the follower holds entries 1..=50 from term 1. The new leader
//! (term 2) has the same entries 1..=10, then its own, different entry 11.

use std::sync::Arc;

use d_engine_proto::common::Entry;
use d_engine_proto::server::replication::{AppendEntriesRequest, append_entries_response};

use crate::MockRaftLog;
use crate::MockTypeConfig;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::ReplicationHandler;
use crate::StateSnapshot;
use crate::test_utils::{RaftLogCoreTestContext, mock_entries};

const OLD_TERM: u64 = 1;
const NEW_TERM: u64 = 2;

fn follower_state() -> StateSnapshot {
    StateSnapshot {
        role: d_engine_proto::common::NodeRole::Follower as i32,
        current_term: NEW_TERM,
        voted_for: None,
        commit_index: 0,
    }
}

/// A follower whose log is 1..=50, all from the old term (entries 11..=50 were never committed).
async fn follower_with_stale_tail(name: &str) -> RaftLogCoreTestContext {
    let ctx = RaftLogCoreTestContext::new(name);
    ctx.append_entries(1, 50, OLD_TERM).await;
    ctx
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
        leader_commit_index: 0,
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
        move |prev_index, prev_term, entries| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(real.filter_out_conflicts_and_append(prev_index, prev_term, entries))
            })
        },
    );
    Arc::new(mock)
}

/// Handles `request` on the follower and returns the index it reports, or `None` for a rejection.
async fn reported_last_match(
    follower: &RaftLogCoreTestContext,
    request: AppendEntriesRequest,
) -> Option<u64> {
    let handler = ReplicationHandler::<MockTypeConfig>::new(2);
    let response = handler
        .handle_append_entries(request, &follower_state(), &log_backed_by(follower))
        .await
        .expect("handle_append_entries")
        .response;

    match response.result {
        Some(append_entries_response::Result::Success(success)) => {
            Some(success.last_match.expect("success carries last_match").index)
        }
        _ => None,
    }
}

/// An empty request verified the logs only up to `prev_log_index`; the reply must not go past it.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_request_reply_does_not_claim_more_than_prev_log_index() {
    let follower = follower_with_stale_tail("empty_request_reply").await;

    let reported = reported_last_match(&follower, request(10, vec![])).await;

    assert_eq!(
        reported,
        Some(10),
        "the follower's own last entry is 50, but the request only verified index 10"
    );
}

/// A request that re-sends entries the follower already has must be answered with the end of
/// the entries it carried, not with the follower's longer log.
#[tokio::test(flavor = "multi_thread")]
async fn test_resent_entries_reply_ends_at_the_last_entry_of_the_request() {
    let follower = follower_with_stale_tail("resent_entries_reply").await;
    let resent = mock_entries(11, 5, OLD_TERM);

    let reported = reported_last_match(&follower, request(10, resent)).await;

    assert_eq!(reported, Some(15), "the request carried entries 11..=15");
}

/// The leader's first request after election carries its own entry at index 11. The follower
/// must drop its stale tail, and the reply must end at index 11.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflicting_entry_replaces_the_stale_tail_and_reply_ends_at_it() {
    let follower = follower_with_stale_tail("conflicting_entry_reply").await;
    let leaders_entry = mock_entries(11, 1, NEW_TERM);

    let reported = reported_last_match(&follower, request(10, leaders_entry)).await;

    assert_eq!(reported, Some(11));
    assert_eq!(
        follower.raft_log.last_entry_id(),
        11,
        "entries 12..=50 belonged to the earlier leader and must be gone"
    );
}

/// An empty request whose `prev_log_index` the follower does not hold must be rejected.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_request_beyond_the_followers_log_is_rejected() {
    let follower = follower_with_stale_tail("empty_request_beyond_log").await;

    let reported = reported_last_match(&follower, request(60, vec![])).await;

    assert_eq!(
        reported, None,
        "the follower has no entry 60 to match against"
    );
}

/// An empty request whose `prev_log_term` differs from the follower's entry must be rejected.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_request_with_mismatching_prev_term_is_rejected() {
    let follower = follower_with_stale_tail("empty_request_term_mismatch").await;
    let mut mismatching = request(10, vec![]);
    mismatching.prev_log_term = NEW_TERM;

    let reported = reported_last_match(&follower, mismatching).await;

    assert_eq!(
        reported, None,
        "the follower's entry 10 is from term 1, not term 2"
    );
}

/// Sanity check for the scenario setup itself: the follower really holds entries 1..=50.
#[tokio::test(flavor = "multi_thread")]
async fn test_scenario_setup_follower_holds_the_stale_tail() {
    let follower = follower_with_stale_tail("scenario_setup").await;

    assert_eq!(follower.raft_log.last_entry_id(), 50);
    assert_eq!(follower.raft_log.entry_term(50), Some(OLD_TERM));
    let _ = Arc::strong_count(&follower.raft_log);
}

/// A heartbeat (empty request) carries `leader_commit_index`, and the follower must cap its
/// commit advance at the position the request actually verified — `prev_log_index` — not at its
/// own (possibly stale) tail. Otherwise a follower holding entries 11..=50 left by an earlier
/// leader would apply them as "committed" when the leader only ever verified up to index 10.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_request_commit_index_does_not_exceed_prev_log_index() {
    let follower = follower_with_stale_tail("empty_request_commit_cap").await;

    // The leader has committed up to 25, but still believes this follower matches only up to 10.
    let mut heartbeat = request(10, vec![]);
    heartbeat.leader_commit_index = 25;

    let handler = ReplicationHandler::<MockTypeConfig>::new(2);
    let commit_index_update = handler
        .handle_append_entries(heartbeat, &follower_state(), &log_backed_by(&follower))
        .await
        .expect("handle_append_entries")
        .commit_index_update;

    assert_eq!(
        commit_index_update,
        Some(10),
        "commit must stop at the verified prev_log_index (10), not jump to the follower's own stale tail (50)"
    );
}
