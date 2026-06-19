//! # process_raft_events drain loop tests
//!
//! Tests for the in-loop `matches!()` merge guard in `Raft::process_raft_events`.
//! These tests verify that `merge_append_entries` fires per-iteration when the
//! front of the queue is an `AppendEntries` event — not only once at the start
//! of the loop (which was the pre-fix behavior flagged in CodeRabbit review #407).
//!
//! ## Assertion style
//! The drain loop empties the buffer, so buffer length is not meaningful here.
//! Dispatch counts are asserted via mock `times(N)`:
//! - `handle_append_entries.times(1)` proves two consecutive AEs were merged.
//! - `handle_append_entries.times(2)` would prove they were dispatched separately.
//!   Mock expectations are verified on drop at the end of each test.

use crate::AppendResponseWithUpdates;
use crate::RoleEvent;
use crate::StateUpdate;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::test_utils::MockBuilder;
use crate::{MockElectionCore, MockReplicationCore, RaftEvent, mock_entries};
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::replication::{AppendEntriesRequest, AppendEntriesResponse};
use std::collections::VecDeque;
use tokio::sync::watch;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn make_request(
    term: u64,
    prev_log_index: u64,
    count: u64,
    leader_commit: u64,
) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term,
        leader_id: 1,
        prev_log_index,
        prev_log_term: term,
        entries: mock_entries(1, count, term),
        leader_commit_index: leader_commit,
    }
}

fn make_ae_event(req: AppendEntriesRequest) -> RaftEvent {
    let (tx, _rx) = MaybeCloneOneshot::new();
    RaftEvent::AppendEntries(req, vec![tx])
}

fn make_fatal_event() -> RoleEvent {
    RoleEvent::FatalError {
        source: "core".to_string(),
        error: "fatal-error".to_string(),
    }
}

fn make_vr_event() -> RaftEvent {
    let (tx, _rx) = MaybeCloneOneshot::new();
    RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 11,
            last_log_term: 3,
        },
        tx,
    )
}

fn default_election_mock() -> MockElectionCore<crate::test_utils::MockTypeConfig> {
    let mut m = MockElectionCore::new();
    m.expect_broadcast_vote_requests().returning(|_, _, _, _, _| Ok(()));
    m
}

fn ok_append_response() -> crate::AppendResponseWithUpdates {
    AppendResponseWithUpdates {
        response: AppendEntriesResponse::success(1, 5, None),
        commit_index_update: None,
    }
}

// -----------------------------------------------------------------------
// Drain loop correctness
// -----------------------------------------------------------------------

/// A non-AE at the front does NOT block AE merging once it is drained.
/// After the VoteRequest is dispatched, the subsequent AE run is merged.
///
/// Buffer before: [VoteRequest, AE(prev=9, 5 entries, term=5), AE(prev=14, 5 entries, term=5)]
///
/// Drain iterations:
/// 1. front=VR  → merge guard skips → dispatch VR        → handle_vote_request ×1
/// 2. front=AE  → merge guard fires → AE+AE → merged AE  → handle_append_entries ×1
///
/// times(1) on handle_append_entries proves merge happened (not ×2 separate dispatches).
#[tokio::test]
async fn test_non_ae_prefix_does_not_block_ae_merge_in_drain() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut election_handler = default_election_mock();
    election_handler.expect_handle_vote_request().times(1).returning(|_, _, _, _| {
        Ok(StateUpdate {
            new_voted_for: None,
            term_update: None,
        })
    });

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .times(1) // ← the assertion: 2 AEs merged → 1 dispatch
        .returning(|_, _, _| Ok(ok_append_response()));

    let mut raft = MockBuilder::new(graceful_rx)
        .with_election_handler(election_handler)
        .with_replication_handler(replication_handler)
        .build_raft();

    let mut queue = VecDeque::new();
    queue.push_back(make_vr_event());
    queue.push_back(make_ae_event(make_request(5, 9, 5, 1)));
    queue.push_back(make_ae_event(make_request(5, 14, 5, 1)));
    raft.buffered_raft_event = queue;

    raft.process_raft_events().await.unwrap();
    // mock drop verifies: handle_vote_request×1, handle_append_entries×1
}

/// When a non-AE splits two AE runs, BOTH runs are independently merged.
///
/// Buffer before: [AE(prev=9,  entries 10-14),
///                 AE(prev=14, entries 15-19),
///                 VoteRequest,
///                 AE(prev=19, entries 20-24),
///                 AE(prev=24, entries 25-29)]
///
/// Expected dispatches:
/// - handle_append_entries × 2   ← each run merged separately, not ×4
/// - handle_vote_request × 1
#[tokio::test]
async fn test_ae_runs_on_both_sides_of_non_ae_are_each_merged() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut election_handler = default_election_mock();
    election_handler.expect_handle_vote_request().times(1).returning(|_, _, _, _| {
        Ok(StateUpdate {
            new_voted_for: None,
            term_update: None,
        })
    });

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .times(2) // ← 2 runs × 1 merged dispatch each = 2 total (not 4)
        .returning(|_, _, _| Ok(ok_append_response()));

    let mut raft = MockBuilder::new(graceful_rx)
        .with_election_handler(election_handler)
        .with_replication_handler(replication_handler)
        .build_raft();

    let mut queue = VecDeque::new();
    // first run: entries 10-19
    queue.push_back(make_ae_event(make_request(5, 9, 5, 1)));
    queue.push_back(make_ae_event(make_request(5, 14, 5, 1)));
    queue.push_back(make_vr_event());
    // second run: entries 20-29 (new entries, continues after first run)
    queue.push_back(make_ae_event(make_request(5, 19, 5, 1)));
    queue.push_back(make_ae_event(make_request(5, 24, 5, 1)));
    raft.buffered_raft_event = queue;

    raft.process_raft_events().await.unwrap();
}

/// Buffer with only non-AE events drains without calling merge and without panicking.
///
/// Buffer before: [VoteRequest, VoteRequest]
///
/// Expected dispatches:
/// - handle_vote_request × 2
#[tokio::test]
async fn test_all_non_ae_drain_no_merge_no_panic() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut election_handler = default_election_mock();
    election_handler.expect_handle_vote_request().times(2).returning(|_, _, _, _| {
        Ok(StateUpdate {
            new_voted_for: None,
            term_update: None,
        })
    });

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .times(0) //  ← no AE events, merge guard never triggers
        .returning(|_, _, _| Ok(ok_append_response()));

    let mut raft = MockBuilder::new(graceful_rx)
        .with_election_handler(election_handler)
        .with_replication_handler(replication_handler)
        .build_raft();

    let mut queue = VecDeque::new();
    queue.push_back(make_vr_event());
    queue.push_back(make_vr_event());
    raft.buffered_raft_event = queue;

    raft.process_raft_events().await.unwrap();
}

/// A single AE sandwiched between non-AE events is dispatched alone.
/// Merge fires but finds no contiguous AE neighbour to coalesce.
///
/// Buffer before: [AE(prev=9, 5 entries), VoteRequest, AE(prev=14, 5 entries)]
///
/// Expected dispatches:
/// - handle_append_entries × 2   ← each AE alone, no merge opportunity
/// - handle_vote_request × 1
#[tokio::test]
async fn test_isolated_ae_between_non_ae_dispatched_alone() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut election_handler = default_election_mock();
    election_handler.expect_handle_vote_request().times(1).returning(|_, _, _, _| {
        Ok(StateUpdate {
            new_voted_for: None,
            term_update: None,
        })
    });

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .times(2) // ← 2 runs × 1 merged dispatch each = 2 total (not 4)
        .returning(|_, _, _| Ok(ok_append_response()));

    let mut raft = MockBuilder::new(graceful_rx)
        .with_election_handler(election_handler)
        .with_replication_handler(replication_handler)
        .build_raft();

    let mut queue = VecDeque::new();
    // first run: entries 10-19
    queue.push_back(make_ae_event(make_request(5, 9, 5, 1)));
    queue.push_back(make_vr_event());
    // second run: entries 20-29 (new entries, continues after first run)
    queue.push_back(make_ae_event(make_request(5, 14, 5, 1)));
    raft.buffered_raft_event = queue;

    raft.process_raft_events().await.unwrap();
}

// -----------------------------------------------------------------------
// process_role_events error propagation
// -----------------------------------------------------------------------

/// A RoleEvent::FatalError in the buffer causes process_role_events to
/// return Err immediately rather than swallowing the error and continuing.
///
/// This guards against the node silently continuing after a fatal signal
/// (e.g. state machine worker crash).
///
/// Buffer before: [RoleEvent::FatalError { source: "sm_worker", error: "disk full" }]
///
/// Expected:
/// - returns Err(e) where e.is_fatal() == true
/// - does NOT return Ok(())
#[tokio::test]
async fn test_process_role_events_propagates_fatal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    let mut queue = VecDeque::new();
    queue.push_back(make_fatal_event());
    raft.buffered_role_event = queue;

    let err = raft.process_role_events().await.unwrap_err();
    assert!(err.is_fatal());
}
