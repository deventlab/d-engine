//! # AppendEntries Merge Tests
//!
//! Tests for `Raft::merge_append_entries()` — the function that coalesces
//! consecutive contiguous `RaftEvent::AppendEntries` events in the buffer
//! into a single event before dispatch.
//!
//! ## Design decisions under test
//! - Only the leading run of contiguous same-term AppendEntries is merged.
//! - Heartbeats (empty entries, same term and prev_log_index as the next slot) are absorbed
//!   into the merge group: their sender is collected and their `leader_commit_index` is folded
//!   in via `max()`, advancing the commit index without a separate dispatch.
//! - Non-AppendEntries events stop the merge and remain in FIFO order.
//! - All senders from merged events are collected into a Vec for broadcast.
//!
//! ## Setup pattern
//! Each test builds a `Raft<MockTypeConfig>` via `MockBuilder`, pushes events
//! directly into `raft.buffered_raft_event`, calls `raft.merge_append_entries()`,
//! then inspects the resulting buffer.

use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::test_utils::MockBuilder;
use crate::{MaybeCloneOneshotReceiver, RaftEvent, RaftNodeConfig, RaftOneshot, mock_entries};
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::{VoteRequest, VoteResponse};
use d_engine_proto::server::replication::{AppendEntriesRequest, AppendEntriesResponse};
use std::collections::VecDeque;
use tokio::sync::watch;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/// Build an `AppendEntriesRequest` covering `count` entries starting after
/// `prev_log_index`, all with the given `term`.
///
/// Entry indices are `prev_log_index + 1 ..= prev_log_index + count`.
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

/// Wrap a request and a fresh oneshot sender into `RaftEvent::AppendEntries`.
/// Returns `(event, receiver)` — the receiver lets tests verify send results.
fn make_event(
    req: AppendEntriesRequest
) -> (
    RaftEvent,
    MaybeCloneOneshotReceiver<Result<AppendEntriesResponse, tonic::Status>>,
) {
    let (tx, rx) = MaybeCloneOneshot::new();
    (RaftEvent::AppendEntries(req, vec![tx]), rx)
}

fn make_none_append_entries_event() -> (
    RaftEvent,
    MaybeCloneOneshotReceiver<Result<VoteResponse, tonic::Status>>,
) {
    let (tx, rx) = MaybeCloneOneshot::new();
    (
        RaftEvent::ReceiveVoteRequest(
            VoteRequest {
                term: 1,
                candidate_id: 2,
                last_log_index: 11,
                last_log_term: 3,
            },
            tx,
        ),
        rx,
    )
}

// -----------------------------------------------------------------------
// Edge cases — buffer state before any merge opportunity
// -----------------------------------------------------------------------

/// Calling `merge_append_entries` on an empty buffer is a no-op.
/// Buffer remains empty; function returns without panicking.
///
/// Buffer before: []
///
/// After merge:
/// - buffer.len() == 0
#[test]
fn test_empty_buffer_is_noop() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    raft.buffered_raft_event = VecDeque::new();

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 0);
}

/// A buffer with exactly one AppendEntries event is returned unchanged.
/// The event is popped and immediately pushed back as-is; no entries are
/// added, no senders are lost.
///
/// Buffer before: [AE(prev=9, [10..14], term=5)]
///
/// After merge:
/// - buffer.len() == 1
/// - entries.len() == 5  (unchanged)
/// - senders.len() == 1  (unchanged)
/// - prev_log_index == 9 (unchanged)
#[test]
fn test_single_append_entries_event_returned_unchanged() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    let request = make_request(5, 9u64, 5, 1);
    let (event, _receiver) = make_event(request);
    queue.push_back(event);

    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.term, 5);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

/// When the first event is not AppendEntries (e.g. VoteRequest), the function
/// pushes it back to the front and returns without touching anything else.
/// Buffer order and length are unchanged.
///
/// Buffer before: [VoteRequest(term=1), AE(prev=9, [10..14], term=5)]
///
/// After merge:
/// - buffer.len() == 2
/// - buffer[0]: VoteRequest (unchanged, still at front)
/// - buffer[1]: AE (unchanged, not merged)
#[test]
fn test_first_non_append_entries_event_is_noop() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (is_ae, prev, count, term) in [(false, 9u64, 5, 15), (true, 9u64, 5, 5)] {
        if is_ae {
            let request = make_request(term, prev, count, 1);
            let (event, _receiver) = make_event(request);
            queue.push_back(event);
        } else {
            let (event, _receiver) = make_none_append_entries_event();
            queue.push_back(event);
        }
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 2);

    // VoteRequest
    if let Some(RaftEvent::ReceiveVoteRequest(request, _senders)) =
        raft.buffered_raft_event.pop_front()
    {
        assert_eq!(request.last_log_index, 11);
    } else {
        panic!("expected RaftEvent::ReceiveVoteRequest, got something else");
    }

    // AE
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Basic merge
// -----------------------------------------------------------------------

/// Three contiguous AppendEntries from the same term are merged into one.
///
/// Buffer before: [AE(prev=9, entries=[10..14], term=5),
///                  AE(prev=14, entries=[15..19], term=5),
///                  AE(prev=19, entries=[20..24], term=5)]
///
/// After merge:
/// - buffer.len() == 1
/// - merged entries.len() == 15
/// - merged senders.len() == 3
/// - prev_log_index == 9  (taken from first request)
/// - term == 5
#[test]
fn test_merge_three_contiguous_entries_same_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for prev in [9u64, 14, 19] {
        let request = make_request(5, prev, 5, 1);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }

    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 15);
            assert_eq!(senders.len(), 3);
            assert_eq!(request.term, 5);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

/// The merged event preserves the metadata (term, leader_id, prev_log_index,
/// prev_log_term, leader_commit_index) from the first request unchanged.
/// Only `entries` and `senders` are accumulated from subsequent events.
///
/// Buffer before: [AE(term=5, leader_id=1, prev=9, prev_term=5, commit=42, [10..14]),
///                  AE(term=5, leader_id=1, prev=14, prev_term=5, commit=99, [15..19])]
///
/// After merge:
/// - term == 5               (from first)
/// - leader_id == 1          (from first)
/// - prev_log_index == 9     (from first)
/// - prev_log_term == 5      (from first)
/// - leader_commit_index == 99  (max leader commit index)
/// - entries.len() == 10
#[test]
fn test_merge_preserves_first_request_metadata() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, term, commit) in [(9u64, 5, 42), (14, 5, 99)] {
        let request = make_request(term, prev, 5, commit);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 10);
            assert_eq!(senders.len(), 2);
            assert_eq!(request.term, 5);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.leader_commit_index, 99);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Merge boundary: term mismatch
// -----------------------------------------------------------------------

/// When term changes mid-sequence, only the prefix with matching term is merged.
/// The event with the new term is pushed back and remains as the next item.
///
/// Buffer before: [AE(term=5, prev=9,  [10..14]),
///                  AE(term=5, prev=14, [15..19]),
///                  AE(term=6, prev=19, [20..24])]
///
/// After merge:
/// - buffer.len() == 2
/// - front:  merged AE(term=5, entries=[10..19], senders=[s1,s2])
/// - back:   AE(term=6, prev=19, entries=[20..24], senders=[s3])  — untouched
#[test]
fn test_term_mismatch_stops_merge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, term) in [(9u64, 5), (14, 5), (19, 6)] {
        let request = make_request(term, prev, 5, 1);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 2);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 10);
            assert_eq!(senders.len(), 2);
            assert_eq!(request.term, 5);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }

    // back
    match raft.buffered_raft_event.pop_back() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.term, 6);
            assert_eq!(request.prev_log_index, 19);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Merge boundary: non-contiguous log range
// -----------------------------------------------------------------------

/// When `prev_log_index` of the next event does not equal the end of the
/// accumulated entries, the merge stops.
///
/// Buffer before: [AE(prev=9, [10..14], term=5),
///                  AE(prev=20, [21..24], term=5)]   ← gap at 15-20
///
/// After merge:
/// - buffer.len() == 2
/// - front: AE with 5 entries, prev=9
/// - back:  AE with prev=20, unchanged
#[test]
fn test_non_contiguous_prev_log_index_stops_merge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, term) in [(9u64, 5), (20, 5)] {
        let request = make_request(term, prev, 5, 1);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 2);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.term, 5);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }

    // back
    match raft.buffered_raft_event.pop_back() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.term, 5);
            assert_eq!(request.prev_log_index, 20);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Merge boundary: heartbeat (empty entries) — Method B absorption
// -----------------------------------------------------------------------

/// A heartbeat (entries=[]) that chains with the preceding AE is absorbed
/// into the merge group rather than stopping it (Method B).
///
/// The heartbeat contributes no entries but its senders are collected and
/// `next_prev` is unchanged, allowing subsequent AEs to continue chaining.
///
/// Buffer before: [AE(prev=9,  entries=[10..14], commit=10),
///                  HB(prev=14, entries=[],        commit=15),
///                  AE(prev=14, entries=[15..19],  commit=10)]
///
/// After merge:
/// - buffer.len() == 1
/// - entries.len() == 10  (5 from first AE + 0 from HB + 5 from last AE)
/// - senders.len() == 3   (one per original event)
/// - prev_log_index == 9  (from first request)
/// - leader_commit_index == 15  (max across all three)
#[test]
fn test_heartbeat_absorbed_into_merge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, count, commit) in [(9u64, 5, 10), (14, 0, 15), (14, 5, 10)] {
        let request = make_request(5, prev, count, commit);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 10);
            assert_eq!(senders.len(), 3);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.leader_commit_index, 15);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

/// Verify that a heartbeat's `leader_commit_index` is promoted via `max()`
/// into the merged result, not silently dropped.
///
/// When the HB carries a higher commit than surrounding AEs, the merged
/// event must reflect that higher value so the follower advances its
/// commit index correctly on the single `workflow()` call.
///
/// Buffer before: [AE(prev=9, [10..14], commit=10),
///                  HB(prev=14, [],       commit=99),   ← high commit
///                  AE(prev=14, [15..19], commit=10)]
///
/// After merge:
/// - buffer.len() == 1
/// - merged.leader_commit_index == 99  (HB value wins via max)
#[test]
fn test_heartbeat_commit_index_promoted_via_max() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, count, commit) in [(9u64, 5, 10), (14, 0, 99), (14, 5, 10)] {
        let request = make_request(5, prev, count, commit);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 10);
            assert_eq!(senders.len(), 3);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.leader_commit_index, 99);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Merge boundary: non-AppendEntries event in sequence (FIFO invariant)
// -----------------------------------------------------------------------

/// A non-AppendEntries event (e.g. VoteRequest) between two AppendEntries
/// stops the merge and preserves full FIFO ordering.
///
/// Rationale: skipping over non-AppendEntries events to merge surrounding
/// AEs would change relative processing order, which can break election
/// safety (§5.2) — a VoteRequest must be processed before a later AE.
///
/// Buffer before: [AE(prev=9, [10..14], term=5),
///                  VoteRequest(...),
///                  AE(prev=14, [15..19], term=5)]
///
/// After merge:
/// - buffer.len() == 3
/// - buffer[0]: AE with 5 entries
/// - buffer[1]: VoteRequest (unchanged, in original position)
/// - buffer[2]: AE with entries=[15..19]  (NOT merged with buffer[0])
#[test]
fn test_non_append_entries_event_stops_merge_fifo_preserved() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (is_ae, prev, count, term) in [(true, 9u64, 5, 5), (false, 14, 0, 0), (true, 14, 5, 5)] {
        if is_ae {
            let request = make_request(term, prev, count, 1);
            let (event, _receiver) = make_event(request);
            queue.push_back(event);
        } else {
            let (event, _receiver) = make_none_append_entries_event();
            queue.push_back(event);
        }
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 3);

    // AE
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }

    // VoteRequest
    if let Some(RaftEvent::ReceiveVoteRequest(request, _senders)) =
        raft.buffered_raft_event.pop_front()
    {
        assert_eq!(request.last_log_index, 11);
    } else {
        panic!("expected RaftEvent::ReceiveVoteRequest, got something else");
    }

    // AE
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 14);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Sender collection
// -----------------------------------------------------------------------

/// All senders from merged events appear in the resulting Vec, in order.
/// Sender count == number of original events that were merged.
///
/// Buffer before: [AE(prev=9, [10..14]), AE(prev=14, [15..19]), AE(prev=19, [20..24])]
///
/// After merge:
/// - senders.len() == 3
/// - senders appear in original push order (s1, s2, s3)
#[tokio::test]
async fn test_all_senders_collected_in_merge_order() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    let mut receivers = vec![];

    for (prev, count) in [(9u64, 5), (14, 5), (19, 5)] {
        let request = make_request(5, prev, count, 1);
        let (event, rx) = make_event(request);
        receivers.push(rx);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    let Some(RaftEvent::AppendEntries(_, senders)) = raft.buffered_raft_event.pop_front() else {
        panic!("expected AppendEntries");
    };
    assert_eq!(senders.len(), 3);

    for (i, sender) in senders.iter().enumerate() {
        let matched = (i as u64 + 1) * 10; // 10, 20, 30
        sender
            .send(Ok(AppendEntriesResponse::success(
                (i + 1) as u32,
                5,
                Some(LogId {
                    term: matched,
                    index: matched,
                }),
            )))
            .ok();
    }

    let mut iter = receivers.into_iter();
    let r1 = iter.next().unwrap().await.unwrap().unwrap();
    let r2 = iter.next().unwrap().await.unwrap().unwrap();
    let r3 = iter.next().unwrap().await.unwrap().unwrap();
    assert_eq!(r1.node_id, 1);
    assert_eq!(r2.node_id, 2);
    assert_eq!(r3.node_id, 3);
}

/// Dropping a receiver before merge does not prevent or corrupt the merge.
/// The merge is a structural operation on RaftEvent variants; receiver
/// validity is only checked at dispatch time (send()), not during merge.
///
/// Buffer before: [AE(prev=9, [10..14], rx dropped), AE(prev=14, [15..19])]
///
/// After merge:
/// - buffer.len() == 1
/// - senders.len() == 2  (dropped-receiver sender still present in Vec)
/// - entries.len() == 10
#[test]
fn test_dropped_receiver_does_not_affect_merge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (rx_dropped, prev, count) in [(true, 9u64, 5), (false, 14, 5)] {
        let request = make_request(5, prev, count, 1);
        if rx_dropped {
            let (event, receiver) = make_event(request);
            drop(receiver);
            queue.push_back(event);
        } else {
            let (event, _receiver) = make_event(request);
            queue.push_back(event);
        }
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 10);
            assert_eq!(senders.len(), 2);
            assert_eq!(request.prev_log_index, 9);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Heartbeat position variants
// -----------------------------------------------------------------------

/// A heartbeat at the end of the sequence (not between two AEs) is absorbed.
/// The merged result has entries from the preceding AE, the HB contributes
/// only its sender and commit index via max().
///
/// Buffer before: [AE(prev=9, [10..14], commit=10),
///                  HB(prev=14, entries=[], commit=50)]
///
/// After merge:
/// - buffer.len() == 1
/// - entries.len() == 5        (from AE only)
/// - senders.len() == 2        (AE sender + HB sender)
/// - leader_commit_index == 50 (HB's higher value wins via max)
#[test]
fn test_heartbeat_at_sequence_end_absorbed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, count, commit) in [(9u64, 5, 10), (14, 0, 50)] {
        let request = make_request(5, prev, count, commit);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 2);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.leader_commit_index, 50);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

/// Two consecutive heartbeats between two AEs are both absorbed.
/// Each HB contributes its sender; commit index is the running max across all.
///
/// Buffer before: [AE(prev=9,  [10..14], commit=10),
///                  HB(prev=14, [],       commit=20),
///                  HB(prev=14, [],       commit=30),
///                  AE(prev=14, [15..19], commit=10)]
///
/// After merge:
/// - buffer.len() == 1
/// - entries.len() == 10       (5 from first AE + 0 + 0 + 5 from last AE)
/// - senders.len() == 4
/// - leader_commit_index == 30 (max across all four)
#[test]
fn test_consecutive_heartbeats_absorbed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    // [AE(5 entries, commit=10), HB(commit=20), HB(commit=30), AE(5 entries, commit=10)]
    for (prev, count, commit) in [(9u64, 5, 10), (14, 0, 20), (14, 0, 30), (14, 5, 10)] {
        let request = make_request(5, prev, count, commit);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            assert_eq!(request.entries.len(), 10); // 5 + 0 + 0 + 5
            assert_eq!(senders.len(), 4);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.leader_commit_index, 30); // max(10, 20, 30, 10)
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

/// When all events in the buffer are heartbeats, they are merged into one
/// empty-entries event with all senders collected and commit = max().
///
/// Buffer before: [HB(prev=9, [], commit=10),
///                  HB(prev=9, [], commit=99)]
///
/// After merge:
/// - buffer.len() == 1
/// - entries.len() == 0
/// - senders.len() == 2
/// - leader_commit_index == 99
#[test]
fn test_all_heartbeats_merged_into_one() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, count, commit) in [(9u64, 0, 10), (9, 0, 99)] {
        let request = make_request(5, prev, count, commit);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 1);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 0);
            assert_eq!(senders.len(), 2);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.leader_commit_index, 99);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// Term ordering: newer-first
// -----------------------------------------------------------------------

/// When the buffer has a newer-term AE followed by an older-term AE
/// (network reorder), the merge stops after the first event.
/// The older-term AE is left at the back of the buffer untouched.
///
/// Rationale: merge only checks `term == first_req.term`; a lower term
/// in the second slot fails that check and falls through to the `_ =>` arm.
///
/// Buffer before: [AE(term=6, prev=9,  [10..14]),
///                  AE(term=5, prev=14, [15..19])]
///
/// After merge:
/// - buffer.len() == 2
/// - front: AE(term=6, entries=[10..14]) — processed alone
/// - back:  AE(term=5, prev=14)         — untouched, will be rejected by follower
#[test]
fn test_newer_term_first_older_term_second_stops_merge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();
    let mut queue = VecDeque::new();
    for (prev, count, term) in [(9u64, 5, 6), (14, 5, 5)] {
        let request = make_request(term, prev, count, 1);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 2);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.term, 6);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
    // back
    match raft.buffered_raft_event.pop_back() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 14);
            assert_eq!(request.term, 5);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}

// -----------------------------------------------------------------------
// max_merge_entries upper bound (requires config wiring)
// -----------------------------------------------------------------------

/// When the accumulated entry count would exceed `max_merge_entries`,
/// the merge stops before the event that would breach the limit.
/// The exceeding event is pushed back and remains as the next item.
///
/// Config: max_merge_entries = 8
///
/// Buffer before: [AE(prev=9,  [10..14], 5 entries),
///                  AE(prev=14, [15..19], 5 entries)]   ← would make total 10 > 8
///
/// After merge:
/// - buffer.len() == 2
/// - front: AE with 5 entries  (first batch alone, did not exceed limit)
/// - back:  AE with prev=14    (held back to respect the limit)
#[test]
fn test_max_merge_entries_limit_stops_merge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut node_config = RaftNodeConfig::new().expect("valid config");
    node_config.raft.batching.max_merge_entries = 8;

    let mut raft = MockBuilder::new(graceful_rx).with_node_config(node_config).build_raft();
    let mut queue = VecDeque::new();
    for (prev, count, term) in [(9u64, 5, 6), (14, 5, 6)] {
        let request = make_request(term, prev, count, 1);
        let (event, _receiver) = make_event(request);
        queue.push_back(event);
    }
    raft.buffered_raft_event = queue;

    raft.merge_append_entries();
    assert_eq!(raft.buffered_raft_event.len(), 2);

    // front
    match raft.buffered_raft_event.pop_front() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 9);
            assert_eq!(request.term, 6);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
    // back
    match raft.buffered_raft_event.pop_back() {
        Some(RaftEvent::AppendEntries(request, senders)) => {
            let entries = request.entries;
            assert_eq!(entries.len(), 5);
            assert_eq!(senders.len(), 1);
            assert_eq!(request.prev_log_index, 14);
            assert_eq!(request.term, 6);
        }
        _ => panic!("expected RaftEvent::AppendEntries, got something else"),
    }
}
