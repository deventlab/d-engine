//! # process_inbound_events drain loop tests
//!
//! Tests for the in-loop `matches!()` merge guard in `Raft::process_inbound_events`.
//! These tests verify that `merge_append_entries` fires per-iteration when the
//! front of the queue is an `AppendEntries` event — not only once at the start
//! of the loop (which was the pre-fix behavior flagged in CodeRabbit review #407).
//!
//! ## Assertion style
//! The drain loop empties the buffer, so buffer length is not meaningful here.
//! Dispatch counts are asserted via mock `times(N)`:
//! - `handle_append_entries.times(1)` proves two consecutive AEs were merged.
//! - `handle_append_entries.times(2)` would prove they were dispatched separately. Mock
//!   expectations are verified on drop at the end of each test.

use std::collections::VecDeque;

use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use tokio::sync::watch;

use crate::AppendResponseWithUpdates;
use crate::InboundEvent;
use crate::InternalEvent;
use crate::MockElectionCore;
use crate::MockReplicationCore;
use crate::StateUpdate;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::mock_entries;
use crate::test_utils::MockBuilder;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/// A follower ignores VoteRequests for `election_timeout_min` after hearing from a leader.
/// Tests that interleave AEs with VoteRequests only care about how AE runs are merged
/// around the VoteRequest, not about voting, so they turn that guard off with a zero window.
fn config_without_leader_contact_guard() -> crate::RaftNodeConfig {
    let mut node_config = crate::RaftNodeConfig::default();
    node_config.raft.election.election_timeout_min = 0;
    node_config
}

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

fn make_ae_event(req: AppendEntriesRequest) -> InboundEvent {
    let (tx, _rx) = MaybeCloneOneshot::new();
    InboundEvent::AppendEntries(req, vec![tx])
}

fn make_fatal_event() -> InternalEvent {
    InternalEvent::FatalError {
        source: "core".to_string(),
        error: "fatal-error".to_string(),
    }
}

fn make_vr_event() -> InboundEvent {
    let (tx, _rx) = MaybeCloneOneshot::new();
    InboundEvent::ReceiveVoteRequest(
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
    raft.buffered_inbound_event = queue;

    raft.process_inbound_events().await.unwrap();
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
        .with_node_config(config_without_leader_contact_guard())
        .turn_on_election(false)
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
    raft.buffered_inbound_event = queue;

    raft.process_inbound_events().await.unwrap();
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
    raft.buffered_inbound_event = queue;

    raft.process_inbound_events().await.unwrap();
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
        .times(2)
        .returning(|_, _, _| Ok(ok_append_response()));

    let mut raft = MockBuilder::new(graceful_rx)
        .with_node_config(config_without_leader_contact_guard())
        .turn_on_election(false)
        .with_election_handler(election_handler)
        .with_replication_handler(replication_handler)
        .build_raft();

    let mut queue = VecDeque::new();
    // isolated AE: entries 10-14
    queue.push_back(make_ae_event(make_request(5, 9, 5, 1)));
    queue.push_back(make_vr_event());
    // isolated AE: entries 15-19
    queue.push_back(make_ae_event(make_request(5, 14, 5, 1)));
    raft.buffered_inbound_event = queue;
    assert_eq!(raft.ctx.node_config().raft.election.election_timeout_min, 0);

    raft.process_inbound_events().await.unwrap();
}

// -----------------------------------------------------------------------
// process_internal_events error propagation
// -----------------------------------------------------------------------

/// A InternalEvent::FatalError in the buffer causes process_internal_events to
/// return Err immediately rather than swallowing the error and continuing.
///
/// This guards against the node silently continuing after a fatal signal
/// (e.g. state machine worker crash).
///
/// Buffer before: [InternalEvent::FatalError { source: "sm_worker", error: "disk full" }]
///
/// Expected:
/// - returns Err(e) where e.is_fatal() == true
/// - does NOT return Ok(())
#[tokio::test]
async fn test_process_internal_events_propagates_fatal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    let mut queue = VecDeque::new();
    queue.push_back(make_fatal_event());
    raft.buffered_internal_event = queue;

    let err = raft.process_internal_events().await.unwrap_err();
    assert!(err.is_fatal());
}

// -----------------------------------------------------------------------
// shutdown (#438)
// -----------------------------------------------------------------------

/// Events still queued when shutdown fires are dropped, not answered — their
/// resp_tx senders are never called. Drives the real `run()` path (shutdown signalled
/// before it starts, so its first step is the shutdown branch), not a re-implementation.
#[tokio::test]
async fn test_shutdown_drops_pending_events_without_responding() {
    let (graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    let (tx1, rx1) = MaybeCloneOneshot::new();
    let (tx2, rx2) = MaybeCloneOneshot::new();
    let mut queue = VecDeque::new();
    queue.push_back(InboundEvent::AppendEntries(make_request(5, 9, 5, 1), vec![
        tx1,
    ]));
    queue.push_back(InboundEvent::AppendEntries(
        make_request(5, 14, 5, 1),
        vec![tx2],
    ));
    raft.buffered_inbound_event = queue;

    graceful_tx.send(()).unwrap();
    raft.run().await.unwrap();
    drop(raft); // matches production: Raft is dropped shortly after run() returns

    assert!(
        rx1.await.is_err(),
        "pending event must be dropped, not answered"
    );
    assert!(
        rx2.await.is_err(),
        "pending event must be dropped, not answered"
    );
}

// -----------------------------------------------------------------------
// reads that arrive while a snapshot installs
// -----------------------------------------------------------------------

/// Business scenario: a follower that fell far behind is installing the leader's snapshot. A
/// client sends it an eventual read; the read fell back from the ReadActor fast path to the Raft
/// loop, which is busy awaiting the install.
///
/// Expected: the read waits behind the install. It is not answered from the half-installed state
/// and it does not fail. Once the install is done it is answered from the new state.
#[tokio::test]
async fn test_read_queued_during_snapshot_install_is_answered_from_new_state() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use d_engine_proto::server::storage::SnapshotChunk;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use crate::ClientCmd;
    use crate::MockStateMachineHandler;
    use crate::PreparedSnapshot;
    use crate::SnapshotApplyResult;
    use crate::StateMachineCommand;
    use crate::StateMachineCommandSender;
    use crate::client::ClientReadRequest;
    use crate::client::ClientResponsePayload;
    use crate::client::ErrorCode;
    use crate::client::KvEntry;
    use crate::config::RaftNodeConfig;
    use crate::config::ReadConsistencyPolicy;

    const INSTALL_TIME: Duration = Duration::from_millis(400);

    // The state machine holds "old" until the worker finishes installing, then "new".
    let installed = Arc::new(AtomicBool::new(false));
    let mut smh = MockStateMachineHandler::new();
    smh.expect_update_pending().returning(|_| {});
    smh.expect_last_applied().returning(|| 0);
    smh.expect_should_snapshot().returning(|_| false);
    smh.expect_get_latest_snapshot_metadata().returning(|| None);
    smh.expect_try_begin_local_snapshot_capture().returning(|| Ok(()));
    smh.expect_end_local_snapshot_capture().returning(|| {});
    smh.expect_prepare_snapshot_stream().once().returning(|_, _, _, _| {
        Ok(PreparedSnapshot {
            metadata: Default::default(),
            temp_dir: tempfile::tempdir().unwrap(),
        })
    });
    let read_view = installed.clone();
    smh.expect_read_from_state_machine().returning(move |keys| {
        let value = if read_view.load(Ordering::SeqCst) {
            "new"
        } else {
            "old"
        };
        Ok(keys
            .into_iter()
            .map(|key| KvEntry {
                key,
                value: Bytes::from(value),
            })
            .collect())
    });

    // The worker takes INSTALL_TIME to install, then switches the state machine to "new".
    let (sm_tx, mut sm_rx) = mpsc::unbounded_channel();
    let (install_started_tx, install_started_rx) = oneshot::channel();
    let worker_state = installed.clone();
    tokio::spawn(async move {
        if let Some(StateMachineCommand::InstallSnapshot { response, .. }) = sm_rx.recv().await {
            let _ = install_started_tx.send(());
            tokio::time::sleep(INSTALL_TIME).await;
            worker_state.store(true, Ordering::SeqCst);
            let _ = response.send(Ok(SnapshotApplyResult::Applied {
                last_included: Default::default(),
            }));
        }
    });

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.default_policy = ReadConsistencyPolicy::EventualConsistency;
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx)
        .with_node_config(node_config)
        .with_state_machine_handler(smh)
        .with_state_machine_commands(StateMachineCommandSender::new(sm_tx))
        .turn_on_election(false)
        .build_raft();
    let event_tx = raft.event_sender();
    let cmd_tx = raft.cmd_sender();
    let _loop = tokio::spawn(async move { raft.run().await });

    // The leader starts a snapshot transfer; the loop now awaits the worker.
    let (chunk_tx, chunk_rx) = mpsc::channel(8);
    chunk_tx
        .send(SnapshotChunk {
            leader_term: 1,
            leader_id: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    drop(chunk_tx);
    let (install_resp_tx, _install_resp_rx) = MaybeCloneOneshot::new();
    event_tx
        .send(InboundEvent::InstallSnapshotChunk(
            chunk_rx,
            install_resp_tx,
        ))
        .await
        .unwrap();
    install_started_rx.await.expect("the install must reach the worker");

    // A read arrives while the install is running.
    let (read_tx, mut read_rx) = MaybeCloneOneshot::new();
    cmd_tx
        .send(ClientCmd::Read(
            ClientReadRequest {
                client_id: 1,
                consistency_policy: None, // server default: eventual
                keys: vec![Bytes::from_static(b"k")],
            },
            read_tx,
        ))
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(INSTALL_TIME / 4, read_rx.recv()).await.is_err(),
        "the read must wait behind the install, not be answered from the old state"
    );

    let response = tokio::time::timeout(Duration::from_secs(5), read_rx.recv())
        .await
        .expect("the queued read must be answered once the install is done")
        .expect("the response channel must stay open")
        .expect("the read must not fail");
    assert_eq!(response.error, ErrorCode::Success);
    match response.result {
        Some(ClientResponsePayload::Read(results)) => {
            assert_eq!(results.entries.len(), 1);
            assert_eq!(
                results.entries[0].value,
                Bytes::from_static(b"new"),
                "the read must see the state the snapshot installed"
            );
        }
        other => panic!("expected read results, got: {other:?}"),
    }
}
