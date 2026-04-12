//! Tests for `send_to_worker_or_spawn` — per-follower replication worker lifecycle.
//!
//! Three scenarios are covered:
//!   1. Worker absent → spawned on first request.
//!   2. Worker alive → request is forwarded, no extra worker is created.
//!   3. Worker dead  → transparently rebuilt, request is delivered to the new worker.
//!
//! Each test drives `process_batch` with a real peer request returned by
//! `prepare_batch_requests` so that Phase 5 of `execute_and_process_raft_rpc`
//! actually executes `send_to_worker_or_spawn`.

use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::common::{EntryPayload, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::{AppendEntriesRequest, AppendEntriesResponse};
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockTransport;
use crate::RaftRequestWithSignal;
use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a two-peer membership mock (peers 2 & 3) so that `single_voter` stays false.
fn two_peer_membership() -> MockMembership<MockTypeConfig> {
    let peers = vec![
        NodeMeta {
            id: 2,
            address: String::new(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
        NodeMeta {
            id: 3,
            address: String::new(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
    ];
    let peers2 = peers.clone();
    let mut m = MockMembership::new();
    m.expect_is_single_node_cluster().returning(|| false);
    m.expect_voters().returning(move || peers.clone());
    m.expect_replication_peers().returning(move || peers2.clone());
    m
}

/// A minimal `AppendEntriesRequest` stub usable as a worker task payload.
fn stub_request() -> AppendEntriesRequest {
    AppendEntriesRequest::default()
}

/// A minimal `AppendEntriesResponse` with `success = true`.
fn stub_response() -> AppendEntriesResponse {
    AppendEntriesResponse::default()
}

/// A minimal one-entry write batch for `process_batch`.
fn one_entry_batch() -> std::collections::VecDeque<RaftRequestWithSignal> {
    let (tx, _rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: "test".into(),
        payloads: vec![EntryPayload::command(Bytes::from_static(b"cmd"))],
        senders: vec![tx],
        wait_for_apply_event: false,
    };
    std::collections::VecDeque::from(vec![req])
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Worker absent → a new worker is spawned on the first request.
///
/// # Scenario
/// - `replication_workers` map is empty (no prior state).
/// - `prepare_batch_requests` returns one request for peer 2.
/// - After `process_batch`, exactly one worker must exist in the map.
///
/// # Guarantees checked
/// - `worker_count_for_test() == 1` after the first batch.
#[tokio::test]
#[traced_test]
async fn test_worker_spawned_on_first_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_spawned_on_first_request",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    // `prepare_batch_requests` returns one request directed at peer 2.
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request())],
                snapshot_targets: vec![],
            })
        });

    // Transport: allow any number of async calls from the spawned worker task.
    // We don't assert the exact call count here because the worker runs in a background
    // tokio task and may not have executed by test teardown — this test's focus is the
    // worker count assertion, not the transport call count.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport
        .expect_send_append_request()
        .returning(|_, _, _, _, _| Ok(stub_response()));
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel(128);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(128);

        // Spawn task to consume requests and send back stub responses
        tokio::spawn(async move {
            while req_rx.recv().await.is_some() {
                let _ = resp_tx.send(Ok(stub_response())).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(resp_rx).boxed();
        Ok(crate::ReplicationStream {
            sender: req_tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    // Log mock: `last_entry_id` drives start_index calculation.
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    // No workers yet.
    assert_eq!(
        state.worker_count_for_test(),
        0,
        "precondition: no workers before first batch"
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();

    // A worker must have been inserted for peer 2.
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "worker should be spawned for peer 2"
    );
}

/// Worker alive → request forwarded to existing worker; no new worker is created.
///
/// # Scenario
/// - First `process_batch` spawns a worker for peer 2.
/// - Second `process_batch` reuses the same worker.
/// - Worker count stays at 1 across both batches.
///
/// # Guarantees checked
/// - `worker_count_for_test() == 1` after both batches.
/// - `prepare_batch_requests` is called exactly twice (once per batch).
#[tokio::test]
#[traced_test]
async fn test_worker_reused_on_subsequent_requests() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_reused_on_subsequent_requests",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    // Called twice: each batch returns one request for peer 2.
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request())],
                snapshot_targets: vec![],
            })
        });

    // Transport: allow async calls from the worker; exact count not asserted here
    // because the worker runs in background and may not have executed by teardown.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport
        .expect_send_append_request()
        .returning(|_, _, _, _, _| Ok(stub_response()));
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel(128);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(128);

        // Spawn task to consume requests and send back stub responses
        tokio::spawn(async move {
            while req_rx.recv().await.is_some() {
                let _ = resp_tx.send(Ok(stub_response())).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(resp_rx).boxed();
        Ok(crate::ReplicationStream {
            sender: req_tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // First batch — spawns the worker.
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "one worker after first batch"
    );

    // Second batch — must reuse the same worker, not spawn a second one.
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "worker count unchanged after second batch"
    );
}

/// Worker dead → transparently rebuilt; request is delivered to the new worker.
///
/// # Scenario
/// - A dead worker handle is injected for peer 2 via `inject_dead_worker_for_test`.
/// - `process_batch` returns one request for peer 2.
/// - `send_to_worker_or_spawn` detects the dead channel and rebuilds the worker.
/// - The new worker calls `send_append_request` and emits `RoleEvent::AppendResult`.
///
/// # Guarantees checked
/// - `worker_count_for_test() == 1` after the rebuild (old dead handle replaced).
/// - `RoleEvent::AppendResult` arrives on `role_rx`, confirming the request was delivered.
#[tokio::test]
#[traced_test]
async fn test_worker_rebuilt_after_death() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_worker_rebuilt_after_death", graceful_rx, None);

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request())],
                snapshot_targets: vec![],
            })
        });

    // Transport: allow async calls from the rebuilt worker. We verify delivery
    // indirectly via AppendResult on role_rx rather than mock call count.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport
        .expect_send_append_request()
        .returning(|_, _, _, _, _| Ok(stub_response()));
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel(128);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(128);

        // Spawn task to consume requests and send back stub responses
        tokio::spawn(async move {
            while req_rx.recv().await.is_some() {
                let _ = resp_tx.send(Ok(stub_response())).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(resp_rx).boxed();
        Ok(crate::ReplicationStream {
            sender: req_tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    // Inject a dead handle — simulates a worker whose task panicked or was cancelled.
    state.inject_dead_worker_for_test(2);
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "precondition: dead handle present"
    );

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();

    // The dead handle must be replaced by a live one (still 1, not 0 or 2).
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "worker rebuilt: count stays 1"
    );

    // Allow the spawned worker task to run and emit its result.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The rebuilt worker must relay AppendResult back to the Raft loop.
    let event = role_rx.try_recv().expect("AppendResult should arrive from rebuilt worker");
    assert!(
        matches!(event, RoleEvent::AppendResult { follower_id: 2, .. }),
        "expected AppendResult for peer 2, got {event:?}"
    );
}
