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
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use d_engine_proto::common::{EntryPayload, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::{AppendEntriesRequest, AppendEntriesResponse};
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing_test::traced_test;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockTransport;
use crate::RaftRequestWithSignal;
use crate::event::InternalEvent;
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

/// Like `stub_request()`, but with one real `Entry` — needed wherever the code
/// under test reads `request.entries.last()` (e.g. RTT sampling), which a
/// default/empty request can never satisfy.
fn stub_request_with_entry(index: u64) -> AppendEntriesRequest {
    AppendEntriesRequest {
        entries: vec![d_engine_proto::common::Entry {
            index,
            term: 1,
            payload: None,
        }],
        ..Default::default()
    }
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
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    // Transport: allow any number of async calls from the spawned worker task.
    // We don't assert the exact call count here because the worker runs in a background
    // tokio task and may not have executed by test teardown — this test's focus is the
    // worker count assertion, not the transport call count.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _, _| {
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

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
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    // Transport: allow async calls from the worker; exact count not asserted here
    // because the worker runs in background and may not have executed by teardown.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _, _| {
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // First batch — spawns the worker.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "one worker after first batch"
    );

    // Second batch — must reuse the same worker, not spawn a second one.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
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
/// - The new worker opens a bidi stream and emits `InternalEvent::AppendResult`.
///
/// # Guarantees checked
/// - `worker_count_for_test() == 1` after the rebuild (old dead handle replaced).
/// - `InternalEvent::AppendResult` arrives on `internal_event_rx`, confirming the request was delivered.
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
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    // Transport: allow async calls from the rebuilt worker. We verify delivery
    // indirectly via AppendResult on internal_event_rx rather than mock call count.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _, _| {
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

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // The dead handle must be replaced by a live one (still 1, not 0 or 2).
    assert_eq!(
        state.worker_count_for_test(),
        1,
        "worker rebuilt: count stays 1"
    );

    // Allow the spawned worker task to run and emit its result.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The rebuilt worker must relay AppendResult back to the Raft loop.
    let event = internal_event_rx
        .try_recv()
        .expect("AppendResult should arrive from rebuilt worker");
    assert!(
        matches!(event, InternalEvent::AppendResult { follower_id: 2, .. }),
        "expected AppendResult for peer 2, got {event:?}"
    );
}

/// Replication worker exits its inner reconnect loop when the task channel is closed.
///
/// # Scenario
/// - Transport always fails `open_replication_stream` (peer unreachable).
/// - Worker is spawned via `process_batch`; it enters the inner reconnect loop.
/// - The `LeaderState` is dropped, which drops `ReplicationWorkerHandle` and closes `task_rx`.
/// - On the next iteration the worker checks `task_rx.is_closed()` and must exit cleanly
///   without making further connection attempts.
///
/// # Guarantees checked
/// - After the handle is dropped, `open_replication_stream` call count stops increasing.
///   This proves the `if task_rx.is_closed() { return; }` guard fires correctly,
///   preventing an infinite reconnect loop that would flood the inbound event loop with
///   zombie signals and block heartbeats (regression: zombie detection + dead peer).
#[tokio::test]
#[traced_test]
async fn test_replication_worker_exits_when_handle_dropped() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replication_worker_exits_when_handle_dropped",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    // Transport: open_replication_stream always fails (simulates dead peer).
    // Counts total invocations so we can assert the worker stopped retrying after drop.
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = Arc::clone(&call_count);
    // Notifier fires after the first failure so the test knows the worker has entered the loop.
    let (first_attempt_tx, mut first_attempt_rx) = tokio::sync::mpsc::channel::<()>(1);

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _, _| {
        let n = call_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            // Signal test that worker reached the reconnect loop.
            let _ = first_attempt_tx.try_send(());
        }
        Err(crate::NetworkError::ConnectError("peer unreachable".into()).into())
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Wait until the worker has attempted at least one connection (entered inner loop).
    tokio::time::timeout(std::time::Duration::from_secs(5), first_attempt_rx.recv())
        .await
        .expect("timed out waiting for first reconnect attempt")
        .expect("signal channel closed without a send — worker never entered reconnect loop");

    // Drop LeaderState — drops all ReplicationWorkerHandles — closes task_rx for all workers.
    drop(state);

    // Record call count at drop time.
    let count_at_drop = call_count.load(Ordering::SeqCst);

    // Give the worker enough time to loop back and hit the is_closed() check.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let count_after_wait = call_count.load(Ordering::SeqCst);

    assert_eq!(
        count_at_drop, count_after_wait,
        "open_replication_stream must not be called after task channel is closed \
         (got {} calls at drop, {} after wait — worker did not exit)",
        count_at_drop, count_after_wait
    );
}

/// Worker reconnects when the bidi stream's receiver yields an error.
///
/// # Scenario
/// - `open_replication_stream` succeeds, but its receiver immediately yields `Err(status)`.
/// - The worker's recv task detects the error; the main loop emits `PeerStreamError`
///   and reconnects (calls `open_replication_stream` again).
///
/// # Why
/// - This is the "peer went away mid-stream" recovery path: without it, a broken
///   stream would silently stop replication for that peer.
#[tokio::test]
#[traced_test]
async fn test_worker_reconnects_on_stream_recv_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_reconnects_on_stream_recv_error",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = Arc::clone(&call_count);

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _, _| {
        let n = call_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            // First open succeeds, but the stream immediately errors.
            let (req_tx, mut req_rx) = mpsc::channel::<AppendEntriesRequest>(128);
            // Drain requests so the worker's Append send succeeds — this test
            // exercises the recv-error path, not a send failure.
            tokio::spawn(async move { while req_rx.recv().await.is_some() {} });
            let stream = futures::stream::iter(vec![Err::<AppendEntriesResponse, tonic::Status>(
                tonic::Status::internal("stream broken"),
            )])
            .boxed();
            Ok(crate::ReplicationStream {
                sender: req_tx,
                receiver: stream,
            })
        } else {
            // Reconnect fails so the worker enters backoff instead of spinning.
            Err(crate::NetworkError::ConnectError("peer unreachable".into()).into())
        }
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Give the worker time to detect the recv error and reconnect.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut saw_peer_stream_error = false;
    while let Ok(event) = internal_event_rx.try_recv() {
        if matches!(event, InternalEvent::PeerStreamError { peer_id: 2 }) {
            saw_peer_stream_error = true;
            break;
        }
    }
    assert!(
        saw_peer_stream_error,
        "PeerStreamError must be emitted when the stream receiver errors"
    );

    assert!(
        call_count.load(Ordering::SeqCst) >= 2,
        "open_replication_stream must be called at least twice (initial + reconnect), got {}",
        call_count.load(Ordering::SeqCst)
    );
}

/// A bidi stream send failure must surface as `PeerStreamError` so the Raft loop
/// resets `next_index` and re-sends unACKed entries.
///
/// # Why it matters
/// A closed sender means the AppendEntries batch never reached the follower;
/// without `PeerStreamError` the leader would wrongly advance its commit index.
///
/// # Covered branch
/// `stream_sender.send(request).await.is_err()`. The ACK stream is kept pending so
/// the biased `select!` cannot resolve the `recv_handle` arm before the Append task.
#[tokio::test]
#[traced_test]
async fn test_replication_worker_sender_closed_emits_peer_stream_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replication_worker_sender_closed_emits_peer_stream_error",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    // The first stream's sender receiver is dropped so `send()` fails immediately;
    // later reconnects park on a pending ACK stream so the worker stops reconnecting.
    let open_count = Arc::new(AtomicUsize::new(0));
    let open_count_clone = Arc::clone(&open_count);
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport
        .expect_open_replication_stream()
        .times(..)
        .returning(move |_, _, _, _| {
            let attempt = open_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
            let (sender, send_receiver) = tokio::sync::mpsc::channel::<AppendEntriesRequest>(128);
            if attempt == 1 {
                drop(send_receiver);
            }
            // Keep the ACK stream pending so the biased select! reaches the Append arm
            // (attempt 1) and the worker parks (later attempts).
            let receiver =
                futures::stream::pending::<Result<AppendEntriesResponse, tonic::Status>>().boxed();
            Ok(crate::ReplicationStream { sender, receiver })
        });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), internal_event_rx.recv())
        .await
        .expect("timed out waiting for PeerStreamError")
        .expect("event channel closed before PeerStreamError");
    assert!(
        matches!(event, InternalEvent::PeerStreamError { peer_id: 2 }),
        "expected PeerStreamError for peer 2, got {event:?}"
    );
}

/// A full (not closed) bidi send buffer must also surface as `PeerStreamError` instead of
/// parking the worker: the peer is alive but not draining, so the leader has to stop trusting
/// its optimistic pipeline for this peer rather than silently wedge behind the buffer.
///
/// # Covered branch
/// `stream_sender.try_send(request)` returning `Full`. The sender's receiver stays alive and
/// undrained, with the channel pre-filled to capacity, so the failure is `Full`, not `Closed`.
/// With a blocking `send().await` this test would time out instead of seeing the event.
#[tokio::test]
#[traced_test]
async fn test_replication_worker_send_buffer_full_emits_peer_stream_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replication_worker_send_buffer_full_emits_peer_stream_error",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    // First stream: capacity-1 channel, already full, receiver kept alive (never drained).
    // Later reconnects park on a pending ACK stream so the worker stops reconnecting.
    let open_count = Arc::new(AtomicUsize::new(0));
    let open_count_clone = Arc::clone(&open_count);
    let held_receivers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let held_receivers_clone = Arc::clone(&held_receivers);
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport
        .expect_open_replication_stream()
        .times(..)
        .returning(move |_, _, _, _| {
            let attempt = open_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
            let (sender, send_receiver) = tokio::sync::mpsc::channel::<AppendEntriesRequest>(1);
            if attempt == 1 {
                sender.try_send(AppendEntriesRequest::default()).unwrap();
            }
            held_receivers_clone.lock().unwrap().push(send_receiver);
            let receiver =
                futures::stream::pending::<Result<AppendEntriesResponse, tonic::Status>>().boxed();
            Ok(crate::ReplicationStream { sender, receiver })
        });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), internal_event_rx.recv())
        .await
        .expect("timed out — worker parked on a full send buffer instead of failing fast")
        .expect("event channel closed before PeerStreamError");
    assert!(
        matches!(event, InternalEvent::PeerStreamError { peer_id: 2 }),
        "expected PeerStreamError for peer 2, got {event:?}"
    );
}

/// Worker forwards a successful stream ACK as `AppendResult`.
///
/// # Scenario
/// - `open_replication_stream` succeeds; its receiver yields `Ok(response)`.
/// - The worker's recv task relays it as `InternalEvent::AppendResult { result: Ok(..) }`.
///
/// # Why
/// - This is the ACK path that lets the leader advance `match_index` / `commit_index`.
#[tokio::test]
#[traced_test]
async fn test_worker_forwards_append_result_on_recv() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_forwards_append_result_on_recv",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = Arc::clone(&call_count);

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _, _| {
        let n = call_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            // First open succeeds and delivers one successful ACK.
            let (req_tx, mut req_rx) = mpsc::channel::<AppendEntriesRequest>(128);
            // Drain requests so the worker's Append send succeeds — this test
            // exercises the ACK-forwarding path, not a send failure.
            tokio::spawn(async move { while req_rx.recv().await.is_some() {} });
            let stream = futures::stream::iter(vec![Ok::<AppendEntriesResponse, tonic::Status>(
                stub_response(),
            )])
            .boxed();
            Ok(crate::ReplicationStream {
                sender: req_tx,
                receiver: stream,
            })
        } else {
            // Reconnect fails so the worker enters backoff instead of spinning.
            Err(crate::NetworkError::ConnectError("peer unreachable".into()).into())
        }
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Give the worker's recv task time to read the ACK and forward it.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let event = internal_event_rx
        .try_recv()
        .expect("AppendResult should arrive from the stream ACK");
    assert!(
        matches!(
            event,
            InternalEvent::AppendResult {
                follower_id: 2,
                result: Ok(_),
                ..
            }
        ),
        "expected AppendResult for peer 2, got {event:?}"
    );
}

/// Reconnecting mid-sample must reset `rtt_sample_pending`, or RTT sampling
/// permanently stops for the rest of this worker's lifetime (#446).
///
/// # Scenario
/// - 16 sends land on the sampling boundary exactly once (the 16th), starting
///   an RTT sample that never resolves — the mock stream stays pending, no ACK.
/// - The stream then errors, forcing `recv_handle.abort()` + reconnect.
///
/// # Guarantee checked
/// The reconnect path must reset the interrupted sample (logged as "RTT sample
/// reset after reconnect"), not leave `rtt_sample_pending` stuck `true` forever.
#[tokio::test]
#[traced_test]
async fn test_rtt_sample_reset_on_reconnect_after_interrupted_sample() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_rtt_sample_reset_on_reconnect_after_interrupted_sample",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    let prepare_count = Arc::new(AtomicUsize::new(0));
    let prepare_count_clone = Arc::clone(&prepare_count);
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(16)
        .returning(move |_, _, _, _, _, _| {
            let n = prepare_count_clone.fetch_add(1, Ordering::SeqCst) as u64 + 1;
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request_with_entry(n), 1)],
                snapshot_targets: vec![],
            })
        });

    let open_count = Arc::new(AtomicUsize::new(0));
    let open_count_clone = Arc::clone(&open_count);
    let (break_tx, break_rx) =
        mpsc::unbounded_channel::<Result<AppendEntriesResponse, tonic::Status>>();
    // `returning` takes an `FnMut`, so it must be able to run more than once —
    // wrap the receiver so only the first (successful) call can move it out.
    let break_rx = std::sync::Mutex::new(Some(break_rx));

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _, _| {
        let n = open_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            let (req_tx, mut req_rx) = mpsc::channel::<AppendEntriesRequest>(128);
            // Drain requests so all 16 sends succeed — this test exercises the
            // sample-reset-on-reconnect path, not a send failure.
            tokio::spawn(async move { while req_rx.recv().await.is_some() {} });
            // Stays pending (no ACK, so the sample started on send #16 never
            // resolves) until the test pushes the break signal below.
            let rx = break_rx
                .lock()
                .unwrap()
                .take()
                .expect("stream should only open successfully once");
            let stream = UnboundedReceiverStream::new(rx).boxed();
            Ok(crate::ReplicationStream {
                sender: req_tx,
                receiver: stream,
            })
        } else {
            // Reconnect fails so the worker enters backoff instead of spinning.
            Err(crate::NetworkError::ConnectError("peer unreachable".into()).into())
        }
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // 16 sends: the 16th lands on the sampling boundary and starts a sample
    // that never resolves (mock stream stays pending, no ACK). A new peer
    // starts in Probe state, which allows only one in-flight request at a
    // time (leader_state.rs:3384) — since this test never sends a real ACK,
    // that flag would never clear naturally, so it's reset by hand here to
    // let all 16 attempts actually reach the worker.
    for i in 0..16 {
        state.reset_in_flight(2);
        state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
        if i == 0 {
            // The first dispatch spawns the worker. A dispatch made while the worker has no
            // open stream is (by design) not queued, so wait for the stream before sending the
            // rest — otherwise the 16th send, which starts the RTT sample, would never happen.
            for _ in 0..1000 {
                if open_count.load(Ordering::SeqCst) >= 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    // Give the worker time to process all 16 sends before breaking the stream.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Break the stream mid-sample to force recv_handle.abort() + reconnect.
    let _ = break_tx.send(Err(tonic::Status::internal("stream broken")));

    // Give the worker time to detect the break, abort, and reconnect.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert!(
        logs_contain("RTT sample reset after reconnect"),
        "reconnecting mid-sample must reset the interrupted RTT sample"
    );
}

/// The queue capacity configured in `replication_send_queue_capacity` must be the value the
/// worker hands to the transport when it opens the stream — otherwise the knob is decorative.
#[tokio::test]
#[traced_test]
async fn test_worker_opens_stream_with_configured_send_queue_capacity() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_opens_stream_with_configured_capacity",
        graceful_rx,
        None,
    );
    ctx.membership = Arc::new(two_peer_membership());

    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.replication_send_queue_capacity = 777;
    ctx.node_config = Arc::new(cfg);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    let seen_capacity = Arc::new(AtomicUsize::new(0));
    let seen = seen_capacity.clone();
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _, capacity| {
        seen.store(capacity, Ordering::SeqCst);
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel(capacity);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while req_rx.recv().await.is_some() {
                let _ = resp_tx.send(Ok(stub_response())).await;
            }
        });
        Ok(crate::ReplicationStream {
            sender: req_tx,
            receiver: tokio_stream::wrappers::ReceiverStream::new(resp_rx).boxed(),
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

    let (internal_event_tx, _rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while seen_capacity.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        seen_capacity.load(Ordering::SeqCst),
        777,
        "the worker must pass replication_send_queue_capacity to open_replication_stream"
    );
}

// ── disconnected-peer dispatch (R3) ───────────────────────────────────────────
//
// Background: while a peer's replication worker has no open stream (it sits in its reconnect
// loop) it does not dequeue tasks, but the leader keeps enqueuing one per write batch into the
// worker's UNBOUNDED task queue. Measured: 37k-66k tasks piled up in ~40 s and were drained in
// one burst after reconnect, each having waited ~20 s.
//
// Contract these tests pin (the fix: a worker-owned `connected` flag read by Phase 5 BEFORE any
// bookkeeping): while the worker has no open stream the leader must dispatch NOTHING to it —
// data and heartbeats alike — and must leave no accounting behind (`next_index`, in-flight), so
// the first send after reconnect starts from the same place. Dropping is safe for Raft: the
// leader only advances on ACKs and retransmits from `next_index`.
//
// The tests observe behaviour only (what the stream receives, what the leader accounted), so
// they compile against the current code and are RED for the right reason until the fix lands.

use crate::raft_role::role_state::PeerReplicationState;

/// Scripted peer: `open_replication_stream` fails until `allow_connect` is set, then succeeds.
/// Counts how many requests each established stream receives.
#[derive(Clone, Default)]
struct PeerProbe {
    allow_connect: Arc<std::sync::atomic::AtomicBool>,
    open_attempts: Arc<AtomicUsize>,
    established: Arc<AtomicUsize>,
    /// Requests delivered to the first established stream.
    received_first: Arc<AtomicUsize>,
    /// Requests delivered to any later (re-established) stream.
    received_later: Arc<AtomicUsize>,
}

/// `break_first_stream`: the first open succeeds immediately but its receiver errors at once
/// (a stream that was up and then broke); every other open needs `allow_connect`.
fn scripted_peer(break_first_stream: bool) -> (MockTransport<MockTypeConfig>, PeerProbe) {
    let probe = PeerProbe::default();
    let p = probe.clone();
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _, _| {
        let attempt = p.open_attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let immediate = break_first_stream && attempt == 1;
        if !immediate && !p.allow_connect.load(Ordering::SeqCst) {
            return Err(crate::NetworkError::ConnectError("peer unreachable".into()).into());
        }
        let n = p.established.fetch_add(1, Ordering::SeqCst) + 1;
        let counter = if n == 1 {
            p.received_first.clone()
        } else {
            p.received_later.clone()
        };
        let (req_tx, mut req_rx) = mpsc::channel::<AppendEntriesRequest>(128);
        let receiver = if immediate {
            tokio::spawn(async move {
                while req_rx.recv().await.is_some() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
            futures::stream::iter(vec![Err::<AppendEntriesResponse, tonic::Status>(
                tonic::Status::unavailable("peer gone"),
            )])
            .boxed()
        } else {
            // Keep the response side open (never answers) so the stream stays healthy.
            let (resp_tx, resp_rx) =
                mpsc::channel::<Result<AppendEntriesResponse, tonic::Status>>(1);
            tokio::spawn(async move {
                while req_rx.recv().await.is_some() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                drop(resp_tx);
            });
            tokio_stream::wrappers::ReceiverStream::new(resp_rx).boxed()
        };
        Ok(crate::ReplicationStream {
            sender: req_tx,
            receiver,
        })
    });
    (transport, probe)
}

/// Leader wired to the scripted peer. `is_heartbeat(k)` decides whether the k-th
/// `prepare_batch_requests` call (1-based) yields an empty request (heartbeat) or a data request.
/// Peer 2 is put in `Replicate` state so the in-flight window (256) does not gate data requests —
/// the situation in which the leader really does enqueue one task per batch.
async fn leader_with_scripted_peer(
    tmp: &str,
    transport: MockTransport<MockTypeConfig>,
    is_heartbeat: fn(usize) -> bool,
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    mpsc::UnboundedSender<InternalEvent>,
    mpsc::UnboundedReceiver<InternalEvent>,
) {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(tmp, graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let calls = Arc::new(AtomicUsize::new(0));
    ctx.handlers.replication_handler.expect_prepare_batch_requests().returning(
        move |_, _, _, _, _, _| {
            let k = calls.fetch_add(1, Ordering::SeqCst) + 1;
            let req = if is_heartbeat(k) {
                stub_request()
            } else {
                stub_request_with_entry(k as u64)
            };
            Ok(crate::PrepareResult {
                append_requests: vec![(2, req, k as u64)],
                snapshot_targets: vec![],
            })
        },
    );
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);

    let (tx, rx) = mpsc::unbounded_channel();
    (state, ctx, tx, rx)
}

/// Polls `cond` every 5 ms for up to 5 s; panics with `what` on timeout.
async fn wait_until(
    what: &str,
    cond: impl Fn() -> bool,
) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// Peer unreachable from the start: nothing dispatched while it is down may be replayed after it
/// comes back, and the leader must not have accounted for it.
///
/// # Scenario
/// - Batch 1 spawns the worker (no handle exists yet, so nothing can be skipped).
/// - The worker is in its reconnect loop (every open fails). Batches 2-6 are dispatched: data
///   requests and heartbeats mixed.
/// - The peer then becomes reachable.
///
/// # Guarantees checked
/// - The re-established stream receives exactly 1 request (the spawn task), not 6: the 5 sent
///   while the peer was down were never enqueued. (Before the fix all 6 are replayed.)
/// - In-flight accounting holds only the 1 data request that really was dispatched; the skipped
///   ones left no trace (before the fix: one slot per data request, 4 in total).
/// - Dispatch resumes once the stream is back: an unreachable peer is not skipped forever.
#[tokio::test]
#[traced_test]
async fn test_unreachable_peer_does_not_accumulate_replication_tasks() {
    let (transport, probe) = scripted_peer(false);
    // k = 3 and 5 are heartbeats, the rest carry an entry.
    let (mut state, ctx, tx, _rx) = leader_with_scripted_peer(
        "/tmp/test_unreachable_peer_does_not_accumulate_replication_tasks",
        transport,
        |k| k == 3 || k == 5,
    )
    .await;

    state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    wait_until("worker enters its reconnect loop", || {
        probe.open_attempts.load(Ordering::SeqCst) >= 1
    })
    .await;
    // After the spawning dispatch (k=1, one entry): next_index = effective(1) + 1 entry.
    let next_index_after_spawn = state.next_index(2);

    for _ in 0..5 {
        state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    }
    assert_eq!(
        state.next_index(2),
        next_index_after_spawn,
        "skipped dispatches must not advance next_index (the first send after reconnect must \
         start from the same place)"
    );

    probe.allow_connect.store(true, Ordering::SeqCst);
    wait_until("stream re-established", || {
        probe.established.load(Ordering::SeqCst) >= 1
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await; // let the worker drain its queue

    assert_eq!(
        probe.received_first.load(Ordering::SeqCst),
        1,
        "requests dispatched while the stream was down must not be queued and replayed on reconnect"
    );
    assert_eq!(
        state.in_flight_count(2),
        1,
        "skipped dispatches must leave no in-flight accounting behind"
    );

    // Control: once the stream is back the peer is served again.
    let before = probe.received_first.load(Ordering::SeqCst);
    for _ in 0..100 {
        state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
        if probe.received_first.load(Ordering::SeqCst) > before {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("dispatch to the peer never resumed after the stream was re-established");
}

/// Heartbeats alone must not pile up either (they are empty, bypass the in-flight window, and
/// were the one request kind still enqueued for a gated peer).
///
/// # Guarantees checked
/// - 5 heartbeats sent while the stream is down are not replayed after reconnect: only the
///   worker-spawning request is delivered. (Before the fix: 6.)
#[tokio::test]
#[traced_test]
async fn test_unreachable_peer_heartbeats_are_not_queued() {
    let (transport, probe) = scripted_peer(false);
    let (mut state, ctx, tx, _rx) = leader_with_scripted_peer(
        "/tmp/test_unreachable_peer_heartbeats_are_not_queued",
        transport,
        |_| true,
    )
    .await;

    state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    wait_until("worker enters its reconnect loop", || {
        probe.open_attempts.load(Ordering::SeqCst) >= 1
    })
    .await;
    for _ in 0..5 {
        state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    }

    probe.allow_connect.store(true, Ordering::SeqCst);
    wait_until("stream re-established", || {
        probe.established.load(Ordering::SeqCst) >= 1
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    assert_eq!(
        probe.received_first.load(Ordering::SeqCst),
        1,
        "heartbeats dispatched while the stream was down must not be queued and replayed"
    );
}

/// A stream that was up and then broke: while the worker reconnects it counts as disconnected
/// exactly like a peer that never connected.
///
/// # Scenario
/// - First stream opens, then its receiver errors at once -> the worker leaves the main loop and
///   reconnects; the reconnect attempts fail.
/// - 4 batches are dispatched during that window; then the peer becomes reachable.
///
/// # Guarantees checked
/// - The second stream receives nothing from the 4 batches sent while reconnecting.
///   (Before the fix: 4.)
/// - Dispatch resumes on the second stream afterwards.
#[tokio::test]
#[traced_test]
async fn test_broken_stream_peer_is_not_fed_while_reconnecting() {
    let (transport, probe) = scripted_peer(true);
    let (mut state, ctx, tx, _rx) = leader_with_scripted_peer(
        "/tmp/test_broken_stream_peer_is_not_fed_while_reconnecting",
        transport,
        |k| k % 2 == 0,
    )
    .await;

    state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    // attempt 1 = the stream that breaks; attempt >= 2 = the worker is reconnecting.
    wait_until("worker is reconnecting after the broken stream", || {
        probe.open_attempts.load(Ordering::SeqCst) >= 2
    })
    .await;

    for _ in 0..4 {
        state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    }

    probe.allow_connect.store(true, Ordering::SeqCst);
    wait_until("second stream established", || {
        probe.established.load(Ordering::SeqCst) >= 2
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    assert_eq!(
        probe.received_later.load(Ordering::SeqCst),
        0,
        "requests dispatched while reconnecting after a broken stream must not reach the new stream"
    );

    for _ in 0..100 {
        state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
        if probe.received_later.load(Ordering::SeqCst) > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("dispatch never resumed on the re-established stream");
}

/// Control: a healthy, connected peer receives every dispatch. Guards against a fix that skips
/// too much (e.g. a flag that never turns true, or turns true too late).
///
/// This test is GREEN before the fix and must stay green after it.
#[tokio::test]
#[traced_test]
async fn test_connected_peer_receives_every_dispatch() {
    let (transport, probe) = scripted_peer(false);
    probe.allow_connect.store(true, Ordering::SeqCst);
    let (mut state, ctx, tx, _rx) = leader_with_scripted_peer(
        "/tmp/test_connected_peer_receives_every_dispatch",
        transport,
        |k| k % 3 == 0,
    )
    .await;

    state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
    wait_until("stream established", || {
        probe.established.load(Ordering::SeqCst) >= 1
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    for _ in 0..5 {
        state.process_batch(one_entry_batch(), &tx, &ctx).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    wait_until("all 6 dispatches delivered to the connected peer", || {
        probe.received_first.load(Ordering::SeqCst) == 6
    })
    .await;
}
