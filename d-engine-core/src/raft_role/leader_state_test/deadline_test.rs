//! Tests for tick()-driven deadline expiry on pending writes and reads.
//!
//! Verifies that `tick()` correctly drains expired entries from
//! `pending_client_writes`, `pending_reads`, `pending_lease_reads`, and
//! `pending_commit_actions`, and leaves live entries intact.

use crate::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::{
    LeaderState, PendingLeaseRead, PendingReadBatch, PostCommitAction, PostCommitEntry,
    WriteMetadata,
};
use crate::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use d_engine_proto::client::ClientReadRequest;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tonic::Code;

// ============================================================================
// Helpers
// ============================================================================

type WriteResponse = std::result::Result<d_engine_proto::client::ClientResponse, tonic::Status>;
type ReadResponse = WriteResponse;

/// Build a minimal context + LeaderState sufficient for tick() deadline tests.
/// The ReplicationTimer starts with a 100ms window so the heartbeat path is
/// never triggered within the duration of these fast unit tests.
async fn setup() -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    mpsc::UnboundedSender<crate::RoleEvent>,
    mpsc::Sender<crate::RaftEvent>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = MockBuilder::new(shutdown_rx).build_context();
    let state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel(1);
    (state, ctx, role_tx, raft_tx)
}

fn expired() -> Instant {
    // Instant::now() minus a comfortable margin — guaranteed to be in the past.
    Instant::now() - Duration::from_secs(1)
}

fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(3600)
}

fn write_metadata(
    deadline: Instant
) -> (
    WriteMetadata,
    crate::MaybeCloneOneshotReceiver<WriteResponse>,
) {
    let (tx, rx) = MaybeCloneOneshot::new();
    let meta = WriteMetadata {
        start_idx: 1,
        senders: vec![tx],
        wait_for_apply: false,
        deadline,
    };
    (meta, rx)
}

fn read_batch(
    deadline: Instant
) -> (
    PendingReadBatch,
    crate::MaybeCloneOneshotReceiver<ReadResponse>,
) {
    let (tx, rx) = MaybeCloneOneshot::new();
    let req = ClientReadRequest {
        keys: vec![],
        ..Default::default()
    };
    let batch = PendingReadBatch {
        deadline,
        requests: VecDeque::from([(req, tx)]),
    };
    (batch, rx)
}

// ============================================================================
// Pending write deadline tests
// ============================================================================

/// Expired write: tick() sends deadline_exceeded and removes the entry.
#[tokio::test]
async fn test_tick_drains_expired_pending_write() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (meta, mut rx) = write_metadata(expired());
    state.pending_client_writes.insert(1, meta);

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    // Sender must have received deadline_exceeded.
    let result = rx.recv().await.expect("channel closed");
    let status = result.expect_err("expected Err(Status)");
    assert_eq!(status.code(), Code::DeadlineExceeded, "wrong status code");

    // Entry must be removed.
    assert!(
        state.pending_client_writes.is_empty(),
        "expired entry not removed"
    );
}

/// Live write: tick() leaves the entry untouched and sends nothing.
#[tokio::test]
async fn test_tick_keeps_non_expired_pending_write() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (meta, mut rx) = write_metadata(far_future());
    state.pending_client_writes.insert(1, meta);

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    // Channel must still be open (no message sent).
    assert!(
        rx.try_recv().is_err(),
        "unexpectedly received message on live write"
    );

    // Entry must still be present.
    assert_eq!(
        state.pending_client_writes.len(),
        1,
        "live entry was removed"
    );
}

// ============================================================================
// Pending read deadline tests
// ============================================================================

/// Expired read: tick() sends deadline_exceeded and removes the batch.
#[tokio::test]
async fn test_tick_drains_expired_pending_read() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (batch, mut rx) = read_batch(expired());
    state.pending_reads.insert(10, batch);

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    let result = rx.recv().await.expect("channel closed");
    let status = result.expect_err("expected Err(Status)");
    assert_eq!(status.code(), Code::DeadlineExceeded, "wrong status code");

    assert!(
        state.pending_reads.is_empty(),
        "expired read batch not removed"
    );
}

/// Live read: tick() leaves the batch untouched and sends nothing.
#[tokio::test]
async fn test_tick_keeps_non_expired_pending_read() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (batch, mut rx) = read_batch(far_future());
    state.pending_reads.insert(10, batch);

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    assert!(
        rx.try_recv().is_err(),
        "unexpectedly received message on live read"
    );
    assert_eq!(state.pending_reads.len(), 1, "live read batch was removed");
}

// ============================================================================
// Mixed expiry test
// ============================================================================

// ============================================================================
// Pending lease read deadline tests
// ============================================================================

/// Expired lease read: tick() sends deadline_exceeded and removes the entry.
#[tokio::test]
async fn test_tick_drains_expired_pending_lease_read() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (tx, mut rx) = MaybeCloneOneshot::new();
    state.pending_lease_reads.push_back(PendingLeaseRead {
        request: ClientReadRequest::default(),
        sender: tx,
        deadline: expired(),
    });

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    let result = rx.recv().await.expect("channel closed");
    assert_eq!(
        result.expect_err("expected Err(Status)").code(),
        Code::DeadlineExceeded,
        "expired lease read must receive DeadlineExceeded"
    );
    assert!(state.pending_lease_reads.is_empty(), "expired entry not removed");
}

/// Live lease read: tick() leaves the entry untouched.
#[tokio::test]
async fn test_tick_keeps_non_expired_pending_lease_read() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (tx, mut rx) = MaybeCloneOneshot::new();
    state.pending_lease_reads.push_back(PendingLeaseRead {
        request: ClientReadRequest::default(),
        sender: tx,
        deadline: far_future(),
    });

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    assert!(rx.try_recv().is_err(), "live lease read must not be drained");
    assert_eq!(state.pending_lease_reads.len(), 1, "live entry was removed");
}

/// Mixed lease reads: one expired, one live.
/// tick() drains only the expired entry; the live entry survives.
#[tokio::test]
async fn test_tick_drains_only_expired_lease_reads_in_mixed_queue() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (exp_tx, mut exp_rx) = MaybeCloneOneshot::new();
    let (live_tx, mut live_rx) = MaybeCloneOneshot::new();

    state.pending_lease_reads.push_back(PendingLeaseRead {
        request: ClientReadRequest::default(),
        sender: exp_tx,
        deadline: expired(),
    });
    state.pending_lease_reads.push_back(PendingLeaseRead {
        request: ClientReadRequest::default(),
        sender: live_tx,
        deadline: far_future(),
    });

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    assert_eq!(
        exp_rx.recv().await.expect("channel closed").expect_err("expected Err").code(),
        Code::DeadlineExceeded,
    );
    assert!(live_rx.try_recv().is_err(), "live lease read must not be drained");
    assert_eq!(state.pending_lease_reads.len(), 1, "only live entry should remain");
}

// ============================================================================
// pending_commit_actions deadline tests
// ============================================================================

/// Expired NodeJoin action: tick() sends deadline_exceeded to the join sender.
#[tokio::test]
async fn test_tick_drains_expired_node_join_commit_action() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (tx, mut rx) = MaybeCloneOneshot::new();
    state.pending_commit_actions.insert(
        42,
        PostCommitEntry {
            deadline: expired(),
            action: PostCommitAction::NodeJoin {
                node_id: 2,
                addr: "127.0.0.1:8080".to_string(),
                sender: tx,
            },
        },
    );

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    let result = rx.recv().await.expect("channel closed");
    assert_eq!(
        result.expect_err("expected Err(Status)").code(),
        Code::DeadlineExceeded,
        "expired NodeJoin must receive DeadlineExceeded"
    );
    assert!(state.pending_commit_actions.is_empty(), "expired action not removed");
}

/// Live NodeJoin action: tick() leaves it untouched.
#[tokio::test]
async fn test_tick_keeps_non_expired_node_join_commit_action() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (tx, mut rx) = MaybeCloneOneshot::new();
    state.pending_commit_actions.insert(
        42,
        PostCommitEntry {
            deadline: far_future(),
            action: PostCommitAction::NodeJoin {
                node_id: 2,
                addr: "127.0.0.1:8080".to_string(),
                sender: tx,
            },
        },
    );

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    assert!(rx.try_recv().is_err(), "live NodeJoin must not be drained");
    assert_eq!(state.pending_commit_actions.len(), 1, "live action was removed");
}

/// Expired LeaderNoop action: tick() emits BecomeFollower to role_tx.
/// A noop that times out means quorum cannot be confirmed — leader must step down.
#[tokio::test]
async fn test_tick_expired_leader_noop_triggers_step_down() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = MockBuilder::new(shutdown_rx).build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    // Keep role_rx alive to receive the BecomeFollower event.
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel(1);

    state.pending_commit_actions.insert(
        1,
        PostCommitEntry {
            deadline: expired(),
            action: PostCommitAction::LeaderNoop { term: 1 },
        },
    );

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    assert!(state.pending_commit_actions.is_empty(), "expired LeaderNoop not removed");

    // tick() must have sent BecomeFollower
    let event = role_rx.try_recv().expect("expected BecomeFollower event");
    assert!(
        matches!(event, crate::RoleEvent::BecomeFollower(_)),
        "expected BecomeFollower, got {:?}",
        event
    );
}

/// Two write entries: one expired, one live.
/// tick() drains only the expired one; the live entry survives.
#[tokio::test]
async fn test_tick_drains_only_expired_writes_in_mixed_map() {
    let (mut state, ctx, role_tx, raft_tx) = setup().await;

    let (expired_meta, mut expired_rx) = write_metadata(expired());
    let (live_meta, mut live_rx) = write_metadata(far_future());

    state.pending_client_writes.insert(1, expired_meta);
    state.pending_client_writes.insert(2, live_meta);

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    // Expired entry notified.
    let result = expired_rx.recv().await.expect("channel closed");
    assert_eq!(
        result.expect_err("expected Err(Status)").code(),
        Code::DeadlineExceeded,
    );

    // Live entry untouched.
    assert!(
        live_rx.try_recv().is_err(),
        "live write was incorrectly drained"
    );
    assert_eq!(
        state.pending_client_writes.len(),
        1,
        "live entry should remain"
    );
    assert!(
        state.pending_client_writes.contains_key(&2),
        "wrong entry retained"
    );
}
