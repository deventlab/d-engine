//! TDD tests for `pending_commit_actions` unified post-commit queue (#333).
//!
//! Red phase: these tests drive the implementation of:
//! - `LeaderState::drain_commit_actions(new_commit_index, role_tx)`
//! - tick() step 5: deadline scan for expired `pending_commit_actions`
//!
//! Three key behaviors:
//! 1. `drain_commit_actions` fires `RoleEvent::NoopCommitted` when a `LeaderNoop` entry commits.
//! 2. `drain_commit_actions` fires `RoleEvent::JoinCommitted` when a `NodeJoin` entry commits.
//! 3. tick() sends `RoleEvent::BecomeFollower` for expired noop entries (leadership lost).

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::MaybeCloneOneshot;
use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::{LeaderState, PostCommitAction, PostCommitEntry};
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn setup() -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    mpsc::UnboundedSender<RoleEvent>,
    mpsc::Sender<crate::RaftEvent>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = MockBuilder::new(shutdown_rx).build_context();
    let state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel(1);
    (state, ctx, role_tx, raft_tx)
}

fn noop_entry(
    term: u64,
    deadline: Instant,
) -> PostCommitEntry {
    PostCommitEntry {
        deadline,
        action: PostCommitAction::LeaderNoop { term },
    }
}

fn join_entry(
    node_id: u32,
    deadline: Instant,
) -> (
    PostCommitEntry,
    crate::MaybeCloneOneshotReceiver<
        std::result::Result<d_engine_proto::server::cluster::JoinResponse, tonic::Status>,
    >,
) {
    let (tx, rx) = MaybeCloneOneshot::new();
    let entry = PostCommitEntry {
        deadline,
        action: PostCommitAction::NodeJoin {
            node_id,
            addr: "127.0.0.1:9000".to_string(),
            sender: tx,
        },
    };
    (entry, rx)
}

fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(3600)
}

fn expired() -> Instant {
    Instant::now() - Duration::from_secs(1)
}

// ── Test 1: drain_commit_actions fires NoopCommitted ─────────────────────────

/// `drain_commit_actions` fires `RoleEvent::NoopCommitted` when commit_index
/// reaches the noop entry's log index.
#[tokio::test]
async fn test_drain_commit_actions_fires_noop_committed() {
    let (mut state, ctx, _role_tx, _raft_tx) = setup().await;

    // Insert noop at log index 5, term 2.
    state.pending_commit_actions.insert(5, noop_entry(2, far_future()));

    // Advance commit to index 5 — should drain and fire NoopCommitted.
    let mut role_rx = {
        let (tx, rx) = mpsc::unbounded_channel::<RoleEvent>();
        // Replace role_tx with one we can read.
        state.drain_commit_actions(5, &ctx, &tx).await;
        rx
    };

    // pending_commit_actions must be empty.
    assert!(
        state.pending_commit_actions.is_empty(),
        "entry must be drained after commit"
    );

    // on_noop_committed must have been called directly (no extra loop iteration needed).
    // noop_log_id is set synchronously inside drain_commit_actions.
    assert!(
        state.noop_log_id.is_some(),
        "noop_log_id must be set synchronously by drain_commit_actions"
    );

    // RoleEvent::NoopCommitted { term: 2 } must have been sent (for notify_leader_change).
    let event = role_rx.try_recv().expect("expected NoopCommitted event");
    match event {
        RoleEvent::NoopCommitted { term } => assert_eq!(term, 2),
        other => panic!("expected NoopCommitted, got {:?}", other),
    }
}

// ── Test 2: drain_commit_actions sends join response directly ─────────────────

/// `drain_commit_actions` calls `send_join_success` directly when commit_index
/// reaches the NodeJoin entry's log index — no channel roundtrip needed.
#[tokio::test]
async fn test_drain_commit_actions_sends_join_response_directly() {
    let (mut state, ctx, _role_tx, _raft_tx) = setup().await;

    let (entry, mut join_rx) = join_entry(42, far_future());
    state.pending_commit_actions.insert(7, entry);

    let (tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    state.drain_commit_actions(7, &ctx, &tx).await;

    // Entry must be drained.
    assert!(
        state.pending_commit_actions.is_empty(),
        "entry must be drained"
    );

    // No JoinCommitted event — response is delivered directly to join_rx.
    assert!(
        role_rx.try_recv().is_err(),
        "no JoinCommitted event should be sent — join response goes directly to client"
    );

    // join_rx must have received Ok(JoinResponse) directly from drain_commit_actions.
    assert!(
        join_rx.try_recv().is_ok(),
        "join_rx must receive response directly from drain_commit_actions"
    );
}

// ── Test 3: entry not yet committed — must not drain ─────────────────────────

/// `drain_commit_actions` must not drain an entry whose index is after commit_index.
#[tokio::test]
async fn test_drain_commit_actions_skips_uncommitted_entries() {
    let (mut state, ctx, _role_tx, _raft_tx) = setup().await;

    state.pending_commit_actions.insert(10, noop_entry(3, far_future()));

    let (tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    state.drain_commit_actions(9, &ctx, &tx).await; // commit only up to 9

    assert_eq!(
        state.pending_commit_actions.len(),
        1,
        "uncommitted entry must remain"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "no event should be sent for uncommitted entry"
    );
}

// ── Test 4: tick() drains expired noop → BecomeFollower ──────────────────────

/// When a `LeaderNoop` entry has expired (deadline passed), tick() must:
/// - Remove the entry from `pending_commit_actions`
/// - Send `RoleEvent::BecomeFollower(None)` (lost quorum, step down)
#[tokio::test]
async fn test_tick_drains_expired_noop_sends_become_follower() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = MockBuilder::new(shutdown_rx).build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (raft_tx, _raft_rx) = mpsc::channel(1);

    state.pending_commit_actions.insert(5, noop_entry(2, expired()));

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    // Entry must be removed.
    assert!(
        state.pending_commit_actions.is_empty(),
        "expired noop entry must be removed by tick"
    );

    // Must have received BecomeFollower(None).
    let event = role_rx.try_recv().expect("expected BecomeFollower event");
    match event {
        RoleEvent::BecomeFollower(None) => {}
        other => panic!("expected BecomeFollower(None), got {:?}", other),
    }
}

// ── Test 5: tick() leaves live noop untouched ────────────────────────────────

/// When a `LeaderNoop` entry has not expired, tick() must not remove it or send any event.
#[tokio::test]
async fn test_tick_keeps_live_noop_in_commit_actions() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = MockBuilder::new(shutdown_rx).build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (raft_tx, _raft_rx) = mpsc::channel(1);

    state.pending_commit_actions.insert(5, noop_entry(2, far_future()));

    state.tick(&role_tx, &raft_tx, &ctx).await.unwrap();

    assert_eq!(
        state.pending_commit_actions.len(),
        1,
        "live noop entry must not be removed by tick"
    );
    // No events should be sent for live entry.
    assert!(role_rx.try_recv().is_err(), "no event for live noop entry");
}
