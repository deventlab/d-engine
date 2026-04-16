//! TDD tests for the min-deadline stale learner mechanism (Phase 3, Issue #339).
//!
//! Design: instead of polling every `membership_maintenance_interval`, LeaderState
//! maintains a single `stale_check_deadline: Option<Instant>` that is set to
//! `front().ready_since + stale_learner_threshold` whenever a learner is enqueued.
//! `tick()` performs an O(1) comparison and acts only when the deadline fires.
//!
//! Test plan:
//! 1. Deadline is None when queue is empty.
//! 2. Deadline is set to `ready_since + threshold` when first learner is enqueued.
//! 3. Deadline is NOT overwritten when a second learner is enqueued (front stays oldest).
//! 4. tick() does NOT call handle_stale_learner before the deadline.
//! 5. tick() DOES call handle_stale_learner when the deadline fires (stale learner removed).
//! 6. After drain_batch empties the queue, deadline resets to None.
//! 7. After drain_batch leaves items, deadline refreshes to new front's deadline.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::raft_role::leader_state::{LeaderState, PendingPromotion};
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig};
use crate::test_utils::node_config;
use d_engine_proto::common::EntryPayload;

// ============================================================================
// Helpers
// ============================================================================

async fn setup_with_threshold(
    threshold: Duration
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    mpsc::UnboundedSender<crate::RoleEvent>,
    mpsc::Sender<crate::RaftEvent>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut cfg = node_config("/tmp/stale_learner_deadline_test");
    cfg.raft.membership.promotion.stale_learner_threshold = threshold;
    let ctx = MockBuilder::new(shutdown_rx).with_node_config(cfg).build_context();
    let state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel(1);
    (state, ctx, role_tx, raft_tx)
}

// ============================================================================
// Test 1: stale_check_deadline is None when queue is empty
// ============================================================================

#[tokio::test]
async fn test_stale_check_deadline_none_when_queue_empty() {
    let (leader, _ctx, _role_tx, _raft_tx) = setup_with_threshold(Duration::from_secs(30)).await;

    assert!(
        leader.stale_check_deadline.is_none(),
        "Deadline must be None when no learners are pending"
    );
}

// ============================================================================
// Test 2: Deadline set to ready_since + threshold on first enqueue
// ============================================================================

#[tokio::test]
async fn test_stale_check_deadline_set_on_first_enqueue() {
    let threshold = Duration::from_secs(30);
    let (mut leader, _ctx, _role_tx, _raft_tx) = setup_with_threshold(threshold).await;

    let before = Instant::now();
    leader.pending_promotions.push_back(PendingPromotion::new(10, Instant::now()));
    leader.refresh_stale_deadline(threshold);
    let after = Instant::now();

    let deadline = leader.stale_check_deadline.expect("Deadline must be set after first enqueue");
    assert!(
        deadline >= before + threshold && deadline <= after + threshold,
        "Deadline should be approximately ready_since + threshold"
    );
}

// ============================================================================
// Test 3: Deadline NOT overwritten when second learner enqueued
// (VecDeque is FIFO — front is always the oldest, so min() == front)
// ============================================================================

#[tokio::test]
async fn test_stale_check_deadline_unchanged_on_second_enqueue() {
    let threshold = Duration::from_secs(30);
    let (mut leader, _ctx, _role_tx, _raft_tx) = setup_with_threshold(threshold).await;

    // Enqueue first learner
    let t0 = Instant::now();
    leader.pending_promotions.push_back(PendingPromotion::new(10, t0));
    leader.refresh_stale_deadline(threshold);
    let first_deadline = leader.stale_check_deadline.unwrap();

    // Enqueue second learner slightly later
    tokio::time::sleep(Duration::from_millis(10)).await;
    leader.pending_promotions.push_back(PendingPromotion::new(11, Instant::now()));
    leader.refresh_stale_deadline(threshold);

    assert_eq!(
        leader.stale_check_deadline.unwrap(),
        first_deadline,
        "Deadline must not change when second learner is enqueued — front is still oldest"
    );
}

// ============================================================================
// Test 4: tick() skips stale check when deadline has not fired
// ============================================================================

#[tokio::test]
async fn test_tick_skips_stale_check_before_deadline() {
    let threshold = Duration::from_secs(3600); // Far future
    let (mut leader, mut ctx, role_tx, raft_tx) = setup_with_threshold(threshold).await;

    let mut membership = MockMembership::new();
    // get_node_status must NOT be called — stale check should be skipped
    membership.expect_get_node_status().times(0).returning(|_| None);
    ctx.membership = Arc::new(membership);

    // Enqueue a learner and set a far-future deadline
    leader.pending_promotions.push_back(PendingPromotion::new(10, Instant::now()));
    leader.refresh_stale_deadline(threshold);

    // Call tick() — deadline is far in the future, stale check must be skipped
    let result = leader.tick(&role_tx, &raft_tx, &ctx).await;
    assert!(result.is_ok());
    // If MockMembership::get_node_status were called, mockall would panic — test passes by not panicking
}

// ============================================================================
// Test 5: tick() triggers stale learner removal when deadline fires
// ============================================================================

#[tokio::test]
async fn test_tick_removes_stale_learner_when_deadline_fires() {
    use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;

    let threshold = Duration::from_millis(1); // Expires almost immediately
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut cfg = node_config("/tmp/test_tick_removes_stale_learner");
    cfg.raft.membership.promotion.stale_learner_threshold = threshold;
    cfg.raft.membership.verify_leadership_persistent_timeout = Duration::from_millis(50);
    let mut ctx = MockBuilder::new(shutdown_rx).with_node_config(cfg).build_context();

    let mut membership = MockMembership::new();
    // Learner 10 has status Promotable — qualifies as stale
    membership.expect_get_node_status().returning(|_| Some(NodeStatus::Promotable));
    membership.expect_voters().returning(|| {
        vec![NodeMeta {
            id: 1,
            address: "addr_1".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active as i32,
        }]
    });
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    ctx.membership = Arc::new(membership);

    let captured = Arc::new(Mutex::new(Vec::<EntryPayload>::new()));
    let captured_clone = captured.clone();
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(..)
        .returning(move |payloads, _, _, _, _| {
            captured_clone.lock().extend(payloads.clone());
            Ok(crate::PrepareResult::default())
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    ctx.storage.raft_log = Arc::new(raft_log);

    let node_config = ctx.node_config();
    let mut leader = LeaderState::<MockTypeConfig>::new(1, node_config);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel(1);

    // Enqueue learner with a ready_since in the past so it's already stale
    let past = Instant::now() - Duration::from_secs(1);
    leader.pending_promotions.push_back(PendingPromotion::new(10, past));
    // Deadline = past + 1ms — already fired
    leader.stale_check_deadline = Some(past + threshold);

    let result = leader.tick(&role_tx, &raft_tx, &ctx).await;
    assert!(result.is_ok());

    // Verify a config change (BatchRemove or RemoveNode) was proposed for node 10
    let payloads = captured.lock();
    assert!(
        !payloads.is_empty(),
        "tick() must propose removal of stale learner"
    );
}

// ============================================================================
// Test 6: drain_batch clears deadline when queue becomes empty
// ============================================================================

#[tokio::test]
async fn test_drain_batch_clears_deadline_when_queue_empty() {
    let threshold = Duration::from_secs(30);
    let (mut leader, _ctx, _role_tx, _raft_tx) = setup_with_threshold(threshold).await;

    leader.pending_promotions.push_back(PendingPromotion::new(10, Instant::now()));
    leader.refresh_stale_deadline(threshold);
    assert!(leader.stale_check_deadline.is_some());

    // Drain the only entry
    let _batch = leader.drain_batch(1);
    leader.refresh_stale_deadline(threshold);

    assert!(
        leader.stale_check_deadline.is_none(),
        "Deadline must reset to None when queue is drained to empty"
    );
}

// ============================================================================
// Test 7: drain_batch refreshes deadline to new front when items remain
// ============================================================================

#[tokio::test]
async fn test_drain_batch_refreshes_deadline_to_new_front() {
    let threshold = Duration::from_secs(30);
    let (mut leader, _ctx, _role_tx, _raft_tx) = setup_with_threshold(threshold).await;

    let t0 = Instant::now() - Duration::from_secs(5); // older
    let t1 = Instant::now() - Duration::from_secs(2); // newer (becomes new front after drain)

    leader.pending_promotions.push_back(PendingPromotion::new(10, t0));
    leader.pending_promotions.push_back(PendingPromotion::new(11, t1));
    leader.refresh_stale_deadline(threshold);

    let initial_deadline = leader.stale_check_deadline.unwrap();
    // Should be t0 + threshold (oldest front)
    assert!(
        (initial_deadline - threshold).saturating_duration_since(t0) < Duration::from_millis(50),
        "Initial deadline should correspond to t0"
    );

    // Drain only the first (t0 node)
    let _batch = leader.drain_batch(1);
    leader.refresh_stale_deadline(threshold);

    let new_deadline = leader.stale_check_deadline.unwrap();
    // Should now be t1 + threshold (new front)
    assert!(
        (new_deadline - threshold).saturating_duration_since(t1) < Duration::from_millis(50),
        "After drain, deadline should correspond to new front t1"
    );
    assert_ne!(
        new_deadline, initial_deadline,
        "Deadline must change after drain to reflect new front"
    );
}
