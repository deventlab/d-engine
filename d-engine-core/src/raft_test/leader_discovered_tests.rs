//! # InternalEvent Handling Tests - Leader Discovery Category
//!
//! **Original location**: raft_test.rs L379-514
//! **Test count**: 5 tests
//! **Category**: B6 (LeaderDiscovered Event) + E1 extension
//!
//! Verifies that LeaderDiscovered events are handled correctly.
//! These events notify listeners of leader discovery without triggering role transitions.
//!
//! **Key Focus**:
//! - LeaderDiscovered event triggers notifications but no state change
//! - Multiple listeners receive the event
//! - Event deduplication and watch channel behavior
//! - InternalEvent::LeaderDiscovered creation and serialization

use tokio::sync::watch;

use crate::InternalEvent;
use crate::Raft;
use crate::test_utils::MockBuilder;
use crate::test_utils::MockTypeConfig;

#[tokio::test]
async fn test_leader_discovered_event_handling() {
    // Test that LeaderDiscovered event triggers leader change notification
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

    // Register leader change listener
    let (leader_tx, mut leader_rx) = watch::channel(None);
    raft.register_leader_change_listener(leader_tx);

    // Send LeaderDiscovered event
    let leader_id = 3;
    let term = 5;
    raft.handle_internal_event(InternalEvent::LeaderDiscovered(leader_id, term))
        .await
        .expect("Should handle LeaderDiscovered");

    // Verify notification was sent
    leader_rx.changed().await.expect("Should receive change notification");
    let leader_info = *leader_rx.borrow();
    assert!(leader_info.is_some());
    let info = leader_info.unwrap();
    assert_eq!(info.leader_id, leader_id);
    assert_eq!(info.term, term);
}

#[tokio::test]
async fn test_leader_discovered_no_state_change() {
    // Test that LeaderDiscovered does NOT change node role
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

    let initial_role = raft.role.as_i32();

    // Send LeaderDiscovered event
    raft.handle_internal_event(InternalEvent::LeaderDiscovered(3, 5))
        .await
        .expect("Should handle LeaderDiscovered");

    // Verify role unchanged (still Follower)
    assert_eq!(raft.role.as_i32(), initial_role);
}

#[tokio::test]
async fn test_leader_discovered_multiple_listeners() {
    // Test that multiple subscribers can receive notifications via watch::Sender::subscribe()
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

    // Register leader change listener
    let (tx, _rx) = watch::channel(None);
    raft.register_leader_change_listener(tx.clone());

    // Create multiple subscribers
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();

    // Send LeaderDiscovered event
    let leader_id = 2;
    let term = 10;
    raft.handle_internal_event(InternalEvent::LeaderDiscovered(leader_id, term))
        .await
        .expect("Should handle LeaderDiscovered");

    // Verify all subscribers receive notification
    rx1.changed().await.expect("Subscriber 1 should receive");
    rx2.changed().await.expect("Subscriber 2 should receive");

    let info1 = (*rx1.borrow()).unwrap();
    let info2 = (*rx2.borrow()).unwrap();

    assert_eq!(info1.leader_id, leader_id);
    assert_eq!(info1.term, term);
    assert_eq!(info2.leader_id, leader_id);
    assert_eq!(info2.term, term);
}

#[tokio::test]
async fn test_leader_discovered_with_deduplication() {
    // Test that watch channel automatically deduplicates identical notifications
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

    let (leader_tx, mut leader_rx) = watch::channel(None);
    raft.register_leader_change_listener(leader_tx);

    // Send same leader multiple times
    raft.handle_internal_event(InternalEvent::LeaderDiscovered(2, 5))
        .await
        .expect("Should handle first");
    raft.handle_internal_event(InternalEvent::LeaderDiscovered(2, 5))
        .await
        .expect("Should handle second (duplicate)");

    // Should receive only one notification (watch channel deduplicates)
    leader_rx.changed().await.expect("Should receive first change");
    let info = (*leader_rx.borrow()).unwrap();
    assert_eq!(info.leader_id, 2);
    assert_eq!(info.term, 5);

    // No second change notification because value is identical
    tokio::select! {
        _ = leader_rx.changed() => {
            panic!("Should not receive duplicate notification");
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
            // Expected: timeout because no new change
        }
    }
}

#[test]
fn test_internal_event_leader_discovered_creation() {
    // Test creating LeaderDiscovered event
    let leader_id = 5;
    let term = 20;
    let event = InternalEvent::LeaderDiscovered(leader_id, term);

    // Verify we can match on it
    match event {
        InternalEvent::LeaderDiscovered(id, t) => {
            assert_eq!(id, leader_id);
            assert_eq!(t, term);
        }
        _ => panic!("Should be LeaderDiscovered variant"),
    }
}

/// Test: the central loop records how long an `ApplyCompleted` event waited before it was
/// handled (`core.raft.central_loop.apply_completed_queue_wait_ms`).
///
/// Why: the apply result is only turned into a client response when the single-threaded
/// loop gets to it; if the loop is busy that wait is part of commit->apply latency and is
/// otherwise invisible.
#[tokio::test]
async fn test_apply_completed_records_central_loop_queue_wait() {
    let capture = crate::test_utils::MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

    raft.handle_internal_event(InternalEvent::ApplyCompleted {
        last_index: 0,
        results: vec![],
        sent_at: tokio::time::Instant::now() - std::time::Duration::from_millis(25),
    })
    .await
    .expect("ApplyCompleted must be handled");

    let waits = capture.histogram("core.raft.central_loop.apply_completed_queue_wait_ms", &[]);
    assert_eq!(waits.len(), 1);
    assert!(
        waits[0] >= 25.0,
        "wait must cover time since send, got {}",
        waits[0]
    );
}

/// Runs the Raft loop's event handling to a standstill: internal events (role changes, replayed
/// requests, notifications) and buffered inbound requests, until nothing is left to do.
async fn run_events_until_quiet(raft: &mut Raft<MockTypeConfig>) {
    loop {
        let mut progressed = false;
        while let Ok(event) = raft.internal_event_rx.try_recv() {
            raft.handle_internal_event(event).await.unwrap();
            progressed = true;
        }
        if !raft.buffered_inbound_event.is_empty() {
            raft.process_inbound_events().await.unwrap();
            progressed = true;
        }
        if !progressed {
            return;
        }
    }
}

/// A restarted node that campaigns before its leader reaches it must still announce that leader
/// once the leader's AppendEntries arrives.
///
/// Scenario (#400): node 1 voted for leader 3 in term 3 and the vote was confirmed on disk
/// before node 1 went down. After the restart its election timer fires first, so it becomes a
/// candidate in the same term 3 (its PreVote is denied, so the term does not move). Leader 3's
/// AppendEntries then arrives: node 1 steps down to follower and replays the request. The
/// leader-change listener must end up on leader 3. It used to stay on "no leader" for the rest
/// of the term, so `wait_ready()` timed out although replication worked.
#[tokio::test]
async fn test_restarted_candidate_stepping_down_to_its_leader_announces_it() {
    use std::sync::Arc;

    use d_engine_proto::common::NodeRole;
    use d_engine_proto::server::election::VotedFor;
    use d_engine_proto::server::replication::AppendEntriesRequest;
    use d_engine_proto::server::replication::AppendEntriesResponse;

    use crate::AppendResponseWithUpdates;
    use crate::HardState;
    use crate::InboundEvent;
    use crate::MockReplicationCore;
    use crate::RaftRole;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::maybe_clone_oneshot::RaftOneshot;
    use crate::raft_role::follower_state::FollowerState;
    use crate::test_utils::node_config;

    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Node 1 as it comes back from disk: term 3, vote for leader 3 already confirmed.
    let restored = HardState {
        current_term: 3,
        voted_for: Some(VotedFor {
            voted_for_id: 3,
            voted_for_term: 3,
            committed: true,
        }),
    };
    let follower = FollowerState::<MockTypeConfig>::new(
        1,
        Arc::new(node_config(
            "/tmp/test_restarted_candidate_announces_leader",
        )),
        Some(restored),
        None,
    );

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .times(1)
        .returning(|_, _, _| {
            Ok(AppendResponseWithUpdates {
                response: AppendEntriesResponse::success(1, 3, None),
                commit_index_update: None,
            })
        });
    let mut raft = MockBuilder::new(graceful_rx)
        .id(1)
        .with_role(RaftRole::Follower(Box::new(follower)))
        .with_replication_handler(replication_handler)
        .build_raft();
    let (leader_tx, leader_rx) = watch::channel(None);
    raft.register_leader_change_listener(leader_tx);

    // The election timer fires before leader 3 reaches the node.
    raft.handle_internal_event(InternalEvent::BecomeCandidate).await.unwrap();
    assert_eq!(
        raft.role.as_i32(),
        NodeRole::Candidate as i32,
        "precondition: node 1 is a candidate"
    );

    // Leader 3's heartbeat arrives (same term).
    let (tx, _rx) = MaybeCloneOneshot::new();
    raft.buffered_inbound_event.push_back(InboundEvent::AppendEntries(
        AppendEntriesRequest {
            term: 3,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 0,
        },
        vec![tx],
    ));
    run_events_until_quiet(&mut raft).await;

    assert_eq!(
        raft.role.as_i32(),
        NodeRole::Follower as i32,
        "the candidate must have stepped down"
    );
    let announced = leader_rx.borrow().map(|info| (info.leader_id, info.term));
    assert_eq!(
        announced,
        Some((3, 3)),
        "after stepping down to leader 3 the node must announce it"
    );
}
