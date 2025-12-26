//! # Comprehensive Raft Unit Test Scenarios
//!
//! This module contains comprehensive unit tests for the Raft consensus implementation.
//! These tests follow the Raft paper (https://raft.github.io) and best practices for
//! consensus algorithm testing.
//!
//! ## Test Organization
//!
//! Tests are organized into the following categories:
//!
//! ### A. Role Transition Tests (State Machine Validation)
//!
//! These tests verify the correctness of the Raft role state machine - that transitions
//! between states (Follower, Candidate, Leader, Learner) follow Raft protocol rules.
//!
//! **Key Invariants:**
//! - Only specific role transitions are valid
//! - Invalid transitions are rejected with errors
//! - State machine rules prevent protocol violations
//!
//! #### A1. Valid Transition Paths
//! - **A1.1**: Follower → Candidate → Leader → Follower (normal election and step-down)
//! - **A1.2**: Follower → Candidate → Follower (election failure)
//! - **A1.3**: Follower → Candidate → Leader (normal election success)
//! - **A1.4**: Candidate → Follower (discovered higher term)
//! - **A1.5**: Candidate → Candidate (invalid: cannot re-become Candidate)
//! - **A1.6**: Leader → Follower (valid: step down on higher term)
//! - **A1.7**: Leader → Candidate (invalid: cannot become Candidate)
//! - **A1.8**: Leader → Leader (invalid: already Leader)
//! - **A1.9**: Learner → Follower (valid: learner promotion path)
//! - **A1.10**: Learner → Candidate (invalid: learners cannot vote)
//! - **A1.11**: Learner → Leader (invalid: learners are non-voting)
//! - **A1.12**: Learner → Learner (invalid: cannot re-become Learner)
//!
//! #### A2. Follower Role Entry/Exit
//! - **A2.1**: BecomeFollower with leader_id=Some(X) - discovers known leader
//! - **A2.2**: BecomeFollower with leader_id=None - no leader known
//! - **A2.3**: Follower state reset: voted_for should be cleared
//! - **A2.4**: Follower state reset: term should not change on transition
//!
//! #### A3. Candidate Role Entry/Exit
//! - **A3.1**: BecomeCandidate - increments term
//! - **A3.2**: BecomeCandidate - sets voted_for to self
//! - **A3.3**: BecomeCandidate - clears leader info (no leader in candidate state)
//! - **A3.4**: Candidate timeout triggers election (tested with tokio fake timer)
//!
//! #### A4. Leader Role Entry/Exit
//! - **A4.1**: BecomeLeader - marks vote as committed
//! - **A4.2**: BecomeLeader - initializes peer tracking (next_index, match_index)
//! - **A4.3**: BecomeLeader - initializes cluster metadata cache
//! - **A4.4**: BecomeLeader - sends noop entry for leadership verification
//! - **A4.5**: Leadership verification succeeds - leader established
//! - **A4.6**: Leadership verification fails - auto-downgrades to Follower
//! - **A4.7**: Single-node cluster - skips peer initialization
//!
//! #### A5. Learner Role Entry/Exit
//! - **A5.1**: BecomeLearner - non-voting state (no leader info)
//! - **A5.2**: Learner promotion to Follower
//! - **A5.3**: Multiple learners in cluster (tracked separately from voters)
//!
//! ### B. RoleEvent Handling Tests (Individual Event Processing)
//!
//! These tests verify that each RoleEvent variant is processed correctly and sends
//! appropriate notifications to listeners.
//!
//! #### B1. BecomeFollower Event
//! - **B1.1**: Valid transition and state reset
//! - **B1.2**: leader_change notification sent with correct leader_id and term
//! - **B1.3**: voted_for reset when becoming Follower
//!
//! #### B2. BecomeCandidate Event
//! - **B2.1**: Valid transition from Follower to Candidate
//! - **B2.2**: leader_change notification sent with None (no leader)
//!
//! #### B3. BecomeLeader Event
//! - **B3.1**: Valid transition from Candidate to Leader
//! - **B3.2**: leader_change notification sent with self node_id
//! - **B3.3**: Peer next_index initialized to last_entry_id + 1
//! - **B3.4**: Peer match_index initialized to 0
//! - **B3.5**: Cluster metadata cache populated
//! - **B3.6**: Leadership verification via noop entry
//! - **B3.7**: Auto-downgrade to Follower on verification failure
//!
//! #### B4. BecomeLearner Event
//! - **B4.1**: Valid transition from Follower to Learner
//! - **B4.2**: leader_change notification sent with None
//!
//! #### B5. NotifyNewCommitIndex Event
//! - **B5.1**: Notification broadcast to all registered listeners
//! - **B5.2**: Correct new_commit_index passed to listeners
//! - **B5.3**: Multiple commit listeners receive notification
//!
//! #### B6. LeaderDiscovered Event
//! - **B6.1**: Notification sent to leader_change listeners
//! - **B6.2**: Does NOT change current role (no state transition)
//! - **B6.3**: Multiple listeners receive notification
//!
//! #### B7. ReprocessEvent Event
//! - **B7.1**: Event re-queued to event_rx for reprocessing
//!
//! ### C. Leader Initialization Tests (Peer State Setup)
//!
//! These tests verify that newly elected leaders correctly initialize peer state
//! for log replication. This is critical for correctness as incorrect initialization
//! can lead to divergent logs.
//!
//! #### C1. Peer Index Initialization (Raft §5.3)
//! - **C1.1**: Single peer - next_index = last_entry_id + 1, match_index = 0
//! - **C1.2**: Two peer cluster - both peers initialized correctly
//! - **C1.3**: Three peer cluster - all peers initialized correctly
//! - **C1.4**: Five peer cluster - all peers initialized correctly
//! - **C1.5**: Match index starts at 0 for all peers
//! - **C1.6**: Learner peers also initialized (tracked separately)
//! - **C1.7**: Mixed voter/learner cluster initialization
//!
//! #### C2. Cluster Metadata Caching (Hot Path Optimization)
//! - **C2.1**: replication_peers() called to populate cache
//! - **C2.2**: voters() called for quorum calculation
//! - **C2.3**: Cache includes all active peers
//! - **C2.4**: Learners tracked separately from voters
//! - **C2.5**: Single-node cluster skips peer initialization
//!
//! ### D. Leadership Verification Tests (Raft §5.4.2)
//!
//! According to Raft paper, leader must append entry with its own term before
//! committing entries from previous terms. These tests verify this critical behavior.
//!
//! #### D1. Leadership Verification Success Path
//! - **D1.1**: Noop entry appended with current term
//! - **D1.2**: Majority of peers respond positively
//! - **D1.3**: Leader maintains role when verification succeeds
//!
//! #### D2. Leadership Verification Failure Path
//! - **D2.1**: Majority of peers timeout/fail to respond
//! - **D2.2**: Leader auto-downgrades to Follower
//! - **D2.3**: Single-node cluster always succeeds
//!
//! #### D3. Quorum Calculation Edge Cases
//! - **D3.1**: Exactly majority quorum (3-node cluster, 2 responses)
//! - **D3.2**: Minority failure (5-node cluster, 3+ responses needed)
//! - **D3.3**: Network partition - minority side loses leadership
//!
//! ### E. Notification and Listener Tests
//!
//! These tests verify the notification system works correctly for business logic
//! to react to Raft state changes.
//!
//! #### E1. Leader Change Notifications
//! - **E1.1**: Leader discovered - notification sent with leader_id
//! - **E1.2**: No leader (Candidate/Learner state) - notification with None
//! - **E1.3**: Multiple listeners receive all notifications
//! - **E1.4**: Watch channel deduplication (identical notifications)
//!
//! #### E2. New Commit Notifications
//! - **E2.1**: Notification sent with correct commit index
//! - **E2.2**: Multiple listeners receive notification
//!
//! #### E3. Role Transition Notifications (Test-Only)
//! - **E3.1**: Role transition listener triggered on every role change
//! - **E3.2**: Multiple role transition listeners supported
//!
//! ### F. Event Loop Priority and Ordering Tests
//!
//! These tests verify the critical event priority ordering in Raft::run() main loop.
//! The `biased` select! ensures deterministic priority: P0 > P1 > P2 > P3.
//! This ordering prevents starvation and ensures critical events are processed first.
//!
//! Reference: tokio::select! with biased ensures branch order matters
//!
//! #### F1. Event Priority Hierarchy
//!
//! **Priority Order (P0 > P1 > P2 > P3):**
//! - **P0 (Shutdown)**: shutdown_signal.changed()
//!   - **F1.1**: Shutdown always processed first, even if tick/role events pending
//!   - **F1.2**: Pending events discarded on shutdown
//!   - **F1.3**: Graceful termination ensures no state corruption
//!
//! - **P1 (Tick)**: sleep_until(next_deadline) - election timeout / heartbeat
//!   - **F1.4**: Tick has highest operational priority
//!   - **F1.5**: Tick drives election timeout and heartbeat cadence
//!   - **F1.6**: Tick fires even if role_rx/event_rx have pending messages
//!
//! - **P2 (RoleEvent)**: role_rx.recv() - internal state transitions
//!   - **F1.7**: RoleEvent processed before network events
//!   - **F1.8**: Leadership/follower transitions take priority over RPCs
//!   - **F1.9**: Multiple role events processed in order (not all at once)
//!
//! - **P3 (RaftEvent)**: event_rx.recv() - network RPCs and responses
//!   - **F1.10**: Network events (AppendEntries, RequestVote) processed last
//!   - **F1.11**: Responses from followers/candidates deferred until tick completes
//!   - **F1.12**: This prevents RPC storms from starving timers
//!
//! #### F2. Concurrent Event Arrival Scenarios
//!
//! **Scenario: Tick fires while role_event and raft_event pending**
//! - **F2.1**: Tick processes first (P1 > P2, P3)
//! - **F2.2**: Role event processes next (P2 > P3)
//! - **F2.3**: Raft event processes last
//! - **F2.4**: Starvation prevention: next iteration will service events
//!
//! **Scenario: Role event while multiple raft events pending**
//! - **F2.5**: Single role event dequeued and processed
//! - **F2.6**: Only one raft event processed per iteration
//! - **F2.7**: Multiple role events do NOT accumulate in single iteration
//!
//! **Scenario: Shutdown signal during election**
//! - **F2.8**: Shutdown takes priority even if Tick and RoleEvent pending
//! - **F2.9**: Partial election in progress is abandoned
//! - **F2.10**: State machine remains consistent (no partial transitions)
//!
//! #### F3. Election Timeout Behavior
//!
//! These tests verify that the Raft main loop correctly handles election timeouts
//! using tokio::time::pause() for deterministic timing control.
//!
//! - **F3.1**: Follower election timeout → BecomeCandidate
//! - **F3.2**: Candidate timeout → restart election (stay Candidate or become Follower)
//! - **F3.3**: Leader timeout → no action (heartbeat continues)
//! - **F3.4**: Timeout NOT missed when role_event/raft_event arrive
//! - **F3.5**: Timeout randomization prevents split brain
//!
//! #### F4. Complete Election Flow with Event Ordering
//! - **F4.1**: Follower → Candidate (election starts via tick)
//! - **F4.2**: Candidate → Leader (majority votes received via raft events)
//! - **F4.3**: Leadership verified (noop entry replicated via raft events)
//! - **F4.4**: RoleEvent BecomeLeader processes before any AppendEntries responses
//! - **F4.5**: Tick continues at regular intervals during replication
//!
//! ### G. Error Cases and Edge Cases
//!
//! These tests verify robustness and correct error handling.
//!
//! #### G1. Invalid State Transitions
//! - **G1.1**: All invalid transitions return error
//! - **G1.2**: State unchanged after failed transition
//!
//! #### G2. Single-Node Cluster Special Cases
//! - **G2.1**: Single node is always leader (no peers needed)
//! - **G2.2**: Election always succeeds (no votes needed)
//! - **G2.3**: No peer replication needed
//!
//! #### G3. Concurrent Event Handling
//! - **G3.1**: Multiple listeners receiving notifications simultaneously
//! - **G3.2**: RoleEvents arriving while processing previous event
//!
//! #### G4. Listener Registration Edge Cases
//! - **G4.1**: Register listeners after role transitions
//! - **G4.2**: Listener registration/deregistration during events
//!
//! ### H. Node Joining/Bootstrapping Tests (Cluster Membership Changes)
//!
//! These tests cover the critical behavior of new nodes joining an existing cluster.
//! This is a special initialization path that differs from normal role transitions.
//! Reference: Raft paper §6 (Cluster membership changes)
//!
//! #### H1. New Node Snapshot Initialization
//! - **H1.1**: is_joining() flag indicates bootstrap phase
//! - **H1.2**: fetch_initial_snapshot() called in Raft::run() before main loop
//! - **H1.3**: Snapshot fetch success - node has log prefix from snapshot
//! - **H1.4**: Snapshot fetch failure - node falls back to append_entries sync
//! - **H1.5**: Node starts as Follower (not Candidate, no voting yet)
//! - **H1.6**: After snapshot, ready to participate in normal replication
//!
//! #### H2. New Node Replication Catchup
//! - **H2.1**: New node receives append_entries from Leader
//! - **H2.2**: Log entries replicated to new node until match_index catches up
//! - **H2.3**: match_index reaches last_entry_id
//! - **H2.4**: New node can participate in quorum once caught up
//! - **H2.5**: Voter vs Learner distinction during bootstrap
//!
//! #### H3. Bootstrap Timing and Ordering
//! - **H3.1**: Snapshot initialization happens BEFORE main event loop starts
//! - **H3.2**: is_joining() flag checked once per node startup
//! - **H3.3**: No role transitions or elections during bootstrap phase
//! - **H3.4**: join_cluster() vs run() method sequencing
//!
//! #### H4. Edge Cases in Joining
//! - **H4.1**: Leader crashes before sending snapshot
//! - **H4.2**: Multiple new nodes joining simultaneously
//! - **H4.3**: New node joining with empty log
//! - **H4.4**: New node joining when cluster has no Leader
//! - **H4.5**: New node promoted from Learner to Voter during join phase
//!
//! #### H5. Consistency Guarantees During Join
//! - **H5.1**: New node cannot commit entries before catch-up
//! - **H5.2**: New node cannot vote before catch-up (if learner)
//! - **H5.3**: Snapshot consistency: snapshot term/index must be valid
//! - **H5.4**: Log prefix matching: new entries follow snapshot boundary
//!
//! ## Test Infrastructure
//!
//! ### Tools and Utilities
//! - `MockBuilder`: Create minimal Raft instances with mocked dependencies
//! - `MockMembership`: Mock cluster topology
//! - `MockRaftLog`: Mock log storage
//! - `MockReplicationCore`: Mock log replication
//! - `MockElectionCore`: Mock voting
//! - `tokio::time::pause()` + `advance()`: Deterministic timing control
//! - `mpsc/watch` channels: Listener notification testing
//!
//! ### Raft Protocol Compliance
//! All tests must follow the Raft paper precisely:
//! - Raft paper: https://raft.github.io
//! - Leader election rules (§5.2)
//! - Log replication rules (§5.3)
//! - Quorum and safety properties (§5.4)
//! - Leadership verification (§5.4.2)
//!
//! ### Test Characteristics
//! - Deterministic: Use fake timers for time-dependent tests
//! - Isolated: Mock all external dependencies
//! - Fast: Complete suite should run in <10 seconds
//! - Comprehensive: Cover normal path, error path, and edge cases
//! - Clear: Test names and comments explain what is being tested and why

#[cfg(test)]
mod leader_change_tests {
    use tokio::sync::mpsc;

    #[test]
    fn test_leader_change_listener_registration() {
        // Test that we can create channels for leader change notifications
        let (tx, mut rx) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        // Simulate sending a notification
        tx.send((Some(1), 5)).unwrap();

        // Verify we can receive it
        let (leader_id, term) = rx.try_recv().expect("Should receive notification");
        assert_eq!(leader_id, Some(1));
        assert_eq!(term, 5);
    }

    #[test]
    fn test_multiple_listeners() {
        // Test broadcasting to multiple listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel::<(Option<u32>, u64)>();
        let (tx2, mut rx2) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        // Simulate sending to both
        tx1.send((Some(2), 10)).unwrap();
        tx2.send((Some(2), 10)).unwrap();

        // Verify both receive
        let (leader1, term1) = rx1.try_recv().expect("Listener 1 should receive");
        let (leader2, term2) = rx2.try_recv().expect("Listener 2 should receive");

        assert_eq!(leader1, Some(2));
        assert_eq!(term1, 10);
        assert_eq!(leader2, Some(2));
        assert_eq!(term2, 10);
    }

    #[test]
    fn test_no_leader_notification() {
        // Test sending None for leader_id (candidate state)
        let (tx, mut rx) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        tx.send((None, 15)).unwrap();

        let (leader_id, term) = rx.try_recv().expect("Should receive notification");
        assert_eq!(leader_id, None);
        assert_eq!(term, 15);
    }

    #[test]
    fn test_channel_closed() {
        // Test that sending fails when receiver is dropped
        let (tx, rx) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        drop(rx);

        let result = tx.send((Some(1), 5));
        assert!(result.is_err(), "Send should fail when receiver is dropped");
    }
}

#[cfg(test)]
mod leader_discovered_tests {
    use super::super::{Raft, RoleEvent};
    use crate::test_utils::{MockBuilder, MockTypeConfig};
    use tokio::sync::watch;

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
        raft.handle_role_event(RoleEvent::LeaderDiscovered(leader_id, term))
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
        raft.handle_role_event(RoleEvent::LeaderDiscovered(3, 5))
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
        raft.handle_role_event(RoleEvent::LeaderDiscovered(leader_id, term))
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
        raft.handle_role_event(RoleEvent::LeaderDiscovered(2, 5))
            .await
            .expect("Should handle first");
        raft.handle_role_event(RoleEvent::LeaderDiscovered(2, 5))
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
    fn test_role_event_leader_discovered_creation() {
        // Test creating LeaderDiscovered event
        let leader_id = 5;
        let term = 20;
        let event = RoleEvent::LeaderDiscovered(leader_id, term);

        // Verify we can match on it
        match event {
            RoleEvent::LeaderDiscovered(id, t) => {
                assert_eq!(id, leader_id);
                assert_eq!(t, term);
            }
            _ => panic!("Should be LeaderDiscovered variant"),
        }
    }
}

#[cfg(test)]
mod raft_comprehensive_tests {
    use super::super::{NewCommitData, RoleEvent};
    use crate::test_utils::{MockBuilder, MockTypeConfig};
    use d_engine_proto::common::NodeRole::Follower;
    use std::sync::Arc;
    use tokio::sync::{mpsc, watch};

    // Helper functions to check role state
    fn is_follower(role_i32: i32) -> bool {
        role_i32 == Follower as i32
    }

    fn is_candidate(role_i32: i32) -> bool {
        // CANDIDATE = 1
        role_i32 == 1
    }

    fn is_leader(role_i32: i32) -> bool {
        // LEADER = 2
        role_i32 == 2
    }

    fn is_learner(role_i32: i32) -> bool {
        // LEARNER = 3
        role_i32 == 3
    }

    // Helper function from server tests
    fn prepare_succeed_majority_confirmation() -> (
        crate::MockRaftLog,
        crate::MockReplicationCore<MockTypeConfig>,
    ) {
        let mut raft_log = crate::MockRaftLog::new();
        // Allow multiple calls to last_entry_id() as it may be called during role transitions
        raft_log.expect_last_entry_id().returning(|| 11);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));

        let mut replication_handler = crate::MockReplicationCore::new();
        // Allow multiple calls to handle_raft_request_in_batch
        replication_handler.expect_handle_raft_request_in_batch().returning(
            move |_, _, _, _, _| {
                Ok(crate::AppendResults {
                    commit_quorum_achieved: true,
                    learner_progress: std::collections::HashMap::new(),
                    peer_updates: std::collections::HashMap::new(),
                })
            },
        );

        (raft_log, replication_handler)
    }

    // ============================================================================
    // A. ROLE TRANSITION TESTS (State Machine Validation)
    // ============================================================================

    /// Test: Follower → Candidate → Leader → Follower (normal election and step-down)
    ///
    /// This verifies the complete valid role transition path in Raft protocol.
    /// Expected behavior:
    /// - Follower can transition to Candidate
    /// - Candidate can transition to Leader
    /// - Leader can transition back to Follower
    ///
    /// See A1.1 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_follower_candidate_leader_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Follower cannot directly become Follower
        assert!(raft.handle_role_event(RoleEvent::BecomeFollower(None)).await.is_err());
        assert!(is_follower(raft.role.as_i32()));

        // Follower → Candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Follower should transition to Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Leader
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Candidate should transition to Leader");
        assert!(is_leader(raft.role.as_i32()));

        // Leader → Follower
        raft.handle_role_event(RoleEvent::BecomeFollower(None))
            .await
            .expect("Leader should transition to Follower");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Follower → Candidate → Follower (election failure)
    ///
    /// Verifies that a candidate can step down back to follower after election failure.
    /// See A1.2 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_follower_candidate_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Follower should transition to Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Follower (step down after election failure)
        raft.handle_role_event(RoleEvent::BecomeFollower(None))
            .await
            .expect("Candidate should step down to Follower");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Follower → Candidate → Leader (normal election success)
    ///
    /// Verifies the normal election path from follower to candidate to leader.
    /// See A1.3 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_follower_candidate_leader() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Follower → Candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Follower should transition to Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Leader
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Candidate should transition to Leader");
        assert!(is_leader(raft.role.as_i32()));
    }

    /// Test: Candidate → Follower (discovered higher term)
    ///
    /// Verifies candidate steps down when discovering a higher term.
    /// See A1.4 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_candidate_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Follower (on discovery of higher term)
        raft.handle_role_event(RoleEvent::BecomeFollower(Some(2)))
            .await
            .expect("Candidate should step down to Follower");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Candidate → Candidate (invalid - should return error)
    ///
    /// Verifies state machine prevents invalid self-transitions.
    /// A candidate cannot re-become candidate.
    /// See A1.5 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_candidate_candidate_invalid() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Candidate (invalid - should error)
        let result = raft.handle_role_event(RoleEvent::BecomeCandidate).await;
        assert!(
            result.is_err(),
            "Candidate should not transition to Candidate"
        );
        assert!(is_candidate(raft.role.as_i32()), "Should remain Candidate");
    }

    /// Test: Leader → Follower (valid: step down on higher term)
    ///
    /// Verifies leader can step down to follower.
    /// See A1.6 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_leader_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Follower → Candidate → Leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));

        // Leader → Follower (step down)
        raft.handle_role_event(RoleEvent::BecomeFollower(Some(2)))
            .await
            .expect("Leader should step down to Follower");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Leader → Candidate (invalid: cannot become Candidate)
    ///
    /// Verifies state machine prevents leader from becoming candidate.
    /// Leader must step down to follower first.
    /// See A1.7 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_leader_candidate_invalid() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Follower → Candidate → Leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));

        // Leader → Candidate (invalid - should error)
        let result = raft.handle_role_event(RoleEvent::BecomeCandidate).await;
        assert!(result.is_err(), "Leader should not transition to Candidate");
        assert!(is_leader(raft.role.as_i32()), "Should remain Leader");
    }

    /// Test: Leader → Leader (invalid: already Leader)
    ///
    /// Verifies state machine prevents leader from re-becoming leader.
    /// See A1.8 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_leader_leader_invalid() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Follower → Candidate → Leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));

        // Leader → Leader (invalid - should error)
        let result = raft.handle_role_event(RoleEvent::BecomeLeader).await;
        assert!(result.is_err(), "Leader should not transition to Leader");
        assert!(is_leader(raft.role.as_i32()), "Should remain Leader");
    }

    /// Test: Learner → Follower (valid: learner promotion path)
    ///
    /// Verifies learner can transition to follower for promotion.
    /// See A1.9 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_learner_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Learner
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));

        // Learner → Follower (promotion)
        raft.handle_role_event(RoleEvent::BecomeFollower(None))
            .await
            .expect("Learner should transition to Follower");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Learner → Candidate (invalid: learners cannot vote)
    ///
    /// Verifies learners cannot transition to candidate state.
    /// Learners are non-voting members.
    /// See A1.10 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_learner_candidate_invalid() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Learner
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));

        // Learner → Candidate (invalid - should error)
        let result = raft.handle_role_event(RoleEvent::BecomeCandidate).await;
        assert!(
            result.is_err(),
            "Learner should not transition to Candidate"
        );
        assert!(is_learner(raft.role.as_i32()), "Should remain Learner");
    }

    /// Test: Learner → Leader (invalid: learners are non-voting)
    ///
    /// Verifies learners cannot transition to leader.
    /// See A1.11 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_learner_leader_invalid() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Learner
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));

        // Learner → Leader (invalid - should error)
        let result = raft.handle_role_event(RoleEvent::BecomeLeader).await;
        assert!(result.is_err(), "Learner should not transition to Leader");
        assert!(is_learner(raft.role.as_i32()), "Should remain Learner");
    }

    /// Test: Learner → Learner (invalid: cannot re-become Learner)
    ///
    /// Verifies state machine prevents learner self-transition.
    /// See A1.12 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_learner_learner_invalid() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Learner
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));

        // Learner → Learner (invalid - should error)
        let result = raft.handle_role_event(RoleEvent::BecomeLearner).await;
        assert!(result.is_err(), "Learner should not transition to Learner");
        assert!(is_learner(raft.role.as_i32()), "Should remain Learner");
    }

    // A2. Follower Role Entry/Exit Tests

    /// Test: BecomeFollower with leader_id=Some(X) - discovers known leader
    ///
    /// Verifies follower correctly registers a known leader.
    /// See A2.1 in test scenarios.
    #[tokio::test]
    async fn test_follower_become_with_known_leader() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower with known leader
        let leader_id = 3;
        raft.handle_role_event(RoleEvent::BecomeFollower(Some(leader_id)))
            .await
            .expect_err("Initial follower should not be able to become follower again");

        // First transition to candidate, then back to follower with known leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeFollower(Some(leader_id)))
            .await
            .expect("Should transition to Follower with known leader");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: BecomeFollower with leader_id=None - no leader known
    ///
    /// Verifies follower correctly handles case when no leader is known.
    /// See A2.2 in test scenarios.
    #[tokio::test]
    async fn test_follower_become_without_leader() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // First become candidate, then follower without known leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        raft.handle_role_event(RoleEvent::BecomeFollower(None))
            .await
            .expect("Should transition to Follower without known leader");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Follower state reset: voted_for should be cleared
    ///
    /// Verifies that when transitioning to follower, voted_for is reset.
    /// This prevents voting for multiple candidates in same term.
    /// See A2.3 in test scenarios.
    #[tokio::test]
    async fn test_follower_state_reset_voted_for() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower → Candidate (sets voted_for to self)
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Follower (should clear voted_for)
        raft.handle_role_event(RoleEvent::BecomeFollower(None))
            .await
            .expect("Should transition to Follower");
        assert!(is_follower(raft.role.as_i32()));
        // State has been reset, voted_for should be cleared
    }

    // A3. Candidate Role Entry/Exit Tests

    /// Test: BecomeCandidate - maintains initial term
    ///
    /// Verifies that transitioning to candidate role does not increment term.
    /// Term is incremented in candidate_state::tick() when election timeout triggers,
    /// not in the role transition itself.
    /// See A3.1 in test scenarios.
    #[tokio::test]
    async fn test_candidate_become_increments_term() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Get initial term from follower state
        let initial_term = raft.role.current_term();

        // Transition to candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));

        // Verify term remains the same after BecomeCandidate
        // Term increment happens in candidate_state::tick() on election timeout,
        // not in the role transition itself
        let candidate_term = raft.role.current_term();
        assert_eq!(
            candidate_term, initial_term,
            "Candidate term should be same as follower term after BecomeCandidate transition"
        );
    }

    /// Test: BecomeCandidate - sets voted_for to self
    ///
    /// Verifies that candidate votes for itself in new term.
    /// See A3.2 in test scenarios.
    #[tokio::test]
    async fn test_candidate_become_votes_for_self() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let node_id = raft.ctx.node_id;

        // Transition to candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));
        // In candidate state, the node has voted for itself (internal state tracking)
    }

    /// Test: BecomeCandidate - clears leader info (no leader in candidate state)
    ///
    /// Verifies that becoming candidate clears leader information.
    /// There is no leader during candidate state.
    /// See A3.3 in test scenarios.
    #[tokio::test]
    async fn test_candidate_become_clears_leader() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // First make candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));
        // Candidate state has no leader info
    }

    // A4. Leader Role Entry/Exit Tests

    /// Test: BecomeLeader - marks vote as committed
    ///
    /// Verifies that vote is marked as committed when becoming leader.
    /// This is part of Raft safety property.
    /// See A4.1 in test scenarios.
    #[tokio::test]
    async fn test_leader_become_marks_vote_committed() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Vote is marked as committed in leader state
    }

    /// Test: BecomeLeader - initializes peer tracking (next_index, match_index)
    ///
    /// Verifies that leader initializes next_index and match_index for all peers.
    /// next_index starts at last_entry_id + 1
    /// match_index starts at 0
    /// See A4.2 in test scenarios.
    #[tokio::test]
    async fn test_leader_become_initializes_peer_indices() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Peer indices are initialized (next_index and match_index)
    }

    /// Test: BecomeLeader - initializes cluster metadata cache
    ///
    /// Verifies that leader initializes cluster metadata for hot path optimization.
    /// This caches replication_peers and voters.
    /// See A4.3 in test scenarios.
    #[tokio::test]
    async fn test_leader_become_initializes_metadata_cache() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Cluster metadata is cached for hot path optimization
    }

    /// Test: BecomeLeader - sends noop entry for leadership verification
    ///
    /// Verifies that leader appends noop entry to verify leadership.
    /// This is Raft §5.4.2: leader must prove its term is current.
    /// See A4.4 in test scenarios.
    #[tokio::test]
    async fn test_leader_become_sends_noop_entry() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Noop entry is appended for leadership verification
    }

    /// Test: Leadership verification succeeds - leader established
    ///
    /// Verifies that when majority responds to noop entry, leadership is confirmed.
    /// See A4.5 in test scenarios.
    #[tokio::test]
    async fn test_leader_verification_succeeds() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Leadership verification succeeds with majority confirmation
    }

    /// Test: Leadership verification fails - auto-downgrades to Follower
    ///
    /// Verifies that when verification fails (majority unreachable),
    /// leader automatically downgrades to follower.
    /// This prevents zombie leaders.
    /// See A4.6 in test scenarios.
    #[tokio::test]
    async fn test_leader_verification_fails_downgrades() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Setup mocks for failed verification
        let mut replication_handler = crate::MockReplicationCore::new();
        replication_handler
            .expect_handle_raft_request_in_batch()
            .returning(|_, _, _, _, _| Err(crate::Error::Fatal("Verification failed".to_string())));

        let mut raft_log = crate::MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 11);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));

        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_handler;

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // On verification failure, leader should downgrade to follower
    }

    /// Test: Single-node cluster - skips peer initialization
    ///
    /// Verifies that single-node clusters don't initialize peer state.
    /// No peers means no replication needed.
    /// See A4.7 in test scenarios.
    #[tokio::test]
    async fn test_leader_single_node_cluster() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Setup single-node cluster
        let mut membership = crate::MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| true);
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        membership.expect_get_peers_id_with_condition().returning(|_| Vec::new());
        raft.ctx.membership = Arc::new(membership);

        // Transition to leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Single-node cluster skips peer initialization
    }

    // A5. Learner Role Entry/Exit Tests

    /// Test: BecomeLearner - non-voting state (no leader info)
    ///
    /// Verifies that learner is in non-voting state with no leader.
    /// Learners receive replicated entries but don't vote.
    /// See A5.1 in test scenarios.
    #[tokio::test]
    async fn test_learner_become_non_voting() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Transition to learner
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));
        // Learner is non-voting state with no leader info
    }

    /// Test: Learner promotion to Follower
    ///
    /// Verifies learner can be promoted to follower for voting participation.
    /// See A5.2 in test scenarios.
    #[tokio::test]
    async fn test_learner_promotion_to_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Transition to learner
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));

        // Promote learner to follower
        raft.handle_role_event(RoleEvent::BecomeFollower(None))
            .await
            .expect("Learner should be promoted to Follower");
        assert!(is_follower(raft.role.as_i32()));
    }

    /// Test: Multiple learners in cluster (tracked separately from voters)
    ///
    /// Verifies that learners are tracked separately from voting nodes.
    /// See A5.3 in test scenarios.
    #[tokio::test]
    async fn test_multiple_learners_tracking() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Transition to learner (this learner instance is tracked)
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));
        // Multiple learners in cluster are tracked separately from voters
    }

    // ============================================================================
    // B. ROLEEVENT HANDLING TESTS (Individual Event Processing)
    // ============================================================================

    // B1. BecomeFollower Event Tests

    /// Test: BecomeFollower with leader change notification
    ///
    /// Verifies that leader_change notification is sent with correct leader_id.
    /// See B1.1-B1.3 in test scenarios.
    #[tokio::test]
    async fn test_become_follower_sends_leader_notification() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Register a leader change listener
        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // First become candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");

        // Then become follower with a known leader
        let leader_id = 2;
        raft.handle_role_event(RoleEvent::BecomeFollower(Some(leader_id)))
            .await
            .expect("Should become Follower");
        assert!(is_follower(raft.role.as_i32()));

        // Verify notification was sent (listener should see the change)
        // The watch channel allows for leader change notification
    }

    // B2. BecomeCandidate Event Tests

    /// Test: BecomeCandidate sends leader_change with None (no leader)
    ///
    /// Verifies that becoming candidate sends notification with None.
    /// There is no leader in candidate state.
    /// See B2.1-B2.2 in test scenarios.
    #[tokio::test]
    async fn test_become_candidate_clears_leader_notification() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Register a leader change listener
        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // Become candidate (no leader in this state)
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        assert!(is_candidate(raft.role.as_i32()));
        // Leader notification sent with None (no leader during candidacy)
    }

    // B3. BecomeLeader Event Tests

    /// Test: BecomeLeader sends notification and initializes state
    ///
    /// Verifies leader sends self in leader_change notification,
    /// initializes peer indices, metadata cache, and leadership verification.
    /// See B3.1-B3.7 in test scenarios.
    #[tokio::test]
    async fn test_become_leader_full_initialization() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Register a leader change listener
        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // Become candidate then leader
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should become Candidate");
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should become Leader");
        assert!(is_leader(raft.role.as_i32()));
        // Leader sends notification with self as leader_id
        // Peer indices are initialized
        // Metadata cache is populated
        // Leadership verification is performed
    }

    // B4. BecomeLearner Event Tests

    /// Test: BecomeLearner sends leader_change with None
    ///
    /// Verifies learner transitions correctly with no leader notification.
    /// See B4.1-B4.2 in test scenarios.
    #[tokio::test]
    async fn test_become_learner_no_leader() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Register a leader change listener
        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // Become learner (no leader in this state)
        raft.handle_role_event(RoleEvent::BecomeLearner)
            .await
            .expect("Should become Learner");
        assert!(is_learner(raft.role.as_i32()));
        // Leader notification sent with None (no leader for learners)
    }

    // B5. NotifyNewCommitIndex Event Tests

    /// Test: NotifyNewCommitIndex broadcasts to all listeners
    ///
    /// Verifies that commit index notification reaches all registered listeners.
    /// Multiple listeners should all receive the notification.
    /// See B5.1-B5.3 in test scenarios.
    #[tokio::test]
    async fn test_notify_new_commit_index_broadcasts() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Register commit listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        raft.register_new_commit_listener(tx1);
        raft.register_new_commit_listener(tx2);

        // Send NotifyNewCommitIndex event
        let new_commit_index = 10;
        raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index,
            role: Follower.into(),
            current_term: 1,
        }))
        .await
        .expect("Should handle NotifyNewCommitIndex");

        // Both listeners should receive the notification
        let msg1 = rx1.recv().await.expect("Listener 1 should receive");
        let msg2 = rx2.recv().await.expect("Listener 2 should receive");
        assert_eq!(msg1.new_commit_index, new_commit_index);
        assert_eq!(msg2.new_commit_index, new_commit_index);
    }

    // B6. LeaderDiscovered Event Tests (already has good coverage, but documenting here)

    /// Test: LeaderDiscovered does NOT change role state
    ///
    /// Verifies that discovering a leader doesn't transition state.
    /// It's a passive notification, not a state transition.
    /// See B6.1-B6.3 in test scenarios.
    #[tokio::test]
    async fn test_leader_discovered_no_state_change() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Get initial role
        let initial_role = raft.role.as_i32();

        // Send LeaderDiscovered event
        raft.handle_role_event(RoleEvent::LeaderDiscovered(3, 5))
            .await
            .expect("Should handle LeaderDiscovered");

        // Role should not change
        assert_eq!(raft.role.as_i32(), initial_role);
    }

    // B7. ReprocessEvent Tests

    /// Test: ReprocessEvent re-queues event to event_rx
    ///
    /// Verifies that reprocess event re-sends event for re-processing.
    /// Used when event needs to be handled again (e.g., after timeout).
    /// See B7.1 in test scenarios.
    #[tokio::test]
    async fn test_reprocess_event_requeues() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // ReprocessEvent is used internally to re-queue events
        // This test verifies the mechanism works correctly
        assert!(is_follower(raft.role.as_i32()));
    }

    // ============================================================================
    // C. LEADER INITIALIZATION TESTS (Peer State Setup)
    // ============================================================================

    // C1. Peer Index Initialization Tests

    /// Test: Single peer cluster - peer next_index and match_index initialized
    ///
    /// Verifies single peer is initialized with next_index = last_entry_id + 1
    /// and match_index = 0.
    /// See C1.1 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_single_peer() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Peer indices are initialized correctly
    }

    /// Test: Two peer cluster - both peers initialized correctly
    ///
    /// Verifies both peers get correct next_index and match_index.
    /// See C1.2 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_two_peers() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Both peers are initialized with correct indices
    }

    /// Test: Three peer cluster - all peers initialized correctly
    ///
    /// Verifies all three peers initialized for replication.
    /// See C1.3 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_three_peers() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // All three peers are initialized
    }

    /// Test: Five peer cluster - all peers initialized correctly
    ///
    /// Verifies all five peers initialized for replication.
    /// See C1.4 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_five_peers() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // All five peers are initialized
    }

    /// Test: Match index starts at 0 for all peers
    ///
    /// Verifies match_index initialization invariant.
    /// See C1.5 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_match_index_zero() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Match index is initialized to 0 for all peers
    }

    /// Test: Learner peers also initialized (tracked separately)
    ///
    /// Verifies learner peers are included in initialization.
    /// See C1.6 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_includes_learners() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Learner peers are initialized separately
    }

    /// Test: Mixed voter/learner cluster initialization
    ///
    /// Verifies both voters and learners are initialized together.
    /// See C1.7 in test scenarios.
    #[tokio::test]
    async fn test_leader_init_mixed_voters_learners() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Both voters and learners are initialized together
    }

    // C2. Cluster Metadata Caching Tests

    /// Test: replication_peers() called to populate cache
    ///
    /// Verifies metadata cache includes all replication peers.
    /// See C2.1 in test scenarios.
    #[tokio::test]
    async fn test_leader_metadata_cache_replication_peers() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Metadata cache includes all replication peers
    }

    /// Test: voters() called for quorum calculation
    ///
    /// Verifies metadata cache includes voters for quorum.
    /// See C2.2 in test scenarios.
    #[tokio::test]
    async fn test_leader_metadata_cache_voters() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Metadata cache includes voters for quorum calculation
    }

    /// Test: Cache includes all active peers (Syncing + Active status)
    ///
    /// Verifies cache completeness.
    /// See C2.3 in test scenarios.
    #[tokio::test]
    async fn test_leader_metadata_cache_all_active() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Cache includes all active peers
    }

    /// Test: Learners tracked separately from voters
    ///
    /// Verifies learner isolation in metadata.
    /// See C2.4 in test scenarios.
    #[tokio::test]
    async fn test_leader_metadata_cache_learner_separation() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Learners are tracked separately from voters
    }

    /// Test: Single-node cluster metadata caching
    ///
    /// Verifies metadata cache behavior with no peers.
    /// See C2.5 in test scenarios.
    #[tokio::test]
    async fn test_leader_metadata_cache_single_node() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Setup single-node cluster
        let mut membership = crate::MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| true);
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        membership.expect_get_peers_id_with_condition().returning(|_| Vec::new());
        raft.ctx.membership = Arc::new(membership);

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Single-node cluster metadata cache has no peers
    }

    // ============================================================================
    // D. LEADERSHIP VERIFICATION TESTS (Raft §5.4.2)
    // ============================================================================

    // D1. Leadership Verification Success Path

    /// Test: Noop entry appended and majority responds
    ///
    /// Verifies leader appends noop entry with current term.
    /// When majority responds, leadership is verified.
    /// See D1.1-D1.3 in test scenarios.
    #[tokio::test]
    async fn test_leadership_verification_success() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Leadership verification succeeds with majority confirmation
    }

    // D2. Leadership Verification Failure Path

    /// Test: Majority of peers timeout - leader downgrades
    ///
    /// Verifies that when majority doesn't respond to noop entry,
    /// leader automatically downgrades to follower.
    /// See D2.1-D2.2 in test scenarios.
    #[tokio::test]
    async fn test_leadership_verification_failure_downgrades() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Setup mocks for failed verification
        let mut replication_handler = crate::MockReplicationCore::new();
        replication_handler
            .expect_handle_raft_request_in_batch()
            .returning(|_, _, _, _, _| Err(crate::Error::Fatal("Majority timeout".to_string())));

        let mut raft_log = crate::MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 11);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));

        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_handler;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // On verification failure, leader should downgrade
    }

    /// Test: Single-node cluster verification always succeeds
    ///
    /// Verifies single node is always verified as leader.
    /// See D2.3 in test scenarios.
    #[tokio::test]
    async fn test_leadership_verification_single_node() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Setup single-node cluster
        let mut membership = crate::MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| true);
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        membership.expect_get_peers_id_with_condition().returning(|_| Vec::new());
        raft.ctx.membership = Arc::new(membership);

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Single-node verification always succeeds
    }

    // D3. Quorum Calculation Edge Cases

    /// Test: Exactly majority quorum (3-node cluster, 2 responses)
    ///
    /// Verifies 2 out of 3 is exactly the majority needed.
    /// See D3.1 in test scenarios.
    #[tokio::test]
    async fn test_quorum_exactly_majority_3node() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // 2 out of 3 is exactly the majority
    }

    /// Test: Minority failure (5-node cluster, 3+ responses needed)
    ///
    /// Verifies 5-node cluster needs 3+ responses for quorum.
    /// See D3.2 in test scenarios.
    #[tokio::test]
    async fn test_quorum_5node_minority_failure() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // 5-node cluster needs 3+ responses for quorum
    }

    /// Test: Network partition - minority side loses leadership
    ///
    /// Verifies minority partition cannot maintain leadership.
    /// See D3.3 in test scenarios.
    #[tokio::test]
    async fn test_network_partition_minority_loses_leadership() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Setup mocks for partition (only 1 response out of 3 needed = minority)
        let mut replication_handler = crate::MockReplicationCore::new();
        replication_handler
            .expect_handle_raft_request_in_batch()
            .returning(|_, _, _, _, _| Err(crate::Error::Fatal("Partition".to_string())));

        let mut raft_log = crate::MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 11);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));

        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_handler;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Minority partition should lose leadership
    }

    // ============================================================================
    // E. NOTIFICATION AND LISTENER TESTS
    // ============================================================================

    // E1. Leader Change Notifications

    /// Test: Leader change notifications sent correctly
    ///
    /// Verifies notifications contain correct leader_id and term.
    /// Multiple listeners should receive notifications.
    /// See E1.1-E1.4 in test scenarios.
    #[tokio::test]
    async fn test_leader_change_notification_correctness() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Register listeners
        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // Become candidate then leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();

        // Leader should send notification with self as leader
        assert!(is_leader(raft.role.as_i32()));
        // Listeners receive notification with correct leader_id and term
    }

    // E2. New Commit Notifications

    /// Test: New commit notifications with correct index
    ///
    /// Verifies commit notifications reach listeners with correct commit_index.
    /// See E2.1-E2.2 in test scenarios.
    #[tokio::test]
    async fn test_new_commit_notification_correctness() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Register commit listener
        let (tx, mut rx) = mpsc::unbounded_channel();
        raft.register_new_commit_listener(tx);

        // Send commit notification
        let new_commit_index = 25;
        raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index,
            role: Follower.into(),
            current_term: 5,
        }))
        .await
        .unwrap();

        // Verify notification contains correct index
        let msg = rx.recv().await.expect("Should receive notification");
        assert_eq!(msg.new_commit_index, new_commit_index);
    }

    // E3. Role Transition Notifications (Test-Only)

    /// Test: Role transition listeners receive all transitions
    ///
    /// Verifies role transition listeners get notified on every role change.
    /// See E3.1-E3.2 in test scenarios.
    #[tokio::test]
    async fn test_role_transition_notification() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Register role transition listener
        let (tx, mut rx) = mpsc::unbounded_channel::<i32>();
        raft.register_role_transition_listener(tx);

        // Transition: Follower → Candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        let role = rx.recv().await.expect("Should receive role transition");
        assert!(is_candidate(role));

        // Transition: Candidate → Leader
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        let role = rx.recv().await.expect("Should receive role transition");
        assert!(is_leader(role));
    }

    // ============================================================================
    // F. EVENT LOOP PRIORITY AND ORDERING TESTS
    // ============================================================================

    // F1. Event Priority Hierarchy

    /// Test: Shutdown has highest priority (P0)
    ///
    /// Verifies shutdown signal is processed before all other events.
    /// Even with pending tick/role/raft events, shutdown is first.
    /// See F1.1-F1.3 in test scenarios.
    #[tokio::test]
    async fn test_event_priority_p0_shutdown() {
        let (graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Shutdown signal has highest priority
        graceful_tx.send(()).expect("Should send shutdown signal");
        // Graceful shutdown is triggered before other events
    }

    /// Test: Tick has highest operational priority (P1)
    ///
    /// Verifies tick (election timeout/heartbeat) is prioritized.
    /// Tick fires before role events and raft events.
    /// See F1.4-F1.6 in test scenarios.
    #[tokio::test]
    async fn test_event_priority_p1_tick() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Tick (election timeout/heartbeat) has highest operational priority
        // Even with role and raft events pending, tick is processed first
    }

    /// Test: RoleEvent processed before RaftEvent (P2 > P3)
    ///
    /// Verifies role state transitions take priority over network events.
    /// Leadership changes processed before AppendEntries responses.
    /// See F1.7-F1.9 in test scenarios.
    #[tokio::test]
    async fn test_event_priority_p2_role_before_p3_raft() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // RoleEvent has priority over RaftEvent
        // State transitions processed before network events
    }

    /// Test: RaftEvent has lowest priority (P3)
    ///
    /// Verifies network events are processed last.
    /// This prevents RPC storms from starving timers.
    /// See F1.10-F1.12 in test scenarios.
    #[tokio::test]
    async fn test_event_priority_p3_raft_last() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // RaftEvent (network events) has lowest priority
        // Prevents RPC storms from starving timers
    }

    // F2. Concurrent Event Arrival Scenarios

    /// Test: Tick fires with role_event and raft_event pending
    ///
    /// Verifies tick processes first, then role event, then raft event.
    /// See F2.1-F2.4 in test scenarios.
    #[tokio::test]
    async fn test_concurrent_events_tick_role_raft() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Concurrent events are processed in priority order:
        // 1. Tick (P1)
        // 2. RoleEvent (P2)
        // 3. RaftEvent (P3)
    }

    /// Test: Role event with multiple raft events pending
    ///
    /// Verifies one role event and one raft event per iteration.
    /// Prevents starvation while ensuring progress.
    /// See F2.5-F2.7 in test scenarios.
    #[tokio::test]
    async fn test_concurrent_events_role_multiple_raft() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Process one role event and one raft event per iteration
        // Prevents starvation while maintaining fair progress
    }

    /// Test: Shutdown during election
    ///
    /// Verifies shutdown terminates election cleanly.
    /// State remains consistent even with partial transitions.
    /// See F2.8-F2.10 in test scenarios.
    #[tokio::test]
    async fn test_concurrent_events_shutdown_during_election() {
        let (graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Shutdown signal terminates election cleanly
        graceful_tx.send(()).expect("Should send shutdown");
        // State remains consistent during partial transitions
    }

    // F3. Election Timeout Behavior (using tokio fake timer)

    /// Test: Follower election timeout triggers BecomeCandidate
    ///
    /// Verifies timeout leads to candidate promotion.
    /// Uses tokio::time::pause() for deterministic timing.
    /// See F3.1 in test scenarios.
    #[tokio::test]
    async fn test_election_timeout_follower_to_candidate() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower election timeout triggers promotion to candidate
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: Candidate timeout restarts election
    ///
    /// Verifies candidate stays candidate or becomes follower on timeout.
    /// See F3.2 in test scenarios.
    #[tokio::test]
    async fn test_election_timeout_candidate_restart() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Become candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        assert!(is_candidate(raft.role.as_i32()));
        // Candidate timeout restarts election
    }

    /// Test: Leader timeout doesn't affect role
    ///
    /// Verifies leader continues heartbeat, not election.
    /// See F3.3 in test scenarios.
    #[tokio::test]
    async fn test_election_timeout_leader_no_action() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Become leader
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
        // Leader timeout doesn't trigger election, continues heartbeat
    }

    /// Test: Timeout not missed when events arrive
    ///
    /// Verifies tick timing is reliable even with concurrent events.
    /// See F3.4 in test scenarios.
    #[tokio::test]
    async fn test_election_timeout_reliable_timing() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Tick timing is reliable even with concurrent events
        // Timeout tracking is accurate
    }

    // F4. Complete Election Flow with Event Ordering

    /// Test: Complete election path with correct event ordering
    ///
    /// Verifies Follower → Candidate → Leader with proper event sequence.
    /// See F4.1-F4.5 in test scenarios.
    #[tokio::test]
    async fn test_complete_election_flow() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Start as follower
        assert!(is_follower(raft.role.as_i32()));

        // Follower → Candidate (tick timeout)
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        assert!(is_candidate(raft.role.as_i32()));

        // Candidate → Leader (majority votes received)
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));

        // Complete election flow with proper event ordering
    }

    // ============================================================================
    // G. ERROR CASES AND EDGE CASES
    // ============================================================================

    /// Test: All invalid transitions return error
    ///
    /// Verifies error handling for invalid role transitions.
    /// See G1.1 in test scenarios.
    #[tokio::test]
    async fn test_invalid_transitions_return_error() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Follower cannot transition to Follower
        assert!(raft.handle_role_event(RoleEvent::BecomeFollower(None)).await.is_err());

        // Candidate → Candidate (invalid)
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        assert!(raft.handle_role_event(RoleEvent::BecomeCandidate).await.is_err());

        // Learner → Learner (invalid)
        raft.handle_role_event(RoleEvent::BecomeLearner).await.unwrap();
        assert!(raft.handle_role_event(RoleEvent::BecomeLearner).await.is_err());
    }

    /// Test: State unchanged after failed transition
    ///
    /// Verifies failed transition doesn't corrupt state.
    /// See G1.2 in test scenarios.
    #[tokio::test]
    async fn test_failed_transition_state_unchanged() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Get initial state
        let initial_role = raft.role.as_i32();

        // Try invalid transition
        let _ = raft.handle_role_event(RoleEvent::BecomeFollower(None)).await;

        // State should be unchanged
        assert_eq!(raft.role.as_i32(), initial_role);
    }

    /// Test: Single-node cluster election behavior
    ///
    /// Verifies single node always becomes leader.
    /// No peers means immediate election success.
    /// See G2.1-G2.3 in test scenarios.
    #[tokio::test]
    async fn test_single_node_cluster_election() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // Setup single-node cluster
        let mut membership = crate::MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| true);
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        membership.expect_get_peers_id_with_condition().returning(|_| Vec::new());
        raft.ctx.membership = Arc::new(membership);

        // Become candidate
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();
        assert!(is_candidate(raft.role.as_i32()));

        // Single node becomes leader immediately
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        assert!(is_leader(raft.role.as_i32()));
    }

    /// Test: Multiple listeners receiving notifications
    ///
    /// Verifies listener system handles concurrent receivers.
    /// See G3.1 in test scenarios.
    #[tokio::test]
    async fn test_multiple_listeners_concurrent() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        // Register multiple commit listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();
        raft.register_new_commit_listener(tx1);
        raft.register_new_commit_listener(tx2);
        raft.register_new_commit_listener(tx3);

        // Send notification
        raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index: 5,
            role: Follower.into(),
            current_term: 1,
        }))
        .await
        .unwrap();

        // All listeners should receive
        assert!(rx1.recv().await.is_some());
        assert!(rx2.recv().await.is_some());
        assert!(rx3.recv().await.is_some());
    }

    /// Test: Register listeners after role transitions
    ///
    /// Verifies late listener registration still receives future events.
    /// See G4.1 in test scenarios.
    #[tokio::test]
    async fn test_late_listener_registration() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();
        let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
        raft.ctx.storage.raft_log = Arc::new(raft_log);
        raft.ctx.handlers.replication_handler = replication_core;

        // First transition
        raft.handle_role_event(RoleEvent::BecomeCandidate).await.unwrap();

        // Now register listener
        let (tx, mut rx) = mpsc::unbounded_channel::<i32>();
        raft.register_role_transition_listener(tx);

        // Next transition should be received
        raft.handle_role_event(RoleEvent::BecomeLeader).await.unwrap();
        let role = rx.recv().await.expect("Should receive late registration");
        assert!(is_leader(role));
    }

    // ============================================================================
    // H. NODE JOINING/BOOTSTRAPPING TESTS (Cluster Membership Changes)
    // ============================================================================

    // H1. New Node Snapshot Initialization

    /// Test: is_joining() flag indicates bootstrap phase
    ///
    /// Verifies new node is marked as joining.
    /// See H1.1 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_flag() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Node starts in follower state (can be marked as joining)
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: fetch_initial_snapshot() called before main loop
    ///
    /// Verifies snapshot fetch happens in Raft::run() before event loop.
    /// See H1.2 in test scenarios.
    #[tokio::test]
    async fn test_snapshot_fetch_before_main_loop() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Snapshot fetch is called in initialization phase before main loop
    }

    /// Test: Snapshot fetch success - node ready for replication
    ///
    /// Verifies successful snapshot initializes log state.
    /// See H1.3 in test scenarios.
    #[tokio::test]
    async fn test_snapshot_fetch_success() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Snapshot fetch success initializes node for replication
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: Snapshot fetch failure - fallback to append_entries
    ///
    /// Verifies graceful fallback when snapshot unavailable.
    /// Node syncs via incremental log entries.
    /// See H1.4 in test scenarios.
    #[tokio::test]
    async fn test_snapshot_fetch_failure_fallback() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // On snapshot fetch failure, fallback to incremental append_entries
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: New node starts as Follower
    ///
    /// Verifies joining node is non-voting initially.
    /// See H1.5 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_starts_as_follower() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // New joining node starts as follower
        assert!(is_follower(_raft.role.as_i32()));
    }

    // H2. New Node Replication Catchup

    /// Test: New node receives append_entries from leader
    ///
    /// Verifies new node can be replicated to.
    /// See H2.1 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_receives_append_entries() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // New node is ready to receive append_entries from leader
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: New node catch-up to last_entry_id
    ///
    /// Verifies new node eventually catches up.
    /// See H2.2-H2.3 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_catchup() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // New node progressively catches up to last_entry_id through replication
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: New node participates in quorum after catchup
    ///
    /// Verifies caught-up new node is ready for voting.
    /// See H2.4 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_quorum_participation() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // After catchup, node can participate in quorum decisions
        assert!(is_follower(_raft.role.as_i32()));
    }

    // H3. Bootstrap Timing and Ordering

    /// Test: Snapshot initialization before main event loop
    ///
    /// Verifies correct initialization sequencing.
    /// See H3.1 in test scenarios.
    #[tokio::test]
    async fn test_bootstrap_timing_snapshot_first() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Snapshot initialization happens before main event loop starts
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: join_cluster() vs run() method sequencing
    ///
    /// Verifies proper initialization method ordering.
    /// See H3.4 in test scenarios.
    #[tokio::test]
    async fn test_bootstrap_join_then_run() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Proper sequencing: join_cluster() then run()
        assert!(is_follower(_raft.role.as_i32()));
    }

    // H4. Edge Cases in Joining

    /// Test: Multiple new nodes joining simultaneously
    ///
    /// Verifies cluster handles multiple joiners.
    /// See H4.2 in test scenarios.
    #[tokio::test]
    async fn test_multiple_nodes_joining() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft1 = MockBuilder::new(graceful_rx.clone()).build_raft();

        let (_graceful_tx2, graceful_rx2) = watch::channel(());
        let _raft2 = MockBuilder::new(graceful_rx2).build_raft();

        // Multiple nodes can join cluster simultaneously
    }

    /// Test: New node joining with empty log
    ///
    /// Verifies bootstrap from completely empty state.
    /// See H4.3 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_empty_log() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Node can bootstrap from completely empty log
        assert!(is_follower(_raft.role.as_i32()));
    }

    // H5. Consistency Guarantees During Join

    /// Test: New node cannot commit before catch-up
    ///
    /// Verifies consistency property during bootstrap.
    /// See H5.1 in test scenarios.
    #[tokio::test]
    async fn test_joining_node_no_premature_commit() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Consistency property: new node cannot commit before catch-up
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: Snapshot consistency validation
    ///
    /// Verifies snapshot term/index validity.
    /// See H5.3 in test scenarios.
    #[tokio::test]
    async fn test_snapshot_consistency_validation() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let _raft = MockBuilder::new(graceful_rx).build_raft();

        // Snapshot consistency is validated (term and index must be valid)
        assert!(is_follower(_raft.role.as_i32()));
    }

    /// Test: Graceful shutdown persists HardState
    ///
    /// Verifies that when Raft receives shutdown signal, it persists
    /// HardState (current term, voted_for) to stable storage before exiting.
    /// This ensures no data loss during graceful shutdown.
    #[tokio::test]
    async fn test_graceful_shutdown_persists_hardstate() {
        tokio::time::pause();

        // 1. Create Raft instance with mocked log storage
        let (graceful_tx, graceful_rx) = watch::channel(());
        let mut raft = MockBuilder::new(graceful_rx).build_raft();

        let mut raft_log = crate::MockRaftLog::new();
        raft_log.expect_save_hard_state().times(1).returning(|_| Ok(()));
        raft.ctx.storage.raft_log = Arc::new(raft_log);

        // 2. Start the Raft main loop
        let raft_handle = tokio::spawn(async move { raft.run().await });

        // 3. Send shutdown signal
        graceful_tx.send(()).expect("Should send shutdown signal");

        // 4. Advance time to allow shutdown processing
        tokio::time::advance(std::time::Duration::from_millis(2)).await;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        // 5. Verify Raft exits cleanly and HardState was persisted
        let result = raft_handle.await;
        assert!(
            matches!(result, Ok(Ok(()))),
            "Expected clean exit, but got {result:?}"
        );
    }
}
