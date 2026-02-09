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
//! - **H1.1**: is_learner() flag indicates bootstrap phase
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
//! - **H3.2**: is_learner() flag checked once per node startup
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
//! ### I. Drain-Based Batch Architecture Tests (NEW)
//!
//! These tests verify the new drain-based batching architecture for client command processing.
//! This architecture replaces timeout-based batching with channel-driven batching:
//! - Main loop: recv() blocks for first command → try_recv() drains all pending
//! - Immediate flush on wakeup (no batch_timeout delay)
//! - Role-specific behavior: Leader batches, Follower/Candidate reject writes
//! - Read consistency policies: Linearizable, Lease, Eventual
//!
//! **Priorities**: P0 (Critical path, 8 tests) + P1 (Performance, 3 tests)
//!
//! #### I1. BatchBuffer Unit Tests
//! - **I1.1**: Single item no batching - immediate processing
//! - **I1.2**: Max batch size cap - respects size limit (300 items)
//! - **I1.3**: Empty flush idempotent - no panic on empty buffer
//! - **I1.4**: Degenerate batch size - max_batch_size=1 processing
//! - **I1.5**: Metrics tracking - drain vs heartbeat trigger counts
//! - **I1.6**: Natural batching - multiple cycles with smaller batches
//!
//! #### I2. Leader Command Drain (P0 - Critical)
//! - **I2.1**: Single command drain immediately - zero latency for single write
//! - **I2.2**: High load max batch cap - 1000 items split across 300-item batches
//!
//! #### I3. Role-Specific Behavior (P0 - Critical)
//! - **I3.1**: Follower eventual read local processing - no redirect to leader
//! - **I3.2**: Follower write request rejection - error response for writes
//! - **I3.3**: Candidate commands handling - buffering during election
//!
//! #### I4. Performance Validation (P1)
//! - **I4.1**: Medium load natural batching - 50 items with 20-item batches
//! - **I4.2**: Mixed workload read/write coexistence - concurrent reads and writes
//! - **I4.3**: Burst load recovery after spike - spike absorption and recovery
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

mod drain_based_batch_architecture_tests;
mod leader_change_tests;
mod leader_discovered_tests;
mod raft_comprehensive_tests;
