use super::super::*;
use d_engine_proto::server::election::VotedFor;

#[test]
fn test_voted_for_backward_compatibility() {
    // Simulate old VotedFor data (without committed field)
    // Protobuf default: bool fields default to false
    let old_vote = VotedFor {
        voted_for_id: 3,
        voted_for_term: 5,
        committed: false, // Old data will deserialize to false
    };

    // Verify default behavior
    assert_eq!(old_vote.voted_for_id, 3);
    assert_eq!(old_vote.voted_for_term, 5);
    assert!(!old_vote.committed);
}

#[test]
fn test_voted_for_committed_flag() {
    // New data with committed=true (leader elected)
    let leader_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: true,
    };

    assert!(leader_vote.committed);

    // Candidate vote (not yet leader)
    let candidate_vote = VotedFor {
        voted_for_id: 2,
        voted_for_term: 10,
        committed: false,
    };

    assert!(!candidate_vote.committed);
}

#[test]
fn test_hard_state_with_voted_for() {
    // Test HardState with VotedFor (migration scenario)
    let hs = HardState {
        current_term: 5,
        voted_for: Some(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: false, // Old data defaults to false
        }),
    };

    assert_eq!(hs.current_term, 5);
    assert!(hs.voted_for.is_some());

    let vote = hs.voted_for.unwrap();
    assert_eq!(vote.voted_for_id, 3);
    assert_eq!(vote.voted_for_term, 5);
    assert!(!vote.committed);
}

#[test]
fn test_candidate_to_leader_committed_vote() {
    // When candidate becomes leader, vote.committed should be true
    let leader_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: true,
    };

    assert!(leader_vote.committed);
    assert_eq!(leader_vote.voted_for_id, 1);
}

#[test]
fn test_step_down_resets_vote() {
    // When node steps down (higher term), voted_for should be reset
    let mut shared = SharedState::new(1, None, None);

    // Initially voted for someone
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 2,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    assert!(shared.voted_for().unwrap().is_some());

    // Step down - reset vote
    shared.reset_voted_for().unwrap();

    assert!(shared.voted_for().unwrap().is_none());
}

#[test]
fn test_committed_vote_represents_leader() {
    // Committed vote with committed=true means this node is leader
    let leader_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: true,
    };

    // Leader exists when vote is committed
    assert!(leader_vote.committed);

    // Uncommitted vote means no confirmed leader yet
    let candidate_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: false,
    };

    assert!(!candidate_vote.committed);
}

#[test]
fn test_follower_learns_leader_from_append_entries() {
    // Simulate follower receiving AppendEntries from leader
    let mut shared = SharedState::new(2, None, None);

    // Before AppendEntries: no leader known
    assert!(shared.voted_for().unwrap().is_none());

    // After successful AppendEntries: learn leader
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true, // Confirmed leader
        })
        .unwrap();

    let vote = shared.voted_for().unwrap().unwrap();
    assert_eq!(vote.voted_for_id, 3);
    assert!(vote.committed);
}

#[test]
fn test_vote_lifecycle() {
    let mut shared = SharedState::new(1, None, None);

    // 1. Initial state: no vote
    assert!(shared.voted_for().unwrap().is_none());

    // 2. Candidate votes for self (uncommitted)
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 1,
            voted_for_term: 5,
            committed: false,
        })
        .unwrap();

    let vote = shared.voted_for().unwrap().unwrap();
    assert!(!vote.committed);

    // 3. Receives quorum, becomes leader (committed)
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 1,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    let vote = shared.voted_for().unwrap().unwrap();
    assert!(vote.committed);

    // 4. Steps down (higher term discovered)
    shared.update_current_term(6);
    shared.reset_voted_for().unwrap();

    assert!(shared.voted_for().unwrap().is_none());
}

#[test]
fn test_committed_vote_persistence() {
    // Test that committed flag persists in HardState
    let hs = HardState {
        current_term: 10,
        voted_for: Some(VotedFor {
            voted_for_id: 2,
            voted_for_term: 10,
            committed: true,
        }),
    };

    // Verify committed flag is stored
    assert!(hs.voted_for.unwrap().committed);

    // Test with uncommitted vote
    let hs2 = HardState {
        current_term: 10,
        voted_for: Some(VotedFor {
            voted_for_id: 2,
            voted_for_term: 10,
            committed: false,
        }),
    };

    assert!(!hs2.voted_for.unwrap().committed);
}

/// Test atomic leader_id operations (Phase 2: performance optimization)
#[test]
fn test_shared_state_current_leader_default() {
    let shared = SharedState::new(1, None, None);

    // Default: no leader (0 = None)
    assert_eq!(shared.current_leader(), None);
}

#[test]
fn test_shared_state_set_current_leader() {
    let shared = SharedState::new(1, None, None);

    // Set leader
    shared.set_current_leader(5);
    assert_eq!(shared.current_leader(), Some(5));

    // Update leader
    shared.set_current_leader(3);
    assert_eq!(shared.current_leader(), Some(3));
}

#[test]
fn test_shared_state_clear_current_leader() {
    let shared = SharedState::new(1, None, None);

    // Set then clear
    shared.set_current_leader(5);
    assert_eq!(shared.current_leader(), Some(5));

    shared.clear_current_leader();
    assert_eq!(shared.current_leader(), None);
}

#[test]
fn test_shared_state_leader_zero_means_none() {
    let shared = SharedState::new(1, None, None);

    // Explicitly set to 0 (same as clear)
    shared.set_current_leader(0);
    assert_eq!(shared.current_leader(), None);

    // Set valid leader
    shared.set_current_leader(2);
    assert_eq!(shared.current_leader(), Some(2));

    // Clear via set_current_leader(0)
    shared.set_current_leader(0);
    assert_eq!(shared.current_leader(), None);
}

#[test]
fn test_shared_state_leader_clone() {
    let shared1 = SharedState::new(1, None, None);
    shared1.set_current_leader(10);

    // Clone preserves leader_id
    let shared2 = shared1.clone();
    assert_eq!(shared2.current_leader(), Some(10));

    // Clones are independent
    shared2.set_current_leader(20);
    assert_eq!(shared1.current_leader(), Some(10)); // Original unchanged
    assert_eq!(shared2.current_leader(), Some(20));
}

#[test]
fn test_shared_state_leader_debug() {
    let shared = SharedState::new(1, None, None);
    shared.set_current_leader(7);

    // Debug format includes current_leader
    let debug_str = format!("{shared:?}");
    assert!(debug_str.contains("current_leader"));
    assert!(debug_str.contains("7"));
}

#[test]
fn test_shared_state_concurrent_updates() {
    use std::sync::Arc;
    use std::thread;

    let shared = Arc::new(SharedState::new(1, None, None));

    // Simulate concurrent leader updates
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                shared.set_current_leader(i as u32);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Final value is one of the concurrent updates
    let final_leader = shared.current_leader();
    assert!(final_leader.is_some());
    assert!(final_leader.unwrap() < 10);
}

#[test]
fn test_shared_state_leader_lifecycle() {
    let shared = SharedState::new(1, None, None);

    // 1. Start: no leader
    assert_eq!(shared.current_leader(), None);

    // 2. AppendEntries from node 3
    shared.set_current_leader(3);
    assert_eq!(shared.current_leader(), Some(3));

    // 3. Leader step down (higher term)
    shared.clear_current_leader();
    assert_eq!(shared.current_leader(), None);

    // 4. New leader elected
    shared.set_current_leader(5);
    assert_eq!(shared.current_leader(), Some(5));
}

// ============================================================================
// update_voted_for() Business Scenario Tests
// ============================================================================

/// Scenario 1: First time discovering leader (follower receives first AppendEntries)
#[test]
fn test_update_voted_for_first_leader_discovery() {
    let mut shared = SharedState::new(2, None, None);

    // Initial state: no voted_for, no leader
    assert!(shared.voted_for().unwrap().is_none());
    assert_eq!(shared.current_leader(), None);

    // Receive first AppendEntries from leader 3
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    // Should trigger LeaderDiscovered event
    assert!(is_new, "First leader discovery should return true");
    assert_eq!(shared.voted_for().unwrap().unwrap().voted_for_id, 3);
    assert_eq!(shared.voted_for().unwrap().unwrap().voted_for_term, 5);
}

/// Scenario 2: Same leader sends heartbeat (no change)
#[test]
fn test_update_voted_for_same_leader_heartbeat() {
    let mut shared = SharedState::new(2, None, None);

    // First heartbeat
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    // Simulate memory update (in real code, caller updates current_leader_id)
    shared.set_current_leader(3);

    // Second heartbeat from same leader
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    // Should NOT trigger event (same leader)
    assert!(!is_new, "Same leader heartbeat should return false");
}

/// Scenario 3: Leader changes (new leader elected)
#[test]
fn test_update_voted_for_leader_change() {
    let mut shared = SharedState::new(2, None, None);

    // Old leader
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();
    shared.set_current_leader(3);

    // New leader elected in higher term
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 5,
            voted_for_term: 6,
            committed: true,
        })
        .unwrap();

    // Should trigger LeaderDiscovered event
    assert!(is_new, "Leader change should return true");
    assert_eq!(shared.voted_for().unwrap().unwrap().voted_for_id, 5);
    assert_eq!(shared.voted_for().unwrap().unwrap().voted_for_term, 6);
}

/// Scenario 4: Candidate votes for self (uncommitted)
#[test]
fn test_update_voted_for_candidate_vote() {
    let mut shared = SharedState::new(1, None, None);

    // Candidate votes for self (not yet leader)
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 1,
            voted_for_term: 5,
            committed: false,
        })
        .unwrap();

    // Should NOT trigger event (not committed)
    assert!(!is_new, "Uncommitted candidate vote should return false");
    assert!(!shared.voted_for().unwrap().unwrap().committed);
}

/// Scenario 5: Candidate becomes leader (uncommitted → committed)
#[test]
fn test_update_voted_for_candidate_to_leader() {
    let mut shared = SharedState::new(1, None, None);

    // Step 1: Candidate votes for self
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 1,
            voted_for_term: 5,
            committed: false,
        })
        .unwrap();

    // Step 2: Receives quorum, becomes leader
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 1,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    // Should trigger event (committed flag transition)
    assert!(is_new, "Candidate to leader transition should return true");
    assert!(shared.voted_for().unwrap().unwrap().committed);
}

/// Scenario 6: Node restart - same leader still exists (CRITICAL FIX)
#[test]
fn test_update_voted_for_node_restart_same_leader() {
    // Simulate node state before shutdown
    let hard_state_before_shutdown = HardState {
        current_term: 4,
        voted_for: Some(VotedFor {
            voted_for_id: 3,
            voted_for_term: 4,
            committed: true,
        }),
    };

    // Node restarts - load hard_state from disk
    let mut shared = SharedState::new(1, Some(hard_state_before_shutdown), None);

    // Key: current_leader_id is reset to 0 (memory cleared)
    assert_eq!(
        shared.current_leader(),
        None,
        "After restart, current_leader should be None"
    );
    assert!(
        shared.voted_for().unwrap().is_some(),
        "voted_for loaded from disk"
    );

    // Leader 3 sends first heartbeat after restart
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 4,
            committed: true,
        })
        .unwrap();

    // CRITICAL: Should trigger LeaderDiscovered event
    // Because current_leader_id is None (memory cleared after restart)
    assert!(
        is_new,
        "Node restart: should rediscover leader even if voted_for unchanged"
    );
}

/// Scenario 7: Node restart - leader changed during downtime
#[test]
fn test_update_voted_for_node_restart_leader_changed() {
    // Node state before shutdown (old leader 3)
    let hard_state_before_shutdown = HardState {
        current_term: 4,
        voted_for: Some(VotedFor {
            voted_for_id: 3,
            voted_for_term: 4,
            committed: true,
        }),
    };

    // Node restarts
    let mut shared = SharedState::new(1, Some(hard_state_before_shutdown), None);

    // New leader 5 sends heartbeat (term advanced)
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 5,
            voted_for_term: 6,
            committed: true,
        })
        .unwrap();

    // Should trigger event (leader changed)
    assert!(is_new, "Node restart with new leader should return true");
    assert_eq!(shared.voted_for().unwrap().unwrap().voted_for_id, 5);
    assert_eq!(shared.voted_for().unwrap().unwrap().voted_for_term, 6);
}

/// Scenario 8: Term advances but no leader yet
#[test]
fn test_update_voted_for_term_advance_no_leader() {
    let mut shared = SharedState::new(2, None, None);

    // Old leader
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();
    shared.set_current_leader(3);

    // Term advances, candidate votes (not yet leader)
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 5,
            voted_for_term: 6,
            committed: false, // Not yet leader
        })
        .unwrap();

    // Should NOT trigger event (uncommitted)
    assert!(
        !is_new,
        "Term advance without committed leader should return false"
    );
}

/// Scenario 9: Leader step down (leader discovers higher term)
#[test]
fn test_update_voted_for_leader_step_down() {
    let mut shared = SharedState::new(3, None, None);

    // Node 3 is leader in term 5
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();
    shared.set_current_leader(3);

    // Discovers higher term, resets vote
    shared.update_current_term(6);
    shared.reset_voted_for().unwrap();
    shared.clear_current_leader();

    // New leader in term 6
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 5,
            voted_for_term: 6,
            committed: true,
        })
        .unwrap();

    // Should trigger event (new leader after step down)
    assert!(is_new, "New leader after step down should return true");
}

/// Scenario 10: Multiple leader changes in sequence
#[test]
fn test_update_voted_for_multiple_leader_changes() {
    let mut shared = SharedState::new(2, None, None);

    // Leader 1 (term 3)
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 1,
            voted_for_term: 3,
            committed: true,
        })
        .unwrap();
    assert!(is_new);
    shared.set_current_leader(1);

    // Leader 2 (term 4)
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 2,
            voted_for_term: 4,
            committed: true,
        })
        .unwrap();
    assert!(is_new, "First leader change");
    shared.set_current_leader(2);

    // Leader 3 (term 5)
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();
    assert!(is_new, "Second leader change");
}

/// Scenario 11: Edge case - same leader but term regressed (network partition recovery)
#[test]
fn test_update_voted_for_term_regression() {
    let mut shared = SharedState::new(2, None, None);

    // Current leader in term 6
    shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 6,
            committed: true,
        })
        .unwrap();
    shared.set_current_leader(3);

    // Receive stale message from term 5 (should be rejected in practice)
    // But test update_voted_for logic
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 5,
            committed: true,
        })
        .unwrap();

    // Term regression is considered a change
    assert!(is_new, "Term regression should return true");
}

/// Scenario 12: Learner node receives leader info
#[test]
fn test_update_voted_for_learner_discovers_leader() {
    let mut shared = SharedState::new(10, None, None); // Learner node

    // Learner discovers leader through snapshot or replication
    let is_new = shared
        .update_voted_for(VotedFor {
            voted_for_id: 3,
            voted_for_term: 8,
            committed: true,
        })
        .unwrap();

    // Should trigger event (first discovery)
    assert!(is_new, "Learner first leader discovery should return true");
}
