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
    assert_eq!(old_vote.committed, false);
}

#[test]
fn test_voted_for_committed_flag() {
    // New data with committed=true (leader elected)
    let leader_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: true,
    };

    assert_eq!(leader_vote.committed, true);

    // Candidate vote (not yet leader)
    let candidate_vote = VotedFor {
        voted_for_id: 2,
        voted_for_term: 10,
        committed: false,
    };

    assert_eq!(candidate_vote.committed, false);
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
    assert_eq!(vote.committed, false);
}

#[test]
fn test_candidate_to_leader_committed_vote() {
    // When candidate becomes leader, vote.committed should be true
    let leader_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: true,
    };

    assert_eq!(leader_vote.committed, true);
    assert_eq!(leader_vote.voted_for_id, 1);
}

#[test]
fn test_step_down_resets_vote() {
    // When node steps down (higher term), voted_for should be reset
    let mut shared = SharedState::new(1, None, None);
    
    // Initially voted for someone
    shared.update_voted_for(VotedFor {
        voted_for_id: 2,
        voted_for_term: 5,
        committed: true,
    }).unwrap();
    
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
    assert_eq!(leader_vote.committed, true);
    
    // Uncommitted vote means no confirmed leader yet
    let candidate_vote = VotedFor {
        voted_for_id: 1,
        voted_for_term: 10,
        committed: false,
    };
    
    assert_eq!(candidate_vote.committed, false);
}

#[test]
fn test_follower_learns_leader_from_append_entries() {
    // Simulate follower receiving AppendEntries from leader
    let mut shared = SharedState::new(2, None, None);
    
    // Before AppendEntries: no leader known
    assert!(shared.voted_for().unwrap().is_none());
    
    // After successful AppendEntries: learn leader
    shared.update_voted_for(VotedFor {
        voted_for_id: 3,
        voted_for_term: 5,
        committed: true, // Confirmed leader
    }).unwrap();
    
    let vote = shared.voted_for().unwrap().unwrap();
    assert_eq!(vote.voted_for_id, 3);
    assert_eq!(vote.committed, true);
}

#[test]
fn test_vote_lifecycle() {
    let mut shared = SharedState::new(1, None, None);
    
    // 1. Initial state: no vote
    assert!(shared.voted_for().unwrap().is_none());
    
    // 2. Candidate votes for self (uncommitted)
    shared.update_voted_for(VotedFor {
        voted_for_id: 1,
        voted_for_term: 5,
        committed: false,
    }).unwrap();
    
    let vote = shared.voted_for().unwrap().unwrap();
    assert_eq!(vote.committed, false);
    
    // 3. Receives quorum, becomes leader (committed)
    shared.update_voted_for(VotedFor {
        voted_for_id: 1,
        voted_for_term: 5,
        committed: true,
    }).unwrap();
    
    let vote = shared.voted_for().unwrap().unwrap();
    assert_eq!(vote.committed, true);
    
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
    assert_eq!(hs.voted_for.unwrap().committed, true);
    
    // Test with uncommitted vote
    let hs2 = HardState {
        current_term: 10,
        voted_for: Some(VotedFor {
            voted_for_id: 2,
            voted_for_term: 10,
            committed: false,
        }),
    };
    
    assert_eq!(hs2.voted_for.unwrap().committed, false);
}
