//! Unit tests for ElectionHandler implementing core Raft leader election protocol (Section 5.2)
//!
//! These tests verify:
//! - Vote request validation and granting logic
//! - Majority quorum calculation
//! - Log recency checks
//! - Term advancement and state transitions
//! - Edge cases in election rules

use std::sync::Arc;

use crate::MockRaftLog;
use crate::MockTypeConfig;
use crate::election::ElectionCore;
use crate::election::ElectionHandler;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VotedFor;

// ============================================================================
// Helper Functions
// ============================================================================

fn create_handler(node_id: u32) -> ElectionHandler<MockTypeConfig> {
    ElectionHandler::new(node_id)
}

fn create_vote_request(
    term: u64,
    candidate_id: u32,
    last_log_index: u64,
    last_log_term: u64,
) -> VoteRequest {
    VoteRequest {
        term,
        candidate_id,
        last_log_index,
        last_log_term,
    }
}

fn create_mock_raft_log(last_log_id: Option<LogId>) -> MockRaftLog {
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_log_id().returning(move || last_log_id);
    raft_log
}

// ============================================================================
// test_handle_vote_request_* - Vote Request Handling
// ============================================================================

/// Test: Voter grants vote when candidate has higher term and valid log
///
/// Scenario:
/// - Current term: 1
/// - Request term: 2 (higher)
/// - Local log: index=1, term=1
/// - Candidate log: index=2, term=2 (more recent)
/// - Voted for: None
///
/// Expected: Vote granted, term updated
#[tokio::test]
async fn test_handle_vote_request_grant_higher_term() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 1, 2, 2);
    let current_term = 1u64;
    let voted_for_option = None;
    let last_log_id = Some(LogId { index: 1, term: 1 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert_eq!(
        state_update.term_update,
        Some(2),
        "Term should be updated to 2"
    );
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted"
    );
    assert_eq!(
        state_update.new_voted_for.unwrap().voted_for_id,
        1,
        "Should vote for candidate 1"
    );
    assert_eq!(
        state_update.new_voted_for.unwrap().voted_for_term,
        2,
        "Vote should be for term 2"
    );
}

/// Test: Voter denies vote when request term is lower than current term
///
/// Scenario:
/// - Current term: 3
/// - Request term: 2 (lower)
/// - Vote should not be granted
///
/// Expected: Vote denied, no state update
#[tokio::test]
async fn test_handle_vote_request_deny_lower_term() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 1, 5, 2);
    let current_term = 3u64;
    let voted_for_option = None;
    let last_log_id = Some(LogId { index: 5, term: 3 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert_eq!(
        state_update.term_update, None,
        "Term should not be updated for lower request term"
    );
    assert_eq!(
        state_update.new_voted_for, None,
        "Vote should not be granted for lower term"
    );
}

/// Test: Voter denies vote when candidate's log is not as recent
///
/// Scenario:
/// - Current term: 1
/// - Request term: 1 (same)
/// - Local log: index=10, term=2 (more recent than candidate)
/// - Candidate log: index=5, term=1 (less recent)
/// - Voted for: None
///
/// Expected: Vote denied because candidate's log is stale
#[tokio::test]
async fn test_handle_vote_request_deny_stale_log() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(1, 1, 5, 1); // Candidate has older log
    let current_term = 1u64;
    let voted_for_option = None;
    let last_log_id = Some(LogId { index: 10, term: 2 }); // Local log is more recent
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert_eq!(
        state_update.new_voted_for, None,
        "Vote should be denied for stale log"
    );
}

/// Test: Voter denies vote when already voted for a different candidate in same term
///
/// Scenario:
/// - Current term: 2
/// - Request term: 2 (same)
/// - Already voted for: node 1 in term 2
/// - Request from: node 3
/// - Local log: index=3, term=2
/// - Candidate log: index=3, term=2
///
/// Expected: Vote denied (already voted for someone else)
#[tokio::test]
async fn test_handle_vote_request_deny_already_voted_different_candidate() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 3, 3, 2); // Request from node 3
    let current_term = 2u64;
    let voted_for_option = Some(VotedFor {
        voted_for_id: 1,
        voted_for_term: 2,
    }); // Already voted for node 1
    let last_log_id = Some(LogId { index: 3, term: 2 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert_eq!(
        state_update.new_voted_for, None,
        "Vote should be denied when already voted for different candidate"
    );
}

/// Test: Voter grants vote when re-voting for the same candidate in same term
///
/// Scenario:
/// - Current term: 2
/// - Request term: 2 (same)
/// - Already voted for: node 1 in term 2
/// - Request from: node 1 (same candidate)
/// - Local log: index=3, term=2
/// - Candidate log: index=3, term=2
///
/// Expected: Vote granted (re-voting for same candidate is allowed)
#[tokio::test]
async fn test_handle_vote_request_grant_revote_same_candidate() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 1, 3, 2); // Request from node 1
    let current_term = 2u64;
    let voted_for_option = Some(VotedFor {
        voted_for_id: 1,
        voted_for_term: 2,
    }); // Already voted for node 1
    let last_log_id = Some(LogId { index: 3, term: 2 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted for re-voting"
    );
    assert_eq!(
        state_update.new_voted_for.unwrap().voted_for_id,
        1,
        "Should vote for the same candidate"
    );
}

/// Test: Voter grants vote when higher term provided (resets voted_for)
///
/// Scenario:
/// - Current term: 2
/// - Request term: 3 (higher)
/// - Already voted for: node 1 in term 2
/// - Request from: node 3
/// - Local log: index=3, term=2
/// - Candidate log: index=4, term=3
///
/// Expected: Vote granted (higher term resets vote)
#[tokio::test]
async fn test_handle_vote_request_grant_higher_term_resets_vote() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(3, 3, 4, 3); // Higher term
    let current_term = 2u64;
    let voted_for_option = Some(VotedFor {
        voted_for_id: 1,
        voted_for_term: 2,
    }); // Voted for node 1 in term 2
    let last_log_id = Some(LogId { index: 3, term: 2 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert_eq!(
        state_update.term_update,
        Some(3),
        "Term should be updated to 3"
    );
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted for higher term"
    );
    assert_eq!(
        state_update.new_voted_for.unwrap().voted_for_id,
        3,
        "Should vote for node 3"
    );
}

/// Test: Voter grants vote when candidate has higher log term
///
/// Scenario:
/// - Current term: 1
/// - Request term: 1 (same)
/// - Local log: index=10, term=1
/// - Candidate log: index=5, term=2 (higher term, less index but more recent)
///
/// Expected: Vote granted (term takes precedence)
#[tokio::test]
async fn test_handle_vote_request_grant_higher_log_term() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(1, 1, 5, 2); // Higher log term
    let current_term = 1u64;
    let voted_for_option = None;
    let last_log_id = Some(LogId { index: 10, term: 1 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted for higher log term"
    );
}

/// Test: Voter grants vote when same log term but higher index
///
/// Scenario:
/// - Current term: 1
/// - Request term: 1 (same)
/// - Local log: index=5, term=2
/// - Candidate log: index=10, term=2 (same term, higher index)
///
/// Expected: Vote granted (higher index is more recent)
#[tokio::test]
async fn test_handle_vote_request_grant_higher_index_same_term() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(1, 1, 10, 2); // Same term, higher index
    let current_term = 1u64;
    let voted_for_option = None;
    let last_log_id = Some(LogId { index: 5, term: 2 });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted for higher index in same term"
    );
}

/// Test: Empty log (no entries) votes for valid candidate
///
/// Scenario:
/// - Local node has no log entries (None)
/// - Candidate has index=1, term=1
/// - Request with valid term
///
/// Expected: Vote granted
#[tokio::test]
async fn test_handle_vote_request_empty_local_log() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(1, 1, 1, 1);
    let current_term = 0u64;
    let voted_for_option = None;
    let last_log_id = None;
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted for candidate with valid log when local log is empty"
    );
}

/// Test: Candidate with empty log votes for someone with entries
///
/// Scenario:
/// - Local node has no entries (None)
/// - Requesting vote from candidate (also empty)
/// - Request has index=0, term=0
///
/// Expected: Vote granted (both have same recency)
#[tokio::test]
async fn test_handle_vote_request_both_empty_logs() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(1, 1, 0, 0);
    let current_term = 0u64;
    let voted_for_option = None;
    let last_log_id = None;
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert!(
        state_update.new_voted_for.is_some(),
        "Vote should be granted when both have empty logs"
    );
}

// ============================================================================
// test_check_vote_request_is_legal_* - Legal Check
// ============================================================================

/// Test: Check vote request legality - lower term is rejected
#[tokio::test]
async fn test_check_vote_request_is_legal_lower_term() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(1, 1, 5, 2);
    let current_term = 2u64;
    let last_log_index = 5u64;
    let last_log_term = 2u64;
    let voted_for_option = None;

    // Act
    let is_legal = handler.check_vote_request_is_legal(
        &request,
        current_term,
        last_log_index,
        last_log_term,
        voted_for_option,
    );

    // Assert
    assert!(!is_legal, "Request with lower term should be rejected");
}

/// Test: Check vote request legality - stale log is rejected
#[tokio::test]
async fn test_check_vote_request_is_legal_stale_log() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 1, 3, 1); // Lower log term
    let current_term = 2u64;
    let last_log_index = 5u64;
    let last_log_term = 2u64; // Local log is more recent
    let voted_for_option = None;

    // Act
    let is_legal = handler.check_vote_request_is_legal(
        &request,
        current_term,
        last_log_index,
        last_log_term,
        voted_for_option,
    );

    // Assert
    assert!(!is_legal, "Request with stale log should be rejected");
}

/// Test: Check vote request legality - already voted for different candidate
#[tokio::test]
async fn test_check_vote_request_is_legal_already_voted_different() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 3, 5, 2); // Request from node 3
    let current_term = 2u64;
    let last_log_index = 5u64;
    let last_log_term = 2u64;
    let voted_for_option = Some(VotedFor {
        voted_for_id: 1,
        voted_for_term: 2,
    }); // Already voted for node 1

    // Act
    let is_legal = handler.check_vote_request_is_legal(
        &request,
        current_term,
        last_log_index,
        last_log_term,
        voted_for_option,
    );

    // Assert
    assert!(
        !is_legal,
        "Request should be rejected when already voted for different candidate"
    );
}

/// Test: Check vote request legality - valid request is accepted
#[tokio::test]
async fn test_check_vote_request_is_legal_valid_request() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(2, 1, 5, 2); // Valid request
    let current_term = 2u64;
    let last_log_index = 5u64;
    let last_log_term = 2u64;
    let voted_for_option = None;

    // Act
    let is_legal = handler.check_vote_request_is_legal(
        &request,
        current_term,
        last_log_index,
        last_log_term,
        voted_for_option,
    );

    // Assert
    assert!(is_legal, "Valid request should be accepted");
}

// ============================================================================
// Edge Cases and Protocol Compliance
// ============================================================================

/// Test: Voter handles term 0 (initialization state)
///
/// Scenario: Testing behavior with uninitialized term=0
#[tokio::test]
async fn test_handle_vote_request_term_zero() {
    // Arrange
    let handler = create_handler(2);
    let request = create_vote_request(0, 1, 0, 0);
    let current_term = 0u64;
    let voted_for_option = None;
    let last_log_id = None;
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert - should handle gracefully without panic
    assert_eq!(state_update.term_update, None);
}

/// Test: Very large term numbers (overflow check)
///
/// Scenario: Testing with u64::MAX term values
#[tokio::test]
async fn test_handle_vote_request_large_term_numbers() {
    // Arrange
    let handler = create_handler(2);
    let large_term = u64::MAX;
    let request = create_vote_request(large_term, 1, 100, large_term);
    let current_term = large_term - 1;
    let voted_for_option = None;
    let last_log_id = Some(LogId {
        index: 100,
        term: large_term,
    });
    let raft_log = Arc::new(create_mock_raft_log(last_log_id));

    // Act
    let state_update = handler
        .handle_vote_request(request, current_term, voted_for_option, &raft_log)
        .await
        .unwrap();

    // Assert
    assert_eq!(state_update.term_update, Some(large_term));
}

// ================================================================================================
// Tests for Single-Node Cluster Support (Issue #179)
// ================================================================================================

#[cfg(test)]
mod single_node_election_tests {
    use super::*;
    use crate::ElectionError;
    use crate::Error;
    use crate::MockMembership;
    use crate::MockTransport;
    use crate::RaftNodeConfig;
    use crate::VoteResult;
    use d_engine_proto::server::cluster::NodeMeta;
    use d_engine_proto::server::election::VoteResponse;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_single_node_auto_wins_election() {
        // Arrange
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let mut mock_membership = MockMembership::new();

        // Mock: is_single_node_cluster() returns true for single-node
        mock_membership.expect_is_single_node_cluster().times(1).returning(|| true);

        // voters() should NOT be called (early return before this check)
        mock_membership.expect_voters().times(0);

        let membership = Arc::new(mock_membership);
        let raft_log = Arc::new(create_mock_raft_log(None));
        let mock_transport = MockTransport::new();
        let transport = Arc::new(mock_transport);
        let settings = Arc::new(RaftNodeConfig::default());

        // Act
        let result = handler
            .broadcast_vote_requests(1, membership, &raft_log, &transport, &settings)
            .await;

        // Assert
        assert!(
            result.is_ok(),
            "Single-node should automatically win election"
        );
    }

    #[tokio::test]
    async fn test_three_node_cluster_goes_through_normal_election() {
        // Arrange
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let mut mock_membership = MockMembership::new();

        // Mock: is_single_node_cluster() returns false for multi-node
        mock_membership.expect_is_single_node_cluster().times(1).returning(|| false);

        // Mock: voters() returns 2 peers
        mock_membership.expect_voters().times(1).returning(|| {
            vec![
                NodeMeta {
                    id: 2,
                    address: "127.0.0.1:9082".to_string(),
                    role: 0,
                    status: 2,
                },
                NodeMeta {
                    id: 3,
                    address: "127.0.0.1:9083".to_string(),
                    role: 0,
                    status: 2,
                },
            ]
        });

        let membership = Arc::new(mock_membership);
        let raft_log = Arc::new(create_mock_raft_log(None));

        let mut mock_transport = MockTransport::new();
        // Mock transport to return majority votes
        mock_transport.expect_send_vote_requests().times(1).returning(
            |_req, _retry, _membership| {
                Ok(VoteResult {
                    peer_ids: HashSet::from([2, 3]),
                    responses: vec![
                        Ok(VoteResponse {
                            term: 1,
                            vote_granted: true,
                            last_log_index: 0,
                            last_log_term: 0,
                        }),
                        Ok(VoteResponse {
                            term: 1,
                            vote_granted: true,
                            last_log_index: 0,
                            last_log_term: 0,
                        }),
                    ],
                })
            },
        );

        let transport = Arc::new(mock_transport);
        let settings = Arc::new(RaftNodeConfig::default());

        // Act
        let result = handler
            .broadcast_vote_requests(1, membership, &raft_log, &transport, &settings)
            .await;

        // Assert
        assert!(
            result.is_ok(),
            "Three-node cluster should complete normal election"
        );
    }

    #[tokio::test]
    async fn test_network_partition_with_empty_voters_still_reports_error() {
        // Arrange
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let mut mock_membership = MockMembership::new();

        // Mock: is_single_node_cluster() returns false for multi-node (network partition scenario)
        mock_membership.expect_is_single_node_cluster().times(1).returning(|| false);

        // Mock: voters() returns empty (network partition)
        mock_membership.expect_voters().times(1).returning(Vec::new);

        let membership = Arc::new(mock_membership);
        let raft_log = Arc::new(create_mock_raft_log(None));
        let mock_transport = MockTransport::new();
        let transport = Arc::new(mock_transport);
        let settings = Arc::new(RaftNodeConfig::default());

        // Act
        let result = handler
            .broadcast_vote_requests(1, membership, &raft_log, &transport, &settings)
            .await;

        // Assert
        assert!(result.is_err(), "Network partition should return error");
        assert!(
            matches!(
                result.unwrap_err(),
                Error::Consensus(crate::ConsensusError::Election(
                    ElectionError::NoVotingMemberFound { .. }
                ))
            ),
            "Should return NoVotingMemberFound error"
        );
    }
}
