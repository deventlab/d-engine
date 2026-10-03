//! Unit tests for ElectionHandler implementing core Raft leader election protocol (Section 5.2)
//!
//! These tests verify:
//! - Vote request validation and granting logic
//! - Majority quorum calculation
//! - Log recency checks
//! - Term advancement and state transitions
//! - Edge cases in election rules

use std::sync::Arc;

use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VotedFor;

use crate::MockRaftLog;
use crate::MockTypeConfig;
use crate::election::ElectionCore;
use crate::election::ElectionHandler;

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
        committed: false,
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
        committed: false,
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
        committed: false,
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
        committed: false,
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

    use d_engine_proto::server::cluster::NodeMeta;
    use d_engine_proto::server::election::VoteResponse;

    use super::*;
    use crate::ConsensusError;
    use crate::ElectionError;
    use crate::Error;
    use crate::MockMembership;
    use crate::MockTransport;
    use crate::RaftNodeConfig;

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
        // Mock transport to return majority votes — core layer now dispatches
        // one send_vote_request call per peer (#428), so mock each of the two
        // peers (2, 3) separately instead of one aggregated call.
        mock_transport.expect_send_vote_request().times(2).returning(
            |_peer_id, _req, _retry, _membership| {
                Ok(VoteResponse {
                    term: 1,
                    vote_granted: true,
                    last_log_index: 0,
                    last_log_term: 0,
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

    // ============================================================================
    // test_broadcast_vote_requests_* - Vote Broadcasting Tests
    // ============================================================================

    /// Test: broadcast_vote_requests returns error when cluster has no voting members
    ///
    /// Scenario:
    /// - Multi-node cluster configuration (not single-node)
    /// - Membership returns empty voters list
    /// - Attempt to broadcast vote requests for election
    ///
    /// Expected:
    /// - Returns ElectionError::NoVotingMemberFound
    /// - No RPC calls are made (raft_log and transport expectations: times(0))
    ///
    /// This validates the early validation check that prevents unnecessary
    /// network operations when there are no peers to vote.
    #[tokio::test]
    async fn test_broadcast_vote_requests_returns_error_when_no_voting_members() {
        // Arrange
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;

        // Mock raft_log - expect NO calls since we fail validation before accessing log
        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(0)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        // Mock transport - expect NO calls since we fail validation before sending RPCs
        let mut transport_mock = MockTransport::new();
        transport_mock.expect_send_vote_request().times(0).returning(|_, _, _, _| {
            Ok(VoteResponse {
                term: 1,
                vote_granted: false,
                last_log_index: 1,
                last_log_term: 1,
            })
        });

        // Mock membership with empty voters (core test_utils provides this default)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_is_single_node_cluster().returning(|| false);

        // Create minimal node_config with TempDir (no file system pollution)
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        // Act
        let result = election_handler
            .broadcast_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await;

        // Assert
        assert!(
            result.is_err(),
            "Should return error when no voting members"
        );
        assert!(
            matches!(
                result.unwrap_err(),
                Error::Consensus(ConsensusError::Election(
                    ElectionError::NoVotingMemberFound { candidate_id: 1 }
                ))
            ),
            "Expected NoVotingMemberFound error with candidate_id=1"
        );
    }

    /// Test: broadcast_vote_requests rejects when peer's log has a higher term
    ///
    /// Scenario:
    /// - Two-node cluster (candidate + 1 peer)
    /// - Candidate's last log: index=1, term=3
    /// - Peer denies the vote and reports last_log_term=4 (higher than candidate's)
    ///
    /// Expected:
    /// - Returns ElectionError::QuorumFailure with only the candidate's own vote: a peer with
    ///   a more recent log term denies, so the candidate must not win this election.
    ///   (A denial is just a missing vote; there is no separate log-conflict outcome.)
    #[tokio::test]
    async fn test_broadcast_vote_requests_rejects_on_peer_log_higher_term() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;
        let my_last_log_index = 1;
        let my_last_log_term = 3;

        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock.expect_last_log_id().times(1).returning(move || {
            Some(LogId {
                index: my_last_log_index,
                term: my_last_log_term,
            })
        });

        let mut transport_mock = MockTransport::new();
        transport_mock
            .expect_send_vote_request()
            .withf(|peer_id, _, _, _| *peer_id == 2)
            .times(1)
            .returning(move |_, _, _, _| {
                Ok(VoteResponse {
                    term,
                    vote_granted: false,
                    last_log_index: my_last_log_index,
                    last_log_term: my_last_log_term + 1,
                })
            });

        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_voters().returning(|| {
            use d_engine_proto::common::NodeRole::Follower;
            use d_engine_proto::common::NodeStatus;
            use d_engine_proto::server::cluster::NodeMeta;

            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });

        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        let e = election_handler
            .broadcast_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::QuorumFailure {
                    required: 2,
                    succeed: 1,
                }))
            ),
            "Expected QuorumFailure with only the candidate's own vote, got: {e:?}"
        );
    }

    /// Test: broadcast_vote_requests succeeds when receiving majority of positive votes
    ///
    /// Scenario:
    /// - Two-node cluster (candidate + 1 peer)
    /// - Candidate broadcasts vote request with term=1
    /// - Peer responds with vote_granted=true
    /// - Candidate achieves majority (1 self + 1 peer = 2/2)
    ///
    /// Expected:
    /// - Returns Ok(()) indicating election success
    ///
    /// This is the core "winning election" scenario in Raft where a candidate
    /// successfully obtains majority votes and can transition to Leader role.
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_broadcast_vote_requests_case3`
    #[tokio::test]
    async fn test_broadcast_vote_requests_wins_election_with_majority_votes() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;

        // Mock raft_log - will be called once to get last log info for vote request
        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(1)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        // Mock transport - returns successful vote from peer 2 (core layer now
        // dispatches one send_vote_request call per peer, #428).
        let mut transport_mock = MockTransport::new();
        transport_mock
            .expect_send_vote_request()
            .withf(|peer_id, _, _, _| *peer_id == 2)
            .times(1)
            .returning(|_, _, _, _| {
                Ok(VoteResponse {
                    term: 1,
                    vote_granted: true,
                    last_log_index: 1,
                    last_log_term: 1,
                })
            });

        // Mock membership - two-node cluster with one voting peer
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_initial_cluster_size().returning(|| 2);
        membership.expect_voters().returning(move || {
            use d_engine_proto::common::NodeRole::Follower;
            use d_engine_proto::common::NodeStatus;
            use d_engine_proto::server::cluster::NodeMeta;

            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });

        // Create minimal node_config with TempDir
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        // Execute
        let result = election_handler
            .broadcast_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await;

        // Verify - should succeed with majority votes
        assert!(
            result.is_ok(),
            "Expected successful election with majority votes, got: {result:?}"
        );
    }

    /// Transport for the slow-peer election test: peer 2 grants the vote at once, any other
    /// peer stays silent for a long time and then fails, like a dead node whose RPC retries
    /// are still running.
    ///
    /// Hand-written instead of `MockTransport`: mockall holds an internal lock while a mock
    /// closure runs, so a "slow" closure would also stall the fast peer's call and make the
    /// timing depend on which task happened to run first.
    struct SlowPeerTransport;

    const SLOW_PEER_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

    /// Peer 2 grants at once; peers 6, 7 and 8 deny at once with a plain denial (same term,
    /// older log); every other peer stays silent for `SLOW_PEER_DELAY` and then fails.
    async fn slow_peer_answer(peer_id: u32) -> crate::Result<VoteResponse> {
        if peer_id == 2 {
            return Ok(VoteResponse {
                term: 1,
                vote_granted: true,
                last_log_index: 1,
                last_log_term: 1,
            });
        }
        if (6..=8).contains(&peer_id) {
            return Ok(VoteResponse {
                term: 1,
                vote_granted: false,
                last_log_index: 0,
                last_log_term: 1,
            });
        }
        tokio::time::sleep(SLOW_PEER_DELAY).await;
        Err(Error::from(crate::NetworkError::ServiceUnavailable(
            "peer is down".to_string(),
        )))
    }

    #[async_trait::async_trait]
    impl crate::Transport<SlowVoteTypeConfig> for SlowPeerTransport {
        async fn send_vote_request(
            &self,
            peer_id: u32,
            _request: VoteRequest,
            _retry: &crate::RetryPolicies,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
        ) -> crate::Result<VoteResponse> {
            slow_peer_answer(peer_id).await
        }

        async fn send_pre_vote_request(
            &self,
            peer_id: u32,
            _request: VoteRequest,
            _retry: &crate::RetryPolicies,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
        ) -> crate::Result<VoteResponse> {
            slow_peer_answer(peer_id).await
        }

        async fn send_cluster_update(
            &self,
            _req: d_engine_proto::server::cluster::ClusterConfChangeRequest,
            _retry: &crate::RetryPolicies,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
        ) -> crate::Result<crate::ClusterUpdateResult> {
            unimplemented!("not used by the election test")
        }

        async fn join_cluster(
            &self,
            _leader_id: u32,
            _request: d_engine_proto::server::cluster::JoinRequest,
            _retry: crate::BackoffPolicy,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
        ) -> crate::Result<d_engine_proto::server::cluster::JoinResponse> {
            unimplemented!("not used by the election test")
        }

        async fn discover_leader(
            &self,
            _request: d_engine_proto::server::cluster::LeaderDiscoveryRequest,
            _rpc_enable_compression: bool,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
        ) -> crate::Result<Vec<d_engine_proto::server::cluster::LeaderDiscoveryResponse>> {
            unimplemented!("not used by the election test")
        }

        async fn send_snapshot(
            &self,
            _peer_id: u32,
            _metadata: d_engine_proto::server::storage::SnapshotMetadata,
            _leader_term: u64,
            _state_machine_handler: Arc<crate::alias::SMHOF<SlowVoteTypeConfig>>,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
            _config: crate::SnapshotConfig,
        ) -> crate::Result<()> {
            unimplemented!("not used by the election test")
        }

        async fn open_replication_stream(
            &self,
            _peer_id: u32,
            _membership: Arc<crate::alias::MOF<SlowVoteTypeConfig>>,
            _compress: bool,
            _send_queue_capacity: usize,
        ) -> crate::Result<crate::ReplicationStream> {
            unimplemented!("not used by the election test")
        }
    }

    /// `MockTypeConfig` with `SlowPeerTransport` as the transport.
    #[derive(Debug)]
    struct SlowVoteTypeConfig;

    impl crate::TypeConfig for SlowVoteTypeConfig {
        type R = MockRaftLog;
        type SE = crate::MockStorageEngine;
        type E = crate::MockElectionCore<Self>;
        type TR = SlowPeerTransport;
        type SM = crate::MockStateMachine;
        type M = MockMembership<Self>;
        type REP = crate::MockReplicationCore<Self>;
        type C = crate::MockCommitHandler;
        type SMH = crate::MockStateMachineHandler<Self>;
        type SMW = crate::MockStateMachineWriterOps<Self>;
        type SNP = crate::MockSnapshotPolicy;
        type PE = crate::MockPurgeExecutor;
    }

    /// Test: broadcast_vote_requests must not wait for slow peers once a majority has voted
    ///
    /// Scenario:
    /// - Three-node cluster: candidate (node 1) + peers 2 and 3
    /// - Peer 2 grants the vote immediately (self + peer 2 = 2/3, a majority)
    /// - Peer 3 is dead: its RPC only fails after a long retry sequence
    ///
    /// Expected:
    /// - Returns Ok(()) as soon as the majority is reached, without waiting for peer 3
    ///
    /// The caller awaits this inside the Raft loop. If it waits for the dead peer, the
    /// candidate's own election timer expires meanwhile and the won election is discarded
    /// by the next term, so a cluster with one node down can fail to elect a leader.
    ///
    /// The clock is paused: time only moves when every task is idle. A broadcast that
    /// waits for peer 3 lets the runtime jump to the 1 s timeout first, deterministically.
    #[tokio::test(start_paused = true)]
    async fn test_broadcast_vote_requests_returns_on_majority_without_waiting_for_slow_peer() {
        let election_handler = ElectionHandler::<SlowVoteTypeConfig>::new(1);

        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(1)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        let mut membership = MockMembership::<SlowVoteTypeConfig>::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_voters().returning(|| {
            [2, 3]
                .into_iter()
                .map(|id| NodeMeta {
                    id,
                    address: format!("http://127.0.0.1:5500{id}"),
                    role: 0,
                    status: 2,
                })
                .collect()
        });

        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            election_handler.broadcast_vote_requests(
                1,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(SlowPeerTransport),
                &Arc::new(node_config),
            ),
        )
        .await
        .expect(
            "broadcast_vote_requests waited for the slow peer instead of returning once a \
             majority had voted",
        );

        assert!(
            result.is_ok(),
            "majority (self + peer 2) must win the election, got: {result:?}"
        );
    }

    /// Test: a round that can no longer reach a majority must not wait for the silent peers.
    ///
    /// Scenario (5 nodes, a majority is 3):
    /// - Peers 6, 7 and 8 deny at once (plain denial: older log, same term).
    /// - Peer 3 is dead: its RPC only fails after a long retry sequence.
    /// - Own vote + peer 3 is at most 2 of 5, so the round is already lost.
    ///
    /// Expected:
    /// - QuorumFailure right after the third denial, without waiting for peer 3. The caller
    ///   awaits this inside the Raft loop; waiting for a dead peer keeps the node from
    ///   handling the heartbeat of whoever won, and (with PreVote) delays the next attempt.
    ///   raft-rs reports `Lost` as soon as the rejections make a majority impossible.
    #[tokio::test(start_paused = true)]
    async fn test_round_returns_quorum_failure_once_majority_is_impossible() {
        for pre_vote in [false, true] {
            let election_handler = ElectionHandler::<SlowVoteTypeConfig>::new(1);

            let mut raft_log_mock = MockRaftLog::new();
            raft_log_mock
                .expect_last_log_id()
                .times(1)
                .returning(|| Some(LogId { index: 1, term: 1 }));

            let mut membership = MockMembership::<SlowVoteTypeConfig>::new();
            membership.expect_is_single_node_cluster().returning(|| false);
            membership.expect_voters().returning(|| {
                [3, 6, 7, 8]
                    .into_iter()
                    .map(|id| NodeMeta {
                        id,
                        address: format!("http://127.0.0.1:5500{id}"),
                        role: 0,
                        status: 2,
                    })
                    .collect()
            });

            let node_config = RaftNodeConfig::new().expect("Should create default config");
            let node_config = node_config.validate().expect("Should validate config");
            let membership = Arc::new(membership);
            let raft_log = Arc::new(raft_log_mock);
            let transport = Arc::new(SlowPeerTransport);
            let node_config = Arc::new(node_config);

            let round = async {
                if pre_vote {
                    election_handler
                        .broadcast_pre_vote_requests(
                            2,
                            membership,
                            &raft_log,
                            &transport,
                            &node_config,
                        )
                        .await
                } else {
                    election_handler
                        .broadcast_vote_requests(1, membership, &raft_log, &transport, &node_config)
                        .await
                }
            };
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), round)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "pre_vote={pre_vote}: the round waited for the dead peer although a \
                         majority was already impossible"
                    )
                });

            assert!(
                matches!(
                    result,
                    Err(Error::Consensus(ConsensusError::Election(
                        ElectionError::QuorumFailure { .. }
                    )))
                ),
                "pre_vote={pre_vote}: expected QuorumFailure, got: {result:?}"
            );
        }
    }

    /// Test: broadcast_vote_requests steps down when a peer responds with a higher term
    ///
    /// Scenario:
    /// - Two-node cluster (candidate + 1 peer)
    /// - Candidate broadcasts vote request for term=1
    /// - Peer denies the vote and reports term=2 (higher than the candidate's)
    ///
    /// Expected:
    /// - Returns ElectionError::HigherTerm(2), signaling the candidate must
    ///   step down rather than continue the election (Raft §5.1)
    #[tokio::test]
    async fn test_broadcast_vote_requests_steps_down_on_peer_higher_term() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;
        let higher_term = term + 1;

        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(1)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        let mut transport_mock = MockTransport::new();
        transport_mock
            .expect_send_vote_request()
            .withf(|peer_id, _, _, _| *peer_id == 2)
            .times(1)
            .returning(move |_, _, _, _| {
                Ok(VoteResponse {
                    term: higher_term,
                    vote_granted: false,
                    last_log_index: 1,
                    last_log_term: 1,
                })
            });

        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_voters().returning(|| {
            use d_engine_proto::common::NodeRole::Follower;
            use d_engine_proto::common::NodeStatus;
            use d_engine_proto::server::cluster::NodeMeta;

            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });

        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        let e = election_handler
            .broadcast_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(t))) if t == higher_term
            ),
            "Expected HigherTerm({higher_term}), got: {e:?}"
        );
    }

    /// Test: broadcast_vote_requests rejects when peer has higher log index (same term)
    ///
    /// Scenario:
    /// - Two-node cluster (candidate + 1 peer)
    /// - Candidate and peer share the same last_log_term
    /// - Peer's last_log_index is higher (more entries in the same term)
    ///
    /// Expected:
    /// - Returns ElectionError::QuorumFailure with only the candidate's own vote: same-term but
    ///   more entries means the peer's log is more recent (Raft §5.4.1), so it denies and the
    ///   candidate must not win.
    #[tokio::test]
    async fn test_broadcast_vote_requests_rejects_on_peer_log_higher_index_same_term() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;
        let my_last_log_index = 1;
        let my_last_log_term = 3;

        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock.expect_last_log_id().times(1).returning(move || {
            Some(LogId {
                index: my_last_log_index,
                term: my_last_log_term,
            })
        });

        let mut transport_mock = MockTransport::new();
        transport_mock
            .expect_send_vote_request()
            .withf(|peer_id, _, _, _| *peer_id == 2)
            .times(1)
            .returning(move |_, _, _, _| {
                Ok(VoteResponse {
                    term,
                    vote_granted: false,
                    last_log_index: my_last_log_index + 1,
                    last_log_term: my_last_log_term,
                })
            });

        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_voters().returning(|| {
            use d_engine_proto::common::NodeRole::Follower;
            use d_engine_proto::common::NodeStatus;
            use d_engine_proto::server::cluster::NodeMeta;

            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });

        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        let e = election_handler
            .broadcast_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::QuorumFailure {
                    required: 2,
                    succeed: 1,
                }))
            ),
            "Expected QuorumFailure with only the candidate's own vote, got: {e:?}"
        );
    }

    /// Test: broadcast_vote_requests fails quorum on a plain denial (no term/log escalation)
    ///
    /// Scenario:
    /// - Two-node cluster (candidate + 1 peer)
    /// - Peer denies the vote with the same term and a strictly older log —
    ///   neither HigherTerm nor LogConflict applies
    ///
    /// Expected:
    /// - Returns ElectionError::QuorumFailure { required: 2, succeed: 1 } —
    ///   only the candidate's own vote counts, 1/2 is not a majority
    #[tokio::test]
    async fn test_broadcast_vote_requests_fails_quorum_on_plain_denial() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;
        let my_last_log_index = 5;
        let my_last_log_term = 5;

        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock.expect_last_log_id().times(1).returning(move || {
            Some(LogId {
                index: my_last_log_index,
                term: my_last_log_term,
            })
        });

        let mut transport_mock = MockTransport::new();
        transport_mock
            .expect_send_vote_request()
            .withf(|peer_id, _, _, _| *peer_id == 2)
            .times(1)
            .returning(move |_, _, _, _| {
                Ok(VoteResponse {
                    term,
                    vote_granted: false,
                    last_log_index: my_last_log_index - 1,
                    last_log_term: my_last_log_term,
                })
            });

        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_voters().returning(|| {
            use d_engine_proto::common::NodeRole::Follower;
            use d_engine_proto::common::NodeStatus;
            use d_engine_proto::server::cluster::NodeMeta;

            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });

        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let node_config = node_config.validate().expect("Should validate config");

        let e = election_handler
            .broadcast_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::QuorumFailure {
                    required: 2,
                    succeed: 1,
                }))
            ),
            "Expected QuorumFailure {{ required: 2, succeed: 1 }}, got: {e:?}"
        );
    }

    // ============================================================================
    // test_handle_vote_request_* - Processing Incoming Vote Requests
    // ============================================================================

    /// Test: handle_vote_request grants vote for valid higher term request
    ///
    /// Scenario:
    /// - Current node is at term 1
    /// - Receives vote request for term 2 (higher)
    /// - Request has more recent log (index 2 vs local index 1)
    /// - Node has not voted in current term
    ///
    /// Expected:
    /// - Returns state update with:
    ///   - new_voted_for = Some(candidate_id)
    ///   - term_update = Some(2) (advance to new term)
    ///
    /// This validates the core Raft rule: grant vote to first valid request
    /// with higher term and at-least-as-up-to-date log.
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_handle_vote_request_case1`
    #[tokio::test]
    async fn test_handle_vote_request_grants_vote_for_valid_higher_term() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        // Mock raft_log with local log state
        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(1)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        let current_term = 1;
        let request_term = current_term + 1;

        // Vote request from candidate with higher term and more recent log
        let vote_request = VoteRequest {
            term: request_term,
            candidate_id: 1,
            last_log_index: 2, // More recent than local (1)
            last_log_term: 1,
        };

        let voted_for_option = None; // Haven't voted yet

        // Execute
        let result = election_handler
            .handle_vote_request(
                vote_request,
                current_term,
                voted_for_option,
                &Arc::new(raft_log_mock),
            )
            .await;

        // Verify
        assert!(
            result.is_ok(),
            "Should grant vote for valid higher term request"
        );

        let state_update = result.unwrap();
        assert!(
            state_update.new_voted_for.is_some(),
            "Should update voted_for"
        );
        assert_eq!(
            state_update.term_update,
            Some(request_term),
            "Should advance term to request term"
        );
    }

    /// Test: handle_vote_request rejects vote for lower term request
    ///
    /// Scenario:
    /// - Current node is at term 10
    /// - Receives vote request for term 9 (lower/stale)
    /// - Request has more recent log (doesn't matter)
    ///
    /// Expected:
    /// - Returns state update with:
    ///   - new_voted_for = None (vote not granted)
    ///   - term_update = None (stay at current term)
    ///
    /// This validates the Raft rule: reject requests from lower terms,
    /// preventing stale candidates from disrupting the cluster.
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_handle_vote_request_case2`
    #[tokio::test]
    async fn test_handle_vote_request_rejects_vote_for_lower_term() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        // Mock raft_log
        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(1)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        let current_term = 10;
        let request_term = current_term - 1; // Stale term

        // Vote request with lower term (should be rejected regardless of log)
        let vote_request = VoteRequest {
            term: request_term,
            candidate_id: 1,
            last_log_index: 2,
            last_log_term: 1,
        };

        let voted_for_option = None;

        // Execute
        let result = election_handler
            .handle_vote_request(
                vote_request,
                current_term,
                voted_for_option,
                &Arc::new(raft_log_mock),
            )
            .await;

        // Verify
        assert!(result.is_ok(), "Should not error on stale request");

        let state_update = result.unwrap();
        assert!(
            state_update.new_voted_for.is_none(),
            "Should NOT grant vote for lower term"
        );
        assert_eq!(
            state_update.term_update, None,
            "Should NOT update term for stale request"
        );
    }

    // ============================================================================
    // test_check_vote_request_is_legal_* - Vote Request Legality Validation
    // ============================================================================

    /// Test: check_vote_request_is_legal rejects when current term >= request term
    ///
    /// TODO(migration): This test uses `setup_raft_components()` unnecessarily.
    /// The `check_vote_request_is_legal()` method is a pure function that only needs
    /// an ElectionHandler instance. No file system or network components are needed.
    ///
    /// **Simplification needed**:
    /// Replace `setup_raft_components()` with simple `ElectionHandler::new(1)`
    ///
    /// Scenario:
    /// - Current term is 1 or 2
    /// - Vote request is for term 1
    /// - Local log: index=1, term=1
    /// - Already voted for candidate 1 in term 1
    ///
    /// Expected:
    /// - Returns false (reject vote)
    /// - Reason: Current term is not less than request term
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_1_1`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_rejects_when_current_term_not_lower() {
        // TODO: Simplify to just `let election_handler = ElectionHandler::<MockTypeConfig>::new(1);`
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let vote_request = VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 1,
            last_log_term: 1,
        };
        let last_log_index = 1;
        let last_log_term = 1;
        let voted_for_id = 1;
        let voted_for_term = 1;

        // Test 1: current_term = request_term (equal)
        let current_term = 1;
        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when current_term equals request term"
        );

        // Test 2: current_term > request_term
        let current_term = 2;
        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when current_term is higher than request term"
        );
    }

    /// Test: check_vote_request_is_legal rejects when request log term is not higher
    ///
    /// TODO(migration): Uses `setup_raft_components()` unnecessarily - can be simplified.
    ///
    /// Scenario:
    /// - Current term = 1, request term = 1
    /// - Request log: index=1, term=1
    /// - Local log: index=1, term=2 (higher) OR term=1 (equal)
    /// - Already voted for candidate 1 in term 1
    ///
    /// Expected:
    /// - Returns false (reject vote)
    /// - Reason: Request log term is not more recent than local
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_1_2`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_rejects_when_request_log_not_more_recent() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let current_term = 1;
        let vote_request = VoteRequest {
            term: current_term,
            candidate_id: 1,
            last_log_index: 1,
            last_log_term: 1,
        };
        let last_log_index = 1;
        let voted_for_id = 1;
        let voted_for_term = 1;

        // Test 1: Local log term is higher (2 > 1)
        let last_log_term = 2;
        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when local log term is higher"
        );

        // Test 2: Log terms are equal (1 = 1)
        let last_log_term = 1;
        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when log terms are equal but already voted"
        );
    }

    /// Test: check_vote_request_is_legal accepts when request log is more recent
    ///
    /// TODO(migration): Uses `setup_raft_components()` unnecessarily - can be simplified.
    ///
    /// Scenario:
    /// - Current term = 1, request term = 1
    /// - Request log: index=2, term=1 (more entries)
    /// - Local log: index=1, term=1
    /// - Have not voted yet (voted_for = None)
    ///
    /// Expected:
    /// - Returns true (grant vote)
    /// - Reason: Request has same term but higher index (more up-to-date)
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_1_3`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_accepts_when_request_log_more_recent() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let current_term = 1;
        let last_log_index = 1;
        let last_log_term = 1;

        let vote_request = VoteRequest {
            term: current_term,
            candidate_id: 1,
            last_log_index: last_log_index + 1, // Higher index
            last_log_term,
        };

        assert!(
            election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                None, // Haven't voted yet
            ),
            "Should accept when request has higher log index (same term)"
        );
    }

    /// Test: check_vote_request_is_legal rejects when request log index is lower
    ///
    /// TODO(migration): Uses `setup_raft_components()` unnecessarily - can be simplified.
    ///
    /// Scenario:
    /// - Current term = 1, request term = 1
    /// - Request log: index=1, term=1
    /// - Local log: index=2, term=1 (more entries)
    /// - Already voted for candidate 1 in term 1
    ///
    /// Expected:
    /// - Returns false (reject vote)
    /// - Reason: Local log has more entries (higher index) in same term
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_1_4`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_rejects_when_local_log_more_recent() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let current_term = 1;
        let vote_request = VoteRequest {
            term: current_term,
            candidate_id: 1,
            last_log_index: 1,
            last_log_term: 1,
        };
        let last_log_index = 2; // Local has more entries
        let last_log_term = 1;
        let voted_for_id = 1;
        let voted_for_term = 1;

        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when local log is more up-to-date"
        );
    }

    /// Test: check_vote_request_is_legal rejects when already voted for different candidate
    ///
    /// TODO(migration): Uses `setup_raft_components()` unnecessarily - can be simplified.
    ///
    /// Scenario:
    /// - Current term = 1, request term = 1
    /// - Request from candidate 1, log: index=3, term=1
    /// - Local log: index=2, term=1 (request is more recent)
    /// - Already voted for candidate 3 (different) in term 1
    ///
    /// Expected:
    /// - Returns false (reject vote)
    /// - Reason: Already granted vote to a different candidate in this term
    ///
    /// This validates the Raft rule: at most one vote per term.
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_2_1`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_rejects_when_already_voted_for_different_candidate() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let current_term = 1;
        let vote_request = VoteRequest {
            term: current_term,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 1,
        };
        let last_log_index = 2;
        let last_log_term = 1;

        let voted_for_id = 3; // Already voted for different candidate
        let voted_for_term = 1;

        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when already voted for different candidate in same term"
        );
    }

    /// Test: check_vote_request_is_legal rejects when voted in higher term
    ///
    /// TODO(migration): Uses `setup_raft_components()` unnecessarily - can be simplified.
    ///
    /// Scenario:
    /// - Current term = 1, request term = 1
    /// - Request from candidate 1, log: index=3, term=1
    /// - Local log: index=2, term=1
    /// - Previously voted for candidate 1 in term 10 (higher term)
    ///
    /// Expected:
    /// - Returns false (reject vote)
    /// - Reason: Already voted in a higher term (should not happen in normal operation)
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_2_2`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_rejects_when_voted_in_higher_term() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let current_term = 1;
        let vote_request = VoteRequest {
            term: current_term,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 1,
        };
        let last_log_index = 2;
        let last_log_term = 1;

        let voted_for_id = 1;
        let voted_for_term = 10; // Voted in higher term

        assert!(
            !election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should reject when already voted in higher term"
        );
    }

    /// Test: check_vote_request_is_legal accepts when re-voting for same candidate
    ///
    /// TODO(migration): Uses `setup_raft_components()` unnecessarily - can be simplified.
    ///
    /// Scenario:
    /// - Current term = 10, request term = 10
    /// - Request from candidate 1, log: index=3, term=1
    /// - Local log: index=2, term=1 (request is more recent)
    /// - Previously voted for candidate 1 in term 1 (lower term)
    ///
    /// Expected:
    /// - Returns true (grant vote)
    /// - Reason: Can vote again for same candidate in new term with more recent log
    ///
    /// This validates idempotent vote granting: same candidate can receive vote
    /// again in a new term.
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_check_vote_request_is_legal_case_2_3`
    #[tokio::test]
    async fn test_check_vote_request_is_legal_accepts_revote_for_same_candidate_new_term() {
        let _temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let node_config = RaftNodeConfig::new().expect("Should create default config");
        let _node_config = node_config.validate().expect("Should validate config");
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

        let current_term = 10; // New term
        let vote_request = VoteRequest {
            term: current_term,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 1,
        };
        let last_log_index = 2;
        let last_log_term = 1;

        let voted_for_id = 1; // Same candidate
        let voted_for_term = 1; // But in older term

        assert!(
            election_handler.check_vote_request_is_legal(
                &vote_request,
                current_term,
                last_log_index,
                last_log_term,
                Some(VotedFor {
                    voted_for_id,
                    voted_for_term,
                    committed: false
                })
            ),
            "Should accept re-vote for same candidate in new term"
        );
    }

    // ========================================================================
    // test_broadcast_pre_vote_requests_* - PreVote (sender side)
    //
    // PreVote asks "would you vote for me at term+1?" before the candidate bumps its
    // term. It must never touch term/vote state: the raft log mocks below only allow
    // `last_log_id`, so any persistence call would panic the test.
    // ========================================================================

    fn pre_vote_voter(id: u32) -> NodeMeta {
        use d_engine_proto::common::NodeRole::Follower;
        use d_engine_proto::common::NodeStatus;

        NodeMeta {
            id,
            address: format!("http://127.0.0.1:{}", 55000 + id),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }
    }

    fn pre_vote_membership(peer_ids: Vec<u32>) -> MockMembership<MockTypeConfig> {
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership
            .expect_voters()
            .returning(move || peer_ids.iter().map(|id| pre_vote_voter(*id)).collect());
        membership
    }

    fn pre_vote_raft_log() -> MockRaftLog {
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_log_id().returning(|| Some(LogId { index: 1, term: 1 }));
        raft_log
    }

    /// The responder's log is strictly behind the candidate's (`pre_vote_raft_log` is index 1,
    /// term 1), a plain denial.
    fn pre_vote_response(
        term: u64,
        granted: bool,
    ) -> VoteResponse {
        VoteResponse {
            term,
            vote_granted: granted,
            last_log_index: 0,
            last_log_term: 1,
        }
    }

    async fn run_pre_vote(
        membership: MockMembership<MockTypeConfig>,
        transport: MockTransport<MockTypeConfig>,
        term: u64,
    ) -> crate::Result<()> {
        let node_config = RaftNodeConfig::new()
            .expect("Should create default config")
            .validate()
            .expect("Should validate config");
        ElectionHandler::<MockTypeConfig>::new(1)
            .broadcast_pre_vote_requests(
                term,
                Arc::new(membership),
                &Arc::new(pre_vote_raft_log()),
                &Arc::new(transport),
                &Arc::new(node_config),
            )
            .await
    }

    /// Test: a majority of PreVote grants lets the candidate proceed to a real election.
    ///
    /// Scenario:
    /// - Three nodes (candidate + peers 2, 3); peer 2 would grant, peer 3 is unreachable.
    ///
    /// Expected:
    /// - Ok(()): own vote + peer 2 is a majority of 3.
    /// - Only PreVote RPCs are sent; no real RequestVote is sent.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_succeeds_with_majority_of_grants() {
        let mut transport = MockTransport::new();
        transport.expect_send_vote_request().times(0);
        transport
            .expect_send_pre_vote_request()
            .times(2)
            .returning(|peer_id, request, _, _| {
                if peer_id == 2 {
                    Ok(pre_vote_response(request.term - 1, true))
                } else {
                    Err(Error::from(crate::NetworkError::ServiceUnavailable(
                        "peer is down".to_string(),
                    )))
                }
            });

        let result = run_pre_vote(pre_vote_membership(vec![2, 3]), transport, 2).await;

        assert!(
            result.is_ok(),
            "expected majority of grants, got: {result:?}"
        );
    }

    /// Test: a single-node cluster passes PreVote without contacting anyone.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_single_node_cluster_succeeds_without_rpc() {
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| true);
        let mut transport = MockTransport::new();
        transport.expect_send_pre_vote_request().times(0);
        transport.expect_send_vote_request().times(0);

        let result = run_pre_vote(membership, transport, 2).await;

        assert!(result.is_ok());
    }

    /// Test: unreachable peers are NOT counted as grants.
    ///
    /// Scenario:
    /// - Three nodes, both peers unreachable (the node is isolated).
    ///
    /// Expected:
    /// - QuorumFailure. If errors counted as grants, an isolated node would pass PreVote on
    ///   its own, bump its term every round, and PreVote would protect nothing.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_does_not_count_unreachable_peers_as_grants() {
        let mut transport = MockTransport::new();
        transport.expect_send_pre_vote_request().times(2).returning(|_, _, _, _| {
            Err(Error::from(crate::NetworkError::ServiceUnavailable(
                "peer is down".to_string(),
            )))
        });

        let e = run_pre_vote(pre_vote_membership(vec![2, 3]), transport, 2).await.unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(
                    ElectionError::QuorumFailure { .. }
                ))
            ),
            "expected QuorumFailure, got: {e:?}"
        );
    }

    /// Test: plain denials from the peers fail the PreVote round.
    ///
    /// Scenario:
    /// - Three nodes; both peers answer "would not vote" (they still hear from a leader).
    ///
    /// Expected:
    /// - QuorumFailure, so the caller keeps its term unchanged.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_fails_quorum_when_peers_deny() {
        let mut transport = MockTransport::new();
        transport
            .expect_send_pre_vote_request()
            .times(2)
            .returning(|_, request, _, _| Ok(pre_vote_response(request.term - 1, false)));

        let e = run_pre_vote(pre_vote_membership(vec![2, 3]), transport, 2).await.unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(
                    ElectionError::QuorumFailure { .. }
                ))
            ),
            "expected QuorumFailure, got: {e:?}"
        );
    }

    /// Test: a peer reporting a higher term than the PreVote's term makes the candidate
    /// step down to catch up.
    ///
    /// Scenario:
    /// - Two nodes; the peer denies and reports term 5 while the PreVote term is 2.
    ///
    /// Expected:
    /// - ElectionError::HigherTerm(5), same as the real vote.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_reports_peer_higher_term() {
        let mut transport = MockTransport::new();
        transport
            .expect_send_pre_vote_request()
            .times(1)
            .returning(|_, _, _, _| Ok(pre_vote_response(5, false)));

        let e = run_pre_vote(pre_vote_membership(vec![2]), transport, 2).await.unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(5)))
            ),
            "expected HigherTerm(5), got: {e:?}"
        );
    }

    /// Test: a peer already at the PreVote's term means this node is behind and must catch up.
    ///
    /// Scenario:
    /// - The node's real term is 1, so the PreVote asks about term 2.
    /// - The peer is already at term 2 (an election happened while this node was away) and
    ///   denies, reporting term 2.
    ///
    /// Expected:
    /// - HigherTerm(2). Comparing the peer's term with the hypothetical term (2) using a strict
    ///   `>` would call this a plain denial: the node would never learn it is behind and would
    ///   keep retrying PreVote at a term the cluster has already passed. The peer's term must
    ///   be compared with the node's real term (the PreVote term minus 1).
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_reports_peer_at_pre_vote_term_as_higher() {
        let mut transport = MockTransport::new();
        transport
            .expect_send_pre_vote_request()
            .times(1)
            .returning(|_, request, _, _| Ok(pre_vote_response(request.term, false)));

        let e = run_pre_vote(pre_vote_membership(vec![2]), transport, 2).await.unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(2)))
            ),
            "expected HigherTerm(2), got: {e:?}"
        );
    }

    /// Test: a peer at the node's real term is a plain denial, not a reason to step down.
    ///
    /// Scenario:
    /// - The node's real term is 1 (PreVote term 2); the peer is also at term 1 and denies
    ///   because it still hears from a leader.
    ///
    /// Expected:
    /// - QuorumFailure, so the node keeps its term and simply retries after the next timeout.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_peer_at_same_real_term_is_plain_denial() {
        let mut transport = MockTransport::new();
        transport
            .expect_send_pre_vote_request()
            .times(1)
            .returning(|_, request, _, _| Ok(pre_vote_response(request.term - 1, false)));

        let e = run_pre_vote(pre_vote_membership(vec![2]), transport, 2).await.unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(
                    ElectionError::QuorumFailure { .. }
                ))
            ),
            "expected QuorumFailure, got: {e:?}"
        );
    }

    // ========================================================================
    // Tally tests shared by the real vote and PreVote
    //
    // Both rounds use the same counting loop, so every scenario runs against both. A denial is
    // just a missing vote: the round only ends when a majority is reached, when a peer reveals
    // a higher term (the node must catch up at once), or when every peer has answered.
    // Reference scenarios: raft-rs `test_dueling_candidates`, `test_prevote_with_split_vote`;
    // openraft `handle_vote_resp_test` / `handle_pre_vote_resp_test` ("reject keeps waiting").
    // ========================================================================

    #[derive(Clone, Copy, Debug)]
    enum Round {
        Vote,
        PreVote,
    }

    const BOTH_ROUNDS: [Round; 2] = [Round::Vote, Round::PreVote];

    /// What a peer answers. The candidate's own log is index 1, term 1 (`pre_vote_raft_log`).
    #[derive(Clone, Copy, Debug)]
    enum Reply {
        Grant,
        /// Plain denial: the peer's term equals the node's real term; only its log differs.
        Deny {
            log_index: u64,
            log_term: u64,
        },
        /// Denial that reveals a term above the node's.
        DenyHigherTerm {
            peer_term: u64,
        },
        /// The RPC fails (peer down).
        Down,
    }

    /// Peer log equal to the candidate's (typical after steady-state replication).
    fn deny_equal_log() -> Reply {
        Reply::Deny {
            log_index: 1,
            log_term: 1,
        }
    }

    /// Peer log strictly newer than the candidate's (higher index, same term).
    fn deny_newer_log() -> Reply {
        Reply::Deny {
            log_index: 5,
            log_term: 1,
        }
    }

    /// Peer log strictly older than the candidate's.
    fn deny_older_log() -> Reply {
        Reply::Deny {
            log_index: 0,
            log_term: 1,
        }
    }

    /// The term the round asks about is 2. For a real vote that is the node's new term; for
    /// PreVote it is the term the node wants to move to, so its real term is 1.
    const ASKED_TERM: u64 = 2;

    fn node_term(round: Round) -> u64 {
        match round {
            Round::Vote => ASKED_TERM,
            Round::PreVote => ASKED_TERM - 1,
        }
    }

    fn answer(
        round: Round,
        replies: &[(u32, Reply)],
        peer_id: u32,
    ) -> crate::Result<VoteResponse> {
        let reply = replies.iter().find(|(id, _)| *id == peer_id).expect("peer has a reply").1;
        match reply {
            Reply::Grant => Ok(VoteResponse {
                term: node_term(round),
                vote_granted: true,
                last_log_index: 1,
                last_log_term: 1,
            }),
            Reply::Deny {
                log_index,
                log_term,
            } => Ok(VoteResponse {
                term: node_term(round),
                vote_granted: false,
                last_log_index: log_index,
                last_log_term: log_term,
            }),
            Reply::DenyHigherTerm { peer_term } => Ok(VoteResponse {
                term: peer_term,
                vote_granted: false,
                last_log_index: 1,
                last_log_term: 1,
            }),
            Reply::Down => Err(Error::from(crate::NetworkError::ServiceUnavailable(
                "peer is down".to_string(),
            ))),
        }
    }

    /// Runs one round against peers `2..` answering as listed (peer ids are taken from `replies`).
    async fn run_round(
        round: Round,
        replies: Vec<(u32, Reply)>,
    ) -> crate::Result<()> {
        let peer_ids: Vec<u32> = replies.iter().map(|(id, _)| *id).collect();

        let mut transport = MockTransport::new();
        match round {
            Round::Vote => {
                transport.expect_send_pre_vote_request().times(0);
                transport
                    .expect_send_vote_request()
                    .returning(move |peer_id, _, _, _| answer(round, &replies, peer_id));
            }
            Round::PreVote => {
                transport.expect_send_vote_request().times(0);
                transport
                    .expect_send_pre_vote_request()
                    .returning(move |peer_id, _, _, _| answer(round, &replies, peer_id));
            }
        }

        let node_config = RaftNodeConfig::new()
            .expect("Should create default config")
            .validate()
            .expect("Should validate config");
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let membership = Arc::new(pre_vote_membership(peer_ids));
        let raft_log = Arc::new(pre_vote_raft_log());
        let transport = Arc::new(transport);
        let node_config = Arc::new(node_config);
        match round {
            Round::Vote => {
                handler
                    .broadcast_vote_requests(
                        ASKED_TERM,
                        membership,
                        &raft_log,
                        &transport,
                        &node_config,
                    )
                    .await
            }
            Round::PreVote => {
                handler
                    .broadcast_pre_vote_requests(
                        ASKED_TERM,
                        membership,
                        &raft_log,
                        &transport,
                        &node_config,
                    )
                    .await
            }
        }
    }

    fn is_quorum_failure(result: &crate::Result<()>) -> bool {
        matches!(
            result,
            Err(Error::Consensus(ConsensusError::Election(
                ElectionError::QuorumFailure { .. }
            )))
        )
    }

    /// Test: a denial from a peer whose log equals ours is just a missing vote.
    ///
    /// Scenario (3 nodes, steady state, all logs equal):
    /// - Peer 2 denies (it already voted for someone else in this term, or still hears a
    ///   leader); peer 3 grants.
    ///
    /// Expected:
    /// - Ok(()): own vote + peer 3 is a majority. An equal log is not a "more recent" log, so
    ///   the denial must not end the round and discard peer 3's grant.
    #[tokio::test]
    async fn test_round_denial_with_equal_log_is_just_a_missing_vote() {
        for round in BOTH_ROUNDS {
            let result = run_round(round, vec![(2, deny_equal_log()), (3, Reply::Grant)]).await;
            assert!(result.is_ok(), "{round:?}: got {result:?}");
        }
    }

    /// Test: a denial from a peer whose log is strictly newer is also just a missing vote.
    ///
    /// Expected:
    /// - Ok(()) when another peer grants. A peer's newer log does not stop a majority of
    ///   other peers (each already checked our log against theirs) from electing us. openraft
    ///   and raft-rs never end the round on a log comparison.
    #[tokio::test]
    async fn test_round_denial_with_newer_log_is_just_a_missing_vote() {
        for round in BOTH_ROUNDS {
            let result = run_round(round, vec![(2, deny_newer_log()), (3, Reply::Grant)]).await;
            assert!(result.is_ok(), "{round:?}: got {result:?}");
        }
    }

    /// Test: the order in which a grant and a denial arrive must not change the outcome.
    ///
    /// Expected:
    /// - Ok(()) both when the grant is answered first and when the denial is.
    #[tokio::test]
    async fn test_round_outcome_does_not_depend_on_answer_order() {
        for round in BOTH_ROUNDS {
            let grant_first =
                run_round(round, vec![(2, Reply::Grant), (3, deny_equal_log())]).await;
            let denial_first =
                run_round(round, vec![(2, deny_equal_log()), (3, Reply::Grant)]).await;
            assert!(
                grant_first.is_ok(),
                "{round:?} grant first: got {grant_first:?}"
            );
            assert!(
                denial_first.is_ok(),
                "{round:?} denial first: got {denial_first:?}"
            );
        }
    }

    /// Test: five nodes, two denials (equal and newer log) and two grants still win.
    ///
    /// Scenario (raft-rs `test_dueling_candidates`: "a rejection is not the end of the round"):
    /// - Own vote + 2 grants = 3 of 5.
    #[tokio::test]
    async fn test_round_five_nodes_two_denials_and_two_grants_wins() {
        for round in BOTH_ROUNDS {
            let result = run_round(
                round,
                vec![
                    (2, deny_equal_log()),
                    (3, deny_newer_log()),
                    (4, Reply::Grant),
                    (5, Reply::Grant),
                ],
            )
            .await;
            assert!(result.is_ok(), "{round:?}: got {result:?}");
        }
    }

    /// Test: five nodes, three denials cannot win and report a plain quorum failure.
    ///
    /// Expected:
    /// - QuorumFailure, never a log-specific error: whatever the denial reason (equal, newer or
    ///   older log), the outcome is "not enough votes".
    #[tokio::test]
    async fn test_round_five_nodes_three_denials_fail_quorum() {
        for round in BOTH_ROUNDS {
            let result = run_round(
                round,
                vec![
                    (2, deny_equal_log()),
                    (3, deny_newer_log()),
                    (4, deny_older_log()),
                    (5, Reply::Grant),
                ],
            )
            .await;
            assert!(is_quorum_failure(&result), "{round:?}: got {result:?}");
        }
    }

    /// Test: a lone peer denying, whatever its log, is a plain quorum failure.
    ///
    /// Scenario (2 nodes: we need both votes):
    /// - The peer's log is equal, newer, or older than ours.
    ///
    /// Expected:
    /// - QuorumFailure in all three cases. The node could not win, which is all the caller
    ///   needs to know; there is no separate "log conflict" outcome.
    #[tokio::test]
    async fn test_round_single_peer_denial_is_quorum_failure_for_any_log() {
        for round in BOTH_ROUNDS {
            for denial in [deny_equal_log(), deny_newer_log(), deny_older_log()] {
                let result = run_round(round, vec![(2, denial)]).await;
                assert!(
                    is_quorum_failure(&result),
                    "{round:?} {denial:?}: got {result:?}"
                );
            }
        }
    }

    /// Test: a peer revealing a higher term ends the round at once, even after a grant.
    ///
    /// Scenario (5 nodes, so one grant is not yet a majority):
    /// - Peer 2 grants, peer 3 denies and reports a term far above ours, peers 4 and 5 would
    ///   grant.
    ///
    /// Expected:
    /// - HigherTerm(peer's term): the node must catch up immediately (raft-rs and openraft both
    ///   adopt a higher term as soon as they see it), not carry on counting.
    #[tokio::test]
    async fn test_round_higher_term_denial_ends_round_even_after_a_grant() {
        for round in BOTH_ROUNDS {
            let result = run_round(
                round,
                vec![
                    (2, Reply::Grant),
                    (3, Reply::DenyHigherTerm { peer_term: 9 }),
                    (4, Reply::Grant),
                    (5, Reply::Grant),
                ],
            )
            .await;
            assert!(
                matches!(
                    result,
                    Err(Error::Consensus(ConsensusError::Election(
                        ElectionError::HigherTerm(9)
                    )))
                ),
                "{round:?}: got {result:?}"
            );
        }
    }

    /// Test: unreachable peers and denials do not stop a majority from forming.
    ///
    /// Scenario (5 nodes): peer 2 down, peer 3 denies (equal log), peers 4 and 5 grant.
    ///
    /// Expected:
    /// - Ok(()): own vote + 2 grants = 3 of 5.
    #[tokio::test]
    async fn test_round_unreachable_peer_and_denial_do_not_block_majority() {
        for round in BOTH_ROUNDS {
            let result = run_round(
                round,
                vec![
                    (2, Reply::Down),
                    (3, deny_equal_log()),
                    (4, Reply::Grant),
                    (5, Reply::Grant),
                ],
            )
            .await;
            assert!(result.is_ok(), "{round:?}: got {result:?}");
        }
    }

    /// Test: an unreachable peer plus a denial is a quorum failure, not a success.
    ///
    /// Scenario (3 nodes): peer 2 down, peer 3 denies. Unreachable is never counted as a grant.
    #[tokio::test]
    async fn test_round_unreachable_peer_and_denial_fail_quorum() {
        for round in BOTH_ROUNDS {
            let result = run_round(round, vec![(2, Reply::Down), (3, deny_equal_log())]).await;
            assert!(is_quorum_failure(&result), "{round:?}: got {result:?}");
        }
    }

    /// Test: `VoteRpc::peer_term_shows_behind` compares with `>` for a real vote and with
    /// `>=` for PreVote.
    ///
    /// - Real vote: asked_term is this node's own term; a peer must be strictly above it.
    /// - PreVote: asked_term is the term this node WANTS; a peer already at it means that
    ///   term is taken.
    #[test]
    fn test_vote_rpc_peer_term_shows_behind_boundaries() {
        use crate::election::election_handler::VoteRpc;

        assert!(!VoteRpc::Vote.peer_term_shows_behind(5, 4));
        assert!(!VoteRpc::Vote.peer_term_shows_behind(5, 5));
        assert!(VoteRpc::Vote.peer_term_shows_behind(5, 6));

        assert!(!VoteRpc::PreVote.peer_term_shows_behind(5, 4));
        assert!(VoteRpc::PreVote.peer_term_shows_behind(5, 5));
        assert!(VoteRpc::PreVote.peer_term_shows_behind(5, 6));
    }

    /// Test: a peer whose log is more recent than the candidate's fails the PreVote.
    ///
    /// Expected:
    /// - ElectionError::QuorumFailure with only the candidate's own vote, same as the real
    ///   vote: the candidate could not win, so it must not start a real election.
    #[tokio::test]
    async fn test_broadcast_pre_vote_requests_rejects_when_peer_log_is_more_recent() {
        let mut transport = MockTransport::new();
        transport.expect_send_pre_vote_request().times(1).returning(|_, request, _, _| {
            Ok(VoteResponse {
                term: request.term - 1,
                vote_granted: false,
                last_log_index: 9,
                last_log_term: 3,
            })
        });

        let e = run_pre_vote(pre_vote_membership(vec![2]), transport, 2).await.unwrap_err();

        assert!(
            matches!(
                e,
                Error::Consensus(ConsensusError::Election(ElectionError::QuorumFailure {
                    required: 2,
                    succeed: 1,
                }))
            ),
            "expected QuorumFailure with only the candidate's own vote, got: {e:?}"
        );
    }

    // ========================================================================
    // test_handle_pre_vote_request_* - PreVote (receiver side)
    //
    // The receiver answers "would you vote for me at request.term?" and records nothing. The
    // raft log mock only allows `last_log_id`, so any persistence call would panic the test.
    // ========================================================================

    /// Our node: term 3, last log index 5 / term 2.
    const RECEIVER_TERM: u64 = 3;

    fn receiver_log() -> Option<LogId> {
        Some(LogId { index: 5, term: 2 })
    }

    fn ask(
        request_term: u64,
        request_log: (u64, u64),
        leader_active: bool,
    ) -> VoteResponse {
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_log_id().returning(receiver_log);
        ElectionHandler::<MockTypeConfig>::new(1).handle_pre_vote_request(
            VoteRequest {
                term: request_term,
                candidate_id: 9,
                last_log_index: request_log.0,
                last_log_term: request_log.1,
            },
            RECEIVER_TERM,
            &Arc::new(raft_log),
            leader_active,
        )
    }

    /// Test: a PreVote for a free term from a requester with an equal log is granted.
    ///
    /// Expected:
    /// - Granted. "At least as up-to-date" includes an equal log: with identical logs everywhere
    ///   (the steady state) nobody could otherwise ever win an election.
    #[test]
    fn test_handle_pre_vote_request_grants_for_free_term_and_equal_log() {
        let response = ask(RECEIVER_TERM + 1, (5, 2), false);

        assert!(response.vote_granted);
    }

    /// Test: a requester whose log is strictly newer is granted (higher term, or same term and
    /// higher index).
    #[test]
    fn test_handle_pre_vote_request_grants_when_requester_log_is_newer() {
        assert!(
            ask(RECEIVER_TERM + 1, (1, 3), false).vote_granted,
            "higher last term"
        );
        assert!(
            ask(RECEIVER_TERM + 1, (9, 2), false).vote_granted,
            "same term, higher index"
        );
    }

    /// Test: a requester whose log is older is denied (lower last term, or same term and lower
    /// index). The last term decides first: a longer log with a lower term is still older.
    #[test]
    fn test_handle_pre_vote_request_denies_when_requester_log_is_older() {
        assert!(
            !ask(RECEIVER_TERM + 1, (99, 1), false).vote_granted,
            "lower last term"
        );
        assert!(
            !ask(RECEIVER_TERM + 1, (4, 2), false).vote_granted,
            "same term, lower index"
        );
    }

    /// Test: while the caller has evidence of a live leader, every PreVote is denied.
    ///
    /// Scenario:
    /// - A node returning from a partition asks about a free term with a perfectly good log.
    ///
    /// Expected:
    /// - Denied. This is what stops it from becoming a candidate and disrupting a healthy
    ///   leader.
    #[test]
    fn test_handle_pre_vote_request_denies_while_leader_is_active() {
        let response = ask(RECEIVER_TERM + 1, (9, 9), true);

        assert!(!response.vote_granted);
    }

    /// Test: a PreVote for a term that is already taken (not greater than ours) is denied.
    #[test]
    fn test_handle_pre_vote_request_denies_when_term_is_not_greater() {
        assert!(
            !ask(RECEIVER_TERM, (9, 9), false).vote_granted,
            "equal term"
        );
        assert!(
            !ask(RECEIVER_TERM - 1, (9, 9), false).vote_granted,
            "lower term"
        );
    }

    /// Test: the response always carries our own term and last log id, whether granted or
    /// denied, so a requester that is behind can see it and catch up.
    #[test]
    fn test_handle_pre_vote_request_response_carries_own_term_and_log() {
        for response in [
            ask(RECEIVER_TERM + 1, (5, 2), false), // granted
            ask(RECEIVER_TERM + 1, (5, 2), true),  // denied: leader active
            ask(RECEIVER_TERM, (5, 2), false),     // denied: term taken
        ] {
            assert_eq!(response.term, RECEIVER_TERM);
            assert_eq!(response.last_log_index, 5);
            assert_eq!(response.last_log_term, 2);
        }
    }

    /// Test: with an empty local log, a requester with an empty log is granted.
    #[test]
    fn test_handle_pre_vote_request_empty_local_log_grants_empty_requester_log() {
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_log_id().returning(|| None);

        let response = ElectionHandler::<MockTypeConfig>::new(1).handle_pre_vote_request(
            VoteRequest {
                term: 2,
                candidate_id: 9,
                last_log_index: 0,
                last_log_term: 0,
            },
            1,
            &Arc::new(raft_log),
            false,
        );

        assert!(response.vote_granted);
        assert_eq!((response.last_log_index, response.last_log_term), (0, 0));
    }

    // ========================================================================
    // test_handle_vote_request_stale_vote_* - a vote cast in an OLDER term must not block
    // a vote in the current term (Raft: on a new term, votedFor is null).
    // raft-rs `reset(term)` clears the vote whenever the term changes; openraft replaces the
    // whole (term, node) vote.
    // ========================================================================

    /// Test: a stale vote from an older term does not block a legitimate candidate in the
    /// current term.
    ///
    /// Scenario:
    /// - The node voted for node 3 in term 1.
    /// - A request at term 2 from a candidate with a bad log made it adopt term 2 and deny it
    ///   (adopting the term does not cast a vote).
    /// - Now a candidate with a good log asks for a vote in the same term 2.
    ///
    /// Expected:
    /// - Granted: the node has not voted in term 2. Denying here costs the candidate a whole
    ///   election round (it retries at term 3) for no safety reason.
    #[tokio::test]
    async fn test_handle_vote_request_stale_vote_from_older_term_does_not_block_current_term() {
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let raft_log = Arc::new(create_mock_raft_log(Some(LogId { index: 1, term: 1 })));

        let state_update = handler
            .handle_vote_request(
                VoteRequest {
                    term: 2,
                    candidate_id: 5,
                    last_log_index: 1,
                    last_log_term: 1,
                },
                2, // current term: already adopted
                Some(VotedFor {
                    voted_for_id: 3,
                    voted_for_term: 1, // cast in an older term
                    committed: false,
                }),
                &raft_log,
            )
            .await
            .unwrap();

        assert!(
            state_update.new_voted_for.is_some(),
            "a vote from term 1 must not block a vote in term 2, got: {state_update:?}"
        );
    }

    /// Test: a vote already cast for ANOTHER candidate in the current term still blocks.
    /// (The boundary of the test above: only the term of the old vote matters.)
    #[tokio::test]
    async fn test_handle_vote_request_vote_for_other_candidate_in_current_term_still_blocks() {
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let raft_log = Arc::new(create_mock_raft_log(Some(LogId { index: 1, term: 1 })));

        let state_update = handler
            .handle_vote_request(
                VoteRequest {
                    term: 2,
                    candidate_id: 5,
                    last_log_index: 1,
                    last_log_term: 1,
                },
                2,
                Some(VotedFor {
                    voted_for_id: 3,
                    voted_for_term: 2, // same term, other candidate
                    committed: false,
                }),
                &raft_log,
            )
            .await
            .unwrap();

        assert!(state_update.new_voted_for.is_none());
    }

    /// Test: PreVote and the real vote agree whenever nothing else differs.
    ///
    /// Property (checked over a grid of terms and logs): for a request at a term above ours,
    /// with no leader active and no vote cast, `handle_pre_vote_request` grants exactly when
    /// `handle_vote_request` would.
    ///
    /// - A PreVote that grants what the real vote denies sends the node into a real election it
    ///   cannot win, bumping terms for nothing (the thing PreVote exists to prevent).
    /// - A PreVote that denies what the real vote would grant makes a node that could win
    ///   unable to start an election: a liveness hole after a leader failure.
    #[tokio::test]
    async fn test_pre_vote_grants_exactly_when_the_real_vote_would() {
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let logs = [None, Some((1, 1)), Some((5, 2)), Some((9, 2)), Some((3, 3))];

        for current_term in [1u64, 3, 7] {
            for my_log in logs {
                for request_term in [current_term + 1, current_term + 5] {
                    for requester_log in [(0, 0), (1, 1), (5, 2), (9, 2), (3, 3), (2, 4)] {
                        let my_log_id = my_log.map(|(index, term)| LogId { index, term });
                        let raft_log = Arc::new(create_mock_raft_log(my_log_id));
                        let request = VoteRequest {
                            term: request_term,
                            candidate_id: 9,
                            last_log_index: requester_log.0,
                            last_log_term: requester_log.1,
                        };

                        let pre_vote = handler
                            .handle_pre_vote_request(request, current_term, &raft_log, false)
                            .vote_granted;
                        let real_vote = handler
                            .handle_vote_request(request, current_term, None, &raft_log)
                            .await
                            .unwrap()
                            .new_voted_for
                            .is_some();

                        assert_eq!(
                            pre_vote, real_vote,
                            "current_term={current_term} my_log={my_log:?} request_term=\
                             {request_term} requester_log={requester_log:?}: PreVote and the real \
                             vote must agree"
                        );
                    }
                }
            }
        }
    }

    // ========================================================================
    // vote_from_any_state (raft-rs): a request with a higher term is adopted whether or not the
    // vote can be granted; a request with the same or a lower term is never adopted.
    // ========================================================================

    /// Test: a higher-term request whose log is stale is denied but its term is still adopted.
    ///
    /// Expected:
    /// - `term_update == Some(request.term)` and no vote recorded (Raft §5.1; the caller persists
    ///   the term and, for a role change, steps down).
    #[tokio::test]
    async fn test_handle_vote_request_denied_for_stale_log_still_adopts_higher_term() {
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let raft_log = Arc::new(create_mock_raft_log(Some(LogId { index: 9, term: 3 })));

        let state_update = handler
            .handle_vote_request(
                VoteRequest {
                    term: 5,
                    candidate_id: 2,
                    last_log_index: 1,
                    last_log_term: 1, // far behind ours
                },
                2,
                None,
                &raft_log,
            )
            .await
            .unwrap();

        assert!(
            state_update.new_voted_for.is_none(),
            "the vote must be denied"
        );
        assert_eq!(
            state_update.term_update,
            Some(5),
            "the higher term must still be adopted"
        );
    }

    /// Test: a request with the same or a lower term never changes our term.
    #[tokio::test]
    async fn test_handle_vote_request_same_or_lower_term_is_never_adopted() {
        let handler = ElectionHandler::<MockTypeConfig>::new(1);
        let raft_log = Arc::new(create_mock_raft_log(Some(LogId { index: 1, term: 1 })));

        for request_term in [4, 3] {
            let state_update = handler
                .handle_vote_request(
                    VoteRequest {
                        term: request_term,
                        candidate_id: 2,
                        last_log_index: 1,
                        last_log_term: 1,
                    },
                    4,
                    None,
                    &raft_log,
                )
                .await
                .unwrap();
            assert_eq!(
                state_update.term_update, None,
                "request term {request_term}"
            );
        }
    }
}
