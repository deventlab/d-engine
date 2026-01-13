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
    use std::collections::HashSet;

    use d_engine_proto::server::cluster::NodeMeta;
    use d_engine_proto::server::election::VoteResponse;

    use super::*;
    use crate::ConsensusError;
    use crate::ElectionError;
    use crate::Error;
    use crate::MockMembership;
    use crate::MockTransport;
    use crate::RaftNodeConfig;
    use crate::VoteResult;

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
        transport_mock.expect_send_vote_requests().times(0).returning(|_, _, _| {
            Ok(VoteResult {
                peer_ids: vec![2].into_iter().collect(),
                responses: vec![Ok(VoteResponse {
                    term: 1,
                    vote_granted: false,
                    last_log_index: 1,
                    last_log_term: 1,
                })],
            })
        });

        // Mock membership with empty voters (core test_utils provides this default)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_is_single_node_cluster().returning(|| false);

        // Create minimal node_config with TempDir (no file system pollution)
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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

    /// Test: broadcast_vote_requests when majority of peers reject vote due to log conflict
    ///
    /// FIXME(migration): This test is a FALSE POSITIVE from the original codebase.
    ///
    /// **Problem**: The test uses `if let` pattern matching without `else` or `assert!`,
    /// causing it to pass silently even when the wrong error type is returned.
    ///
    /// **Current behavior**:
    /// - Uses `mock_membership()` which returns empty voters
    /// - Function returns `NoVotingMemberFound` error immediately
    /// - `if let LogConflict` fails to match, skips assertions, test passes silently
    ///
    /// **Intended behavior**: Should test the scenario where:
    /// - Cluster has voting members
    /// - Peers reject votes because their logs are more up-to-date
    /// - Should return `LogConflict` error
    ///
    /// **Fix required**:
    /// 1. Configure membership with actual voters
    /// 2. Configure transport to return `vote_granted=false` responses
    /// 3. Change `if let` to `assert!(matches!())` for proper validation
    ///
    /// **Original test location**:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_broadcast_vote_requests_case2`
    ///
    /// Scenario (intended, not currently tested):
    /// - Cluster has multiple voting members
    /// - Candidate broadcasts vote requests
    /// - Majority of peers reject due to having more recent logs
    ///
    /// Expected (intended):
    /// - Returns ElectionError::LogConflict with conflict details
    #[tokio::test]
    #[ignore = "False positive test - needs fix before enabling (see FIXME above)"]
    async fn test_broadcast_vote_requests_majority_reject_due_to_log_conflict() {
        // Original test setup - preserved for reference
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let term = 1;

        // Mock raft_log - times(0) because function returns early with NoVotingMemberFound
        let mut raft_log_mock = MockRaftLog::new();
        raft_log_mock
            .expect_last_log_id()
            .times(0)
            .returning(|| Some(LogId { index: 1, term: 1 }));

        // Mock transport - times(0) because function returns early
        let mut transport_mock = MockTransport::new();
        transport_mock.expect_send_vote_requests().times(0).returning(|_, _, _| {
            Ok(VoteResult {
                peer_ids: vec![2].into_iter().collect(),
                responses: vec![Ok(VoteResponse {
                    term: 1,
                    vote_granted: false,
                    last_log_index: 1,
                    last_log_term: 1,
                })],
            })
        });

        // Mock membership with empty voters (causes immediate NoVotingMemberFound error)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_is_single_node_cluster().returning(|| false);

        // Create minimal node_config with TempDir
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
        let node_config = node_config.validate().expect("Should validate config");

        // Execute - will return NoVotingMemberFound, not LogConflict
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

        // Original assertion - NEVER EXECUTES because pattern doesn't match
        // Test passes silently without validating anything
        if let Error::Consensus(ConsensusError::Election(ElectionError::LogConflict {
            index,
            expected_term,
            actual_term,
        })) = e
        {
            assert_eq!(index, 1);
            assert_eq!(actual_term, 1);
            assert_eq!(expected_term, 1);
        }

        // TODO: Replace above with proper test implementation:
        // assert!(
        //     matches!(
        //         e,
        //         Error::Consensus(ConsensusError::Election(
        //             ElectionError::LogConflict { index: 1, expected_term: 1, actual_term: 1 }
        //         ))
        //     ),
        //     "Expected LogConflict error, got: {:?}", e
        // );
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

        // Mock transport - returns successful vote from peer
        let mut transport_mock = MockTransport::new();
        transport_mock.expect_send_vote_requests().times(1).returning(|_, _, _| {
            Ok(VoteResult {
                peer_ids: vec![2].into_iter().collect(),
                responses: vec![Ok(VoteResponse {
                    term: 1,
                    vote_granted: true,
                    last_log_index: 1,
                    last_log_term: 1,
                })],
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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

    /// Test: broadcast_vote_requests when peer responds with higher term
    ///
    /// FIXME(migration): This test is a FALSE POSITIVE from the original codebase.
    ///
    /// **Problem**: Uses `if let` without proper assertion, passes silently with wrong error.
    ///
    /// **Current behavior**:
    /// - Uses `mock_membership()` which returns empty voters
    /// - Function returns `NoVotingMemberFound` immediately
    /// - `if let HigherTerm` fails to match, test passes silently
    ///
    /// **Intended behavior**: Should test the scenario where:
    /// - Candidate broadcasts vote requests to peers
    /// - Peer responds with higher term in vote response
    /// - Should return `HigherTerm` error causing candidate to step down
    ///
    /// **Fix required**:
    /// 1. Configure membership with actual voters
    /// 2. Configure transport to return response with higher term
    /// 3. Change `if let` to `assert!(matches!())` for proper validation
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_broadcast_vote_requests_case4`
    #[tokio::test]
    #[ignore = "False positive test - needs fix before enabling (see FIXME above)"]
    async fn test_broadcast_vote_requests_peer_has_higher_term() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let my_last_log_term = 3;

        // Mock transport - never called because function returns early
        let transport_mock = MockTransport::new();

        // Mock raft_log - never called
        let raft_log_mock = MockRaftLog::new();

        // Mock membership with empty voters (causes immediate NoVotingMemberFound)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_is_single_node_cluster().returning(|| false);

        // Create minimal node_config with TempDir
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
        let node_config = node_config.validate().expect("Should validate config");

        // Execute - will return NoVotingMemberFound, not HigherTerm
        let e = election_handler
            .broadcast_vote_requests(
                my_last_log_term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await
            .unwrap_err();

        // Original assertion - NEVER EXECUTES because pattern doesn't match
        if let Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(higher_term))) =
            e
        {
            assert_eq!(higher_term, my_last_log_term + 1);
        }

        // TODO: Replace with proper implementation that tests HigherTerm scenario
    }

    /// Test: broadcast_vote_requests when peer has higher log index (same term)
    ///
    /// FIXME(migration): This test is a FALSE POSITIVE from the original codebase.
    ///
    /// **Problem**: Uses `if let` without proper assertion, passes silently with wrong error.
    ///
    /// **Current behavior**:
    /// - Uses `mock_membership()` which returns empty voters
    /// - Function returns `NoVotingMemberFound` immediately
    /// - `if let LogConflict` fails to match, test passes silently
    ///
    /// **Intended behavior**: Should test the scenario where:
    /// - Candidate and peer have same last_log_term
    /// - Peer has higher last_log_index (more entries in same term)
    /// - Should return `LogConflict` error
    ///
    /// **Fix required**:
    /// 1. Configure membership with actual voters
    /// 2. Configure transport to return response with higher log index
    /// 3. Change `if let` to `assert!(matches!())` for proper validation
    ///
    /// Original test location:
    /// `d-engine-server/tests/components/election/election_handler_test.rs::test_broadcast_vote_requests_case5`
    #[tokio::test]
    #[ignore = "False positive test - needs fix before enabling (see FIXME above)"]
    async fn test_broadcast_vote_requests_peer_has_higher_log_index() {
        let election_handler = ElectionHandler::<MockTypeConfig>::new(1);
        let my_last_log_index = 1;
        let my_last_log_term = 3;

        // Mock transport - never called
        let transport_mock = MockTransport::new();

        // Mock raft_log - never called
        let raft_log_mock = MockRaftLog::new();

        // Mock membership with empty voters (causes immediate NoVotingMemberFound)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_is_single_node_cluster().returning(|| false);

        // Create minimal node_config with TempDir
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
        let node_config = node_config.validate().expect("Should validate config");

        // Execute - will return NoVotingMemberFound, not LogConflict
        let e = election_handler
            .broadcast_vote_requests(
                my_last_log_term,
                Arc::new(membership),
                &Arc::new(raft_log_mock),
                &Arc::new(transport_mock),
                &Arc::new(node_config),
            )
            .await
            .unwrap_err();

        // Original assertion - NEVER EXECUTES because pattern doesn't match
        if let Error::Consensus(ConsensusError::Election(ElectionError::LogConflict {
            index,
            expected_term,
            actual_term,
        })) = e
        {
            assert_eq!(index, my_last_log_index);
            assert_eq!(expected_term, my_last_log_term);
            assert_eq!(actual_term, my_last_log_term);
        }

        // TODO: Replace with proper implementation that tests LogConflict scenario
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::new().expect("Should create default config");
        node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
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
}
