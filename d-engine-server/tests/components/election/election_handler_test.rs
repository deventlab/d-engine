use d_engine_server::test_utils::*;

use std::sync::Arc;
use tokio::sync::watch;
use tracing::debug;
use tracing_test::traced_test;

use d_engine_core::ConsensusError;
use d_engine_core::ElectionCore;
use d_engine_core::ElectionError;
use d_engine_core::ElectionHandler;
use d_engine_core::Error;
use d_engine_core::MockMembership;
use d_engine_core::MockRaftLog;
use d_engine_core::MockTransport;
use d_engine_core::MockTypeConfig;
use d_engine_core::VoteResult;
use d_engine_core::alias::ROF;
use d_engine_core::alias::TROF;
use d_engine_core::mock_membership;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;

struct TestConext {
    election_handler: ElectionHandler<MockTypeConfig>,
    raft_log_mock: ROF<MockTypeConfig>,
}
async fn setup() -> TestConext {
    let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

    let raft_log_mock: ROF<MockTypeConfig> = MockRaftLog::new();

    TestConext {
        election_handler,
        raft_log_mock,
    }
}

/// # Case 1: Receive election failed error if there is zero peers
///
/// ## Validation Criterias:
/// 1. Receive ElectionError::QuorumFailure error
#[tokio::test]
#[traced_test]
async fn test_broadcast_vote_requests_case1() {
    // 1. Create a ElectionHandler instance
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case1", None, false);

    let election_handler = ElectionHandler::<MockTypeConfig>::new(1);

    // 2.
    let term = 1;
    let mut raft_log_mock: ROF<MockTypeConfig> = MockRaftLog::new();
    raft_log_mock
        .expect_last_log_id()
        .times(0)
        .returning(|| Some(LogId { index: 1, term: 1 }));

    let mut transport_mock: TROF<MockTypeConfig> = MockTransport::new();
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

    let err = election_handler
        .broadcast_vote_requests(
            term,
            Arc::new(mock_membership()),
            &Arc::new(raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_node_config,
        )
        .await
        .unwrap_err();

    debug!(?err);

    assert!(matches!(
        err,
        Error::Consensus(ConsensusError::Election(
            ElectionError::NoVotingMemberFound { candidate_id: _ }
        ))
    ));
}

/// # Case 2: Test failed to receive majority peers' failed vote
///
/// ## Validation criterias:
/// 1. Test should receive Err(ElectionError::LogConflict)
#[tokio::test]
#[traced_test]
async fn test_broadcast_vote_requests_case2() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case2", None, false);
    let mut test_context = setup().await;
    test_context
        .raft_log_mock
        .expect_last_log_id()
        .times(0)
        .returning(|| Some(LogId { index: 1, term: 1 }));

    let mut transport_mock: TROF<MockTypeConfig> = MockTransport::new();
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
    let term = 1;

    let e = test_context
        .election_handler
        .broadcast_vote_requests(
            term,
            Arc::new(mock_membership()),
            &Arc::new(test_context.raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_node_config,
        )
        .await
        .unwrap_err();

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
}
/// # Case 3: Test after receiving majority peers' success vote
///
/// ## Validation criterias:
/// 1. Test should receive Ok(())
#[tokio::test]
#[traced_test]
async fn test_broadcast_vote_requests_case3() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case3", None, false);
    let mut test_context = setup().await;
    test_context
        .raft_log_mock
        .expect_last_log_id()
        .times(1)
        .returning(|| Some(LogId { index: 1, term: 1 }));
    let mut transport_mock: TROF<MockTypeConfig> = MockTransport::new();
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

    let term = 1;
    let mut membership = MockMembership::new();
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });
    let r = test_context
        .election_handler
        .broadcast_vote_requests(
            term,
            Arc::new(membership),
            &Arc::new(test_context.raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_node_config,
        )
        .await;
    debug!("test_broadcast_vote_requests_case3: {:?}", &r);
    assert!(r.is_ok())
}

/// # Case 4: Test if vote response returns higher last_log_term
// ## Setup:
// 1. prepare one peers, which returns failed response
// 2. Peer1 returns higher term of last log index,
//
// ## Criterias:
// 1. return Err(ElectionError::HigherTerm)
#[tokio::test]
#[traced_test]
async fn test_broadcast_vote_requests_case4() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case4", None, false);
    let test_context = setup().await;

    let my_last_log_term = 3;
    let transport_mock: TROF<MockTypeConfig> = MockTransport::new();

    let e = test_context
        .election_handler
        .broadcast_vote_requests(
            my_last_log_term,
            Arc::new(mock_membership()),
            &Arc::new(test_context.raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_node_config,
        )
        .await
        .unwrap_err();

    debug!("test_broadcast_vote_requests_case4: {:?}", &e);

    if let Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(higher_term))) = e {
        assert_eq!(higher_term, my_last_log_term + 1);
    }
}

/// # Case 5: Test if vote response returns higher last_log_index
// ## Setup:
// 1. prepare one peers, which returns failed response
// 2. Peer1 returns last log term is the same as candidate one, but last log index is higher than
//    candidate last log index,
//
// ## Criterias:
// 1. return Err(ElectionError::LogConflict)
#[tokio::test]
#[traced_test]
async fn test_broadcast_vote_requests_case5() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case5", None, false);
    let test_context = setup().await;
    let my_last_log_index = 1;
    let my_last_log_term = 3;

    let transport_mock: TROF<MockTypeConfig> = MockTransport::new();

    let e = test_context
        .election_handler
        .broadcast_vote_requests(
            my_last_log_term,
            Arc::new(mock_membership()),
            &Arc::new(test_context.raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_node_config,
        )
        .await
        .unwrap_err();

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
}

/// # Case 1: Test if vote request is legal
///
/// ## Validation criterias:
/// 1. Switch back to Follower
/// 2. Returns with Ok(state_update)
///     - step_to_follower = true
///     - voted_for_option = Some(x)
///     - term_update = Some(y)
#[tokio::test]
#[traced_test]
async fn test_handle_vote_request_case1() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let mut test_context = setup().await;
    test_context
        .raft_log_mock
        .expect_last_log_id()
        .times(1)
        .returning(|| Some(LogId { index: 1, term: 1 }));

    let current_term = 1;
    let last_log_index = 1;
    let last_log_term = 1;

    let request_term = current_term + 1;
    let vote_request = VoteRequest {
        term: request_term,
        candidate_id: 1,
        last_log_index: last_log_index + 1,
        last_log_term,
    };
    let voted_for_option = None;
    assert!(
        test_context
            .election_handler
            .handle_vote_request(
                vote_request,
                current_term,
                voted_for_option,
                &Arc::new(test_context.raft_log_mock),
            )
            .await
            .is_ok_and(move |state_update| state_update.new_voted_for.is_some()
                && state_update.term_update.unwrap() == request_term)
    );
}

/// # Case 2: Test if vote request is illegal
///     (current_term >= VoteRequest Term)
///
/// ## Validation criterias:
/// 1. role_rx.try_recv with None
/// 2. Returns with Ok(None)
#[tokio::test]
#[traced_test]
async fn test_handle_vote_request_case2() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let mut test_context = setup().await;
    test_context
        .raft_log_mock
        .expect_last_log_id()
        .times(1)
        .returning(|| Some(LogId { index: 1, term: 1 }));

    let current_term = 10;
    let last_log_index = 1;
    let last_log_term = 1;

    let vote_request = VoteRequest {
        term: current_term - 1,
        candidate_id: 1,
        last_log_index: last_log_index + 1,
        last_log_term,
    };
    let voted_for_option = None;

    let state_update = test_context
        .election_handler
        .handle_vote_request(
            vote_request,
            current_term,
            voted_for_option,
            &Arc::new(test_context.raft_log_mock),
        )
        .await
        .unwrap();
    assert!(state_update.new_voted_for.is_none());
}

/// Case 1.1: Term and local log index compare
/// - current_term >= VoteRequest Term
///
/// Criterias:
/// - false
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_1_1() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_1_1",
        None,
        false,
    );
    let election_controller = context.election_handler;

    let vote_request = VoteRequest {
        term: 1,
        candidate_id: 1,
        last_log_index: 1,
        last_log_term: 1,
    };
    let current_term = 1;
    let last_log_index = 1;
    let last_log_term = 1;
    let voted_for_id = 1;
    let voted_for_term = 1;

    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
    let current_term = 2;
    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
}

/// Case 1.2: Term and local log index compare
/// - current_term <= VoteRequest Term
/// - request.last_log_term <= last_log_term
///
/// Criterias:
/// - false
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_1_2() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_1_2",
        None,
        false,
    );
    let election_controller = context.election_handler;
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

    let last_log_term = 2;
    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));

    let last_log_term = 1;
    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
}

/// Case 1.3: Term and local log index compare
/// - current_term <= VoteRequest Term
/// - request.last_log_term = last_log_term
/// - request.last_log_index > last_log_index
///
/// Criterias:
/// - true
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_1_3() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_1_3",
        None,
        false,
    );
    let election_controller = context.election_handler;
    let current_term = 1;
    let last_log_index = 1;
    let last_log_term = 1;

    let vote_request = VoteRequest {
        term: current_term,
        candidate_id: 1,
        last_log_index: last_log_index + 1,
        last_log_term,
    };
    assert!(election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        None,
    ));
}

/// Case 1.4: Term and local log index compare
/// - current_term <= VoteRequest Term
/// - request.last_log_term = last_log_term
/// - request.last_log_index < last_log_index
///
/// Criterias:
/// - false
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_1_4() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_1_4",
        None,
        false,
    );
    let election_controller = context.election_handler;
    let current_term = 1;

    let vote_request = VoteRequest {
        term: current_term,
        candidate_id: 1,
        last_log_index: 1,
        last_log_term: 1,
    };
    let last_log_index = 2;
    let voted_for_id = 1;
    let voted_for_term = 1;

    let last_log_term = 1;
    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
}

/// Case 2.1: granted_vote checking
/// - current_term <= VoteRequest Term
/// - request.last_log_term = last_log_term
/// - request.last_log_index >= last_log_index
/// - current node has granted vote
///
/// Criterias:
/// - false
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_2_1() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_2_1",
        None,
        false,
    );
    let election_controller = context.election_handler;
    let current_term = 1;

    let vote_request = VoteRequest {
        term: current_term,
        candidate_id: 1,
        last_log_index: 3,
        last_log_term: 1,
    };
    let last_log_index = 2;
    let last_log_term = 1;

    let voted_for_id = 3;
    let voted_for_term = 1;

    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
}

/// Case 2.2: granted_vote checking
/// - current_term <= VoteRequest Term
/// - request.last_log_term = last_log_term
/// - request.last_log_index >= last_log_index
/// - current node has granted vote, and the granted vote term is bigger than the request one.
///
/// Criterias:
/// - false
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_2_2() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_2_2",
        None,
        false,
    );
    let election_controller = context.election_handler;
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
    let voted_for_term = 10;

    assert!(!election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
}
/// Case 2.3: granted_vote checking
/// - current_term <= VoteRequest Term
/// - request.last_log_term = last_log_term
/// - request.last_log_index >= last_log_index
/// - current node has granted vote, and the granted vote term is bigger than the request one.
///
/// Criterias:
/// - true
#[tokio::test]
#[traced_test]
async fn test_check_vote_request_is_legal_case_2_3() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components(
        "/tmp/test_check_vote_request_is_legal_case_2_3",
        None,
        false,
    );
    let election_controller = context.election_handler;
    let current_term = 10;

    let vote_request = VoteRequest {
        term: current_term,
        candidate_id: 1,
        last_log_index: 3,
        last_log_term: 1,
    };
    let last_log_index = 2;
    let last_log_term = 1;

    let voted_for_id = 1;
    let voted_for_term = 1;

    assert!(election_controller.check_vote_request_is_legal(
        &vote_request,
        current_term,
        last_log_index,
        last_log_term,
        Some(VotedFor {
            voted_for_id,
            voted_for_term
        })
    ));
}
