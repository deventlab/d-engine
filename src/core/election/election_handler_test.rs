use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::alias::ROF;
use crate::alias::TROF;
use crate::proto::VoteRequest;
use crate::proto::VotedFor;
use crate::test_utils::setup_raft_components;
use crate::test_utils::MockNode;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::MOCK_ELECTION_HANDLER_PORT_BASE;
use crate::ChannelWithAddressAndRole;
use crate::ElectionCore;
use crate::ElectionHandler;
use crate::Error;
use crate::MockRaftLog;
use crate::MockTransport;
use crate::FOLLOWER;

struct TestConext {
    election_handler: ElectionHandler<MockTypeConfig>,
    voting_members: Vec<ChannelWithAddressAndRole>,
    raft_log_mock: ROF<MockTypeConfig>,
}
async fn setup(port: u64) -> TestConext {
    let (event_tx, _event_rx) = mpsc::channel(1);
    let election_handler = ElectionHandler::<MockTypeConfig>::new(1, event_tx);

    // 2. Prepare Peers fake address
    //  Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr = MockNode::simulate_mock_service_without_reps(port, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let voting_members: Vec<ChannelWithAddressAndRole> = vec![ChannelWithAddressAndRole {
        id: 2,
        channel_with_address: addr,
        role: FOLLOWER,
    }];

    let mut raft_log_mock: ROF<MockTypeConfig> = MockRaftLog::new();
    raft_log_mock
        .expect_get_last_entry_metadata()
        .times(1)
        .returning(|| (1, 1));

    TestConext {
        election_handler,
        voting_members,
        raft_log_mock,
    }
}
/// # Case 1: Receive election failed error if there is zero peers
///
/// ## Validation Criterias:
/// 1. Receive Error::ElectionFailed error
#[tokio::test]
async fn test_broadcast_vote_requests_case1() {
    // 1. Create a ElectionHandler instance
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case1", None, false);
    let (event_tx, _event_rx) = mpsc::channel(1);
    let election_handler = ElectionHandler::<MockTypeConfig>::new(1, event_tx);

    // 2.
    let term = 1;
    let voting_members = Vec::new();
    let raft_log_mock: Arc<ROF<MockTypeConfig>> = Arc::new(MockRaftLog::new());
    let transport_mock: Arc<TROF<MockTypeConfig>> = Arc::new(MockTransport::new());

    if let Err(Error::ElectionFailed(_)) = election_handler
        .broadcast_vote_requests(term, voting_members, &raft_log_mock, &transport_mock, &ctx.arc_settings)
        .await
    {
        assert!(true);
    } else {
        assert!(false);
    }
}

/// # Case 2: Test failed to receive majority peers' failed vote
///
/// ## Validation criterias:
/// 1. Test should receive Err(Error::ElectionFailed)
#[tokio::test]
async fn test_broadcast_vote_requests_case2() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case2", None, false);
    let port = MOCK_ELECTION_HANDLER_PORT_BASE + 1;
    let test_context = setup(port).await;
    let mut transport_mock: TROF<MockTypeConfig> = MockTransport::new();
    transport_mock
        .expect_send_vote_requests()
        .times(1)
        .returning(|_, _, _| Ok(false));
    let term = 1;

    if let Err(Error::ElectionFailed(_)) = test_context
        .election_handler
        .broadcast_vote_requests(
            term,
            test_context.voting_members,
            &Arc::new(test_context.raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_settings,
        )
        .await
    {
        assert!(true);
    } else {
        assert!(false);
    }
}
/// # Case 3: Test after receiving majority peers' success vote
///
/// ## Validation criterias:
/// 1. Test should receive Ok(())
#[tokio::test]
async fn test_broadcast_vote_requests_case3() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let ctx = setup_raft_components("/tmp/test_broadcast_vote_requests_case3", None, false);
    let port = MOCK_ELECTION_HANDLER_PORT_BASE + 2;
    let test_context = setup(port).await;
    let mut transport_mock: TROF<MockTypeConfig> = MockTransport::new();
    transport_mock
        .expect_send_vote_requests()
        .times(1)
        .returning(|_, _, _| Ok(true));

    let term = 1;

    assert!(test_context
        .election_handler
        .broadcast_vote_requests(
            term,
            test_context.voting_members,
            &Arc::new(test_context.raft_log_mock),
            &Arc::new(transport_mock),
            &ctx.arc_settings,
        )
        .await
        .is_ok())
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
async fn test_handle_vote_request_case1() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let port = MOCK_ELECTION_HANDLER_PORT_BASE + 10;
    let test_context = setup(port).await;

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
    match test_context
        .election_handler
        .handle_vote_request(
            vote_request,
            current_term,
            voted_for_option,
            &Arc::new(test_context.raft_log_mock),
        )
        .await
    {
        Ok(state_update) => {
            assert!(state_update.step_to_follower);
            assert!(state_update.new_voted_for.is_some());
            assert_eq!(state_update.term_update.unwrap(), request_term);
        }
        Err(_) => assert!(false),
    }
}

/// # Case 2: Test if vote request is illegal
///     (current_term >= VoteRequest Term)
///
/// ## Validation criterias:
/// 1. role_rx.try_recv with None
/// 2. Returns with Ok(None)
#[tokio::test]
async fn test_handle_vote_request_case2() {
    let (_graceful_tx, _graceful_rx) = watch::channel(());
    let port = MOCK_ELECTION_HANDLER_PORT_BASE + 11;
    let test_context = setup(port).await;

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

    match test_context
        .election_handler
        .handle_vote_request(
            vote_request,
            current_term,
            voted_for_option,
            &Arc::new(test_context.raft_log_mock),
        )
        .await
    {
        Ok(state_update) => {
            assert!(!state_update.step_to_follower);
            assert!(state_update.new_voted_for.is_none());
        }
        Err(_) => assert!(false),
    }
}

/// Case 1.1: Term and local log index compare
/// - current_term >= VoteRequest Term
///
/// Criterias:
/// - false
#[tokio::test]
async fn test_check_vote_request_is_legal_case_1_1() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_1_1", None, false);
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
async fn test_check_vote_request_is_legal_case_1_2() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_1_2", None, false);
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
async fn test_check_vote_request_is_legal_case_1_3() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_1_3", None, false);
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
async fn test_check_vote_request_is_legal_case_1_4() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_1_4", None, false);
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
async fn test_check_vote_request_is_legal_case_2_1() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_2_1", None, false);
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
async fn test_check_vote_request_is_legal_case_2_2() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_2_2", None, false);
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
async fn test_check_vote_request_is_legal_case_2_3() {
    // 1. Prepare RaftContext mock
    let context = setup_raft_components("/tmp/test_check_vote_request_is_legal_case_2_3", None, false);
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
