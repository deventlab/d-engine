use std::sync::Arc;

use tokio::sync::watch;

use crate::{
    alias::{ROF, TROF},
    grpc::rpc_service::{VoteRequest, VotedFor},
    test_utils::{mock_raft, setup_raft_components, MockTypeConfig},
    ElectionCore, MockRaftLog, MockTransport,
};

/// Case 1:
/// - as candidate
/// - I have voted myself already
///
/// Criterias:
/// - get_followers_candidates_channel_and_role will be called zero time
/// - node term been increased
///
#[tokio::test]
async fn test_broadcast_vote_requests_case1() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_broadcast_vote_requests_case1", graceful_rx, None);
    let election_handler = raft.ctx.election_handler();

    // 2.
    let term = 1;
    let voting_members = Vec::new();
    let mut raft_log_mock: Arc<ROF<MockTypeConfig>> = Arc::new(MockRaftLog::new());
    let mut transport_mock: Arc<TROF<MockTypeConfig>> = Arc::new(MockTransport::new());
    let settings = raft.settings.clone();

    election_handler
        .broadcast_vote_requests(
            term,
            voting_members,
            &raft_log_mock,
            &transport_mock,
            &settings,
        )
        .await;

    // let c = test_utils::setup("/tmp/test_broadcast_vote_requests_case1", None).await;
    // let raft = c.node.raft.clone();
    // let state = raft.state();
    // let my_id = raft.id();
    // let my_current_term = state.current_term();

    // //prepare the node as follower
    // let mut cluster_membership_controller_mock = MockClusterMembershipControllerApis::new();
    // cluster_membership_controller_mock
    //     .expect_is_node_candidate()
    //     .times(1)
    //     .returning(|_| true);
    // cluster_membership_controller_mock
    //     .expect_get_followers_candidates_channel_and_role()
    //     .times(0)
    //     .returning(|| vec![]);

    // // prepare I have voted myself already
    // state.update_voted_for(VotedFor {
    //     voted_for_id: my_id,
    //     voted_for_term: my_current_term + 2, // make sure I can not vote myself anymore
    // });

    // let mut append_entries_controller_mock = MockLogReplicationController::new();
    // append_entries_controller_mock
    //     .expect_send_write_request()
    //     .returning(|_| Ok(()));
    // let mut mock_ctx = MockRaftContextApis::new();

    // let controller = ElectionController::new(
    //     Arc::new(mock_ctx),
    //     raft.settings.clone(),
    //     Arc::new(cluster_membership_controller_mock),
    //     raft.raft_event_sender(),
    //     Arc::new(append_entries_controller_mock),
    //     raft.rpc_client(),
    // );
    // controller.broadcast_vote_requests().await.expect("should succeed");
    // assert_eq!(my_current_term + 1, state.current_term());
}

/// Case 2:
/// - as candidate
/// - I have not voted myself yet
/// - I didn't receive majority votes from peers
///
/// Criterias:
/// - raft_event_receiver should not receive my role change event
/// - node term been increased
///
#[tokio::test]
async fn test_broadcast_vote_requests_case2() {
    // let c = test_utils::setup("/tmp/test_broadcast_vote_requests_case2", None).await;
    // let raft = c.node.raft.clone();
    // let state = raft.state();
    // let my_id = raft.id();
    // let peer2_id = 2;
    // let my_current_term = state.current_term();

    // //0. prepare a real Network address
    // let port = MOCK_CLUSTER_MEMBERSHIP_CONTROLLER_PORT_BASE + 2;
    // let (tx, rx) = oneshot::channel::<()>();
    // let address = test_utils::MockNode::simulate_mock_service_without_reps(port, rx)
    //     .await
    //     .expect("should succeed");

    // // 1. prepare cluster membership controller
    // let mut cluster_membership_controller_mock = MockClusterMembershipControllerApis::new();
    // // 1.2 prepare peer address so that we could compose ChannelWithAddressAndRole result
    // cluster_membership_controller_mock
    //     .expect_get_followers_candidates_channel_and_role()
    //     .times(1)
    //     .returning(move || {
    //         vec![ChannelWithAddressAndRole {
    //             id: peer2_id,
    //             channel_with_address: address.clone(),
    //             role: RaftRole::Follower,
    //         }]
    //     });
    // // 1.2. prepare the node as follower
    // cluster_membership_controller_mock
    //     .expect_is_node_candidate()
    //     .times(1)
    //     .returning(|_| true);

    // // 2. prepare I didn't receive majority votes from peers
    // let mut rpc_client_mock = MockTransport::new();
    // rpc_client_mock
    //     .expect_send_vote_requests()
    //     .returning(|_, _, _| Ok(false));

    // let mut append_entries_controller_mock = MockLogReplicationController::new();
    // append_entries_controller_mock
    //     .expect_send_write_request()
    //     .returning(|_| Ok(()));
    // let (raft_event_sender, raft_event_receiver) = mpsc::unbounded_channel::<EventType>();
    // let mut mock_ctx = MockRaftContextApis::new();

    // let controller = ElectionController::new(
    //     Arc::new(mock_ctx),
    //     raft.settings.clone(),
    //     Arc::new(cluster_membership_controller_mock),
    //     raft_event_sender,
    //     Arc::new(append_entries_controller_mock),
    //     Arc::new(rpc_client_mock),
    // );
    // controller.broadcast_vote_requests().await.expect("should succeed");

    // let event_listner = raft.event_listener();

    // match event_listner.try_recv(raft_event_receiver).await {
    //     Ok(_) => {
    //         assert!(false, "Expected no event, but received one.");
    //     }
    //     Err(e) => {
    //         assert!(true); // The channel is empty, as expected.
    //     }
    // }

    // assert_eq!(my_current_term + 1, state.current_term());
}

/// Case 3:
/// - as candidate
/// - I have not voted myself yet
/// - I receives majority votes from peers
///
/// Criterias:
/// - raft_event_receiver should receive my role change event as Leader
/// - node term been increased
///
#[tokio::test]
async fn test_broadcast_vote_requests_case3() {
    // let c = test_utils::setup("/tmp/test_broadcast_vote_requests_case3", None).await;
    // let raft = c.node.raft.clone();
    // let state = raft.state();
    // let my_id = raft.id();
    // let peer2_id = 2;
    // let my_current_term = state.current_term();

    // //0. prepare a real Network address
    // let port = MOCK_CLUSTER_MEMBERSHIP_CONTROLLER_PORT_BASE + 3;
    // let (tx, rx) = oneshot::channel::<()>();
    // let address = test_utils::MockNode::simulate_mock_service_without_reps(port, rx)
    //     .await
    //     .expect("should succeed");

    // // 1. prepare cluster membership controller
    // let mut cluster_membership_controller_mock = MockClusterMembershipControllerApis::new();
    // // 1.2 prepare peer address so that we could compose ChannelWithAddressAndRole result
    // cluster_membership_controller_mock
    //     .expect_get_followers_candidates_channel_and_role()
    //     .times(1)
    //     .returning(move || {
    //         vec![ChannelWithAddressAndRole {
    //             id: peer2_id,
    //             channel_with_address: address.clone(),
    //             role: RaftRole::Follower,
    //         }]
    //     });
    // // 1.2. prepare the node as follower
    // cluster_membership_controller_mock
    //     .expect_is_node_candidate()
    //     .times(1)
    //     .returning(|_| true);

    // // 2. prepare I didn't receive majority votes from peers
    // let mut rpc_client_mock = MockTransport::new();
    // rpc_client_mock
    //     .expect_send_vote_requests()
    //     .returning(|_, _, _| Ok(true));

    // let mut append_entries_controller_mock = MockLogReplicationController::new();
    // append_entries_controller_mock
    //     .expect_send_write_request()
    //     .returning(|_| Ok(()));
    // let (raft_event_sender, raft_event_receiver) = mpsc::unbounded_channel::<EventType>();
    // let mut mock_ctx = MockRaftContextApis::new();

    // let controller = ElectionController::new(
    //     Arc::new(mock_ctx),
    //     raft.settings.clone(),
    //     Arc::new(cluster_membership_controller_mock),
    //     raft_event_sender,
    //     Arc::new(append_entries_controller_mock),
    //     Arc::new(rpc_client_mock),
    // );
    // controller.broadcast_vote_requests().await.expect("should succeed");
    // let event_listner = raft.event_listener();
    // match event_listner.try_recv(raft_event_receiver).await {
    //     Ok(_) => {
    //         assert!(true);
    //     }
    //     Err(e) => {
    //         panic!("Unexpected error while checking channel: {:?}", e);
    //     }
    // }

    // assert_eq!(my_current_term + 1, state.current_term());
}

/// Case 1.1: Term and local log index compare
/// - current_term >= VoteRequest Term
///
/// Criterias:
/// - false
#[tokio::test]
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
    // let voted_id = 0;
    // let voted_term = 0;

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
