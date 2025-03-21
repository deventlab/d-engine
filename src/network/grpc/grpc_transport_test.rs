use crate::{
    grpc::{
        grpc_transport::GrpcTransport,
        rpc_service::{AppendEntriesResponse, VoteResponse},
    },
    test_utils::{
        self, setup_raft_components, MockNode, MockRpcService, MOCK_RPC_CLIENT_PORT_BASE,
    },
    AppendResults, ChannelWithAddress, ChannelWithAddressAndRole, RaftSettings, Settings,
    Transport, CANDIDATE, FOLLOWER,
};
use std::collections::HashMap;
use tokio::sync::oneshot;

use super::{
    rpc_service::{AppendEntriesRequest, VoteRequest},
    *,
};

// Case 1: no followers or candidates found in cluster,
// Criterias: function should return false
//
#[tokio::test]
async fn test_send_append_requests_case1() {
    let my_id = 1;
    let client = GrpcTransport { my_id };
    if let Err(Error::AppendEntriesNoPeerFound) = client
        .send_append_requests(1, vec![], RaftSettings::default())
        .await
    {
        assert!(true);
    } else {
        assert!(false);
    }
}

async fn simulate_append_entries_mock_server(
    port: u64,
    response: AppendEntriesResponse,
    rx: oneshot::Receiver<()>,
) -> Result<ChannelWithAddress> {
    //prepare learner's channel address inside membership config
    let mut mock_service = MockRpcService::default();
    mock_service.expected_append_entries_response = Some(Ok(response));
    let addr = match test_utils::MockNode::mock_listener(mock_service, port, rx, true).await {
        Ok(a) => a,
        Err(e) => {
            assert!(false);
            return Err(Error::GeneralServerError(format!(
                "test_utils::MockNode::mock_listener failed: {:?}",
                e
            )));
        }
    };
    Ok(test_utils::MockNode::mock_channel_with_address(addr.to_string(), port).await)
}

// Case 2: find new leader.
//
// Setup:
// - peer2's term 10 > leader's term:1
//
// Criterias:
// - even peer response with match_index: 10, the leader's state match_index should not be updated
// - the function should return FoundNewLeader error
//
#[tokio::test]
async fn test_send_append_requests_case2() {
    test_utils::enable_logger();
    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_2_term = 10; //key of unit test
    let peer_2_match_index = 1;
    let response = AppendEntriesResponse {
        id: peer_2_id,
        term: peer_2_term,
        success: false,
        match_index: peer_2_match_index,
    };
    let (tx, rx) = oneshot::channel::<()>();
    let addr = simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 2, response, rx)
        .await
        .expect("should succeed");

    let peer_2_address = addr;
    let peer_2_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![(peer_2_id, peer_2_address, peer_2_req)];

    //step2: testing
    // let mut mock_state = MockStateApis::new();
    // mock_state.expect_current_term().times(1).returning(|| 1);
    // mock_state
    //     .expect_update_match_index()
    //     .with(eq(peer_2_id), eq(peer_2_match_index))
    //     .times(0)
    //     .returning(|_, _| {});
    // mock_state
    //     .expect_update_next_index()
    //     .with(eq(peer_2_id), eq(peer_2_match_index + 1))
    //     .times(0)
    //     .returning(|_, _| {});

    let settings = Settings::new().expect("Should succeed to init Settings.");

    let my_id = 1;
    let client = GrpcTransport { my_id };
    if let Err(e) = client
        .send_append_requests(1, requests_with_peer_address, settings.raft_settings)
        .await
    {
        assert!(matches!(e, Error::FoundNewLeaderError(_)));
    } else {
        assert!(false);
    }
}

// # Case 3: didn't receive majory confirmation.
//
// ## Setup:
// 1. prepare two peers, peer2 success, while peer3 failed
//
// ## Criterias:
// 1. peer2's match index and next index will be updated
// 2. peer3's match index and next index will not be updated
// 3. return Ok(true)
//
#[tokio::test]
async fn test_send_append_requests_case3() {
    test_utils::enable_logger();

    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_3_id = 3;
    let peer_2_term = leader_current_term;
    let peer_2_match_index = 10;
    let peer_3_term = leader_current_term;
    let peer_3_match_index = 1;
    let peer_2_response = AppendEntriesResponse {
        id: peer_2_id,
        term: peer_2_term,
        success: true,
        match_index: peer_2_match_index,
    };
    let peer_3_response = AppendEntriesResponse {
        id: peer_3_id,
        term: peer_3_term,
        success: false,
        match_index: peer_3_match_index,
    };
    let (_tx2, rx2) = oneshot::channel::<()>();
    let addr2 =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 3, peer_2_response, rx2)
            .await
            .expect("should succeed");
    let (_tx3, rx3) = oneshot::channel::<()>();
    let addr3 =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 4, peer_3_response, rx3)
            .await
            .expect("should succeed");

    let peer_2_address = addr2;
    let peer_3_address = addr3;
    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![
        (peer_2_id, peer_2_address, peer_req.clone()),
        (peer_3_id, peer_3_address, peer_req),
    ];

    let settings = Settings::new().expect("Should succeed to init Settings.");

    let my_id = 1;
    let client = GrpcTransport { my_id };
    match client
        .send_append_requests(1, requests_with_peer_address, settings.raft_settings)
        .await
    {
        Ok(AppendResults {
            commit_quorum_achieved,
            peer_updates,
        }) => {
            let peer2_result = peer_updates.get(&peer_2_id).unwrap();
            let peer3_result = peer_updates.get(&peer_3_id).unwrap();
            assert!(commit_quorum_achieved);
            assert_eq!(peer2_result.match_index, peer_2_match_index);
            assert!(peer2_result.success);
            assert_eq!(peer3_result.match_index, peer_3_match_index);
            assert!(!peer3_result.success);
        }
        Err(_) => assert!(false),
    }

    // mock_persistent_state.checkpoint();
}

// Case 4: received majory confirmation.
//
// Setup:
// - prepare two peers, both peer2 and peer3 returns success
//
// Criterias:
// - both peer2 and peer3's match index and next index been updated
// - return Ok(true)
//
#[tokio::test]
async fn test_send_append_requests_case4() {
    test_utils::enable_logger();

    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_3_id = 3;
    let peer_2_term = leader_current_term;
    let peer_2_match_index = 11;
    let peer_3_term = leader_current_term;
    let peer_3_match_index = 12;
    let peer_2_response = AppendEntriesResponse {
        id: peer_2_id,
        term: peer_2_term,
        success: true,
        match_index: peer_2_match_index,
    };
    let peer_3_response = AppendEntriesResponse {
        id: peer_3_id,
        term: peer_3_term,
        success: true,
        match_index: peer_3_match_index,
    };
    let (tx2, rx2) = oneshot::channel::<()>();
    let addr2 =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 5, peer_2_response, rx2)
            .await
            .expect("should succeed");
    let (tx3, rx3) = oneshot::channel::<()>();
    let addr3 =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 6, peer_3_response, rx3)
            .await
            .expect("should succeed");

    let peer_2_address = addr2;
    let peer_3_address = addr3;
    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![
        (peer_2_id, peer_2_address, peer_req.clone()),
        (peer_3_id, peer_3_address, peer_req),
    ];

    let settings = Settings::new().expect("Should succeed to init Settings.");
    let my_id = 1;
    let client = GrpcTransport { my_id };
    match client
        .send_append_requests(1, requests_with_peer_address, settings.raft_settings)
        .await
    {
        Ok(AppendResults {
            commit_quorum_achieved,
            peer_updates,
        }) => {
            let peer2_result = peer_updates.get(&peer_2_id).unwrap();
            let peer3_result = peer_updates.get(&peer_3_id).unwrap();
            assert!(commit_quorum_achieved);
            assert_eq!(peer2_result.match_index, peer_2_match_index);
            assert!(peer2_result.success);
            assert_eq!(peer3_result.match_index, peer_3_match_index);
            assert!(peer3_result.success);
        }
        Err(_) => assert!(false),
    }

    // mock_persistent_state.checkpoint();
}

// # Case 5: didn't receive majory confirmation.
//
// ## Setup:
// 1. prepare one peer which is node itself
//
// ## Criterias:
// 1. return Ok(false)
//
#[tokio::test]
async fn test_send_append_requests_case5() {
    test_utils::enable_logger();

    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let leader_response = AppendEntriesResponse {
        id: leader_id,
        term: leader_current_term,
        success: false,
        match_index: leader_commit_index,
    };
    let (_tx2, rx2) = oneshot::channel::<()>();
    let leader_address =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 7, leader_response, rx2)
            .await
            .expect("should succeed");

    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![(leader_id, leader_address, peer_req)];

    let settings = Settings::new().expect("Should succeed to init Settings.");
    let client = GrpcTransport { my_id: leader_id };
    if let Err(Error::AppendEntriesNoPeerFound) = client
        .send_append_requests(1, vec![], settings.raft_settings.clone())
        .await
    {
        assert!(true);
    } else {
        assert!(false);
    }
}

// # Case 6: passed peers with duplicates
//
// ## Setup:
// 1. prepare [peer1, peer1, peer2] as `peers` parameter
// 2. both peer1 and peer2 return success
//
// ## Criterias:
// 1. return Ok(true)
//
#[tokio::test]
async fn test_send_append_requests_case6() {
    test_utils::enable_logger();

    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_3_id = 3;
    let peer_2_term = leader_current_term;
    let peer_2_match_index = 1;
    let peer_3_term = leader_current_term;
    let peer_3_match_index = 1;
    let peer_2_response = AppendEntriesResponse {
        id: peer_2_id,
        term: peer_2_term,
        success: true,
        match_index: peer_2_match_index,
    };
    let peer_3_response = AppendEntriesResponse {
        id: peer_3_id,
        term: peer_3_term,
        success: true,
        match_index: peer_3_match_index,
    };
    let (_tx2, rx2) = oneshot::channel::<()>();
    let addr2 =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 8, peer_2_response, rx2)
            .await
            .expect("should succeed");
    let (_tx3, rx3) = oneshot::channel::<()>();
    let addr3 =
        simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 9, peer_3_response, rx3)
            .await
            .expect("should succeed");

    let peer_2_address = addr2;
    let peer_3_address = addr3;
    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![
        (peer_2_id, peer_2_address.clone(), peer_req.clone()),
        (peer_2_id, peer_2_address, peer_req.clone()),
        (peer_3_id, peer_3_address, peer_req),
    ];
    let settings = Settings::new().expect("Should succeed to init Settings.");

    let client = GrpcTransport { my_id: leader_id };
    match client
        .send_append_requests(1, requests_with_peer_address, settings.raft_settings)
        .await
    {
        Ok(AppendResults {
            commit_quorum_achieved,
            peer_updates,
        }) => {
            let peer2_result = peer_updates.get(&peer_2_id).unwrap();
            let peer3_result = peer_updates.get(&peer_3_id).unwrap();
            assert!(commit_quorum_achieved);
            assert_eq!(peer2_result.match_index, peer_2_match_index);
            assert!(peer2_result.success);
            assert_eq!(peer3_result.match_index, peer_3_match_index);
            assert!(peer3_result.success);
        }
        Err(_) => assert!(false),
    }
}

// # Case 7: Leader solve conflicts (Figure 7 in raft paper, github#112)
//
// ## Setup:
// 1.
//     leader:     log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6)
//     follower_a: log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6),
//     follower_b: log1(1), log2(1), log3(1), log4(4),
//     follower_c: log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6)
//     follower_d: log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6), log11(7), log12(7)
//     follower_e: log1(1), log2(1), log3(1), log4(4), log5(4), log6(4), log7(4)
//     follower_f: log1(1), log2(1), log3(1), log4(2), log5(2), log6(2), log7(3), log8(3), log9(3), log10(3), log11(3)
//
// ## Criterias:
// 1. next_id been updated to:
// Follower	lastIndex	nextIndex
// follower_a	    9	    10
// follower_b	    4	    5
// follower_c	    10	    11
// follower_d	    12	    11
// follower_e	    7	    6
// follower_f	    11  	4
//
#[tokio::test]
async fn test_send_append_requests_case7() {
    let mut c = setup_raft_components("/tmp/test_send_append_requests_case7", None, false);

    // 1: Setup - Initialize leader and followers
    let leader_id = 1;
    let leader_current_term = 6;
    let leader_commit_index = 5;
    // Leader's log
    let leader_log = vec![
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 4),
        (5, 4),
        (6, 5),
        (7, 5),
        (8, 6),
        (9, 6),
        (10, 6),
    ];
    for (index, term) in leader_log {
        test_utils::simulate_insert_proposal(&c.raft_log, vec![index], term);
    }

    // 2. write down expected test result
    let mut expected_next_indexes: HashMap<u32, u64> =
        [(2, 10), (3, 5), (4, 11), (5, 11), (6, 5), (7, 3)]
            .into_iter()
            .collect();

    // 3. Simulate RPC service response

    //server shutdown signal's lifetime should be the same as this unit test, otherwise server will die
    let (_tx, rx2) = oneshot::channel::<()>();
    let (_tx, rx3) = oneshot::channel::<()>();
    let (_tx, rx4) = oneshot::channel::<()>();
    let (_tx, rx5) = oneshot::channel::<()>();
    let (_tx, rx6) = oneshot::channel::<()>();
    let (_tx, rx7) = oneshot::channel::<()>();
    // Prepare the response messages for followers
    let peer_responses = vec![
        // (id, term, success, match_index)
        (2, 6, true, 9, rx2),  // Follower 2 response
        (3, 6, true, 4, rx3),  // Follower 3 response
        (4, 6, true, 10, rx4), // Follower 4 response
        (5, 6, true, 10, rx5), // Follower 5 response
        (6, 6, false, 5, rx6), // Follower 6 response
        (7, 6, false, 3, rx7), // Follower 7 response
    ];

    let mut peer_addresses = vec![];

    // Loop to create mock servers for each peer and store addresses
    for (id, term, success, match_index, rx) in peer_responses {
        let peer_response = AppendEntriesResponse {
            id,
            term,
            success,
            match_index,
        };

        let addr = simulate_append_entries_mock_server(
            MOCK_RPC_CLIENT_PORT_BASE + 10 + id as u64,
            peer_response,
            rx,
        )
        .await
        .expect("should succeed");

        peer_addresses.push((id, addr));
    }

    // 4. Prepare the request message to be sent to all followers
    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };

    // Construct the requests with addresses for each peer
    let requests_with_peer_address = peer_addresses
        .iter()
        .map(|(id, addr)| (id.clone(), addr.clone(), peer_req.clone()))
        .collect::<Vec<_>>();

    // 5. Test - Send AppendEntries RPCs
    let settings = Settings::new().expect("Should succeed to init settings");
    let client = GrpcTransport { my_id: leader_id };

    match client
        .send_append_requests(
            leader_current_term,
            requests_with_peer_address,
            settings.raft_settings,
        )
        .await
    {
        Ok(AppendResults {
            commit_quorum_achieved,
            peer_updates,
        }) => {
            // 6. Validate Results
            for (peer_id, expected_next) in &expected_next_indexes {
                match peer_updates.get(peer_id) {
                    Some(update) => {
                        if update.success {
                            assert_eq!(
                                update.next_index, *expected_next,
                                "Peer {}: next_index should be {} on success, but is {}",
                                peer_id, expected_next, update.next_index
                            );
                        } else {
                            assert_eq!(
                                update.next_index, update.match_index,
                                "Peer {}: next_index should equal match_index {} on failure, but is {}",
                                peer_id, update.match_index, update.next_index
                            );
                        }
                    }
                    None => panic!("Peer {} not found in results", peer_id),
                }
            }
        }
        Err(_) => assert!(false),
    }
}

// # Case 1: no peers passed
//
// ## Criterias:
// 1. return Ok(false)
//
#[tokio::test]
async fn test_send_vote_requests_case1() {
    test_utils::enable_logger();

    let my_id = 1;
    let settings = Settings::new().expect("Should succeed to init Settings.");
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(vec![], request, &settings.raft_settings)
        .await
    {
        Ok(res) => assert!(!res),
        Err(_) => assert!(false),
    }
}

// # Case 2: passed peers only include the node itself
//
// ## Criterias:
// 1. return Ok(false)
//
#[tokio::test]
async fn test_send_vote_requests_case2() {
    test_utils::enable_logger();

    let my_id = 1;
    let settings = Settings::new().expect("Should succeed to init Settings.");

    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let my_response = VoteResponse {
        term: 1,
        vote_granted: false,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let addr1 =
        MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 10, my_response, rx1)
            .await
            .expect("should succeed");
    let requests_with_peer_address = vec![ChannelWithAddressAndRole {
        id: my_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    }];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &settings.raft_settings)
        .await
    {
        Ok(res) => assert!(!res),
        Err(_) => assert!(false),
    }
}

// # Case 3: passed peers with duplicates
//
// ## Setup
// 1. prepare [peer1, peer1, peer2] as `peers` parameter
// 2. both peer1 and peer2 return success
//
// ## Criterias:
// 1. return Ok(true)
//
#[tokio::test]
async fn test_send_vote_requests_case3() {
    test_utils::enable_logger();

    let my_id = 1;
    let settings = Settings::new().expect("Should succeed to init Settings.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let my_response = VoteResponse {
        term: 1,
        vote_granted: true,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let addr1 =
        MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 21, my_response, rx1)
            .await
            .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &settings.raft_settings)
        .await
    {
        Ok(res) => assert!(res),
        Err(_) => assert!(false),
    }
}

// # Case 4: two peers passed
//
// ## Setup:
// 1. prepare two peers, both peer failed
//
// ## Criterias:
// 1. return Ok(false)
//
#[tokio::test]
async fn test_send_vote_requests_case4() {
    test_utils::enable_logger();

    let my_id = 1;
    let settings = Settings::new().expect("Should succeed to init Settings.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let my_response = VoteResponse {
        term: 1,
        vote_granted: false,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let addr1 =
        MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 22, my_response, rx1)
            .await
            .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &settings.raft_settings)
        .await
    {
        Ok(res) => assert!(!res),
        Err(_) => assert!(false),
    }
}

// # Case 5: two peers passed
//
// ## Setup:
// 1. prepare two peers, one success while another failed
//
// ## Criterias:
// 1. return Ok(true)
//
#[tokio::test]
async fn test_send_vote_requests_case5() {
    test_utils::enable_logger();

    let my_id = 1;
    let settings = Settings::new().expect("Should succeed to init Settings.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_tx2, rx2) = oneshot::channel::<()>();
    let peer1_response = VoteResponse {
        term: 1,
        vote_granted: false,
    };
    let peer2_response = VoteResponse {
        term: 1,
        vote_granted: true,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let addr1 = MockNode::simulate_send_votes_mock_server(
        MOCK_RPC_CLIENT_PORT_BASE + 23,
        peer1_response,
        rx1,
    )
    .await
    .expect("should succeed");
    let addr2 = MockNode::simulate_send_votes_mock_server(
        MOCK_RPC_CLIENT_PORT_BASE + 24,
        peer2_response,
        rx2,
    )
    .await
    .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr2.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &settings.raft_settings)
        .await
    {
        Ok(res) => assert!(res),
        Err(_) => assert!(false),
    }
}
