use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    grpc::rpc_service::{AppendEntriesResponse, ClientCommand},
    role_state::RaftRoleState,
    test_utils::{
        setup_raft_components, simulate_insert_proposal, MockNode, MockTypeConfig,
        MOCK_RAFT_PORT_BASE, MOCK_REPLICATION_HANDLER_PORT_BASE,
    },
    utils::util::kv,
    AppendResults, ChannelWithAddress, ChannelWithAddressAndRole, ClientRequestWithSignal, Error,
    LeaderStateSnapshot, MockRaftLog, MockTransport, PeerUpdate, RaftRole, RaftTypeConfig,
    StateSnapshot, FOLLOWER,
};

use super::{ReplicationCore, ReplicationHandler};

/// # Case 1: No peers found
/// ## Validation Criteria
/// 1. Return Error::AppendEntriesNoPeerFound
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case1() {
    let context = setup_raft_components(
        "/tmp/test_handle_client_proposal_in_batch_case1",
        None,
        false,
    );
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Prepare fun parameters
    let commands = Vec::new();
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 1,
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        noop_log_id: None,
    };
    let replication_members: Vec<ChannelWithAddressAndRole> = vec![];

    let raft_log = MockRaftLog::new();
    let transport = MockTransport::new();
    let raft_settings = context.settings.raft_settings.clone();

    if let Err(Error::AppendEntriesNoPeerFound) = handler
        .handle_client_proposal_in_batch(
            commands,
            state_snapshot,
            leader_state_snapshot,
            &replication_members,
            &Arc::new(raft_log),
            &Arc::new(transport),
            raft_settings,
        )
        .await
    {
        assert!(true);
    } else {
        assert!(false);
    }
}

/// # Case 2.1: Successful Client Proposal Replication
/// Validates leader behavior when sending heartbeat(empty commands)
///
/// ## Scenario Setup
/// Log State Initialization:
/// - Peer1:
///   - Log entries: [1, 2, 3]
///   - next_index: 3
///   - to_be_synced_logs: [4, 5, 6, 7]
/// - Peer2:
///   - Log entries: [1, 2, 3, 4]
///   - next_index: 4
///   - to_be_synced_logs: [5, 6, 7]
/// - Leader:
///   - Log entries: [1, 2, 3, 4, 5, 6, 7]
///   - commit_index: 4 (pre-operation)
///
/// Test Context:
/// - Batch contains 1 client command
/// - Peer2 responds with successful acknowledgment (Ok(true))
///
/// ## Validation Criteria
///- handle_client_proposal_in_batch returns append_result which responsed from network layer
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case2_1() {
    let context = setup_raft_components(
        "/tmp/test_handle_client_proposal_in_batch_case2",
        None,
        false,
    );
    let my_id = 1;
    let peer2_id = 2;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Prepare fun parameters
    let commands = Vec::new();
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 1,
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 =
        MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 1, rx1)
            .await
            .expect("should succeed");

    // Prepare AppendResults
    let replication_members: Vec<ChannelWithAddressAndRole> = vec![ChannelWithAddressAndRole {
        id: peer2_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    }];
    let append_result = AppendResults {
        commit_quorum_achieved: true,
        peer_updates: HashMap::from([(
            peer2_id,
            PeerUpdate {
                match_index: 3,
                next_index: 4,
                success: true,
            },
        )]),
    };
    let append_result_clone = append_result.clone();

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log
        .expect_pre_allocate_raft_logs_next_index()
        .returning(|| 1);
    raft_log.expect_get_entries_between().returning(|_| vec![]);
    raft_log.expect_prev_log_term().returning(|_, _| 0);
    let mut transport = MockTransport::new();
    transport
        .expect_send_append_requests()
        .returning(move |_, _, _| Ok(append_result_clone.clone()));
    let raft_settings = context.settings.raft_settings.clone();

    if let Ok(append_result) = handler
        .handle_client_proposal_in_batch(
            commands,
            state_snapshot,
            leader_state_snapshot,
            &replication_members,
            &Arc::new(raft_log),
            &Arc::new(transport),
            raft_settings,
        )
        .await
    {
        assert_eq!(append_result.commit_quorum_achieved, true);
        println!("append_result: {:?}", &append_result);
    } else {
        assert!(false);
    }
}

/// # Case 2.2: replicate client proposal successfully
///
/// ## Setup
/// - batch: commands contains 10 command
/// - one peer responds Ok(true)
///
/// ## Criterias
/// - handle_client_proposal_in_batch returns with Ok(())
/// - receive 10 success event from reciver signal: Ok(ClientResponse::write_success)
/// - leader commit index must be updated with new one
///
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case2_2() {}

/// # Case 3.1: replicate client proposal failed
///
/// ## Setup
/// - batch: commands contains 10 command
/// - one peer responds Ok(false)
///
/// ## Criterias
/// - handle_client_proposal_in_batch returns with Ok(())
/// - receive 10 event from reciver signal: Ok(ClientResponse::write_error)
/// - leader commit index must not be updated
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case3_1() {}

/// # Case 3.2: replicate client proposal failed, with FoundNewLeaderError
///
/// ## Setup
/// - batch: commands contains 10 command
/// - one peer responds Error::FoundNewLeaderError(new_leader_id)
///
/// ## Criterias
/// - handle_client_proposal_in_batch returns with Ok(())
/// - receive BecomeFollower event from reciver signal: Ok(ClientResponse::write_error)
/// - receive 10 event from reciver signal: Ok(ClientResponse::write_error)
/// - leader commit index must not be updated
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case3_2() {}

/// # Case 3.3: replicate client proposal failed, with GeneralLocalLogIOError
///
/// ## Setup
/// - batch: commands contains 10 command
/// - one peer responds Error::FoundNewLeaderError(new_leader_id)
///
/// ## Criterias
/// - handle_client_proposal_in_batch returns with Ok(())
/// - receive BecomeFollower event from reciver signal: Ok(ClientResponse::write_error)
/// - receive 10 event from reciver signal: Ok(ClientResponse::write_error)
/// - leader commit index must not be updated
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case3_3() {}

/// # Case 4: Heartbeat Request with Peer Catching Up
/// Validates leader behavior when handling empty client proposals (heartbeat)
/// with unsynchronized peers in a batch operation context.
///
/// ## Scenario Setup
/// Log State Initialization:
/// - Peer1:
///   - Log entries: [1, 2, 3]
///   - next_index: 3
/// - Peer2:
///   - Log entries: [1, 2, 3, 4]
///   - next_index: 4
/// - Leader:
///   - Log entries: [1, 2, 3, 4]
///   - commit_index: <current state>
///
/// Test Context:
/// - Batch proposal contains empty commands (heartbeat signal)
///
/// ## Validation Criteria
/// The test succeeds if all following conditions are met:
/// 1. Return Value:
///    - `handle_client_proposal_in_batch()` returns `Ok(())`
/// 2. Event Signaling:
///    - Success event received through notification channel
/// 3. State Consistency:
///    - Leader's commit_index remains unchanged
///    - No new log entries appended to leader's storage
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case4() {}

//------------------------

// / Case 4.2 : It is a client insert request
// /     one peer has not catched up with leader's latest status
// /
// / ## Setup:
// / 1. peer1 has log-1,log-2,log-3
// / 2. peer2 has log-1,log-2,log-3,log-4
// / 3. leader has log-1,log-2,log-3,log-4
// / 4. leader receive one client command
// /
// /
// / Criterias:
// / - leader's local log should have latest log-5
// / - leader state commit index been updated to 5

// #[tokio::test]
// async fn test_process_batch_case4_2() {
//     let c = test_utils::setup("/tmp/test_process_batch_case4_2", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let raft_log = raft.raft_log();
//     let state = raft.state();
//     let leader_commit_index = 4;
//     let peer1_id = 2;
//     let peer1_next_index = 4;
//     let peer1_match_index = 5;
//     let peer2_id = 3;
//     let peer2_next_index = 5;
//     let peer2_match_index = 5;
//     state.update_commit_index(leader_commit_index);
//     state.update_next_index(peer1_id, peer1_next_index);
//     state.update_next_index(peer2_id, peer2_next_index);
//     state.update_match_index(peer1_id, peer1_match_index);
//     state.update_match_index(peer2_id, peer2_match_index);
//     simulate_insert_proposal(raft_log.clone(), (1..=4).collect(), 1);

//     //prepare learner's channel address inside membership config
//     let response1 = AppendEntriesResponse {
//         id: peer1_id,
//         term: 1,
//         success: true,
//         match_index: peer1_match_index,
//     };
//     let port = MOCK_APPEND_ENTRIES_CONTROLLER_PORT_BASE + 6;
//     let (_tx, rx) = oneshot::channel::<()>();
//     let address1 =
//         test_utils::MockNode::simulate_mock_service_with_append_reps(port, response1, rx)
//             .await
//             .expect("should succeed");

//     let response2 = AppendEntriesResponse {
//         id: peer2_id,
//         term: 1,
//         success: true,
//         match_index: peer2_match_index,
//     };
//     let port = MOCK_APPEND_ENTRIES_CONTROLLER_PORT_BASE + 7;
//     let (_tx, rx) = oneshot::channel::<()>();
//     let address2 =
//         test_utils::MockNode::simulate_mock_service_with_append_reps(port, response2, rx)
//             .await
//             .expect("should succeed");

//     let mut rpc_client_mock = MockTransport::new();
//     rpc_client_mock
//         .expect_send_append_requests()
//         .times(1)
//         .return_once(|_, _, _| Ok(true));

//     let mut membership_controller_mock = MockClusterMembershipControllerApis::new();
//     membership_controller_mock
//         .expect_get_followers_candidates_channel_and_role()
//         .times(1)
//         .returning(move || {
//             vec![
//                 ChannelWithAddressAndRole {
//                     id: peer1_id,
//                     channel_with_address: address1.clone(),
//                     role: RaftRole::follower_role_i32(),
//                 },
//                 ChannelWithAddressAndRole {
//                     id: peer2_id,
//                     channel_with_address: address2.clone(),
//                     role: RaftRole::follower_role_i32(),
//                 },
//             ]
//         });

//     let mut command_batch = VecDeque::new();
//     let (append_result_signal_sender, _append_result_signal_receiver) =
//         tokio::sync::mpsc::channel::<crate::Result<Vec<String>>>(1);
//     // Step 1: Send the client request with the signal
//     command_batch.push_back(ClientRequestWithSignal {
//         id: "1".to_string(),
//         commands: vec![ClientCommand::insert(kv(1), kv(1))],
//         sender: append_result_signal_sender,
//     });
//     let (_graceful_tx, graceful_rx) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft_log.clone(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         Arc::new(rpc_client_mock),
//         graceful_rx,
//     )
//     .await;
//     match append_entries_controller.process_batch(command_batch).await {
//         Ok(_r) => {
//             assert!(true)
//         }
//         Err(_e) => assert!(false),
//     }
//     assert_eq!(state.commit_index(), 5);
//     assert_eq!(raft_log.last_entry_id(), 5);
// }

// /// Case 4.3 : It is a no-op request
// ///     one peer has not catched up with leader's latest status
// ///
// /// ## Setup:
// /// 1. peer1 has log-1,log-2,log-3
// /// 2. peer2 has log-1,log-2,log-3,log-4
// /// 3. leader has log-1,log-2,log-3,log-4
// /// 4. leader receive one client command
// ///
// ///
// /// Criterias:
// /// - leader's local log should have latest log-5
// /// - leader state commit index been updated to 5
// ///
// #[tokio::test]
// async fn test_process_batch_case4_3() {
//     let c = test_utils::setup("/tmp/test_process_batch_case4_3", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let raft_log = raft.raft_log();
//     let state = raft.state();
//     let leader_commit_index = 4;
//     let peer1_id = 2;
//     let peer1_next_index = 4;
//     let peer1_match_index = 5;
//     let peer2_id = 3;
//     let peer2_next_index = 5;
//     let peer2_match_index = 5;
//     state.update_commit_index(leader_commit_index);
//     state.update_next_index(peer1_id, peer1_next_index);
//     state.update_next_index(peer2_id, peer2_next_index);
//     state.update_match_index(peer1_id, peer1_match_index);
//     state.update_match_index(peer2_id, peer2_match_index);
//     test_utils::simulate_insert_proposal(raft_log.clone(), (1..=4).collect(), 1);

//     //prepare learner's channel address inside membership config
//     let response1 = AppendEntriesResponse {
//         id: peer1_id,
//         term: 1,
//         success: true,
//         match_index: peer1_match_index,
//     };
//     let port = MOCK_APPEND_ENTRIES_CONTROLLER_PORT_BASE + 8;
//     let (_tx, rx) = oneshot::channel::<()>();
//     let address1 =
//         test_utils::MockNode::simulate_mock_service_with_append_reps(port, response1, rx)
//             .await
//             .expect("should succeed");

//     let response2 = AppendEntriesResponse {
//         id: peer2_id,
//         term: 1,
//         success: true,
//         match_index: peer2_match_index,
//     };
//     let port = MOCK_APPEND_ENTRIES_CONTROLLER_PORT_BASE + 9;
//     let (_tx, rx) = oneshot::channel::<()>();
//     let address2 =
//         test_utils::MockNode::simulate_mock_service_with_append_reps(port, response2, rx)
//             .await
//             .expect("should succeed");

//     let mut rpc_client_mock = MockTransport::new();
//     rpc_client_mock
//         .expect_send_append_requests()
//         .times(1)
//         .return_once(|_, _, _| Ok(true));

//     let mut membership_controller_mock = MockClusterMembershipControllerApis::new();
//     membership_controller_mock
//         .expect_get_followers_candidates_channel_and_role()
//         .times(1)
//         .returning(move || {
//             vec![
//                 ChannelWithAddressAndRole {
//                     id: peer1_id,
//                     channel_with_address: address1.clone(),
//                     role: RaftRole::Follower,
//                 },
//                 ChannelWithAddressAndRole {
//                     id: peer2_id,
//                     channel_with_address: address2.clone(),
//                     role: RaftRole::Follower,
//                 },
//             ]
//         });

//     let mut command_batch = VecDeque::new();
//     let (append_result_signal_sender, _append_result_signal_receiver) =
//         tokio::sync::mpsc::channel::<crate::Result<Vec<String>>>(1);
//     // Step 1: Send the client request with the signal
//     command_batch.push_back(ClientRequestWithSignal {
//         id: "1".to_string(),
//         commands: vec![ClientCommand::no_op()],
//         append_result_signal_sender,
//     });
//     let (_graceful_tx, graceful_rx) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft_log.clone(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         Arc::new(rpc_client_mock),
//         graceful_rx,
//     )
//     .await;
//     match append_entries_controller.process_batch(command_batch).await {
//         Ok(_r) => {
//             assert!(true)
//         }
//         Err(_e) => assert!(false),
//     }
//     assert_eq!(state.commit_index(), 5);
//     assert_eq!(raft_log.last_entry_id(), 5);
// }

// /// # Case 1: when there is no new entries, but peer1 is not synced with leader
// /// ## Setup:
// /// 1. peer1 has log-1,log-2,log-3 (peer1's next_index = 3)
// /// 2. peer2 has log-1,log-2,log-3,log-4 (peer2's next_index = 4)
// /// 3. leader has log-1,log-2,log-3,log-4
// ///
// /// Criterias:
// /// - return peer1: log-4
// /// - return peer2: []
// #[tokio::test]
// async fn test_retrieve_to_be_synced_logs_for_peers_case1() {
//     let c = test_utils::setup("/tmp/test_retrieve_to_be_synced_logs_for_peers_case1", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let raft_log = raft.raft_log();
//     let state = raft.state();
//     let leader_id = state.id();
//     let leader_commit_index = 4;
//     let leader_match_index = 4;
//     let peer1_id = 2;
//     let peer1_match_index = 3;
//     let peer2_id = 3;
//     let peer2_match_index = 4;
//     state.update_commit_index(leader_commit_index);
//     state.update_match_index(leader_id, leader_match_index);
//     state.update_match_index(peer1_id, peer1_match_index);
//     state.update_next_index(peer1_id, peer1_match_index + 1);
//     state.update_match_index(peer2_id, peer2_match_index);
//     state.update_next_index(peer2_id, peer2_match_index + 1);
//     test_utils::simulate_insert_proposal(raft_log.clone(), (1..=4).collect(), 1);
//     let leader_last_index = raft_log.last_entry_id();

//     let rpc_client_mock = MockTransport::new();
//     let membership_controller_mock = MockClusterMembershipControllerApis::new();
//     let (_graceful_tx, graceful_rx) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft_log.clone(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         Arc::new(rpc_client_mock),
//         graceful_rx,
//     )
//     .await;

//     let map = append_entries_controller.retrieve_to_be_synced_logs_for_peers(
//         vec![peer1_id, peer2_id],
//         vec![],
//         leader_last_index,
//         100,
//     );

//     println!("map: {:?}", &map);
//     let peer1_entries = match map.clone().get(&peer1_id) {
//         Some(v) => v.clone(),
//         None => vec![],
//     };
//     assert_eq!(1, peer1_entries.len());
//     assert_eq!(4, peer1_entries[0].index);

//     if let None = map.get(&peer2_id) {
//         assert!(true);
//     } else {
//         assert!(false);
//     };
// }

// /// # Case 2: when there is new entries [log-5], but peer1 is not synced with leader
// /// ## Setup:
// /// 1. peer1 has log-1,log-2,log-3 (peer1's next_index = 3)
// /// 2. peer2 has log-1,log-2,log-3,log-4 (peer2's next_index = 4)
// /// 3. leader has log-1,log-2,log-3,log-4
// ///
// /// Criterias:
// /// - return peer1: log-4,log-5
// /// - return peer2: log-5
// #[tokio::test]
// async fn test_retrieve_to_be_synced_logs_for_peers_case2() {
//     let c = test_utils::setup("/tmp/test_retrieve_to_be_synced_logs_for_peers_case2", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let raft_log = raft.raft_log();
//     let state = raft.state();
//     let leader_id = state.id();
//     let leader_commit_index = 4;
//     let leader_match_index = 5;
//     let peer1_id = 2;
//     let peer1_match_index = 3;
//     let peer2_id = 3;
//     let peer2_match_index = 4;
//     state.update_commit_index(leader_commit_index);
//     state.update_match_index(leader_id, leader_match_index);
//     state.update_match_index(peer1_id, peer1_match_index);
//     state.update_next_index(peer1_id, peer1_match_index + 1);
//     state.update_match_index(peer2_id, peer2_match_index);
//     state.update_next_index(peer2_id, peer2_match_index + 1);
//     test_utils::simulate_insert_proposal(raft_log.clone(), (1..=4).collect(), 1);
//     let leader_last_index1 = raft_log.last_entry_id();
//     //simulate new entry been insert into leader's local log firstly
//     test_utils::simulate_insert_proposal(raft_log.clone(), vec![5], 1);
//     let leader_last_index2 = raft_log.last_entry_id();
//     let new_entries = raft_log.get_entries_between((leader_last_index1 + 1)..=leader_last_index2);

//     let rpc_client_mock = MockTransport::new();
//     let membership_controller_mock = MockClusterMembershipControllerApis::new();
//     let (_graceful_tx, graceful_rx) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         Arc::new(rpc_client_mock),
//         graceful_rx,
//     )
//     .await;

//     let map = append_entries_controller.retrieve_to_be_synced_logs_for_peers(
//         vec![peer1_id, peer2_id],
//         new_entries,
//         leader_last_index1,
//         100,
//     );

//     println!("map: {:?}", &map);
//     let peer1_entries = match map.clone().get(&peer1_id) {
//         Some(v) => v.clone(),
//         None => vec![],
//     };
//     assert_eq!(2, peer1_entries.len());
//     assert_eq!(4, peer1_entries[0].index);

//     if let Some(v) = map.get(&peer2_id) {
//         assert_eq!(v.len(), 1);
//         assert_eq!(5, v[0].index);
//     } else {
//         assert!(false);
//     };
// }

// /// # Case 3: when there is no new entries, all peer has been synced with Leader
// ///
// /// ## Setup:
// /// 1. peer1 has log-1,log-2,log-3,log-4  (peer1's next_index = 4)
// /// 2. peer2 has log-1,log-2,log-3,log-4 (peer2's next_index = 4)
// /// 3. leader has log-1,log-2,log-3,log-4
// ///
// /// Criterias:
// /// - retrieve_to_be_synced_logs_for_peers return empty map, which mean len = 0.
// ///
// #[tokio::test]
// async fn test_retrieve_to_be_synced_logs_for_peers_case3() {
//     let c = test_utils::setup("/tmp/test_retrieve_to_be_synced_logs_for_peers_case3", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let raft_log = raft.raft_log();
//     let state = raft.state();
//     let leader_id = state.id();
//     let leader_commit_index = 4;
//     let leader_match_index = 4;
//     let peer1_id = 2;
//     let peer1_match_index = 4;
//     let peer2_id = 3;
//     let peer2_match_index = 4;
//     state.update_commit_index(leader_commit_index);
//     state.update_match_index(leader_id, leader_match_index);
//     state.update_match_index(peer1_id, peer1_match_index);
//     state.update_next_index(peer1_id, peer1_match_index + 1);
//     state.update_match_index(peer2_id, peer2_match_index);
//     state.update_next_index(peer2_id, peer2_match_index + 1);
//     test_utils::simulate_insert_proposal(raft_log.clone(), (1..=4).collect(), 1);
//     let leader_last_index = raft_log.last_entry_id();

//     let rpc_client_mock = MockTransport::new();
//     let membership_controller_mock = MockClusterMembershipControllerApis::new();
//     let (_graceful_tx, graceful_rx) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft_log.clone(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         Arc::new(rpc_client_mock),
//         graceful_rx,
//     )
//     .await;

//     let map = append_entries_controller.retrieve_to_be_synced_logs_for_peers(
//         vec![peer1_id, peer2_id],
//         vec![],
//         leader_last_index,
//         100,
//     );
//     assert_eq!(map.len(), 0);
// }

// #[tokio::test]
// async fn test_append_entries_controller_shutdown() {
//     let c = test_utils::setup("/tmp/test_append_entries_controller_shutdown", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let state = raft.state();

//     let mut rpc_client_mock = MockTransport::new();
//     rpc_client_mock
//         .expect_send_append_requests()
//         .times(0)
//         .return_once(|_, _, _| Ok(true));

//     let (_graceful_tx, graceful_rx) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         raft.cluster_membership_controller(),
//         Arc::new(rpc_client_mock),
//         graceful_rx,
//     )
//     .await;

//     let (shutdown_signal_tx, shutdown_signal) = watch::channel(());

//     shutdown_signal_tx.send(()).expect("should succeed");
//     assert!(!append_entries_controller.is_running().await);
//     // h.await.expect("should succeed");
// }

// /// Case 1: test if engine is not turned on. Even there is message received,
// ///     but "get_followers_candidates_channel_and_role" should not be invoked
// ///
// /// Setup: make sure new entries send to new_entries_receiver
// ///
// /// Criterias:
// /// - get_followers_candidates_channel_and_role called 0 time
// ///
// #[tokio::test]
// async fn test_append_entries_controller_block_thread_until_case1() {
//     let c = test_utils::setup(
//         "/tmp/test_append_entries_controller_block_thread_until_case1",
//         None,
//     )
//     .await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let state = raft.state();

//     // prepare mocks
//     let mut membership_controller_mock = MockClusterMembershipControllerApis::new();
//     membership_controller_mock
//         .expect_get_followers_candidates_channel_and_role()
//         .times(0)
//         .returning(|| vec![]);
//     // finish preparing mocks

//     let (shutdown_signal_tx, shutdown_signal) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         raft.rpc_client(),
//         shutdown_signal,
//     )
//     .await;

//     let (append_result_signal_sender, _append_result_signal_receiver) =
//         mpsc::channel::<Result<Vec<String>>>(1);

//     append_entries_controller
//         .new_entries_sender
//         .send(ClientRequestWithSignal {
//             id: "abc123".to_string(),
//             commands: vec![],
//             append_result_signal_sender,
//         })
//         .expect("should succeed");
//     shutdown_signal_tx.send(()).expect("should succeed");
//     assert!(!append_entries_controller.is_running().await);
//     // h.await.expect("should succeed");
// }

// /// Case 2: test if engine is not turned on before the new entries sending. Even there is message received,
// ///     but the message will be received only after the controller been turned on
// ///
// /// Setup: make sure new entries send to new_entries_receiver
// ///
// /// Criterias:
// /// - get_followers_candidates_channel_and_role called 1 time
// /// - after the engine been turned on, heartbeat_signal_receiver receives messages
// ///
// #[tokio::test]
// async fn test_append_entries_controller_block_thread_until_case2() {
//     let c = test_utils::setup(
//         "/tmp/test_append_entries_controller_block_thread_until_case2",
//         None,
//     )
//     .await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let state = raft.state();

//     // prepare mocks
//     let mut membership_controller_mock = MockClusterMembershipControllerApis::new();
//     membership_controller_mock
//         .expect_get_followers_candidates_channel_and_role()
//         // .times(1)
//         .returning(|| vec![]);
//     // finish preparing mocks

//     let (heartbeat_signal_sender, mut heartbeat_signal_receiver) =
//         mpsc::unbounded_channel::<HeartbeatSignal>();

//     let (shutdown_signal_tx, shutdown_signal) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         heartbeat_signal_sender,
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         raft.rpc_client(),
//         shutdown_signal,
//     )
//     .await;

//     let (append_result_signal_sender, _append_result_signal_receiver) =
//         mpsc::channel::<Result<Vec<String>>>(1);

//     append_entries_controller
//         .new_entries_sender
//         .send(ClientRequestWithSignal {
//             id: "abc123".to_string(),
//             commands: vec![],
//             append_result_signal_sender,
//         })
//         .expect("should succeed");
//     append_entries_controller.turn_on().await;

//     if let Some(_h) = heartbeat_signal_receiver.recv().await {
//         assert!(true);
//     } else {
//         assert!(false);
//     }
//     shutdown_signal_tx.send(()).expect("should succeed");
//     sleep(Duration::from_millis(100)).await;
//     assert!(!append_entries_controller.is_running().await);
// }

// #[tokio::test]
// async fn test_append_entries_controller_start() {
//     let c = test_utils::setup("/tmp/test_append_entries_controller_start", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let state = raft.state();

//     let (heartbeat_signal_sender, mut heartbeat_signal_receiver) =
//         mpsc::unbounded_channel::<HeartbeatSignal>();
//     let (shutdown_signal_tx, shutdown_signal) = watch::channel(());
//     let mut engine = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         heartbeat_signal_sender,
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         raft.cluster_membership_controller(),
//         raft.rpc_client(),
//         shutdown_signal,
//     )
//     .await;
//     engine.turn_on().await;

//     let h1 = tokio::spawn(async move {
//         if let Some(_signal) = heartbeat_signal_receiver.recv().await {
//             println!("heartbeat received!");
//             assert!(true);
//         } else {
//             assert!(false);
//         }
//     });
//     tokio::time::sleep(Duration::from_millis(100)).await;
//     shutdown_signal_tx.send(()).expect("should succeed");
//     let _ = tokio::join!(h1,);
// }

// /// Case 1: leader commit index > majority matchied index
// ///
// /// Criterias:
// /// - returned false. with leader commit_index
// #[tokio::test]
// async fn test_if_update_commit_index_as_follower_case1() {
//     let c = test_utils::setup("/tmp/test_update_commit_index_case1", None).await;
//     let raft = c.node.raft.clone();
//     let state = raft.state();
//     let raft_log = raft.raft_log();
//     //follower commit index : 2
//     state.update_commit_index(2);
//     let leader_commit_index = 1;
//     // log length is 3
//     test_utils::simulate_insert_proposal(raft_log.clone(), vec![1, 2, 3], state.current_term());
//     assert_eq!(
//         None,
//         AppendEntriesController::if_update_commit_index_as_follower(
//             state.commit_index(),
//             raft_log.last_entry_id(),
//             state,
//             leader_commit_index
//         )
//     );
// }

// /// Case 2: leader commit index > majority matchied index
// /// rules: if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
// ///
// #[tokio::test]
// async fn test_if_update_commit_index_as_follower_case2() {
//     let c = test_utils::setup("/tmp/test_update_commit_index_case2", None).await;
//     let raft = c.node.raft.clone();
//     let state = raft.state();
//     let raft_log = raft.raft_log();
//     //follower commit index : 1
//     state.update_commit_index(1);
//     let leader_commit_index = 2;
//     // log length is 3
//     test_utils::simulate_insert_proposal(raft_log.clone(), vec![1, 2, 3], state.current_term());
//     assert_eq!(
//         Some(2),
//         AppendEntriesController::if_update_commit_index_as_follower(
//             state.commit_index(),
//             raft_log.last_entry_id(),
//             state,
//             leader_commit_index
//         )
//     );
// }
// /// rules: if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
// /// case 3:
// #[tokio::test]
// async fn test_if_update_commit_index_as_follower_case3() {
//     let c = test_utils::setup("/tmp/test_update_commit_index_case3", None).await;
//     let raft = c.node.raft.clone();
//     let state = raft.state();
//     let raft_log = raft.raft_log();
//     //follower commit index : 1
//     state.update_commit_index(1);
//     let leader_commit_index = 4;
//     test_utils::simulate_insert_proposal(raft_log.clone(), vec![1, 2, 3], state.current_term());
//     assert_eq!(
//         Some(3),
//         AppendEntriesController::if_update_commit_index_as_follower(
//             state.commit_index(),
//             raft_log.last_entry_id(),
//             state,
//             leader_commit_index
//         )
//     );
// }
// /// Case 1: (Figure 7 in raft paper, github#112)
// ///     Leader send to all peers:
// ///         - prev_log_index: 10
// ///         - prev_log_term: 6
// ///         - entries: [log11]
// ///
// /// ## Setup:
// /// 1.
// ///     leader:     log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6)
// ///     follower_a: log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6),
// ///     follower_b: log1(1), log2(1), log3(1), log4(4),
// ///     follower_c: log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6)
// ///     follower_d: log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6), log11(7), log12(7)
// ///     follower_e: log1(1), log2(1), log3(1), log4(4), log5(4), log6(4), log7(4)
// ///     follower_f: log1(1), log2(1), log3(1), log4(2), log5(2), log6(2), log7(3), log8(3), log9(3), log10(3), log11(3)
// ///
// /// ## Criterias:
// /// 1. follower's response:
// /// Follower	    success	    term    matchIndex
// /// follower_a	    true	    6	    11
// /// follower_b	    false		4       4
// /// follower_c	    true	    6	    11
// /// follower_d	    true	    6	    11
// /// follower_e	    false	  	4       7
// /// follower_f	    false  	    3	    9
// ///
// #[tokio::test]
// async fn test_upon_receive_append_request_case1() {
//     let c = test_utils::setup("/tmp/test_upon_receive_append_request_case1", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();

//     let leader_id = 1;
//     let leader_commit_index = 3;

//     let rpc_client_mock = Arc::new(MockTransport::new());
//     let membership_controller_mock = Arc::new(MockClusterMembershipControllerApis::new());

//     let prev_log_index = 10;
//     let prev_log_term = 6;
//     let leader_append_req = AppendEntriesRequest {
//         term: leader_commit_index,
//         leader_id,
//         prev_log_index,
//         prev_log_term,
//         entries: Vec::new(),
//         leader_commit_index,
//     };

//     let followers = vec![
//         // (id, term, last_applied, last_log_entry_id, prev_log_ok, expected_result)
//         (1, 6, 0, 11, true, (true, 6, 11)), // follower_a
//         (2, 4, 0, 4, false, (false, 4, 4)), // follower_b
//         (3, 6, 0, 11, true, (true, 6, 11)), // follower_c
//         (4, 6, 0, 11, true, (true, 6, 11)), // follower_d
//         (5, 4, 0, 7, false, (false, 4, 7)), // follower_e
//         (6, 3, 0, 9, false, (false, 3, 9)), // follower_f
//     ];

//     for (id, term, last_applied, last_log_entry_id, prev_log_ok, expected) in followers {
//         let mut follower_state = MockStateApis::new();

//         // Mock common methods
//         follower_state
//             .expect_current_term()
//             .times(1)
//             .returning(move || term);
//         follower_state
//             .expect_update_commit_index()
//             .returning(|_| {});
//         follower_state.expect_catch_up_state().returning(|_, _| {});
//         follower_state
//             .expect_commit_index()
//             .returning(move || last_log_entry_id);
//         follower_state
//             .expect_last_applied()
//             .times(1)
//             .returning(move || last_applied);

//         let mut follower_raft_log = MockRaftLog::new();
//         follower_raft_log
//             .expect_last_entry_id()
//             .returning(move || last_log_entry_id);
//         follower_raft_log
//             .expect_prev_log_ok()
//             .with(eq(prev_log_index), eq(prev_log_term), eq(last_applied))
//             .returning(move |_, _, _| prev_log_ok);

//         // Initialize the AppendEntriesController
//         let (_shutdown_signal_tx, shutdown_signal) = watch::channel(());
//         let append_entries_controller = AppendEntriesController::new(
//             id,
//             Arc::new(follower_state),
//             Arc::new(follower_raft_log),
//             raft.settings.clone(),
//             raft.raft_event_sender(),
//             raft.heartbeat_signal_sender(),
//             raft.commit_success_sender(),
//             #[cfg(feature = "flow_control")]
//             raft.append_request_flow_controller(),
//             membership_controller_mock.clone(),
//             rpc_client_mock.clone(),
//             shutdown_signal,
//         )
//         .await;

//         // Test append request
//         let (success, current_term, last_matched_id) =
//             append_entries_controller.upon_receive_append_request(&leader_append_req);

//         // Assert results
//         assert_eq!(success, expected.0);
//         assert_eq!(current_term, expected.1);
//         assert_eq!(last_matched_id, expected.2);
//     }
// }

// /// Case 2: (Figure 7 in raft paper, github#112)
// ///   l -> f_b
// ///     - prev_log_index: 3
// ///     - prev_log_term: 1
// ///     - entries: [log4,...log10]
// ///
// ///   l -> f_e
// ///     - prev_log_index: 6
// ///     - prev_log_term: 5
// ///     - entries: [log7,...log10]
// ///
// ///   l -> f_f
// ///     - prev_log_index: 8
// ///     - prev_log_term: 6
// ///     - entries: [log9,...log10]
// ///
// /// ## Setup:
// /// 1.
// ///     leader:     log1(1), log2(1), log3(1), log4(4), log5(4), log6(5), log7(5), log8(6), log9(6), log10(6)
// ///     follower_b: log1(1), log2(1), log3(1), log4(4),
// ///     follower_e: log1(1), log2(1), log3(1), log4(4), log5(4), log6(4), log7(4)
// ///     follower_f: log1(1), log2(1), log3(1), log4(2), log5(2), log6(2), log7(3), log8(3), log9(3), log10(3), log11(3)
// ///
// /// ## Criterias:
// /// Follower	    success	    matchIndex
// /// follower_b	    true		    4
// /// follower_e	    false	  	    5
// /// follower_f	    false  	    	7
// ///
// #[tokio::test]
// async fn test_upon_receive_append_request_case2() {
//     let c = test_utils::setup("/tmp/test_upon_receive_append_request_case2", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();

//     let leader_id = 1;
//     let leader_commit_index = 3;

//     let rpc_client_mock = Arc::new(MockTransport::new());
//     let membership_controller_mock = Arc::new(MockClusterMembershipControllerApis::new());

//     let followers = vec![
//         // (id, term, last_applied, last_log_entry_id, prev_log_index, prev_log_term, prev_log_ok, expected_result(success, term, match_index))
//         (2, 4, 0, 4, 3, 1, true, (true, 4, 4)),   // follower_b
//         (5, 4, 0, 7, 6, 5, false, (false, 4, 5)), // follower_e
//         (6, 3, 0, 9, 8, 6, false, (false, 3, 7)), // follower_f
//     ];

//     for (
//         id,
//         term,
//         last_applied,
//         last_log_entry_id,
//         prev_log_index,
//         prev_log_term,
//         prev_log_ok,
//         expected,
//     ) in followers
//     {
//         let leader_append_req = AppendEntriesRequest {
//             term: leader_commit_index,
//             leader_id,
//             prev_log_index,
//             prev_log_term,
//             entries: Vec::new(),
//             leader_commit_index,
//         };

//         let mut follower_state = MockStateApis::new();

//         // Mock common methods
//         follower_state
//             .expect_current_term()
//             .times(1)
//             .returning(move || term);
//         follower_state
//             .expect_update_commit_index()
//             .returning(|_| {});
//         follower_state.expect_catch_up_state().returning(|_, _| {});
//         follower_state
//             .expect_commit_index()
//             .returning(move || last_log_entry_id);
//         follower_state
//             .expect_last_applied()
//             .times(1)
//             .returning(move || last_applied);
//         let mut follower_log = MockRaftLog::new();
//         follower_log
//             .expect_last_entry_id()
//             .returning(move || last_log_entry_id);
//         follower_log
//             .expect_prev_log_ok()
//             .with(eq(prev_log_index), eq(prev_log_term), eq(last_applied))
//             .returning(move |_, _, _| prev_log_ok);

//         // Initialize the AppendEntriesController
//         let (_shutdown_signal_tx, shutdown_signal) = watch::channel(());
//         let append_entries_controller = AppendEntriesController::new(
//             id,
//             Arc::new(follower_state),
//             Arc::new(follower_log),
//             raft.settings.clone(),
//             raft.raft_event_sender(),
//             raft.heartbeat_signal_sender(),
//             raft.commit_success_sender(),
//             #[cfg(feature = "flow_control")]
//             raft.append_request_flow_controller(),
//             membership_controller_mock.clone(),
//             rpc_client_mock.clone(),
//             shutdown_signal,
//         )
//         .await;

//         // Test append request
//         let (success, current_term, last_matched_id) =
//             append_entries_controller.upon_receive_append_request(&leader_append_req);

//         // Assert results
//         assert_eq!(success, expected.0);
//         assert_eq!(current_term, expected.1);
//         assert_eq!(last_matched_id, expected.2);
//     }
// }

// /// # Case 1: test if send_write_request could work as normal
// #[tokio::test]
// async fn test_send_write_request_case1() {
//     let c = test_utils::setup("/tmp/test_send_write_request_case1", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let raft_log = raft.raft_log();
//     let state = raft.state();

//     // - simulate peer response -
//     let port = MOCK_APPEND_ENTRIES_CONTROLLER_PORT_BASE + 10;
//     let response = AppendEntriesResponse {
//         id: 2,
//         term: 1,
//         success: true,
//         match_index: 1,
//     };
//     let (_tx, rx) = oneshot::channel::<()>();
//     let address = test_utils::MockNode::simulate_mock_service_with_append_reps(port, response, rx)
//         .await
//         .expect("should succeed");

//     let mut rpc_client_mock = MockTransport::new();
//     rpc_client_mock
//         .expect_send_append_requests()
//         .times(1)
//         .return_once(|_, _, _| Ok(true));

//     let mut membership_controller_mock = MockClusterMembershipControllerApis::new();
//     membership_controller_mock
//         .expect_get_followers_candidates_channel_and_role()
//         .times(1)
//         .returning(move || {
//             vec![ChannelWithAddressAndRole {
//                 id: 2,
//                 channel_with_address: address.clone(),
//                 role: RaftRole::Follower,
//             }]
//         });
//     // - finish simulating peer response -

//     // - turn on controller  -
//     let (shutdown_signal_tx, shutdown_signal) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         Arc::new(membership_controller_mock),
//         Arc::new(rpc_client_mock),
//         shutdown_signal,
//     )
//     .await;
//     append_entries_controller.turn_on().await;

//     let req = ClientProposeRequest {
//         client_id: INTERNAL_CLIENT_ID,
//         commands: vec![ClientCommand::no_op()],
//     };

//     if let Ok(_) = append_entries_controller.send_write_request(req).await {
//         assert!(true);
//     } else {
//         assert!(false);
//     }
//     shutdown_signal_tx.send(()).expect("should succeed");
//     assert_eq!(raft_log.last_entry_id(), 1);
//     // h.await.expect("should succeed");
// }

// /// # Case 2: if no majority confirmation receives should return
// ///        Err(Error::AppendEntriesCommitNotConfirmed)
// ///
// /// ## Setup
// /// 1. peer not started yet
// ///
// /// ## Criteria
// /// 1. compare error with error.into(), should equal to "ClientRequestError::CommitNotConfirmed"
// ///
// ///
// #[tokio::test]
// async fn test_send_write_request_case2() {
//     let c = test_utils::setup("/tmp/test_send_write_request_case2", None).await;
//     let node = c.node.clone();
//     let raft = node.raft.clone();
//     let state = raft.state();

//     let (shutdown_signal_tx, shutdown_signal) = watch::channel(());
//     let append_entries_controller = AppendEntriesController::new(
//         raft.id(),
//         state.clone(),
//         raft.raft_log(),
//         raft.settings.clone(),
//         raft.raft_event_sender(),
//         raft.heartbeat_signal_sender(),
//         raft.commit_success_sender(),
//         #[cfg(feature = "flow_control")]
//         raft.append_request_flow_controller(),
//         raft.cluster_membership_controller(),
//         raft.rpc_client(),
//         shutdown_signal,
//     )
//     .await;
//     append_entries_controller.turn_on().await;

//     let req = ClientProposeRequest {
//         client_id: INTERNAL_CLIENT_ID,
//         commands: vec![ClientCommand::no_op()],
//     };

//     if let Err(e) = append_entries_controller.send_write_request(req).await {
//         eprintln!("{:?}", e);
//         assert!(matches!(e, Error::AppendEntriesCommitNotConfirmed));
//     } else {
//         assert!(false);
//     }
//     shutdown_signal_tx.send(()).expect("should succeed");
//     sleep(Duration::from_millis(100)).await;
//     assert!(!append_entries_controller.is_running().await);
// }
