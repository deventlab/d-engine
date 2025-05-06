use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio::sync::watch;

use super::ReplicationCore;
use super::ReplicationData;
use super::ReplicationHandler;
use crate::convert::safe_kv;
use crate::proto::append_entries_response;
use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientCommand;
use crate::proto::ConflictResult;
use crate::proto::Entry;
use crate::proto::LogId;
use crate::proto::SuccessResult;
use crate::test_utils::mock_peer_channels;
use crate::test_utils::mock_raft_context;
use crate::test_utils::setup_raft_components;
use crate::test_utils::simulate_insert_proposal;
use crate::test_utils::MockNode;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::MOCK_REPLICATION_HANDLER_PORT_BASE;
use crate::AppendResult;
use crate::ChannelWithAddressAndRole;
use crate::ConsensusError;
use crate::Error;
use crate::LeaderStateSnapshot;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockTransport;
use crate::NetworkError;
use crate::PeerUpdate;
use crate::RaftLog;
use crate::RaftTypeConfig;
use crate::ReplicationError;
use crate::StateSnapshot;
use crate::SystemError;
use crate::FOLLOWER;

/// # Case 1: The peer3's next_index is equal to
///     the end of the leader's old log,
///     and only the new log is sent
///
/// ## Validate criterias
/// 1. only new_entries returned
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case1() {
    let context = setup_raft_components("/tmp/test_retrieve_to_be_synced_logs_for_peers_case1", None, false);
    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![Entry {
        index: 1,
        term: 1,
        command: vec![1; 8],
    }];
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;
    let peer_next_indices = HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == new_entries));
}

/// # Case 2: Peer3 needs old log + new log
///     (not exceeding the max limit)
///     and returning latencies and the new log
///
/// ## Prepration setup
/// 1. Simulate one entry in local raft log(log-1)
/// 2. Peer 3 next_index is 1
///
/// ## Validate criterias
/// 1. both log-1 and new_entries are returned
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case2() {
    let context = setup_raft_components("/tmp/test_retrieve_to_be_synced_logs_for_peers_case2", None, false);

    // Simulate one entry in local raft log
    let raft_log = context.raft_log;
    simulate_insert_proposal(&raft_log, vec![1], 1);

    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![Entry {
        index: 2,
        term: 1,
        command: vec![1; 8],
    }];
    let leader_last_index_before_inserting_new_entries = 1;
    let max_entries = 100;
    let peer_next_indices = HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &raft_log,
    );
    let last_log_entry = raft_log.last().unwrap();
    let mut merged_entries = vec![last_log_entry];
    merged_entries.extend(new_entries);
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == merged_entries));
}

/// # Case 3: new_entries is empty and Peer3 has latencies
///     (not exceeding the max limit)
///
/// ## Prepration setup
/// 1. Simulate one entry in local raft log(log-1)
/// 2. Peer 3 next_index is 1
///
/// ## Validate criterias
/// 1. only log-1 is returned
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case3() {
    let context = setup_raft_components("/tmp/test_retrieve_to_be_synced_logs_for_peers_case3", None, false);

    // Simulate one entry in local raft log
    let raft_log = context.raft_log;
    simulate_insert_proposal(&raft_log, vec![1], 1);

    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![];
    let leader_last_index_before_inserting_new_entries = 1;
    let max_entries = 100;
    let peer_next_indices = HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &raft_log,
    );
    let last_log_entry = raft_log.last().unwrap();
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == vec![last_log_entry]));
}

/// # Case 4.1: Peer3 needs old log + new log - max_legacy_entries_per_peer = 2
///     (while exceeding the max limit)
///     and returning latencies and the new log
///
/// ## Prepration setup
/// 1. Simulate entries in local raft log(log-1, log-2, log-3)
/// 2. Peer 3 next_index is 1
/// 3. max_legacy_entries_per_peer = 2
///
/// ## Validate criterias
/// 1. both log-1,log-2 and new_entries are returned
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case4_1() {
    let context = setup_raft_components("/tmp/test_retrieve_to_be_synced_logs_for_peers_case4_1", None, false);

    let my_id = 1;
    let peer3_id = 3;
    let peer3_next_id = 1;
    // Simulate one entry in local raft log
    let max_legacy_entries_per_peer = 2;
    let leader_last_index_before_inserting_new_entries = 3;
    let raft_log = context.raft_log;
    simulate_insert_proposal(
        &raft_log,
        (1..=leader_last_index_before_inserting_new_entries).collect(),
        1,
    );

    let new_entries = vec![Entry {
        index: 3,
        term: 1,
        command: vec![1; 8],
    }];
    let peer_next_indices = HashMap::from([(peer3_id, peer3_next_id)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &raft_log,
    );
    let mut lagency_entries =
        raft_log.get_entries_between(peer3_next_id..=(peer3_next_id + max_legacy_entries_per_peer - 1));
    lagency_entries.extend(new_entries);
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == lagency_entries));
}

/// # Case 4.2: Peer3 needs old log + new log - max_legacy_entries_per_peer = 0
///     (while exceeding the max limit)
///     and returning latencies and the new log
///
/// ## Prepration setup
/// 1. Simulate entriesin local raft log(log-1, log-2, log-3)
/// 2. Peer 3 next_index is 1
/// 3. max_legacy_entries_per_peer = 0
///
/// ## Validate criterias
/// 1. only new_entries are returned
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case4_2() {
    let context = setup_raft_components("/tmp/test_retrieve_to_be_synced_logs_for_peers_case4_2", None, false);

    let my_id = 1;
    let peer3_id = 3;
    let peer3_next_id = 1;
    // Simulate one entry in local raft log
    let max_legacy_entries_per_peer = 0;
    let leader_last_index_before_inserting_new_entries = 3;
    let raft_log = context.raft_log;
    simulate_insert_proposal(
        &raft_log,
        (1..=leader_last_index_before_inserting_new_entries).collect(),
        1,
    );

    let new_entries = vec![Entry {
        index: 3,
        term: 1,
        command: vec![1; 8],
    }];
    let peer_next_indices = HashMap::from([(peer3_id, peer3_next_id)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &raft_log,
    );

    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == new_entries));
}

/// # Case 5: returned entries should not has leader ones
///
/// ## Validate criterias
/// 1. No leader ones should be retruned
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case5() {
    let context = setup_raft_components("/tmp/test_retrieve_to_be_synced_logs_for_peers_case5", None, false);
    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![Entry {
        index: 1,
        term: 1,
        command: vec![1; 8],
    }];
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;
    let peer_next_indices = HashMap::from([(my_id, 1), (peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );

    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == new_entries));

    assert!(r.get(&my_id).is_none());
}

/// # Case 1: Test with empty commands
///
/// ## Validation criterias:
/// 1. fun returns Ok(vec![])
/// 2. no update on local raft log
#[test]
fn test_generate_new_entries_case1() {
    let context = setup_raft_components("/tmp/test_generate_new_entries_case1", None, false);
    let my_id = 1;
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);
    let last_id = context.raft_log.last_entry_id();
    let commands = vec![];
    let current_term = 1;
    let r = handler.generate_new_entries(commands, current_term, &context.raft_log);
    assert_eq!(r.unwrap(), vec![]);
    assert_eq!(context.raft_log.last_entry_id(), last_id);
}

/// # Case 2: Test with one command
///
/// ## Validation criterias:
/// 1. fun returns Ok(vec![log-1])
/// 2. update on local raft log with one extra entry
#[test]
fn test_generate_new_entries_case2() {
    let context = setup_raft_components("/tmp/test_generate_new_entries_case2", None, false);
    let my_id = 1;
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);
    let last_id = context.raft_log.last_entry_id();
    let commands = vec![ClientCommand::get(safe_kv(1))];
    let current_term = 1;
    assert_eq!(
        handler
            .generate_new_entries(commands, current_term, &context.raft_log)
            .unwrap()
            .len(),
        1
    );

    assert_eq!(context.raft_log.last_entry_id(), last_id + 1);
}

/// # Case: Test retrieve expected items for peer 2
///
/// ## Scenario Setup
/// 1. entries_per_peer: peer-2: log-3 peer-3: log-1, log-2, log-3
///
/// ## Validation criterias:
/// 1. retrieved entries' length is 2
#[tokio::test]
async fn test_build_append_request_case() {
    let context = setup_raft_components("/tmp/test_build_append_request_case", None, false);
    let my_id = 1;
    let peer2_id = 2;
    let peer2_next_index = 3;
    let peer3_id = 3;
    let peer3_next_index = 1;
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    // Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 1, rx1, true)
        .await
        .expect("should succeed");
    let peer: ChannelWithAddressAndRole = ChannelWithAddressAndRole {
        id: peer2_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    };

    // Prepare entries to be replicated for each peer
    let entries_per_peer: DashMap<u32, Vec<Entry>> = DashMap::new();
    entries_per_peer.insert(peer2_id, vec![Entry {
        index: 3,
        term: 1,
        command: vec![1; 8],
    }]);
    entries_per_peer.insert(peer3_id, vec![
        Entry {
            index: 1,
            term: 1,
            command: vec![1; 8],
        },
        Entry {
            index: 2,
            term: 1,
            command: vec![1; 8],
        },
        Entry {
            index: 3,
            term: 1,
            command: vec![1; 8],
        },
    ]);

    let data = ReplicationData {
        leader_last_index_before: 3,
        current_term: 1,
        commit_index: 1,
        peer_next_indices: HashMap::from([(peer2_id, peer2_next_index), (peer3_id, peer3_next_index)]),
    };

    let (_id, _address, to_be_replicated_request) =
        handler.build_append_request(&context.raft_log, &peer, &entries_per_peer, &data);
    assert_eq!(to_be_replicated_request.entries.len(), 1);
}

/// # Case 1: No peers found
/// ## Validation Criteria
/// 1. Return Error::AppendEntriesNoPeerFound
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case1", graceful_rx, None);
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

    let peer_channels = Arc::new(mock_peer_channels());
    let e = handler
        .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
        .await
        .unwrap_err();
    assert!(matches!(
        e,
        Error::Consensus(ConsensusError::Replication(ReplicationError::NoPeerFound {
            leader_id: _
        }))
    ));
}

/// # Case 2.1: Successful Client Proposal Replication
/// Validates leader behavior when sending heartbeat(empty commands)
///     (not exceeding the max_legacy_entries_per_peer)
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
/// - Transport returns Ok
///
/// ## Validation Criteria
/// - function returns Ok
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case2_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case2_1", graceful_rx, None);

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
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 10, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        }]
    });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_pre_allocate_raft_logs_next_index().returning(|| 1);
    raft_log.expect_get_entries_between().returning(|_| vec![]);
    raft_log.expect_prev_log_term().returning(|_, _| 0);

    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |_, _| {
        Ok(AppendResult {
            peer_ids: vec![peer2_id].into_iter().collect(),
            responses: vec![Ok(AppendEntriesResponse::success(
                peer2_id,
                1,
                Some(LogId { term: 1, index: 3 }),
            ))],
        })
    });

    context.membership = Arc::new(membership);
    context.storage.raft_log = Arc::new(raft_log);
    context.transport = Arc::new(transport);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(
        handler
            .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
            .await
            .unwrap()
            .commit_quorum_achieved
    );
}

/// # Case 2.2: Successful Client Proposal Replication
/// Validates leader behavior when sending heartbeat(empty commands)
///     (not exceeding the max_legacy_entries_per_peer)
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
/// - Transport returns Error
///
/// ## Validation Criteria
/// - function returns Error
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case2_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case2_2", graceful_rx, None);
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
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 11, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        }]
    });
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_pre_allocate_raft_logs_next_index().returning(|| 1);
    raft_log.expect_get_entries_between().returning(|_| vec![]);
    raft_log.expect_prev_log_term().returning(|_, _| 0);

    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().returning(move |_, _| {
        Err(NetworkError::EmptyPeerList {
            request_type: "send_vote_requests",
        }
        .into())
    });
    context.membership = Arc::new(membership);
    context.storage.raft_log = Arc::new(raft_log);
    context.transport = Arc::new(transport);
    let peer_channels = Arc::new(mock_peer_channels());

    let e = handler
        .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
        .await
        .unwrap_err();

    assert!(matches!(
        e,
        Error::System(SystemError::Network(NetworkError::EmptyPeerList { request_type: _ }))
    ));
}

/// # Case3: Ignore success responses from stale terms
/// ## Validation Criteria
/// - Responses with term < leader's current term are ignored
/// - Success counter remains unchanged, no peer updates
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case3", graceful_rx, None);

    let my_id = 1;
    let peer2_id = 2;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let commands = vec![];
    let state_snapshot = StateSnapshot {
        current_term: 2, // Leader's term is 2
        voted_for: None,
        commit_index: 1,
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::from_iter(vec![(peer2_id, 3)]),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    let (_tx, rx) = oneshot::channel();
    let addr = MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 12, rx, true)
        .await
        .unwrap();

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr.clone(),
            role: FOLLOWER,
        }]
    });

    // Response with term=1 (stale)
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |_, _| {
        Ok(AppendResult {
            peer_ids: vec![peer2_id].into_iter().collect(),
            responses: vec![Ok(AppendEntriesResponse::success(
                peer2_id,
                1,
                Some(LogId { term: 1, index: 3 }),
            ))],
        })
    });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(3_u64);
    raft_log.expect_prev_log_term().return_const(1_u64);
    raft_log.expect_get_entries_between().returning(|_| vec![]);

    context.membership = Arc::new(membership);
    context.storage.raft_log = Arc::new(raft_log);
    context.transport = Arc::new(transport);
    let peer_channels = Arc::new(mock_peer_channels());

    let result = handler
        .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
        .await;

    assert!(result.is_ok());
    let append_result = result.unwrap();
    assert!(!append_result.commit_quorum_achieved); // successes=1 (leader only)
    assert!(append_result.peer_updates.is_empty()); // No updates due to stale term
}

/// # Case4: Higher term response triggers leader step down
/// ## Validation Criteria
/// - HigherTerm response with term > leader's term returns error
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case4", graceful_rx, None);
    let my_id = 1;
    let peer2_id = 2;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let commands = vec![];
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

    let (_tx, rx) = oneshot::channel();
    let addr = MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 13, rx, true)
        .await
        .unwrap();

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr.clone(),
            role: FOLLOWER,
        }]
    });

    // HigherTerm response with term=2
    let higher_term = 2;
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |_, _| {
        Ok(AppendResult {
            peer_ids: vec![peer2_id].into_iter().collect(),
            responses: vec![Ok(AppendEntriesResponse::higher_term(peer2_id, higher_term))],
        })
    });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(1_u64);

    context.membership = Arc::new(membership);
    context.storage.raft_log = Arc::new(raft_log);
    context.transport = Arc::new(transport);
    let peer_channels = Arc::new(mock_peer_channels());

    let result = handler
        .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
        .await;

    assert!(matches!(
        result,
        Err(Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(term)))) if term == higher_term
    ));
}

/// # Case 5: Test prepare_peer_entries ensures new commands appear only once in AppendEntries.
///
/// ## Validation Criteria
/// - For each peer, entries in AppendEntries start exactly at their `next_index`.
/// - New commands are only included in the first replication attempt.
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case5() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case5", graceful_rx, None);
    let my_id = 1;
    let peer2_id = 2;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // ----------------------
    // Initialization state
    // ----------------------
    //Leader's current term and initial log
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(5_u64);
    raft_log.expect_pre_allocate_raft_logs_next_index().returning(|| 1);
    raft_log.expect_get_entries_between().returning(|_| vec![]);
    raft_log.expect_prev_log_term().returning(|_, _| 0);
    raft_log.expect_insert_batch().returning(|_| Ok(()));

    // New commands submitted by the client generate logs with index=6~7
    let commands = vec![
        ClientCommand::insert(safe_kv(100), safe_kv(100)),
        ClientCommand::insert(safe_kv(200), safe_kv(200)),
    ];

    // Leader status snapshot
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 5,
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::from([(peer2_id, 6)]),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // ----------------------
    // Configure MockTransport to capture requests and verify parameters
    // ----------------------
    let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
    let mut transport = MockTransport::new();

    // Use `with` to capture request parameters
    transport
        .expect_send_append_requests()
        .withf(move |requests, _| {
            // Send the request to the channel for subsequent assertions
            let _ = tx.send(requests.clone());
            true // Return true to indicate that the parameters match successfully
        })
        .return_once(|_, _| {
            Ok(AppendResult {
                peer_ids: vec![].into_iter().collect(),
                responses: vec![],
            })
        }); // Returns an empty response without affecting the test logic

    // ----------------------
    //Call the function to be tested
    // ----------------------
    let (_stx, srx) = oneshot::channel();
    let addr = MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 14, srx, true)
        .await
        .unwrap();

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr.clone(),
            role: FOLLOWER,
        }]
    });

    context.membership = Arc::new(membership);
    context.storage.raft_log = Arc::new(raft_log);
    context.transport = Arc::new(transport);
    let peer_channels = Arc::new(mock_peer_channels());

    let _ = handler
        .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
        .await;

    // ----------------------
    // Get the captured request from the channel and verify
    // ----------------------
    let captured_requests = rx.recv().unwrap();

    // Verify Peer2's request
    let peer2_request = captured_requests
        .iter()
        .find(|(peer_id, _, _)| *peer_id == peer2_id)
        .unwrap();
    let peer2_entries = &peer2_request.2.entries;
    assert_eq!(peer2_entries.len(), 2);
}

/// # Case7: Leader resolves log conflicts across divergent followers
/// Validates next_index updates per Raft's conflict resolution rules (§5.3)
///
/// ## Scenario Setup
/// Leader log: [1(1), 2(1), 3(1), 4(4), 5(4), 6(5), 7(5), 8(6), 9(6), 10(6)]
/// Followers with varying log states:
/// - follower_a: log1-9 (match index 9)
/// - follower_b: log1-4 (match index 4)
/// - follower_c: log1-10 (match index 10)
/// - follower_d: log1-12 (higher term)
/// - follower_e: log1-7 (term 4)
/// - follower_f: log1-11 (term 3)
///
/// ## Validation Criteria
/// Verify next_index updates match Raft's conflict resolution rules:
/// - follower_a: next_index=10 (no conflict)
/// - follower_b: next_index=5 (missing entries)
/// - follower_c: next_index=11 (caught up)
/// - follower_d: next_index=11 (term mismatch)
/// - follower_e: next_index=6 (term regression)
/// - follower_f: next_index=4 (deep conflict)
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case6() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_client_proposal_in_batch_case6", graceful_rx, None);
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Prepare client commands (new entries to replicate)
    let commands = vec![
        ClientCommand::insert(safe_kv(300), safe_kv(300)), // Will create log index 11
    ];

    // Initialize leader state
    let state_snapshot = StateSnapshot {
        current_term: 6,
        voted_for: None,
        commit_index: 10,
    };

    // Initial next_index values from test case description
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::from_iter(vec![
            (2, 10), // follower_a
            (3, 5),  // follower_b
            (4, 11), // follower_c
            (5, 11), // follower_d
            (6, 6),  // follower_e
            (7, 4),  // follower_f
        ]),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // Configure mock Raft log - leader has logs 1-10
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(10_u64);
    raft_log.expect_pre_allocate_raft_logs_next_index().returning(|| 11);
    raft_log.expect_get_entries_between().returning(|range| {
        // Simulate log entries for conflict resolution
        match range.start() {
            5..=10 => vec![
                mk_log(5, 5),
                mk_log(5, 6),
                mk_log(6, 7),
                mk_log(6, 8),
                mk_log(6, 9),
                mk_log(6, 10),
            ],
            4 => vec![mk_log(4, 4), mk_log(4, 5)],
            _ => vec![],
        }
    });
    raft_log.expect_insert_batch().returning(|_| Ok(()));
    raft_log.expect_prev_log_term().returning(|prev_index, _| {
        match prev_index {
            10 => 6, // log 10 term
            9 => 6,
            8 => 6,
            7 => 5,
            6 => 5,
            5 => 4,
            4 => 4,
            _ => 0,
        }
    });

    // Configure mock transport responses
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |requests, _| {
        Ok(AppendResult {
            peer_ids: requests.iter().map(|(id, _, _)| *id).collect(),
            responses: vec![
                // follower_a (id=2) - success
                Ok(AppendEntriesResponse::success(2, 6, Some(LogId { term: 6, index: 10 }))),
                // follower_b (id=3) - conflict at index 5 (term 4)
                Ok(AppendEntriesResponse::conflict(3, 6, Some(4), Some(5))),
                // follower_c (id=4) - success (already up-to-date)
                Ok(AppendEntriesResponse::success(4, 6, Some(LogId { term: 6, index: 10 }))),
                // follower_d (id=5) - higher term (7)
                Ok(AppendEntriesResponse::higher_term(5, 7)),
                // follower_e (id=6) - conflict at index 6 (term 4)
                Ok(AppendEntriesResponse::conflict(6, 6, Some(4), Some(6))),
                // follower_f (id=7) - conflict at index 4 (term 2)
                Ok(AppendEntriesResponse::conflict(7, 6, Some(2), Some(4))),
            ],
        })
    });

    // Setup replication members
    let futures: Vec<_> = (2..=7)
        .map(|id| async move {
            let (_tx, rx) = oneshot::channel();
            let addr =
                MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 20 + id, rx, true)
                    .await
                    .unwrap();

            ChannelWithAddressAndRole {
                id: id as u32,
                channel_with_address: addr,
                role: FOLLOWER,
            }
        })
        .collect();

    let replication_members = futures::future::join_all(futures).await;
    let mut membership = MockMembership::new();
    membership
        .expect_voting_members()
        .returning(move |_| replication_members.clone());

    context.membership = Arc::new(membership);
    context.storage.raft_log = Arc::new(raft_log);
    context.transport = Arc::new(transport);
    let peer_channels = Arc::new(mock_peer_channels());

    // Execute test
    let result = handler
        .handle_client_proposal_in_batch(commands, state_snapshot, leader_state_snapshot, &context, peer_channels)
        .await;

    // Verify results
    match result {
        Ok(append_result) => {
            // Check quorum (should fail due to follower_d's higher term)
            assert!(!append_result.commit_quorum_achieved);

            // Verify peer updates
            let updates = &append_result.peer_updates;

            // follower_a (success)
            assert_eq!(updates[&2], PeerUpdate {
                match_index: 10,
                next_index: 11,
                success: true
            });

            // follower_b (conflict at term 4 index 5)
            assert_eq!(updates[&3], PeerUpdate {
                match_index: 4, // 5-1
                next_index: 5,
                success: false
            });

            // follower_c (success)
            assert_eq!(updates[&4], PeerUpdate {
                match_index: 10,
                next_index: 11,
                success: true
            });

            // follower_d (higher term) - no update (error handled)
            assert!(!updates.contains_key(&5));

            // follower_e (conflict at term 4 index 6)
            assert_eq!(updates[&6], PeerUpdate {
                match_index: 5, // 6-1
                next_index: 6,
                success: false
            });

            // follower_f (conflict at term 2 index 4)
            assert_eq!(updates[&7], PeerUpdate {
                match_index: 3, // 4-1
                next_index: 4,
                success: false
            });
        }
        Err(e) => {
            // Verify higher term error from follower_d
            assert!(matches!(
                e,
                Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(7)))
            ));
        }
    }
}

fn mk_log(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        command: vec![0; 8],
    }
}
#[test]
fn test_valid_request() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    let entry_term = 10;
    let prev_log_index = 5;
    let prev_log_term = entry_term;
    raft_log.expect_last_log_id().return_once(move || {
        Some(LogId {
            term: entry_term,
            index: 1,
        })
    });
    // entry_term != prev_log_term
    raft_log.expect_entry_term().return_once(move |_| Some(entry_term));

    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index,
        prev_log_term,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response = handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_success());

    if let append_entries_response::Result::Success(SuccessResult {
        last_match: Some(last_match),
    }) = response.result.unwrap()
    {
        assert_eq!(last_match.index, prev_log_index);
        assert_eq!(last_match.term, prev_log_term);
    }
}

#[test]
fn test_stale_term() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    raft_log.expect_has_log_at().returning(|_, _| true);
    raft_log.expect_last().returning(|| None);

    let request = AppendEntriesRequest {
        term: my_term - 1,
        prev_log_index: 5,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    assert!(handler
        .check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log))
        .is_higher_term());
}

/// # Case 1: follower local raft log length > prev_log_index
#[test]
fn test_mismatched_prev_term_case1() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 1;
    let entry_term = 10;
    let local_last_log_id = 2;
    let prev_log_index = local_last_log_id - 1;
    raft_log.expect_last_log_id().return_once(move || {
        Some(LogId {
            term: my_term,
            index: local_last_log_id,
        })
    });
    // entry_term != prev_log_term
    raft_log.expect_entry_term().return_once(move |_| Some(entry_term));

    let request = AppendEntriesRequest {
        term: 1,
        prev_log_index,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response = handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_conflict());
    if let append_entries_response::Result::Conflict(ConflictResult {
        conflict_term,
        conflict_index,
    }) = response.result.unwrap()
    {
        assert_eq!(conflict_term.unwrap(), entry_term);
        assert_eq!(conflict_index.unwrap(), prev_log_index.saturating_sub(1));
    }
}

/// # Case 2: follower local raft log length > prev_log_index
#[test]
fn test_mismatched_prev_term_case2() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 1;
    let entry_term = 10;
    let local_last_log_id = 2;
    let prev_log_index = local_last_log_id + 1;
    raft_log.expect_last_log_id().return_once(move || {
        Some(LogId {
            term: my_term,
            index: local_last_log_id,
        })
    });
    // entry_term != prev_log_term
    raft_log.expect_entry_term().return_once(move |_| Some(entry_term));

    let request = AppendEntriesRequest {
        term: 1,
        prev_log_index,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response = handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_conflict());
    if let append_entries_response::Result::Conflict(ConflictResult {
        conflict_term,
        conflict_index,
    }) = response.result.unwrap()
    {
        assert_eq!(conflict_term.unwrap(), entry_term);
        assert_eq!(conflict_index.unwrap(), local_last_log_id + 1);
    }
}

#[test]
fn test_virtual_log_handling() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_last_log_id()
        .returning(|| Some(LogId { term: 1, index: 5 }));

    let my_term = 2;
    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    assert!(handler
        .check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log))
        .is_success());
}
#[test]
fn test_virtual_log_with_non_empty_log() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    raft_log
        .expect_last_log_id()
        .returning(|| Some(LogId { term: 1, index: 5 }));

    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![Entry {
            term: 2,
            index: 1,
            command: vec![1; 8],
        }],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response = handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    // Should accept the request and handle the actual conflict later
    assert!(response.is_success());
}

/// # Case 1: conflict_with_term_and_index
#[test]
fn test_handle_conflict_response_case1() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: Some(3),
        conflict_index: Some(5),
    };
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler.handle_conflict_response(2, conflict_result, &raft_log).unwrap();

    // Temporary logic: next_index = 5 - 1 = 4
    assert_eq!(update.next_index, 4);
    assert_eq!(update.match_index, 3);
}

/// # Case 2: conflict_with_index_only
#[test]
fn test_handle_conflict_response_case2() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: Some(5),
    };
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler.handle_conflict_response(2, conflict_result, &raft_log).unwrap();
    assert_eq!(update.next_index, 5);
    assert_eq!(update.match_index, 4);
}

/// # Case 3: conflict_with_no_info
#[test]
fn test_handle_conflict_response_case3() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: None,
    };
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler.handle_conflict_response(2, conflict_result, &raft_log).unwrap();
    assert_eq!(update.next_index, 1);
    assert_eq!(update.match_index, 0);
}

/// # Case 4: conflict_with_index_zero
#[test]
fn test_handle_conflict_response_case4() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: Some(0), // Illegal but needs to be defended
    };
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler.handle_conflict_response(2, conflict_result, &raft_log).unwrap();

    // next_index is forced to be >= 1
    assert_eq!(update.next_index, 1);
    assert_eq!(update.match_index, 0);
}

/// # Case 1: test_higher_responder_term_triggers_step_down
#[test]
fn test_handle_success_response_case1() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 5; // Follower term > leader term (4)
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let result = handler.handle_success_response(2, responder_term, success_result, 4);
    assert!(matches!(
        result,
        Err(Error::Consensus(ConsensusError::Replication(
            ReplicationError::HigherTerm(5)
        )))
    ));
}

/// # Case 2: test_valid_success_response_updates_indices
#[test]
fn test_handle_success_response_case2() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let update = handler
        .handle_success_response(2, responder_term, success_result, 3)
        .unwrap();
    assert_eq!(update.match_index, 10);
    assert_eq!(update.next_index, 11);
}

/// # Case 3: test_lower_responder_term_ignored
#[test]
fn test_handle_success_response_case3() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let update = handler
        .handle_success_response(2, responder_term, success_result, 5)
        .unwrap();
    assert_eq!(update.match_index, 10); // Update normally, do not trigger step down
}

/// # Case 4: test_empty_follower_log_handling
#[test]
fn test_handle_success_response_case4() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult { last_match: None };
    let update = handler
        .handle_success_response(2, responder_term, success_result, 3)
        .unwrap();
    assert_eq!(update.match_index, 0); // Synchronize from index 0
    assert_eq!(update.next_index, 1);
}

/// # Case 5: test_zero_index_handling
#[test]
fn test_handle_success_response_case5() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 0 }), // Legal scenario (initial state)
    };
    let update = handler
        .handle_success_response(2, responder_term, success_result, 3)
        .unwrap();
    assert_eq!(update.next_index, 1); // Ensure next_index is at least 1
}
