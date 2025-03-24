use super::{ReplicationCore, ReplicationData, ReplicationHandler};
use crate::{
    grpc::rpc_service::{ClientCommand, Entry},
    test_utils::{
        setup_raft_components, simulate_insert_proposal, MockNode, MockTypeConfig,
        MOCK_REPLICATION_HANDLER_PORT_BASE,
    },
    utils::util::kv,
    AppendResults, ChannelWithAddressAndRole, Error, LeaderStateSnapshot, MockRaftLog,
    MockTransport, NewLeaderInfo, PeerUpdate, RaftLog, RaftTypeConfig, StateSnapshot, FOLLOWER,
};
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::oneshot;

/// # Case 1: The peer3's next_index is equal to
///     the end of the leader's old log,
///     and only the new log is sent
///
/// ## Validate criterias
/// 1. only new_entries returned
///
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case1() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case1",
        None,
        false,
    );
    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![Entry {
        index: 1,
        term: 1,
        command: vec![1; 8],
    }];
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );
    if let Some(entries) = r.get(&peer3_id) {
        assert_eq!(*entries, new_entries, "Entries do not match expected value");
    } else {
        assert!(false);
    };
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
///
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case2() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case2",
        None,
        false,
    );

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
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
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
    if let Some(entries) = r.get(&peer3_id) {
        assert_eq!(
            *entries, merged_entries,
            "Entries do not match expected value"
        );
    } else {
        assert!(false);
    };
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
///
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case3() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case3",
        None,
        false,
    );

    // Simulate one entry in local raft log
    let raft_log = context.raft_log;
    simulate_insert_proposal(&raft_log, vec![1], 1);

    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![];
    let leader_last_index_before_inserting_new_entries = 1;
    let max_entries = 100;
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &raft_log,
    );
    let last_log_entry = raft_log.last().unwrap();
    if let Some(entries) = r.get(&peer3_id) {
        assert_eq!(
            *entries,
            vec![last_log_entry],
            "Entries do not match expected value"
        );
    } else {
        assert!(false);
    };
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
///
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case4_1() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case4_1",
        None,
        false,
    );

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
    let mut lagency_entries = raft_log
        .get_entries_between(peer3_next_id..=(peer3_next_id + max_legacy_entries_per_peer - 1));
    lagency_entries.extend(new_entries);
    if let Some(entries) = r.get(&peer3_id) {
        assert_eq!(
            *entries, lagency_entries,
            "Entries do not match expected value"
        );
    } else {
        assert!(false);
    };
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
///
#[test]
fn test_retrieve_to_be_synced_logs_for_peers_case4_2() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case4_2",
        None,
        false,
    );

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

    if let Some(entries) = r.get(&peer3_id) {
        assert_eq!(*entries, new_entries, "Entries do not match expected value");
    } else {
        assert!(false);
    };
}

/// # Case 1: Test with empty commands
///
/// ## Validation criterias:
/// 1. fun returns Ok(vec![])
/// 2. no update on local raft log
///
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
///
#[test]
fn test_generate_new_entries_case2() {
    let context = setup_raft_components("/tmp/test_generate_new_entries_case2", None, false);
    let my_id = 1;
    let handler = ReplicationHandler::<RaftTypeConfig>::new(my_id);
    let last_id = context.raft_log.last_entry_id();
    let commands = vec![ClientCommand::get(kv(1))];
    let current_term = 1;
    if let Ok(r) = handler.generate_new_entries(commands, current_term, &context.raft_log) {
        assert_eq!(r.len(), 1);
    } else {
        assert!(false);
    }
    assert_eq!(context.raft_log.last_entry_id(), last_id + 1);
}

/// # Case: Test retrieve expected items for peer 2
///
/// ## Scenario Setup
/// 1. entries_per_peer:
///     peer-2: log-3
///     peer-3: log-1, log-2, log-3
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
    let addr1 =
        MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 1, rx1)
            .await
            .expect("should succeed");
    let peer: ChannelWithAddressAndRole = ChannelWithAddressAndRole {
        id: peer2_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    };

    // Prepare entries to be replicated for each peer
    let entries_per_peer: DashMap<u32, Vec<Entry>> = DashMap::new();
    entries_per_peer.insert(
        peer2_id,
        vec![Entry {
            index: 3,
            term: 1,
            command: vec![1; 8],
        }],
    );
    entries_per_peer.insert(
        peer3_id,
        vec![
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
        ],
    );

    let data = ReplicationData {
        leader_last_index_before: 3,
        current_term: 1,
        commit_index: 1,
        peer_next_indices: HashMap::from([
            (peer2_id, peer2_next_index),
            (peer3_id, peer3_next_index),
        ]),
    };

    let (_id, _address, to_be_replicated_request) =
        handler.build_append_request(&context.raft_log, &peer, &entries_per_peer, &data);
    assert_eq!(to_be_replicated_request.entries.len(), 1);
}

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
///- function returns Ok
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case2_1() {
    let context = setup_raft_components(
        "/tmp/test_handle_client_proposal_in_batch_case2_1",
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
        MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 10, rx1)
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
    } else {
        assert!(false);
    }
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
///- function returns Error
///
#[tokio::test]
async fn test_handle_client_proposal_in_batch_case2_2() {
    let context = setup_raft_components(
        "/tmp/test_handle_client_proposal_in_batch_case2_2",
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
        MockNode::simulate_mock_service_without_reps(MOCK_REPLICATION_HANDLER_PORT_BASE + 11, rx1)
            .await
            .expect("should succeed");

    // Prepare AppendResults
    let replication_members: Vec<ChannelWithAddressAndRole> = vec![ChannelWithAddressAndRole {
        id: peer2_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    }];
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
        .returning(move |_, _, _| {
            Err(Error::FoundNewLeaderError(NewLeaderInfo {
                term: 1,
                leader_id: 7,
            }))
        });
    let raft_settings = context.settings.raft_settings.clone();

    if let Err(Error::FoundNewLeaderError(new_leader)) = handler
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
        assert_eq!(new_leader.leader_id, 7);
    } else {
        assert!(false);
    }
}
