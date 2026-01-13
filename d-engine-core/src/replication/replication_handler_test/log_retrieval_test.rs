//! Log retrieval scenarios for ReplicationHandler
//!
//! Tests verify correct log retrieval and synchronization logic:
//! - Case 1: Peer at end of old log (only new entries needed)
//! - Case 2: Peer needs old + new logs (not exceeding max)
//! - Case 3: Peer behind with max entries limit
//! - Case 4: Multiple scenarios with different peer positions
//! - Case 5: Edge cases in log retrieval

use std::collections::HashMap;
use std::sync::Arc;

use crate::MockTypeConfig;
use crate::ReplicationCore;
use crate::ReplicationHandler;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::mock_insert_log_entries;
use crate::test_utils::mock_log_entries_exist;
use crate::test_utils::setup_mock_replication_test_context;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;

/// Peer at end of old log should receive only new entries.
///
/// # Scenario
/// - Leader has old logs up to index 10
/// - New entries start at index 1 (to be replicated)
/// - Peer's next_index = 10 (caught up with old logs)
/// - Expected: Only new_entries returned (no old logs needed)
///
/// # Test Purpose
/// Validates that when peer is already caught up with existing logs,
/// only the newly generated entries are included in sync payload.
#[tokio::test]
async fn test_retrieve_only_new_entries_when_peer_caught_up() {
    // Arrange: Setup test context
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer3_id = 3;

    // Arrange: Configure mock to return empty old logs (peer is caught up)
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, vec![]);

    // Arrange: New entries to be replicated
    let new_entries = vec![Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }];

    // Arrange: Leader's state before new entries
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;

    // Arrange: Peer is caught up (next_index = end of old logs)
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );

    // Assert: Only new entries returned (peer already has old logs)
    assert!(
        result.get(&peer3_id).is_some_and(|entries| *entries == new_entries),
        "peer at end of old log should receive only new entries"
    );
}

/// Peer behind should receive old logs plus new entries.
///
/// # Scenario
/// - Leader has one old log entry at index 1
/// - New entries to be replicated at index 2
/// - Peer's next_index = 1 (needs old log entry)
/// - Expected: Both old log (index 1) and new entries (index 2) returned
///
/// # Test Purpose
/// Validates that when peer is behind, both historical logs and
/// new entries are combined in the sync payload.
#[tokio::test]
async fn test_retrieve_old_and_new_entries_when_peer_behind() {
    // Arrange: Setup test context
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer3_id = 3;

    // Arrange: Existing old log entry at index 1
    let old_entries = mock_insert_log_entries(vec![100], 1, 1);

    // Arrange: Configure mock to return old entry
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, old_entries.clone());

    // Arrange: New entry at index 2 (next after old log)
    let new_entries = mock_insert_log_entries(vec![200], 1, 2);

    // Arrange: Leader's last index before new entries (index 1)
    let leader_last_index_before_inserting_new_entries = 1;
    let max_entries = 100;

    // Arrange: Peer needs both old and new (next_index = 1)
    let peer_next_indices = HashMap::from([(peer3_id, 1_u64)]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );

    // Assert: Both old and new entries returned
    let mut expected_entries = old_entries.clone();
    expected_entries.extend(new_entries.clone());
    assert!(
        result.get(&peer3_id).is_some_and(|entries| *entries == expected_entries),
        "peer behind should receive both old logs and new entries"
    );
}

/// Empty new entries should return only old logs for behind peer.
///
/// # Scenario
/// - Leader has one old log entry at index 1
/// - No new entries to replicate (empty batch)
/// - Peer's next_index = 1 (needs old log)
/// - Expected: Only old log returned
///
/// # Test Purpose
/// Validates heartbeat scenario where no new entries exist,
/// but peer still needs historical logs.
#[tokio::test]
async fn test_retrieve_only_old_entries_when_no_new_entries() {
    // Arrange: Setup test context with one old entry
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer3_id = 3;

    // Arrange: One old log entry at index 1
    let old_entries = mock_insert_log_entries(vec![1], 1, 1);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, old_entries.clone());

    // Arrange: No new entries (heartbeat scenario)
    let new_entries = vec![];
    let leader_last_index_before_inserting_new_entries = 1;
    let max_entries = 100;
    let peer_next_indices = HashMap::from([(peer3_id, 1_u64)]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries,
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );

    // Assert: Only old entry returned (no new entries)
    assert!(
        result.get(&peer3_id).is_some_and(|entries| *entries == old_entries),
        "peer should receive only old logs when no new entries exist"
    );
}

/// Peer behind with max entries limit should receive limited old logs plus new entries.
///
/// # Scenario
/// - Leader has 3 old log entries (index 1-3)
/// - New entry to replicate at index 4
/// - Peer's next_index = 1 (needs all old logs)
/// - Max entries limit = 2 (can only send 2 old entries)
/// - Expected: First 2 old logs (index 1-2) + new entry
///
/// # Test Purpose
/// Validates that max_entries limit is enforced for old logs,
/// preventing overwhelming peers with too much historical data.
#[tokio::test]
async fn test_retrieve_limited_old_entries_with_max_limit() {
    // Arrange: Setup test context with 3 old entries
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer3_id = 3;

    // Arrange: Three old log entries at index 1-3
    let old_entries = mock_insert_log_entries(vec![1, 2, 3], 1, 1);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, old_entries.clone());

    // Arrange: New entry at index 4
    let new_entries = mock_insert_log_entries(vec![100], 1, 4);
    let leader_last_index_before_inserting_new_entries = 3;
    let max_legacy_entries_per_peer = 2; // Limit to 2 old entries

    // Arrange: Peer needs all old logs (next_index = 1)
    let peer_next_indices = HashMap::from([(peer3_id, 1_u64)]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &context.raft_log,
    );

    // Assert: Only first 2 old entries + new entry (limited by max)
    let mut expected = vec![old_entries[0].clone(), old_entries[1].clone()];
    expected.extend(new_entries.clone());
    assert!(
        result.get(&peer3_id).is_some_and(|entries| *entries == expected),
        "peer should receive limited old logs (max=2) plus new entries"
    );
}

/// Max entries limit of zero should return only new entries.
///
/// # Scenario
/// - Leader has 3 old log entries (index 1-3)
/// - New entry to replicate at index 4
/// - Peer's next_index = 1 (needs old logs)
/// - Max entries limit = 0 (no old logs allowed)
/// - Expected: Only new entry (no old logs)
///
/// # Test Purpose
/// Validates that max_entries=0 skips all historical logs,
/// useful for scenarios prioritizing new data over catch-up.
#[tokio::test]
async fn test_retrieve_only_new_entries_when_max_limit_zero() {
    // Arrange: Setup test context with 3 old entries
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer3_id = 3;

    // Arrange: Three old log entries at index 1-3
    let old_entries = mock_insert_log_entries(vec![1, 2, 3], 1, 1);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, old_entries);

    // Arrange: New entry at index 4
    let new_entries = mock_insert_log_entries(vec![100], 1, 4);
    let leader_last_index_before_inserting_new_entries = 3;
    let max_legacy_entries_per_peer = 0; // No old entries allowed

    // Arrange: Peer needs old logs (next_index = 1)
    let peer_next_indices = HashMap::from([(peer3_id, 1_u64)]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &context.raft_log,
    );

    // Assert: Only new entry (no old logs due to max=0)
    assert!(
        result.get(&peer3_id).is_some_and(|entries| *entries == new_entries),
        "peer should receive only new entries when max_legacy=0"
    );
}

/// Leader's own ID in peer list should be ignored.
///
/// # Scenario
/// - Peer list includes leader's own ID (my_id) and peer3
/// - Both have different next_index values
/// - Expected: Only peer3 receives entries, leader ignored
///
/// # Test Purpose
/// Validates that leader doesn't include itself in replication targets,
/// preventing self-replication.
#[tokio::test]
async fn test_leader_id_excluded_from_replication_targets() {
    // Arrange: Setup test context
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer3_id = 3;

    // Arrange: Configure empty old logs
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, vec![]);

    // Arrange: New entry to replicate
    let new_entries = mock_insert_log_entries(vec![1], 1, 1);
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;

    // Arrange: Peer map includes BOTH leader and peer3
    let peer_next_indices = HashMap::from([
        (my_id, 1_u64),                                             // Leader itself
        (peer3_id, leader_last_index_before_inserting_new_entries), // Peer3
    ]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );

    // Assert: Peer3 receives entries
    assert!(
        result.get(&peer3_id).is_some_and(|entries| *entries == new_entries),
        "peer3 should receive new entries"
    );

    // Assert: Leader's own ID is excluded
    assert!(
        result.get(&my_id).is_none(),
        "leader should not replicate to itself"
    );
}
