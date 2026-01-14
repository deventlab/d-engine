//! Mock helpers for replication handler tests
//!
//! Provides simplified test context and utilities for unit testing ReplicationHandler
//! without requiring actual storage implementations.

use std::sync::Arc;

use crate::MockRaftLog;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;

/// Simplified test context for replication handler unit tests.
///
/// This context contains only the essential components needed for testing
/// ReplicationHandler logic, using mocks instead of real implementations.
///
/// # Fields
/// - `id`: Node ID
/// - `raft_log`: Mocked raft log for testing log operations
///
/// # Example
/// ```ignore
/// let context = setup_mock_replication_test_context(1);
/// // Use context.raft_log in tests
/// ```
pub struct MockReplicationTestContext {
    /// Node ID in the cluster
    pub id: u32,
    /// Mocked raft log
    pub raft_log: Arc<MockRaftLog>,
}

/// Setup a mock test context for replication handler tests.
///
/// Creates a simplified test context with mocked components, suitable for
/// unit testing ReplicationHandler without requiring real storage.
///
/// # Arguments
/// - `node_id`: The node ID for this test context
///
/// # Returns
/// A `MockReplicationTestContext` with configured mocks
///
/// # Example
/// ```ignore
/// let context = setup_mock_replication_test_context(1);
/// let handler = ReplicationHandler::new(context.id);
/// // Test handler methods...
/// ```
pub fn setup_mock_replication_test_context(node_id: u32) -> MockReplicationTestContext {
    let raft_log = MockRaftLog::new();

    MockReplicationTestContext {
        id: node_id,
        raft_log: Arc::new(raft_log),
    }
}

/// Configure mock raft log to simulate existing log entries.
///
/// This helper configures the MockRaftLog to return specific entries when queried,
/// simulating the behavior of a raft log that already contains data.
///
/// # Arguments
/// - `raft_log`: Mutable reference to MockRaftLog to configure
/// - `entries`: Vector of entries that should "exist" in the log
///
/// # Example
/// ```ignore
/// let mut raft_log = MockRaftLog::new();
/// let entries = vec![
///     Entry { index: 1, term: 1, payload: Some(...) },
///     Entry { index: 2, term: 1, payload: Some(...) },
/// ];
/// mock_log_entries_exist(&mut raft_log, entries);
/// ```
pub fn mock_log_entries_exist(
    raft_log: &mut MockRaftLog,
    entries: Vec<Entry>,
) {
    let entries_clone1 = entries.clone();
    let entries_clone2 = entries.clone();
    let entries_clone3 = entries.clone();
    let last_index = entries.last().map(|e| e.index).unwrap_or(0);

    // Mock last_entry_id to return the last entry's index
    raft_log.expect_last_entry_id().returning(move || last_index);

    // Mock get_entries_range to return the entries
    raft_log.expect_get_entries_range().returning(move |range| {
        let start = *range.start();
        let end = *range.end();
        let filtered: Vec<Entry> = entries_clone1
            .iter()
            .filter(|e| e.index >= start && e.index <= end)
            .cloned()
            .collect();
        Ok(filtered)
    });

    // Mock entry method for individual entry retrieval
    raft_log
        .expect_entry()
        .returning(move |index| Ok(entries_clone2.iter().find(|e| e.index == index).cloned()));

    // Mock entry_term for term lookup
    raft_log
        .expect_entry_term()
        .returning(move |index| entries_clone3.iter().find(|e| e.index == index).map(|e| e.term));
}

/// Create mock log entries with insert commands.
///
/// Helper to generate Entry structures with insert command payloads,
/// commonly used in replication tests.
///
/// # Arguments
/// - `ids`: Vector of IDs to use in insert commands
/// - `term`: Term for all entries
/// - `start_index`: Starting index for entries
///
/// # Returns
/// Vector of Entry objects with insert command payloads
///
/// # Example
/// ```ignore
/// let entries = mock_insert_log_entries(vec![1, 2, 3], 1, 1);
/// // Returns 3 entries: index 1,2,3 with term 1, each with insert command
/// ```
pub fn mock_insert_log_entries(
    ids: Vec<u64>,
    term: u64,
    start_index: u64,
) -> Vec<Entry> {
    use crate::test_utils::generate_insert_commands;

    ids.into_iter()
        .enumerate()
        .map(|(offset, id)| Entry {
            index: start_index + offset as u64,
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        })
        .collect()
}
