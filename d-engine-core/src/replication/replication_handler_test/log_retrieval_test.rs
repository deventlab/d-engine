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

use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;

use super::PeerInterval;
use super::plan_peer_interval;
use crate::MockTypeConfig;
use crate::ReplicationCore;
use crate::ReplicationHandler;
use crate::mock_insert_log_entries;
use crate::mock_log_entries_exist;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::setup_mock_replication_test_context;

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

    // Arrange: Peer is caught up (next_index strictly past the leader's last old
    // entry, so the legacy-fetch branch is skipped entirely — not just equal to
    // it, which would still mean "I need index 10 itself").
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries + 1)]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_entries,
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    // Assert: Only new entries returned (peer already has old logs)
    assert_eq!(
        result.get(&peer3_id),
        Some(&crate::PeerEntriesResult::Ready(new_entries.clone())),
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
        &new_entries,
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    // Assert: Both old and new entries returned
    let mut expected_entries = old_entries.clone();
    expected_entries.extend(new_entries.clone());
    assert_eq!(
        result.get(&peer3_id),
        Some(&crate::PeerEntriesResult::Ready(expected_entries)),
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
        &new_entries,
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    // Assert: Only old entry returned (no new entries)
    assert_eq!(
        result.get(&peer3_id),
        Some(&crate::PeerEntriesResult::Ready(old_entries.clone())),
        "peer should receive only old logs when no new entries exist"
    );
}

/// A peer that lags beyond the limit receives only the capped old logs, without the new entry.
///
/// # Scenario
/// - Leader has 3 old log entries (index 1-3)
/// - New entry to replicate at index 4
/// - Peer's next_index = 1 (needs all old logs)
/// - Max entries limit = 2 (can only send 2 old entries)
/// - Expected: old logs 1-2 only; the peer catches up on 3 and 4 in later rounds
///
/// # Test Purpose
/// Validates two things at once: the limit is enforced for old logs, and the cut-short batch
/// is not extended with the new entry. Sending 1, 2 and 4 would leave a hole at 3 that the
/// follower accepts and acknowledges.
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
        &new_entries,
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    // Assert: only the first 2 old entries; the new entry at 4 must not follow them
    let expected = vec![old_entries[0].clone(), old_entries[1].clone()];
    assert_eq!(
        result.get(&peer3_id),
        Some(&crate::PeerEntriesResult::Ready(expected)),
        "peer should receive only the capped old logs (max=2), no new entries after a cut"
    );
}

/// With a limit of zero, a peer that needs old logs gets nothing (the caller sends a heartbeat).
///
/// # Scenario
/// - Leader has 3 old log entries (index 1-3)
/// - New entry to replicate at index 4
/// - Peer's next_index = 1 (needs old logs)
/// - Max entries limit = 0 (no old logs allowed)
/// - Expected: no entry for this peer
///
/// # Test Purpose
/// A batch of only the new entry 4 would start after the peer's next_index 1 and leave a
/// hole at 1-3. Configuration validation rejects 0, so this is a defensive check that a
/// disabled catch-up never produces a gapped batch.
#[tokio::test]
async fn test_retrieve_nothing_when_max_limit_zero_and_peer_behind() {
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
        &new_entries,
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    // Assert: nothing for this peer, so dispatch falls back to a heartbeat
    assert_eq!(
        result.get(&peer3_id),
        None,
        "peer behind with max_legacy=0 must not get a batch that skips its old logs"
    );
}

/// A batch sent to a lagging peer must be contiguous from `next_index`.
///
/// # Scenario
/// - Leader's last index before the new write is 5000; three new entries 5001..=5003
/// - Peer 2 reconnected after an outage: next_index = 1000, so prev_log_index = 999
/// - The per-RPC limit is 500
/// - The follower appends a batch right after prev_log_index without any gap check, so a hole
///   inside the batch (for example 1499 followed by 5001) becomes a hole in its log
///
/// # Test Purpose
/// The batch must start at `next_index` and every index must be the previous one plus one,
/// whatever the limit truncates. This asserts the property, not a particular payload shape.
#[tokio::test]
async fn test_retrieve_batch_stays_contiguous_when_peer_lags_beyond_limit() {
    let mut context = setup_mock_replication_test_context(1);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let peer_id = 2;

    let old_entries = mock_insert_log_entries((1000..=5000).collect(), 1, 1000);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, old_entries);

    let new_entries = mock_insert_log_entries(vec![1, 2, 3], 1, 5001);
    let peer_next_indices = HashMap::from([(peer_id, 1000_u64)]);

    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_entries,
        5000, // leader_last_index_before_inserting_new_entries
        500,  // max_legacy_entries_per_peer
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    let Some(crate::PeerEntriesResult::Ready(entries)) = result.get(&peer_id) else {
        panic!(
            "expected Ready entries for the lagging peer, got {:?}",
            result.get(&peer_id)
        );
    };
    assert_eq!(
        entries.first().map(|e| e.index),
        Some(1000),
        "batch must start at the peer's next_index, which prev_log_index is derived from"
    );
    for pair in entries.windows(2) {
        assert_eq!(
            pair[1].index,
            pair[0].index + 1,
            "hole in batch between {} and {}",
            pair[0].index,
            pair[1].index
        );
    }
}

/// The request built for a peer must carry `prev_log_index` right before its first entry.
///
/// # Scenario
/// - Leader's log ended at 5000; this round appended 5001..=5003, so the log now ends at 5003
/// - Peer 2's recorded next_index is 6000, beyond the leader's log: an earlier optimistic advance
///   ran past what the peer really has (for example after a reconnect)
/// - The peer's entries are chosen from the recorded next_index (6000), while prev_log_index is
///   derived from a next_index clamped to the log end (5004)
///
/// # Test Purpose
/// Entries and prev_log_index must describe the same position, whatever batch is selected
/// (a heartbeat with prev=5003 is fine). Otherwise the follower is asked to match prev=5003
/// and is then handed 5001..=5003, which starts before prev.
#[tokio::test]
async fn test_build_request_prev_index_precedes_first_entry_when_next_index_overshoots() {
    let mut context = setup_mock_replication_test_context(1);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let peer_id = 2;

    let new_entries = mock_insert_log_entries(vec![1, 2, 3], 1, 5001);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, new_entries.clone());
    raft_log_mut.expect_first_entry_id().returning(|| 1);

    let peer_next_indices = HashMap::from([(peer_id, 6000_u64)]);
    let mut result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_entries,
        5000, // leader_last_index_before_inserting_new_entries
        100,  // max_legacy_entries_per_peer
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );
    // Whatever batch was selected (possibly none: the peer is past the log end), the request
    // built from it must stay self-consistent.
    let entries = match result.remove(&peer_id) {
        Some(crate::PeerEntriesResult::Ready(entries)) => entries,
        None => Vec::new(),
        Some(other) => panic!("unexpected result for the overshooting peer: {other:?}"),
    };

    let data = crate::ReplicationData {
        leader_last_index_before: 5000,
        current_term: 1,
        commit_index: 0,
        peer_next_indices,
    };
    let mut entries_per_peer = HashMap::from([(peer_id, entries)]);
    let (_, outcome) = handler
        .build_append_request(&context.raft_log, peer_id, &mut entries_per_peer, &data)
        .expect("build_append_request should not fail");

    let crate::BuildAppendOutcome::Append { request, .. } = outcome else {
        panic!("expected an Append outcome");
    };
    if let Some(first) = request.entries.first() {
        assert_eq!(
            request.prev_log_index + 1,
            first.index,
            "prev_log_index {} does not precede the first entry {}",
            request.prev_log_index,
            first.index
        );
    }
}

/// A peer whose next index was purged needs a snapshot, whatever the catch-up limit is.
///
/// # Scenario
/// - Leader retained window: first_index = 11, last = 15; New entry at index 16
/// - Peer 2 next_index = 5, below the retained window (its entries were purged)
/// - The catch-up limit is 0, so no old entries can be sent in any case
///
/// # Test Purpose
/// The purge check decides between "send entries" and "send a snapshot". If it only runs
/// when there is something to send, a peer that can never be caught up from the log is
/// silently left on heartbeats forever instead of being routed to a snapshot.
#[tokio::test]
async fn test_retrieve_need_snapshot_when_next_index_purged_even_if_limit_zero() {
    let context = setup_mock_replication_test_context(1);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let peer_id = 2;

    let new_entries = mock_insert_log_entries(vec![1], 1, 16);
    let peer_next_indices = HashMap::from([(peer_id, 5_u64)]);

    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_entries,
        15, // leader_last_index_before_inserting_new_entries
        0,  // max_legacy_entries_per_peer
        &peer_next_indices,
        &context.raft_log,
        11, // first_index
        &HashMap::new(),
    );

    assert_eq!(
        result.get(&peer_id),
        Some(&crate::PeerEntriesResult::NeedSnapshot),
        "a purged next_index must route the peer to a snapshot even when no catch-up is allowed"
    );
}

/// Runs `retrieve_to_be_synced_logs_for_peers` for one peer against a leader log of 1..=20
/// plus `new_entries`, and returns the indices of the selected batch (`None` = no batch).
///
/// The mock log only answers range reads when `stored` is true. When it is false any read
/// panics, so a test using it also proves that no read from the log happened.
fn select_batch(
    stored: bool,
    new_entries: &[Entry],
    cap: u64,
    peer_next: u64,
    first_index: u64,
) -> Option<crate::PeerEntriesResult> {
    let mut context = setup_mock_replication_test_context(1);
    if stored {
        let log = mock_insert_log_entries((1..=20).collect(), 1, 1);
        mock_log_entries_exist(Arc::get_mut(&mut context.raft_log).unwrap(), log);
    }
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut result = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries,
        20, // leader_last_index_before_inserting_new_entries
        cap,
        &HashMap::from([(2, peer_next)]),
        &context.raft_log,
        first_index,
        &HashMap::new(),
    );
    result.remove(&2)
}

fn indices(result: Option<crate::PeerEntriesResult>) -> Option<Vec<u64>> {
    match result {
        Some(crate::PeerEntriesResult::Ready(entries)) => {
            Some(entries.iter().map(|e| e.index).collect())
        }
        Some(other) => panic!("expected Ready or no batch, got {other:?}"),
        None => None,
    }
}

fn new_21_to_23() -> Vec<Entry> {
    mock_insert_log_entries(vec![1, 2, 3], 1, 21)
}

/// The catch-up reaches the old tail exactly, so this round's entries join the same batch.
///
/// # Scenario
/// Log 1..=20, new 21..=23, cap 5, peer next 16: the catch-up covers 16..=20.
///
/// # Test Purpose
/// Boundary of the rule "new entries ride along only when the catch-up reaches the old
/// tail". One index short (next 15) must not join; see the next test.
#[tokio::test]
async fn test_retrieve_catchup_reaching_old_tail_exactly_includes_new_entries() {
    let got = indices(select_batch(true, &new_21_to_23(), 5, 16, 1));
    assert_eq!(got, Some((16..=23).collect()));
}

/// The catch-up stops one index short of the old tail, so this round's entries must wait.
///
/// # Scenario
/// Log 1..=20, new 21..=23, cap 5, peer next 15: the catch-up covers 15..=19.
///
/// # Test Purpose
/// Joining 21..=23 here would skip index 20 and hand the follower a hole.
#[tokio::test]
async fn test_retrieve_catchup_one_short_of_old_tail_excludes_new_entries() {
    let got = indices(select_batch(true, &new_21_to_23(), 5, 15, 1));
    assert_eq!(got, Some((15..=19).collect()));
}

/// The cap bounds the catch-up only; a caught-up peer receives the whole new batch.
///
/// # Scenario
/// Ten new entries 21..=30, cap 5, peer already at 21. The log must not be read.
///
/// # Test Purpose
/// New entries are bounded by the batching limit, not by the catch-up cap. Cutting them
/// here would make every round leave a remainder behind under sustained load.
#[tokio::test]
async fn test_retrieve_caught_up_peer_gets_whole_new_batch_above_cap() {
    let ten_new = mock_insert_log_entries((1..=10).collect(), 1, 21);
    let got = indices(select_batch(false, &ten_new, 5, 21, 1));
    assert_eq!(got, Some((21..=30).collect()));
}

/// A zero catch-up limit does not stop a caught-up peer from receiving new entries.
///
/// # Test Purpose
/// The limit switches off catching up, not replication. Peer at 21, cap 0, new 21..=23.
#[tokio::test]
async fn test_retrieve_cap_zero_still_sends_new_entries_to_caught_up_peer() {
    let got = indices(select_batch(false, &new_21_to_23(), 0, 21, 1));
    assert_eq!(got, Some(vec![21, 22, 23]));
}

/// A peer whose next index falls inside this round's entries receives only the missing suffix.
///
/// # Test Purpose
/// The slice must start at the peer's next index, not at the first new entry, otherwise the
/// batch would begin before `prev_log_index`. Peer next 22, new 21..=23 -> 22..=23.
#[tokio::test]
async fn test_retrieve_peer_inside_new_entries_gets_only_missing_suffix() {
    let got = indices(select_batch(false, &new_21_to_23(), 100, 22, 1));
    assert_eq!(got, Some(vec![22, 23]));
}

/// A peer at or past the end of the leader's log has nothing to receive.
///
/// # Test Purpose
/// next 24 is exactly one past the log end (new 21..=23); next 50 is far past it (a stale
/// optimistic advance). Both must yield no batch so the caller sends a heartbeat.
#[tokio::test]
async fn test_retrieve_peer_at_or_beyond_log_end_gets_no_batch() {
    assert_eq!(
        indices(select_batch(false, &new_21_to_23(), 100, 24, 1)),
        None
    );
    assert_eq!(
        indices(select_batch(false, &new_21_to_23(), 100, 50, 1)),
        None
    );
}

/// A caught-up peer with no new entries in this round has nothing to receive.
///
/// # Test Purpose
/// An idle round (no client writes) must not produce an empty `Ready` batch.
#[tokio::test]
async fn test_retrieve_caught_up_peer_without_new_entries_gets_no_batch() {
    assert_eq!(indices(select_batch(false, &[], 100, 21, 1)), None);
}

/// A next index equal to the oldest kept entry is still readable; only below it needs a snapshot.
///
/// # Test Purpose
/// Off-by-one guard on the purge boundary. first_index 11, next 11, cap 100 -> 11..=23.
#[tokio::test]
async fn test_retrieve_next_index_at_first_index_reads_log_instead_of_snapshot() {
    let got = indices(select_batch(true, &new_21_to_23(), 100, 11, 11));
    assert_eq!(got, Some((11..=23).collect()));
}

/// A failed log read is reported as storage corruption, never as a snapshot or an empty batch.
///
/// # Test Purpose
/// The caller must not hide a broken log behind a snapshot, and must not send the follower
/// a batch that silently lacks the entries it asked for.
#[tokio::test]
async fn test_retrieve_log_read_error_is_reported_as_corrupt_gap() {
    let mut context = setup_mock_replication_test_context(1);
    Arc::get_mut(&mut context.raft_log)
        .unwrap()
        .expect_get_entries_range()
        .returning(|_| Err(crate::StorageError::LogStorage("disk unreadable".into()).into()));
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_21_to_23(),
        20,
        100,
        &HashMap::from([(2, 5_u64)]),
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    assert_eq!(
        result.get(&2),
        Some(&crate::PeerEntriesResult::CorruptGap {
            start: 5,
            end: 20,
            first_seen: None,
            last_seen: None,
            count: 0,
        })
    );
}

/// Each follower gets its own interval; one peer's state never changes another's batch.
///
/// # Scenario
/// Peer 2 lags (next 1, cap 5); peer 3 is caught up (next 21).
///
/// # Test Purpose
/// Peer 2 is cut to 1..=5 without new entries, while peer 3 still receives 21..=23.
#[tokio::test]
async fn test_retrieve_peers_are_decided_independently() {
    let mut context = setup_mock_replication_test_context(1);
    let log = mock_insert_log_entries((1..=20).collect(), 1, 1);
    mock_log_entries_exist(Arc::get_mut(&mut context.raft_log).unwrap(), log);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_21_to_23(),
        20,
        5,
        &HashMap::from([(2, 1_u64), (3, 21_u64)]),
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    assert_eq!(indices(result.remove(&2)), Some((1..=5).collect()));
    assert_eq!(indices(result.remove(&3)), Some(vec![21, 22, 23]));
}

/// A batch with a hole inside must never be turned into a request.
///
/// # Scenario
/// - Leader log 1..=20; peer 2 is recorded at next_index 10
/// - The entries handed to `build_append_request` are 10, 11 and 13 (12 is missing)
///
/// # Test Purpose
/// The follower checks only `prev_log_index` against its own log. It would accept this batch
/// and store the hole, then acknowledge up to 13. The leader is the last place that can still
/// refuse, so a non-contiguous batch must fail loudly instead of being sent.
#[tokio::test]
async fn test_build_request_rejects_batch_with_a_hole() {
    let mut context = setup_mock_replication_test_context(1);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let peer_id = 2;

    let log = mock_insert_log_entries((1..=20).collect(), 1, 1);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, log.clone());
    raft_log_mut.expect_first_entry_id().returning(|| 1);

    let holed: Vec<Entry> = [10_u64, 11, 13].iter().map(|&i| log[i as usize - 1].clone()).collect();
    let mut holed = holed;
    holed[2].index = 13; // keep the payload of entry 13 but make the gap visible

    let data = crate::ReplicationData {
        leader_last_index_before: 20,
        current_term: 1,
        commit_index: 0,
        peer_next_indices: HashMap::from([(peer_id, 10_u64)]),
    };
    let mut entries_per_peer = HashMap::from([(peer_id, holed)]);

    let result =
        handler.build_append_request(&context.raft_log, peer_id, &mut entries_per_peer, &data);

    assert!(
        result.is_err(),
        "a batch with indices 10, 11, 13 must be refused, got {:?}",
        result.map(|(_, outcome)| matches!(outcome, crate::BuildAppendOutcome::Append { .. }))
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
    // peer3's next_index is strictly past the leader's last old entry, so the
    // legacy-fetch branch is skipped entirely (see
    // test_retrieve_only_new_entries_when_peer_caught_up).
    let peer_next_indices = HashMap::from([
        (my_id, 1_u64),                                                 // Leader itself
        (peer3_id, leader_last_index_before_inserting_new_entries + 1), // Peer3
    ]);

    // Arrange: Create handler
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Act: Retrieve logs to sync
    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &new_entries,
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::new(),
    );

    // Assert: Peer3 receives entries
    assert_eq!(
        result.get(&peer3_id),
        Some(&crate::PeerEntriesResult::Ready(new_entries.clone())),
        "peer3 should receive new entries"
    );

    // Assert: Leader's own ID is excluded
    assert!(
        !result.contains_key(&my_id),
        "leader should not replicate to itself"
    );
}

/// A legacy range read that returns fewer entries than requested must be
/// classified as `CorruptGap` — never silently treated as "nothing to send".
///
/// # Scenario
/// - Leader retained window: first_index = 11, last = 15
/// - Peer 2 next_index = 11 (at the purge boundary, so not `NeedSnapshot`)
/// - `get_entries_range(11..=15)` returns only indices 11, 12
/// - Expected: `PeerEntriesResult::CorruptGap` (start=11, end=15, count=2)
#[tokio::test]
async fn test_retrieve_corrupt_gap_when_range_read_short() {
    let mut context = setup_mock_replication_test_context(1);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let peer_id = 2;

    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    raft_log_mut.expect_get_entries_range().returning(|_| {
        Ok(vec![
            Entry {
                index: 11,
                term: 1,
                payload: Some(EntryPayload::noop()),
            },
            Entry {
                index: 12,
                term: 1,
                payload: Some(EntryPayload::noop()),
            },
        ])
    });

    let peer_next_indices = HashMap::from([(peer_id, 11_u64)]);

    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &[],
        15,  // leader_last_index_before_inserting_new_entries
        100, // max_legacy_entries_per_peer
        &peer_next_indices,
        &context.raft_log,
        11, // first_index
        &HashMap::new(),
    );

    assert_eq!(
        result.get(&peer_id),
        Some(&crate::PeerEntriesResult::CorruptGap {
            start: 11,
            end: 15,
            first_seen: Some(11),
            last_seen: Some(12),
            count: 2,
        }),
        "short range read must be classified as CorruptGap"
    );
}

/// A legacy range read with a hole in the middle must also be classified as
/// `CorruptGap` — a gap is a storage inconsistency, not a clean purge.
///
/// # Scenario
/// - Leader retained window: first_index = 11, last = 15
/// - Peer 2 next_index = 11
/// - `get_entries_range(11..=15)` returns 11, 12, 14, 15 (index 13 missing)
/// - Expected: `PeerEntriesResult::CorruptGap` (count=4, hole at 13)
#[tokio::test]
async fn test_retrieve_corrupt_gap_when_range_read_gapped() {
    let mut context = setup_mock_replication_test_context(1);
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let peer_id = 2;

    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    raft_log_mut.expect_get_entries_range().returning(|_| {
        Ok(vec![
            Entry {
                index: 11,
                term: 1,
                payload: Some(EntryPayload::noop()),
            },
            Entry {
                index: 12,
                term: 1,
                payload: Some(EntryPayload::noop()),
            },
            Entry {
                index: 14,
                term: 1,
                payload: Some(EntryPayload::noop()),
            },
            Entry {
                index: 15,
                term: 1,
                payload: Some(EntryPayload::noop()),
            },
        ])
    });

    let peer_next_indices = HashMap::from([(peer_id, 11_u64)]);

    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &[],
        15,
        100,
        &peer_next_indices,
        &context.raft_log,
        11,
        &HashMap::new(),
    );

    assert_eq!(
        result.get(&peer_id),
        Some(&crate::PeerEntriesResult::CorruptGap {
            start: 11,
            end: 15,
            first_seen: Some(11),
            last_seen: Some(15),
            count: 4,
        }),
        "gapped range read must be classified as CorruptGap"
    );
}

/// A peer whose in-flight window is gated must get no prepared entries, while an ungated peer
/// with the same position still does. The gate is checked before the log is touched.
///
/// # Scenario
/// - Peers 2 and 3 are both behind (`next_index = 1`), one old entry exists at index 1.
/// - `peer_gating_decisions = {2: true, 3: false}`.
/// - Expected: peer 3 is `Ready(old entry)`; peer 2 has no entry in the result.
#[tokio::test]
async fn test_gated_peer_gets_no_entries_while_ungated_peer_does() {
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;

    let old_entries = mock_insert_log_entries(vec![1], 1, 1);
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    mock_log_entries_exist(raft_log_mut, old_entries.clone());

    let peer_next_indices = HashMap::from([(2_u32, 1_u64), (3_u32, 1_u64)]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let result = handler.retrieve_to_be_synced_logs_for_peers(
        &[],
        1,
        100,
        &peer_next_indices,
        &context.raft_log,
        1,
        &HashMap::from([(2_u32, true), (3_u32, false)]),
    );

    assert!(
        !result.contains_key(&2),
        "a window-full peer must not have entries prepared"
    );
    assert_eq!(
        result.get(&3),
        Some(&crate::PeerEntriesResult::Ready(old_entries)),
        "an ungated peer at the same position must still get its entries"
    );
}

// ---------------------------------------------------------------------------
// plan_peer_interval: which indices one follower gets this round.
//
// Fixture: the leader's log is 1..=10 (`old_last`), this round adds 11..=13 (`new_last`),
// the oldest kept entry is 1 and the cap is 100, unless a test says otherwise.
// ---------------------------------------------------------------------------

fn plan(
    peer_next_id: u64,
    first_index: u64,
    cap: u64,
) -> PeerInterval {
    plan_peer_interval(peer_next_id, 10, 13, first_index, cap)
}

/// A follower that already has the whole old log gets only this round's entries.
#[test]
fn test_plan_interval_caught_up_peer_gets_only_new_entries() {
    assert!(matches!(plan(11, 1, 100), PeerInterval::Send {
        start: 11,
        end: 13
    }));
}

/// A follower behind but within the cap gets the old entries it lacks plus this round's,
/// as one run.
#[test]
fn test_plan_interval_lagging_within_cap_gets_old_and_new_entries() {
    assert!(matches!(plan(8, 1, 100), PeerInterval::Send {
        start: 8,
        end: 13
    }));
}

/// When the cap stops the catch-up short of the old tail, this round's entries are left out.
/// Adding them would put a hole between the catch-up and the new entries.
#[test]
fn test_plan_interval_cap_cuts_catchup_and_drops_new_entries() {
    // 1..=10 lacking, cap 4: catch-up 1..=4 only.
    assert!(matches!(plan(1, 1, 4), PeerInterval::Send {
        start: 1,
        end: 4
    }));
}

/// Boundary: the cap lands exactly on the old tail, so this round's entries are included;
/// one fewer and they are not.
#[test]
fn test_plan_interval_cap_reaching_old_tail_exactly_includes_new_entries() {
    // next=7, cap=4 -> 7..=10 reaches old_last.
    assert!(matches!(plan(7, 1, 4), PeerInterval::Send {
        start: 7,
        end: 13
    }));
    // next=6, cap=4 -> 6..=9 stops one short.
    assert!(matches!(plan(6, 1, 4), PeerInterval::Send {
        start: 6,
        end: 9
    }));
}

/// A follower whose next index falls inside this round's entries gets the rest of them.
#[test]
fn test_plan_interval_peer_inside_new_entries_gets_the_rest() {
    assert!(matches!(plan(12, 1, 100), PeerInterval::Send {
        start: 12,
        end: 13
    }));
}

/// A recorded next index past the log end is clamped, and then there is nothing to send.
#[test]
fn test_plan_interval_peer_past_log_end_gets_nothing() {
    assert!(matches!(plan(50, 1, 100), PeerInterval::Nothing));
    // Next is exactly one past the log end.
    assert!(matches!(plan(14, 1, 100), PeerInterval::Nothing));
}

/// No new entries this round and the follower is caught up: heartbeat only.
#[test]
fn test_plan_interval_no_new_entries_and_caught_up_gets_nothing() {
    assert!(matches!(
        plan_peer_interval(11, 10, 10, 1, 100),
        PeerInterval::Nothing
    ));
}

/// Cap 0 allows no catch-up: a lagging follower gets nothing, a caught-up one still gets
/// this round's entries.
#[test]
fn test_plan_interval_cap_zero_blocks_catchup_but_not_new_entries() {
    assert!(matches!(plan(5, 1, 0), PeerInterval::Nothing));
    assert!(matches!(plan(11, 1, 0), PeerInterval::Send {
        start: 11,
        end: 13
    }));
}

/// A follower below the oldest kept entry can only be caught up by a snapshot, even when the
/// cap would allow no catch-up at all.
#[test]
fn test_plan_interval_purged_peer_needs_snapshot_regardless_of_cap() {
    assert!(matches!(plan(3, 5, 100), PeerInterval::NeedSnapshot));
    assert!(matches!(plan(3, 5, 0), PeerInterval::NeedSnapshot));
}

/// Boundary: next index equal to the oldest kept entry is still servable from the log.
#[test]
fn test_plan_interval_peer_at_first_index_is_not_purged() {
    assert!(matches!(plan(5, 5, 100), PeerInterval::Send {
        start: 5,
        end: 13
    }));
}

/// A huge cap must not overflow when added to the start index.
#[test]
fn test_plan_interval_max_cap_does_not_overflow() {
    assert!(matches!(plan(1, 1, u64::MAX), PeerInterval::Send {
        start: 1,
        end: 13
    }));
}

/// Over every small combination, the chosen interval keeps the properties replication relies on:
/// it ends within the log, respects the cap on the catch-up part, never skips indices between
/// catch-up and new entries, and never starts below the oldest kept entry.
#[test]
fn test_plan_interval_properties_hold_for_all_small_inputs() {
    for old_last in 0u64..=6 {
        for new_count in 0u64..=3 {
            let new_last = old_last + new_count;
            for first_index in 1u64..=old_last + 1 {
                for peer_next in 1u64..=new_last + 3 {
                    for cap in [0u64, 1, 2, 5, u64::MAX] {
                        let ctx = format!(
                            "old_last={old_last} new_last={new_last} first={first_index} \
                             next={peer_next} cap={cap}"
                        );
                        match plan_peer_interval(peer_next, old_last, new_last, first_index, cap) {
                            PeerInterval::NeedSnapshot => {
                                assert!(peer_next <= old_last && peer_next < first_index, "{ctx}");
                            }
                            PeerInterval::Nothing => {}
                            PeerInterval::Send { start, end } => {
                                assert!(start <= end && end <= new_last, "{ctx}");
                                assert_eq!(start, peer_next.min(new_last + 1), "{ctx}");
                                if start <= old_last {
                                    assert!(start >= first_index, "{ctx}");
                                    // Catch-up part is capped.
                                    assert!(end.min(old_last) - start < cap.max(1), "{ctx}");
                                    // New entries only follow a catch-up that reached the tail.
                                    if end > old_last {
                                        assert!(start.saturating_add(cap) > old_last, "{ctx}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
