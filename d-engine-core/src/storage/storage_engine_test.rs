use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VotedFor;
use prost::Message;

use crate::Error;
use crate::HardState;
use crate::storage::StorageEngine;

/// Test suite for StorageEngine implementations
///
/// This suite provides comprehensive tests that can be used to validate
/// any StorageEngine implementation. Developers should implement the
/// `StorageEngineBuilder` trait and then call `run_all_storage_engine_tests`
/// with their builder.
pub struct StorageEngineTestSuite;

/// Builder trait for creating StorageEngine instances for testing
#[async_trait]
pub trait StorageEngineBuilder: Send + Sync {
    /// The concrete StorageEngine type returned by this builder
    type Engine: StorageEngine + Send + Sync + 'static;

    /// Creates an engine on new, empty storage. Every call starts from nothing: whatever an earlier
    /// call wrote is not visible, so a test never has to wipe an engine first.
    async fn build(&self) -> Result<Arc<Self::Engine>, Error>;

    /// Opens an engine on the storage the last `build()` or `reopen()` used, with whatever that
    /// storage holds now: a restart. The engine returned by that call has been dropped first.
    async fn reopen(&self) -> Result<Arc<Self::Engine>, Error>;

    /// Opens an engine on a copy of the files the last `build()` or `reopen()` used, exactly as they are on disk
    /// right now, without closing or dropping the live engine: what a restart after a crash (the
    /// process killed, no `close()`, no `Drop`) would find. Return `None` if the builder cannot do
    /// this; the crash test is then skipped.
    /// [`copy_storage_dir`](crate::state_machine_test::copy_storage_dir) copies a directory.
    ///
    /// A copy of the files reads what the operating system holds in its page cache, so this
    /// finds data a store keeps only in the memory of the process (a write buffer that `flush()`
    /// forgot to write out, data written only at `Drop`). It cannot tell whether `fsync` happened,
    /// nor what a crash in the middle of a write leaves behind: only a fake disk that drops
    /// unsynced data and cuts writes short can (#473).
    async fn build_from_crash_image(&self) -> Result<Option<Arc<Self::Engine>>, Error> {
        Ok(None)
    }

    /// Clean up any resources after testing
    async fn cleanup(&self) -> Result<(), Error>;
}

impl StorageEngineTestSuite {
    /// Run all storage engine tests
    pub async fn run_all_tests<B: StorageEngineBuilder>(builder: B) -> Result<(), Error> {
        Self::test_empty_storage(builder.build().await?).await?;
        Self::test_single_entry_persistence(builder.build().await?).await?;
        Self::test_batch_persistence(builder.build().await?).await?;
        Self::test_purge_logs(builder.build().await?).await?;
        Self::test_truncation(builder.build().await?).await?;
        Self::test_truncate_past_end_keeps_last_index(builder.build().await?).await?;
        Self::test_truncate_then_persist_replaces_tail(builder.build().await?).await?;
        Self::test_edge_cases(builder.build().await?).await?;
        Self::test_purge_empty_range(builder.build().await?).await?;
        Self::test_hard_state_persistence(builder.build().await?).await?;
        Self::test_flush_persists_all_data(builder.build().await?).await?;

        // Durability tests (crash recovery)
        Self::test_persist_entries_durability(&builder).await?;
        Self::test_hard_state_crash_recovery(&builder).await?;
        Self::test_truncate_durability(&builder).await?;
        Self::test_purge_durability(&builder).await?;
        Self::test_flush_makes_log_and_hard_state_survive_a_crash(&builder).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Test empty storage behavior
    pub async fn test_empty_storage<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        assert_eq!(engine.last_index(), 0);
        assert!(engine.entry(1).await?.is_none());
        assert!(engine.get_entries(1..=5)?.is_empty());

        Ok(())
    }

    /// Test single entry persistence and retrieval
    pub async fn test_single_entry_persistence<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        let entries = create_test_entries(1..=1);

        // Persist and retrieve
        engine.persist_entries(entries.clone()).await?;
        assert_eq!(engine.last_index(), 1);
        assert_eq!(engine.entry(1).await?.unwrap(), entries[0]);
        assert_eq!(engine.get_entries(1..=1)?, entries);

        Ok(())
    }

    /// Test batch entry persistence and retrieval
    pub async fn test_batch_persistence<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        let entries = create_test_entries(1..=100);

        engine.persist_entries(entries.clone()).await?;

        // Verify all entries
        assert_eq!(engine.last_index(), 100);

        // Spot check random entries
        assert_eq!(engine.entry(1).await?.unwrap(), entries[0]);
        assert_eq!(engine.entry(50).await?.unwrap(), entries[49]);
        assert_eq!(engine.entry(100).await?.unwrap(), entries[99]);

        // Verify range query
        let range = engine.get_entries(25..=75)?;
        assert_eq!(range.len(), 51);
        assert_eq!(range[0], entries[24]);
        assert_eq!(range[50], entries[74]);

        Ok(())
    }

    /// Test log purging functionality
    pub async fn test_purge_logs<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        engine.persist_entries(create_test_entries(1..=100)).await?;

        // Purge first 50 entries
        engine
            .purge(LogId {
                index: 50,
                term: 50,
            })
            .await?;

        assert!(engine.entry(1).await?.is_none());
        assert!(engine.entry(50).await?.is_none());
        assert!(engine.entry(51).await?.is_some());

        Ok(())
    }

    /// Test log truncation functionality (in-memory consistency)
    pub async fn test_truncation<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        engine.persist_entries(create_test_entries(1..=100)).await?;

        // Truncate from index 76 onward
        engine.truncate(76).await?;

        assert_eq!(engine.last_index(), 75);
        assert!(engine.entry(76).await?.is_none());
        assert!(engine.entry(100).await?.is_none());
        assert!(engine.entry(75).await?.is_some());

        Ok(())
    }

    /// Truncating at or past the end of the log removes nothing, so `last_index` must not move.
    ///
    /// `last_index` must report the last entry actually stored. Setting it to `from_index - 1`
    /// would point it at an entry that does not exist (log 1..=10, truncate(20) -> 19).
    pub async fn test_truncate_past_end_keeps_last_index<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        engine.persist_entries(create_test_entries(1..=10)).await?;

        // Right after the last entry: nothing to remove.
        engine.truncate(11).await?;
        assert_eq!(engine.last_index(), 10);

        // Far past the last entry: still nothing to remove.
        engine.truncate(20).await?;
        assert_eq!(
            engine.last_index(),
            10,
            "truncate past the end must not move last_index beyond the stored entries"
        );
        assert!(engine.entry(10).await?.is_some(), "entry 10 must survive");
        assert_eq!(engine.get_entries(1..=10)?.len(), 10);

        Ok(())
    }

    /// `truncate` followed by `persist_entries` replaces the old tail from `from_index`.
    ///
    /// Raft requirement: on a term conflict a follower replaces its divergent suffix; nothing
    /// of the old suffix may survive past the new entries, and the prefix stays untouched.
    pub async fn test_truncate_then_persist_replaces_tail<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        engine.persist_entries(create_test_entries(1..=10)).await?;

        let mut replacement = create_test_entries(6..=7);
        for e in &mut replacement {
            e.term = 99;
        }
        engine.truncate(6).await?;
        engine.persist_entries(replacement).await?;

        assert_eq!(engine.last_index(), 7);
        assert_eq!(engine.entry(6).await?.map(|e| e.term), Some(99));
        assert_eq!(engine.entry(7).await?.map(|e| e.term), Some(99));
        assert!(engine.entry(8).await?.is_none(), "old tail must be gone");
        assert!(engine.entry(10).await?.is_none(), "old tail must be gone");
        assert_eq!(
            engine.entry(5).await?.map(|e| e.term),
            Some(5),
            "prefix is untouched"
        );
        Ok(())
    }

    /// Test edge cases and error conditions
    pub async fn test_edge_cases<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        // Empty persistence
        engine.persist_entries(vec![]).await?;

        // Out-of-range access
        assert!(engine.get_entries(100..=200)?.is_empty());

        Ok(())
    }

    /// Test purge with empty range (edge case)
    ///
    /// Verifies purge operations with index=0 or empty log don't panic
    /// and behave correctly as no-op operations.
    pub async fn test_purge_empty_range<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        // Purge on empty log - should not panic
        engine.purge(LogId { index: 0, term: 0 }).await?;
        assert_eq!(engine.last_index(), 0);

        // Write entries then purge index 0 (no-op)
        engine.persist_entries(create_test_entries(1..=10)).await?;
        engine.purge(LogId { index: 0, term: 0 }).await?;

        // Verify: no entries deleted
        assert!(
            engine.entry(1).await?.is_some(),
            "Entry 1 should still exist"
        );
        assert_eq!(engine.last_index(), 10);

        Ok(())
    }

    /// Test hard state persistence and retrieval
    pub async fn test_hard_state_persistence<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        let hard_state = create_test_hard_state(5, Some((10, 4)));

        // Save and verify
        engine.save_hard_state(&hard_state)?;
        let loaded = engine.load_hard_state()?.unwrap();
        assert_eq!(loaded.current_term, 5);

        Ok(())
    }

    /// Test that flush persists all data
    pub async fn test_flush_persists_all_data<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
    {
        // Write log entries and the hard state
        engine.persist_entries(create_test_entries(1..=5)).await?;
        engine.save_hard_state(&create_test_hard_state(2, Some((1, 2))))?;

        // One flush covers both
        engine.flush()?;

        Ok(())
    }

    /// Test persist_entries guarantees durability (crash recovery without explicit flush)
    ///
    /// Critical Raft requirement: Log entries MUST survive crashes.
    /// This test verifies that persist_entries() alone (without explicit flush)
    /// ensures durability by dropping the engine and reopening.
    pub async fn test_persist_entries_durability<B: StorageEngineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        // Step 1: Write entries and drop (simulate crash)
        {
            let engine = builder.build().await?;

            let entries = create_test_entries(1..=10);
            engine.persist_entries(entries).await?;
        }

        // Small delay for filesystem sync (especially macOS)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Reopen and verify all entries survived
        let engine2 = builder.reopen().await?;

        assert_eq!(
            engine2.last_index(),
            10,
            "BUG: persist_entries() must guarantee durability! \
             Raft protocol requires log entries to survive crashes. \
             Either: 1) Call sync/flush inside persist_entries, or \
             2) Use write-ahead log with sync=true"
        );

        // Verify all entries are readable
        assert!(
            engine2.entry(1).await?.is_some(),
            "Entry 1 should survive crash"
        );
        assert!(
            engine2.entry(5).await?.is_some(),
            "Entry 5 should survive crash"
        );
        assert!(
            engine2.entry(10).await?.is_some(),
            "Entry 10 should survive crash"
        );

        Ok(())
    }

    /// After `flush()` returns, the log entries and the hard state are in the files, not only in
    /// the memory of the process.
    ///
    /// A node answers a vote or an AppendEntries only after `flush()` returned. If the term, the
    /// vote or an acknowledged entry were still in a write buffer of the process and the node is
    /// killed, it restarts with an older term: it can vote twice in one term, or forget an entry
    /// it told the leader it had stored. The other durability tests close the engine before they
    /// reopen it, and closing writes out whatever was still in memory; here the node is killed
    /// instead, so only what `flush()` itself wrote is found.
    ///
    /// Skipped, with a message, when the builder does not implement
    /// [`StorageEngineBuilder::build_from_crash_image`]. It does not prove that the data
    /// survives a power loss; see that method.
    pub async fn test_flush_makes_log_and_hard_state_survive_a_crash<B: StorageEngineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        let engine = builder.build().await?;
        // The builder may hand out storage an earlier test wrote to: append after what is there.
        let last = engine.last_index() + 10;
        engine.persist_entries(create_test_entries(last - 9..=last)).await?;
        engine.save_hard_state(&create_test_hard_state(7, Some((3, 7))))?;
        engine.flush()?;

        // The node is killed here: no close(), no drop.
        let Some(restarted) = builder.build_from_crash_image().await? else {
            eprintln!("crash image not supported by this builder; crash test skipped");
            return Ok(());
        };
        let restarted_store = restarted.clone();

        assert_eq!(
            restarted_store.last_index(),
            last,
            "flush() must write the log entries out of the process: after a crash the log is shorter"
        );
        assert!(
            restarted_store.entry(last).await?.is_some(),
            "entry {last} was flushed and must be readable after a crash"
        );
        let hard_state = restarted_store
            .load_hard_state()?
            .expect("flush() must write the hard state out of the process");
        assert_eq!(hard_state.current_term, 7, "the term must survive a crash");
        assert_eq!(
            hard_state.voted_for.as_ref().map(|v| v.voted_for_id),
            Some(3),
            "the vote must survive a crash"
        );
        Ok(())
    }

    /// Test save_hard_state followed by flush() survives a restart
    ///
    /// Critical Raft requirement: HardState (current_term, voted_for) MUST survive crashes.
    /// This prevents voting for multiple candidates in same term. The node always calls `flush()`
    /// after `save_hard_state()` and before it answers, so that is the sequence checked here.
    pub async fn test_hard_state_crash_recovery<B: StorageEngineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        let expected_term = 42;
        let expected_node_id = 7;

        // Step 1: Save hard state and drop
        {
            let engine = builder.build().await?;

            let hard_state =
                create_test_hard_state(expected_term, Some((expected_node_id, expected_term)));
            engine.save_hard_state(&hard_state)?;
            engine.flush()?;
        }

        // Small delay for filesystem sync
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Reopen and verify
        let engine2 = builder.reopen().await?;

        let loaded = engine2.load_hard_state()?.unwrap();
        assert_eq!(
            loaded.current_term, expected_term,
            "BUG: save_hard_state() must guarantee durability! \
             Raft safety depends on HardState surviving crashes. \
             Without durability, nodes may vote for multiple candidates in same term."
        );

        assert!(loaded.voted_for.is_some());
        assert_eq!(
            loaded.voted_for.as_ref().unwrap().voted_for_id,
            expected_node_id
        );

        Ok(())
    }

    /// Test truncate survives crash without explicit flush
    ///
    /// Raft requirement: Log truncation (resolving conflicts) must be durable.
    pub async fn test_truncate_durability<B: StorageEngineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        // Step 1: Write entries, truncate, and drop
        {
            let engine = builder.build().await?;

            engine.persist_entries(create_test_entries(1..=100)).await?;
            engine.truncate(76).await?;
        }

        // Small delay for filesystem sync
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Reopen and verify truncation survived
        let engine2 = builder.reopen().await?;

        assert_eq!(
            engine2.last_index(),
            75,
            "BUG: truncate() must guarantee durability! \
             Raft log truncation (conflict resolution) must survive crashes."
        );
        assert!(
            engine2.entry(76).await?.is_none(),
            "Truncated entry 76 should not exist"
        );
        assert!(
            engine2.entry(100).await?.is_none(),
            "Truncated entry 100 should not exist"
        );
        assert!(
            engine2.entry(75).await?.is_some(),
            "Entry 75 should still exist"
        );

        Ok(())
    }

    /// Test purge survives crash without explicit flush
    ///
    /// Raft requirement: Log compaction (purge) must be durable.
    pub async fn test_purge_durability<B: StorageEngineBuilder>(builder: &B) -> Result<(), Error> {
        // Step 1: Write entries, purge, and drop
        {
            let engine = builder.build().await?;

            engine.persist_entries(create_test_entries(1..=100)).await?;
            engine
                .purge(LogId {
                    index: 50,
                    term: 50,
                })
                .await?;
        }

        // Small delay for filesystem sync
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Reopen and verify purge survived
        let engine2 = builder.reopen().await?;

        assert!(
            engine2.entry(1).await?.is_none(),
            "BUG: purge() must guarantee durability! \
             Purged entries should not reappear after crash."
        );
        assert!(
            engine2.entry(50).await?.is_none(),
            "Purged entry 50 should not exist"
        );
        assert!(
            engine2.entry(51).await?.is_some(),
            "Entry 51 should still exist after purge"
        );

        Ok(())
    }
}

/// Helper function to create test entries
fn create_test_entries(range: RangeInclusive<u64>) -> Vec<Entry> {
    range
        .map(|i| Entry {
            index: i,
            term: i,
            payload: Some(create_test_command_payload(i)),
        })
        .collect()
}

/// Helper function to create test command payload
fn create_test_command_payload(index: u64) -> d_engine_proto::common::EntryPayload {
    // Create a simple insert command
    let key = Bytes::from(format!("key_{index}"));
    let value = Bytes::from(format!("value_{index}"));

    let insert = Insert { key, value };
    let operation = d_engine_proto::client::write_command::Operation::Insert(insert);
    let write_cmd = d_engine_proto::client::WriteCommand {
        operation: Some(operation),
    };
    let mut buf = Vec::new();
    write_cmd.encode(&mut buf).expect("Failed to encode WriteCommand");
    let cmd_bytes = Bytes::from(buf); // convert Vec<u8> to Bytes
    d_engine_proto::common::EntryPayload {
        payload: Some(d_engine_proto::common::entry_payload::Payload::Command(
            cmd_bytes,
        )),
    }
}

/// Helper function to create test hard state
fn create_test_hard_state(
    current_term: u64,
    voted_for: Option<(u32, u64)>,
) -> HardState {
    let voted_for = voted_for.map(|(id, term)| VotedFor {
        voted_for_id: id,
        voted_for_term: term,
        committed: false,
    });

    HardState {
        current_term,
        voted_for,
    }
}
