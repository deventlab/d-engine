use std::ops::RangeInclusive;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tonic::async_trait;

use crate::Error;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::storage::StorageEngine;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VotedFor;

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

    /// Create a new StorageEngine instance for testing
    async fn build(&self) -> Result<Arc<Self::Engine>, Error>;

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
        Self::test_reset_operation(builder.build().await?).await?;
        Self::test_edge_cases(builder.build().await?).await?;
        Self::test_hard_state_persistence(builder.build().await?).await?;
        Self::test_reset_preserves_meta(builder.build().await?).await?;
        Self::test_flush_persists_all_data(builder.build().await?).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Test empty storage behavior
    async fn test_empty_storage<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();

        assert_eq!(log_store.last_index(), 0);
        assert!(log_store.entry(1).await?.is_none());
        assert!(log_store.get_entries(1..=5)?.is_empty());

        Ok(())
    }

    /// Test single entry persistence and retrieval
    async fn test_single_entry_persistence<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        let entries = create_test_entries(1..=1);

        // Persist and retrieve
        log_store.persist_entries(entries.clone()).await?;
        assert_eq!(log_store.last_index(), 1);
        assert_eq!(log_store.entry(1).await?.unwrap(), entries[0]);
        assert_eq!(log_store.get_entries(1..=1)?, entries);

        Ok(())
    }

    /// Test batch entry persistence and retrieval
    async fn test_batch_persistence<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        let entries = create_test_entries(1..=100);

        log_store.persist_entries(entries.clone()).await?;

        // Verify all entries
        assert_eq!(log_store.last_index(), 100);

        // Spot check random entries
        assert_eq!(log_store.entry(1).await?.unwrap(), entries[0]);
        assert_eq!(log_store.entry(50).await?.unwrap(), entries[49]);
        assert_eq!(log_store.entry(100).await?.unwrap(), entries[99]);

        // Verify range query
        let range = log_store.get_entries(25..=75)?;
        assert_eq!(range.len(), 51);
        assert_eq!(range[0], entries[24]);
        assert_eq!(range[50], entries[74]);

        Ok(())
    }

    /// Test log purging functionality
    async fn test_purge_logs<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        log_store.persist_entries(create_test_entries(1..=100)).await?;

        // Purge first 50 entries
        log_store
            .purge(LogId {
                index: 50,
                term: 50,
            })
            .await?;

        assert!(log_store.entry(1).await?.is_none());
        assert!(log_store.entry(50).await?.is_none());
        assert!(log_store.entry(51).await?.is_some());

        Ok(())
    }

    /// Test log truncation functionality
    async fn test_truncation<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        log_store.reset().await?;
        log_store.persist_entries(create_test_entries(1..=100)).await?;
        // Ensure all operations are flushed to disk before truncation
        log_store.flush()?;

        // Truncate from index 76 onward
        log_store.truncate(76).await?;
        // Ensure truncation is persisted
        log_store.flush()?;

        assert_eq!(log_store.last_index(), 75);
        assert!(log_store.entry(76).await?.is_none());
        assert!(log_store.entry(100).await?.is_none());
        assert!(log_store.entry(75).await?.is_some());

        Ok(())
    }

    /// Test storage reset functionality
    async fn test_reset_operation<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        log_store.persist_entries(create_test_entries(1..=50)).await?;

        log_store.reset().await?;

        assert_eq!(log_store.last_index(), 0);
        assert!(log_store.entry(1).await?.is_none());

        Ok(())
    }

    /// Test edge cases and error conditions
    async fn test_edge_cases<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();

        // Empty persistence
        log_store.persist_entries(vec![]).await?;

        // Out-of-range access
        assert!(log_store.get_entries(100..=200)?.is_empty());

        Ok(())
    }

    /// Test hard state persistence and retrieval
    async fn test_hard_state_persistence<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let meta_store = engine.meta_store();
        let hard_state = create_test_hard_state(5, Some((10, 4)));

        // Save and verify
        meta_store.save_hard_state(&hard_state)?;
        let loaded = meta_store.load_hard_state()?.unwrap();
        assert_eq!(loaded.current_term, 5);

        Ok(())
    }

    /// Test that reset preserves metadata
    async fn test_reset_preserves_meta<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        let meta_store = engine.meta_store();
        let hard_state = create_test_hard_state(3, Some((5, 4)));

        meta_store.save_hard_state(&hard_state)?;

        // Reset should clear logs but keep meta
        log_store.reset().await?;

        let loaded = meta_store.load_hard_state()?;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().current_term, 3);

        Ok(())
    }

    /// Test that flush persists all data
    async fn test_flush_persists_all_data<E>(engine: Arc<E>) -> Result<(), Error>
    where
        E: StorageEngine + Send + Sync + 'static,
        E::LogStore: Send + Sync,
        E::MetaStore: Send + Sync,
    {
        let log_store = engine.log_store();
        let meta_store = engine.meta_store();

        // Write to both stores
        log_store.persist_entries(create_test_entries(1..=5)).await?;
        meta_store.save_hard_state(&create_test_hard_state(2, Some((1, 2))))?;

        // Flush both stores
        log_store.flush()?;
        meta_store.flush()?;

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

    let insert = Insert {
        key,
        value,
        ttl_secs: None,
    };
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
    });

    HardState {
        current_term,
        voted_for,
    }
}
