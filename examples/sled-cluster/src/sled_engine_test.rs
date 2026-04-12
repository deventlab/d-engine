use super::*;
use bytes::{Bytes, BytesMut};
use d_engine::{LogStore, Result, StorageEngine};
use d_engine_proto::client::{
    WriteCommand,
    write_command::{Insert, Operation},
};
use d_engine_proto::common::{Entry, EntryPayload, entry_payload::Payload};
use prost::Message;
use std::sync::Arc;
use tempfile::TempDir;
use tracing::debug;
use tracing_test::traced_test;
use uuid::Uuid;

/// Helper for building temporary sled storage engines for testing
pub struct SledStorageEngineBuilder {
    temp_dir: TempDir,
}

impl SledStorageEngineBuilder {
    /// Creates a new builder with a temporary directory
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        Self { temp_dir }
    }

    /// Builds a new storage engine instance
    pub async fn build(&self) -> Result<Arc<SledStorageEngine>> {
        let unique_path = format!("sled_{}", Uuid::new_v4());
        let path = self.temp_dir.path().join(unique_path);
        let engine = SledStorageEngine::new(path, 1)?;
        Ok(Arc::new(engine))
    }
}

#[tokio::test]
#[traced_test]
async fn test_sled_storage_engine_basic() -> Result<()> {
    let builder = SledStorageEngineBuilder::new();
    let engine = builder.build().await?;
    let log_store = engine.log_store();

    // Basic test: persist a few entries
    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: Some(create_test_command_payload(1)),
        },
        Entry {
            index: 2,
            term: 1,
            payload: Some(create_test_command_payload(2)),
        },
    ];

    log_store.persist_entries(entries).await?;

    // Verify entries can be retrieved
    let retrieved = log_store.entry(1).await?;
    assert!(retrieved.is_some(), "Entry 1 should exist");

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_sled_performance() -> Result<()> {
    let builder = SledStorageEngineBuilder::new();
    let engine = builder.build().await?;
    let log_store = engine.log_store();

    // Performance test: persist 10,000 entries
    let start = std::time::Instant::now();
    let entries = (1..=10000)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: Some(create_test_command_payload(i)),
        })
        .collect();

    log_store.persist_entries(entries).await?;
    let duration = start.elapsed();

    debug!("Persisted 10,000 entries in {duration:?}");
    assert!(
        duration.as_millis() < 5000,
        "Should persist 10k entries in <5s"
    );

    Ok(())
}

/// replace_range removes stale tail and persists new entries in one atomic batch.
///
/// Verifies the three invariants:
///   1. Entries before from_index are untouched
///   2. Old entries at from_index and beyond are gone
///   3. New entries replace them, last_index reflects new tail
#[tokio::test]
#[traced_test]
async fn test_replace_range_removes_stale_tail_and_persists_new_entries() -> Result<()> {
    let builder = SledStorageEngineBuilder::new();
    let engine = builder.build().await?;
    let log_store = engine.log_store();

    // Arrange: [1(t1)..5(t1)]
    log_store
        .persist_entries(
            (1..=5)
                .map(|i| Entry { index: i, term: 1, payload: None })
                .collect(),
        )
        .await?;

    // Act: conflict at index=3, new suffix [3(t2), 4(t2)] — entry 5 must vanish
    log_store
        .replace_range(
            3,
            vec![
                Entry { index: 3, term: 2, payload: None },
                Entry { index: 4, term: 2, payload: None },
            ],
        )
        .await?;

    assert_eq!(log_store.entry(1).await?.unwrap().term, 1);
    assert_eq!(log_store.entry(2).await?.unwrap().term, 1);
    assert_eq!(log_store.entry(3).await?.unwrap().term, 2);
    assert_eq!(log_store.entry(4).await?.unwrap().term, 2);
    assert!(log_store.entry(5).await?.is_none(), "entry 5 must be removed");
    assert_eq!(log_store.last_index(), 4);
    Ok(())
}

/// replace_range with empty new_entries truncates only.
#[tokio::test]
#[traced_test]
async fn test_replace_range_with_empty_entries_truncates_only() -> Result<()> {
    let builder = SledStorageEngineBuilder::new();
    let engine = builder.build().await?;
    let log_store = engine.log_store();

    log_store
        .persist_entries(
            (1..=5)
                .map(|i| Entry { index: i, term: 1, payload: None })
                .collect(),
        )
        .await?;

    log_store.replace_range(3, vec![]).await?;

    assert_eq!(log_store.entry(2).await?.unwrap().term, 1);
    assert!(log_store.entry(3).await?.is_none());
    assert_eq!(log_store.last_index(), 2);
    Ok(())
}

/// Helper function to create a test command payload
fn create_test_command_payload(index: u64) -> EntryPayload {
    let key = Bytes::from(format!("key_{index}").into_bytes());
    let value = Bytes::from(format!("value_{index}").into_bytes());

    let insert = Insert {
        key,
        value,
        ttl_secs: 0,
    };
    let operation = Operation::Insert(insert);
    let write_cmd = WriteCommand {
        operation: Some(operation),
    };

    let mut buffer = BytesMut::new();
    write_cmd.encode(&mut buffer).expect("Failed to encode insert command");
    let buffer = buffer.freeze();

    EntryPayload {
        payload: Some(Payload::Command(buffer)),
    }
}
