use super::*;
use bytes::{Bytes, BytesMut};
use d_engine::client::{
    WriteCommand,
    write_command::{Insert, Operation},
};
use d_engine::common::{Entry, EntryPayload, entry_payload::Payload};
use d_engine::storage_engine_test::EntryAt;
use d_engine::{Result, StorageEngine};
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

    engine.persist_entries(entries).await?;

    // Verify entries can be retrieved
    let retrieved = engine.entry_at(1)?;
    assert!(retrieved.is_some(), "Entry 1 should exist");

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_sled_performance() -> Result<()> {
    let builder = SledStorageEngineBuilder::new();
    let engine = builder.build().await?;

    // Performance test: persist 10,000 entries
    let start = std::time::Instant::now();
    let entries = (1..=10000)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: Some(create_test_command_payload(i)),
        })
        .collect();

    engine.persist_entries(entries).await?;
    let duration = start.elapsed();

    debug!("Persisted 10,000 entries in {duration:?}");
    assert!(
        duration.as_millis() < 5000,
        "Should persist 10k entries in <5s"
    );

    Ok(())
}

/// Truncating a conflicting tail and persisting the new entries replaces the tail.
///
/// Verifies the three invariants:
///   1. Entries before from_index are untouched
///   2. Old entries at from_index and beyond are gone
///   3. New entries replace them, last_index reflects new tail
#[tokio::test]
#[traced_test]
async fn test_truncate_then_persist_replaces_stale_tail() -> Result<()> {
    let builder = SledStorageEngineBuilder::new();
    let engine = builder.build().await?;

    // Arrange: [1(t1)..5(t1)]
    engine
        .persist_entries(
            (1..=5)
                .map(|i| Entry {
                    index: i,
                    term: 1,
                    payload: None,
                })
                .collect(),
        )
        .await?;

    // Act: conflict at index=3, new suffix [3(t2), 4(t2)] — entry 5 must vanish
    engine.truncate(3).await?;
    engine
        .persist_entries(vec![
            Entry {
                index: 3,
                term: 2,
                payload: None,
            },
            Entry {
                index: 4,
                term: 2,
                payload: None,
            },
        ])
        .await?;

    assert_eq!(engine.entry_at(1)?.unwrap().term, 1);
    assert_eq!(engine.entry_at(2)?.unwrap().term, 1);
    assert_eq!(engine.entry_at(3)?.unwrap().term, 2);
    assert_eq!(engine.entry_at(4)?.unwrap().term, 2);
    assert!(engine.entry_at(5)?.is_none(), "entry 5 must be removed");
    assert_eq!(engine.last_index(), 4);
    Ok(())
}

/// `purge` writes the boundary before it removes the entries. A crash between the two leaves the
/// boundary on disk with the purged entries still in the log. Reopening must drop those entries
/// and keep the boundary: they are already purged, and the term of the last purged entry must
/// survive the restart.
#[tokio::test]
#[traced_test]
async fn test_reopen_after_crash_between_boundary_and_removal_drops_purged_entries() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sled");
    {
        let engine = SledStorageEngine::new(&path, 1)?;
        engine
            .persist_entries(
                (1..=10)
                    .map(|i| Entry { index: i, term: 1, payload: None })
                    .collect(),
            )
            .await?;
        engine.flush()?;
    }

    // The state right after purge(5) wrote its boundary and before it removed any entry.
    let boundary = d_engine::common::LogId { index: 5, term: 1 };
    {
        let (_log_tree, meta_tree) = init_sled_log_tree_and_meta_tree(&path, 1).unwrap();
        meta_tree.insert(b"purge_boundary", boundary.encode_to_vec()).unwrap();
        meta_tree.flush().unwrap();
    }

    let engine = SledStorageEngine::new(&path, 1)?;
    assert_eq!(engine.load_purge_boundary()?, Some(boundary));
    assert!(
        engine.entry_at(5)?.is_none(),
        "an entry at the purge boundary is already purged"
    );
    assert!(engine.entry_at(1)?.is_none(), "entries below the boundary are already purged");
    assert!(engine.entry_at(6)?.is_some(), "entries after the boundary stay");
    assert_eq!(engine.last_index(), 10);
    Ok(())
}

/// Helper function to create a test command payload
fn create_test_command_payload(index: u64) -> EntryPayload {
    let key = Bytes::from(format!("key_{index}").into_bytes());
    let value = Bytes::from(format!("value_{index}").into_bytes());

    let insert = Insert { key, value };
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

/// After a restart the engine needs the term of the last purged entry, to answer an
/// AppendEntries whose prev_log_index is that entry. `purge(cutoff)` must record `cutoff`, and
/// it must still be there after the store is reopened.
#[tokio::test]
#[traced_test]
async fn test_purge_boundary_survives_reopen() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sled");
    let boundary = d_engine::common::LogId { index: 5, term: 1 };
    {
        let engine = SledStorageEngine::new(&path, 1)?;
        assert_eq!(engine.load_purge_boundary()?, None);
        engine
            .persist_entries(
                (1..=10)
                    .map(|i| Entry {
                        index: i,
                        term: 1,
                        payload: None,
                    })
                    .collect(),
            )
            .await?;
        engine.purge(boundary).await?;
        assert_eq!(engine.load_purge_boundary()?, Some(boundary));
        engine.flush()?;
    }

    let engine = SledStorageEngine::new(&path, 1)?;
    assert_eq!(
        engine.load_purge_boundary()?,
        Some(boundary),
        "the boundary must survive a reopen"
    );
    assert!(engine.entry_at(5)?.is_none(), "purged entries stay gone");
    assert!(
        engine.entry_at(6)?.is_some(),
        "entries after the boundary stay"
    );
    Ok(())
}
