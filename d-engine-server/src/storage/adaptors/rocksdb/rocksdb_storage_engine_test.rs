use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use d_engine_core::Error;
use d_engine_core::LogStore;
use d_engine_core::StorageEngine;
use d_engine_core::storage_engine_test::StorageEngineBuilder;
use d_engine_core::storage_engine_test::StorageEngineTestSuite;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::common::Entry;
use prost::Message;
use tempfile::TempDir;
use tonic::async_trait;
use tracing::debug;
use tracing_test::traced_test;
use uuid::Uuid;

use super::*;

pub struct RocksDBStorageEngineBuilder {
    temp_dir: TempDir,
    instance_id: String,
    // Partner SM kept alive alongside storage; must be dropped before the next build().
    sm: Mutex<Option<RocksDBStateMachine>>,
}

impl RocksDBStorageEngineBuilder {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let instance_id = Uuid::new_v4().to_string();
        Self {
            temp_dir,
            instance_id,
            sm: Mutex::new(None),
        }
    }
}

#[async_trait]
impl StorageEngineBuilder for RocksDBStorageEngineBuilder {
    type Engine = RocksDBStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        // Drop old SM so Arc<DB> refcount falls to 0 and RocksDB releases the lock.
        {
            *self.sm.lock().unwrap() = None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let path = self.temp_dir.path().join(format!("rocksdb-{}", self.instance_id));
        let (engine, sm) = RocksDBUnifiedEngine::open(&path)?;
        *self.sm.lock().unwrap() = Some(sm);

        // Ensure the engine is fully initialized before returning
        engine.log_store().flush_async().await?;
        Ok(Arc::new(engine))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        *self.sm.lock().unwrap() = None;
        let delay = if std::env::var("CI").is_ok() {
            std::time::Duration::from_millis(500)
        } else {
            std::time::Duration::from_millis(100)
        };
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

#[tokio::test]
#[traced_test]
async fn test_rocksdb_storage_engine() -> Result<(), Error> {
    let builder = RocksDBStorageEngineBuilder::new();
    StorageEngineTestSuite::run_all_tests(builder).await
}

#[tokio::test]
#[traced_test]
async fn test_rocksdb_performance() -> Result<(), Error> {
    let builder = RocksDBStorageEngineBuilder::new();
    let engine = builder.build().await?;
    let log_store = engine.log_store();

    // Performance test: persist 10,000 entries
    let start = std::time::Instant::now();
    let entries = (1..=10000)
        .map(|i| Entry {
            index: i,
            term: i,
            payload: Some(create_test_command_payload(i)),
        })
        .collect();

    log_store.persist_entries(entries).await?;
    let duration = start.elapsed();

    debug!("Persisted 10,000 entries in {duration:?}");
    assert!(
        duration.as_millis() < 1000,
        "Should persist 10k entries in <1s"
    );

    builder.cleanup().await?;
    Ok(())
}

fn make_entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: None,
    }
}

async fn build_engine() -> (Arc<RocksDBStorageEngine>, tempfile::TempDir) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("rocksdb");
    let (engine, _sm) = RocksDBUnifiedEngine::open(&path).unwrap();
    (Arc::new(engine), temp_dir)
}

/// replace_range removes stale tail and persists new entries atomically.
///
/// Verifies the three invariants of replace_range:
///   1. Entries before from_index are untouched
///   2. Old entries at from_index and beyond are gone
///   3. New entries replace them correctly, last_index reflects new tail
#[tokio::test]
#[traced_test]
async fn test_replace_range_removes_stale_tail_and_persists_new_entries() {
    let (engine, _dir) = build_engine().await;
    let log_store = engine.log_store();

    // Arrange: [1(t1), 2(t1), 3(t1), 4(t1), 5(t1)]
    log_store
        .persist_entries((1..=5).map(|i| make_entry(i, 1)).collect())
        .await
        .unwrap();

    // Act: conflict at index=3, new suffix [3(t2), 4(t2)] — entry 5 must vanish
    log_store
        .replace_range(3, vec![make_entry(3, 2), make_entry(4, 2)])
        .await
        .unwrap();

    // Entries before conflict are unchanged
    assert_eq!(log_store.entry(1).await.unwrap().unwrap().term, 1);
    assert_eq!(log_store.entry(2).await.unwrap().unwrap().term, 1);
    // New entries present with correct term
    assert_eq!(log_store.entry(3).await.unwrap().unwrap().term, 2);
    assert_eq!(log_store.entry(4).await.unwrap().unwrap().term, 2);
    // Stale tail removed
    assert!(
        log_store.entry(5).await.unwrap().is_none(),
        "entry 5 must be removed"
    );
    assert_eq!(log_store.last_index(), 4);
}

/// replace_range with empty new_entries truncates only — no new entries written.
///
/// Edge case: leader sends truncate-only (no replacement entries),
/// e.g. when follower log is longer than leader's.
#[tokio::test]
#[traced_test]
async fn test_replace_range_with_empty_entries_truncates_only() {
    let (engine, _dir) = build_engine().await;
    let log_store = engine.log_store();

    log_store
        .persist_entries((1..=5).map(|i| make_entry(i, 1)).collect())
        .await
        .unwrap();

    log_store.replace_range(3, vec![]).await.unwrap();

    assert_eq!(log_store.entry(2).await.unwrap().unwrap().term, 1);
    assert!(log_store.entry(3).await.unwrap().is_none());
    assert_eq!(log_store.last_index(), 2);
}

fn create_test_command_payload(index: u64) -> d_engine_proto::common::EntryPayload {
    // Create a simple insert command
    let key = Bytes::from(format!("key_{index}").into_bytes());
    let value = Bytes::from(format!("value_{index}").into_bytes());

    let insert = Insert {
        key,
        value,
        ttl_secs: 0,
    };
    let operation = d_engine_proto::client::write_command::Operation::Insert(insert);
    let write_cmd = d_engine_proto::client::WriteCommand {
        operation: Some(operation),
    };

    d_engine_proto::common::EntryPayload {
        payload: Some(d_engine_proto::common::entry_payload::Payload::Command(
            write_cmd.encode_to_vec().into(),
        )),
    }
}
