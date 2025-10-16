use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tempfile::TempDir;
use tonic::async_trait;
use tracing::debug;
use tracing_test::traced_test;
use uuid::Uuid;

use super::*;
use crate::proto::client::write_command::Insert;
use crate::proto::common::Entry;
use crate::storage::storage_engine_test::StorageEngineBuilder;
use crate::storage::storage_engine_test::StorageEngineTestSuite;
use crate::Error;
use crate::LogStore;
use crate::StorageEngine;

pub struct RocksDBStorageEngineBuilder {
    temp_dir: TempDir,
    instance_id: String,
}

impl RocksDBStorageEngineBuilder {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let instance_id = Uuid::new_v4().to_string();
        Self {
            temp_dir,
            instance_id,
        }
    }
}

#[async_trait]
impl StorageEngineBuilder for RocksDBStorageEngineBuilder {
    type Engine = RocksDBStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        let path = self.temp_dir.path().join(format!("rocksdb-{}", self.instance_id));
        let engine = RocksDBStorageEngine::new(path)?;

        // Ensure the engine is fully initialized before returning
        engine.log_store().flush_async().await?;
        Ok(Arc::new(engine))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // Increase delay for CI environments to ensure all operations complete
        let delay = if std::env::var("CI").is_ok() {
            std::time::Duration::from_millis(500)
        } else {
            std::time::Duration::from_millis(100)
        };
        tokio::time::sleep(delay).await;

        // TempDir will be cleaned up automatically when dropped
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

fn create_test_command_payload(index: u64) -> crate::proto::common::EntryPayload {
    // Create a simple insert command
    let key = Bytes::from(format!("key_{index}").into_bytes());
    let value = Bytes::from(format!("value_{index}").into_bytes());

    let insert = Insert { key, value };
    let operation = crate::proto::client::write_command::Operation::Insert(insert);
    let write_cmd = crate::proto::client::WriteCommand {
        operation: Some(operation),
    };

    crate::proto::common::EntryPayload {
        payload: Some(crate::proto::common::entry_payload::Payload::Command(
            write_cmd.encode_to_vec().into(),
        )),
    }
}
