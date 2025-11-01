use super::*;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use d_engine_server::StorageEngine;
use prost::Message;
use std::sync::Arc;
use tempfile::TempDir;
use tracing::debug;
use tracing_test::traced_test;
use uuid::Uuid;

pub struct SledStorageEngineBuilder {
    temp_dir: TempDir,
}

impl SledStorageEngineBuilder {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        Self { temp_dir }
    }
}

#[async_trait]
impl StorageEngineBuilder for SledStorageEngineBuilder {
    type Engine = SledStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        let unique_path = format!("sled_{}", Uuid::new_v4());
        let path = self.temp_dir.path().join(unique_path);

        let engine = SledStorageEngine::new(path, 1)?;
        Ok(Arc::new(engine))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // TempDir will be cleaned up automatically when dropped
        Ok(())
    }
}

#[tokio::test]
#[traced_test]
async fn test_sled_storage_engine() -> Result<(), Error> {
    let builder = SledStorageEngineBuilder::new();
    StorageEngineTestSuite::run_all_tests(builder).await
}

#[tokio::test]
#[traced_test]
async fn test_sled_performance() -> Result<(), Error> {
    let builder = SledStorageEngineBuilder::new();
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

fn create_test_command_payload(index: u64) -> common::EntryPayload {
    // Create a simple insert command
    let key = Bytes::from(format!("key_{index}").into_bytes());
    let value = Bytes::from(format!("value_{index}").into_bytes());

    let insert = Insert { key, value };
    let operation = client::write_command::Operation::Insert(insert);
    let write_cmd = client::WriteCommand {
        operation: Some(operation),
    };

    let mut buffer = BytesMut::new();
    write_cmd.encode(&mut buffer).expect("Failed to encode insert command");
    let buffer = buffer.freeze();
    common::EntryPayload {
        payload: Some(common::entry_payload::Payload::Command(buffer)),
    }
}
