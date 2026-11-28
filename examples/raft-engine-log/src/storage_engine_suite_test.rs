//! Runs d-engine's shared `StorageEngine` contract suite against the raft-engine
//! adapter, so it is held to the same durability and ordering rules as the
//! built-in engines.

use std::sync::Arc;

use async_trait::async_trait;
use d_engine::Error;
use d_engine::common::Entry;
use d_engine::storage_engine_test::{StorageEngineBuilder, StorageEngineTestSuite};
use d_engine::StorageEngine;
use tempfile::TempDir;

use crate::raft_engine_storage::RaftEngineStorage;

/// Every `build()` opens a new directory; `reopen()` opens the last one again, which is how the
/// suite simulates a restart: the previous engine is dropped before it reopens.
struct RaftEngineStorageBuilder {
    dir: TempDir,
    builds: std::sync::atomic::AtomicUsize,
}

impl RaftEngineStorageBuilder {
    fn new() -> Self {
        Self {
            dir: TempDir::new().unwrap(),
            builds: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn current_dir(&self) -> std::path::PathBuf {
        let n = self.builds.load(std::sync::atomic::Ordering::SeqCst);
        self.dir.path().join(format!("log-{n}"))
    }
}

#[async_trait]
impl StorageEngineBuilder for RaftEngineStorageBuilder {
    type Engine = RaftEngineStorage;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        self.builds.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.reopen().await
    }

    async fn reopen(&self) -> Result<Arc<Self::Engine>, Error> {
        Ok(Arc::new(RaftEngineStorage::open(self.current_dir())?))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// The adapter must satisfy the whole shared contract, including durability
/// across a reopen without an explicit flush.
#[tokio::test]
async fn test_raft_engine_storage_engine_passes_contract_suite() -> Result<(), Error> {
    StorageEngineTestSuite::run_all_tests(RaftEngineStorageBuilder::new()).await
}

/// Persisting 10,000 entries in one call must stay within the throughput the
/// storage guide promises for a storage engine.
#[tokio::test]
async fn test_raft_engine_persist_10k_entries_within_one_second() -> Result<(), Error> {
    let builder = RaftEngineStorageBuilder::new();
    let engine = builder.build().await?;
    let entries: Vec<Entry> = (1..=10_000)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();

    let start = std::time::Instant::now();
    engine.persist_entries(entries).await?;

    assert!(
        start.elapsed().as_millis() < 1000,
        "persisting 10k entries took {:?}",
        start.elapsed()
    );
    Ok(())
}
