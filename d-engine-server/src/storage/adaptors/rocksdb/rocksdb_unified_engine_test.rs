use std::sync::Arc;
use std::sync::Mutex;

use d_engine_core::Error;
use d_engine_core::Lease;
use d_engine_core::LogStore;
use d_engine_core::StorageEngine;
use d_engine_core::config::LeaseConfig;
use d_engine_core::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use d_engine_core::storage_engine_test::{StorageEngineBuilder, StorageEngineTestSuite};
use tempfile::TempDir;
use tonic::async_trait;

use super::RocksDBStateMachine;
use super::RocksDBStorageEngine;
use super::RocksDBUnifiedEngine;
use crate::StateMachine;
use crate::storage::DefaultLease;

// Both test suites call build() twice within a single persistence test (write data → drop →
// build() again → verify data survived). Both builders therefore:
//   1. Use a fixed path (`self.temp_dir`) so data persists across build() calls.
//   2. Drop the "partner" handle inside build() before reopening, to release the shared
//      Arc<DB> and allow RocksDB to close before the next open.

// ── Storage engine suite via unified open ────────────────────────────────────

struct UnifiedStorageBuilder {
    temp_dir: TempDir,
    // Partner SM kept alive alongside storage; must be dropped before the next build().
    sm: Mutex<Option<RocksDBStateMachine>>,
}

impl UnifiedStorageBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("temp dir"),
            sm: Mutex::new(None),
        }
    }
}

#[async_trait]
impl StorageEngineBuilder for UnifiedStorageBuilder {
    type Engine = RocksDBStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        // Drop old SM so Arc<DB> refcount falls to 0 and RocksDB releases the lock.
        {
            *self.sm.lock().unwrap() = None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (storage, sm) = RocksDBUnifiedEngine::open(self.temp_dir.path())?;
        *self.sm.lock().unwrap() = Some(sm);
        storage.log_store().flush_async().await?;
        Ok(Arc::new(storage))
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

/// Storage engine opened via `RocksDBUnifiedEngine` passes the full storage engine test suite.
#[tokio::test]
async fn test_unified_storage_engine_suite() {
    let builder = UnifiedStorageBuilder::new();
    StorageEngineTestSuite::run_all_tests(builder)
        .await
        .expect("unified storage engine should pass all tests");
}

// ── State machine suite via unified open ─────────────────────────────────────

struct UnifiedStateMachineBuilder {
    temp_dir: TempDir,
    // Partner storage kept alive alongside SM; must be dropped before the next build().
    storage: Mutex<Option<RocksDBStorageEngine>>,
}

impl UnifiedStateMachineBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("temp dir"),
            storage: Mutex::new(None),
        }
    }
}

#[async_trait]
impl StateMachineBuilder for UnifiedStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        // Drop old storage so Arc<DB> refcount falls to 0 and RocksDB releases the lock.
        {
            *self.storage.lock().unwrap() = None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (storage, sm) = RocksDBUnifiedEngine::open(self.temp_dir.path())?;
        *self.storage.lock().unwrap() = Some(storage);
        Ok(Arc::new(sm))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        *self.storage.lock().unwrap() = None;
        let delay = if std::env::var("CI").is_ok() {
            std::time::Duration::from_millis(500)
        } else {
            std::time::Duration::from_millis(100)
        };
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

/// State machine opened via `RocksDBUnifiedEngine` passes the full state machine test suite.
#[tokio::test]
async fn test_unified_state_machine_suite() {
    let builder = UnifiedStateMachineBuilder::new();
    StateMachineTestSuite::run_all_tests(builder)
        .await
        .expect("unified state machine should pass all tests");
}

// ── Lifecycle tests ───────────────────────────────────────────────────────────

/// open() on a fresh path succeeds and creates the DB.
#[test]
fn test_open_creates_db_at_fresh_path() {
    let dir = TempDir::new().expect("temp dir");
    let result = RocksDBUnifiedEngine::open(dir.path());
    assert!(result.is_ok(), "open() must succeed on a fresh path");
}

/// After all handles are dropped, re-opening the same path succeeds (RocksDB lock released).
#[test]
fn test_reopen_after_drop_succeeds() {
    let dir = TempDir::new().expect("temp dir");
    {
        let _handles = RocksDBUnifiedEngine::open(dir.path()).expect("first open");
    } // both storage and sm dropped; Arc<DB> refcount → 0

    let result = RocksDBUnifiedEngine::open(dir.path());
    assert!(
        result.is_ok(),
        "reopen after all handles dropped must succeed"
    );
}

/// Opening the same path twice while the first instance is alive must fail.
#[test]
fn test_concurrent_open_same_path_fails() {
    let dir = TempDir::new().expect("temp dir");
    let _first = RocksDBUnifiedEngine::open(dir.path()).expect("first open");
    let second = RocksDBUnifiedEngine::open(dir.path());
    assert!(
        second.is_err(),
        "second open on a live DB must fail with lock error"
    );
}

// ── Lease round-trip tests ────────────────────────────────────────────────────

fn make_lease() -> Arc<DefaultLease> {
    Arc::new(DefaultLease::new(LeaseConfig {
        enabled: true,
        interval_ms: 1000,
        max_cleanup_duration_ms: 10,
    }))
}

/// load_lease_data() is a no-op when no lease is configured (lease = None).
#[tokio::test]
async fn test_load_lease_data_without_lease_is_noop() {
    let dir = TempDir::new().expect("temp dir");
    let (_storage, sm) = RocksDBUnifiedEngine::open(dir.path()).expect("open");

    // SM was opened without a lease — load_lease_data must succeed silently.
    let result = sm.load_lease_data().await;
    assert!(
        result.is_ok(),
        "load_lease_data with no lease must not fail"
    );
}

/// Registering a lease key, stopping the SM (which persists TTL state), then
/// reloading from the same DB must restore the lease entry.
#[tokio::test]
async fn test_persist_and_reload_lease_round_trip() {
    let dir = TempDir::new().expect("temp dir");
    let lease = make_lease();

    // --- Phase 1: write and persist ---
    {
        let (_storage, mut sm) = RocksDBUnifiedEngine::open(dir.path()).expect("open");
        sm.set_lease(Arc::clone(&lease));
        // Register a key with a long TTL so it survives the reload.
        lease.register(bytes::Bytes::from("persistent_key"), 3600);
        // stop() calls persist_ttl_metadata() → writes TTL_STATE_KEY to RocksDB.
        sm.stop().expect("stop");
        // Ensure DB is flushed before drop.
    }
    // Wait for RocksDB to release the file lock.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // --- Phase 2: reopen and reload ---
    let new_lease = make_lease();
    let (_storage2, mut sm2) = RocksDBUnifiedEngine::open(dir.path()).expect("reopen");
    sm2.set_lease(Arc::clone(&new_lease));
    sm2.load_lease_data().await.expect("load_lease_data");

    // The key registered in Phase 1 must be present in the reloaded lease.
    assert!(
        new_lease.has_lease_keys(),
        "reloaded lease must contain the persisted key"
    );
    assert_eq!(new_lease.len(), 1, "exactly one key should be restored");
}

/// load_lease_data() succeeds when TTL_STATE_KEY is absent in the DB (first start).
#[tokio::test]
async fn test_load_lease_data_with_empty_db_succeeds() {
    let dir = TempDir::new().expect("temp dir");
    let (_storage, mut sm) = RocksDBUnifiedEngine::open(dir.path()).expect("open");

    // Inject lease but do NOT persist anything first.
    sm.set_lease(make_lease());

    // load_lease_data on a fresh DB must succeed without error.
    let result = sm.load_lease_data().await;
    assert!(result.is_ok(), "load_lease_data on empty DB must succeed");
}
