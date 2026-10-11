//! Shutdown ordering of `EmbeddedEngine::stop()`.
//!
//! `close_storage()` flushes the state machine and releases its storage (for RocksDB: the DB
//! handle and the LOCK file). It may therefore only run once the state machine worker has applied
//! everything the node had already committed; otherwise those commits are applied onto closed
//! storage, or never applied.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_core::ApplyEntry;
use d_engine_core::ApplyResult;
use d_engine_core::Error;
use d_engine_core::ScanResult;
use d_engine_core::SnapshotApplyResult;
use d_engine_core::StateMachine;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;

use crate::api::EmbeddedEngine;
use crate::storage::FileStateMachine;
use crate::storage::FileStorageEngine;

/// A file state machine whose `apply_chunk` is slow (so commits are still being applied when the
/// engine is stopped) and which records whether storage was closed while applying.
#[derive(Debug)]
struct SlowApplyProbe {
    inner: FileStateMachine,
    apply_delay: Duration,
    apply_started: AtomicBool,
    storage_closed: AtomicBool,
    /// Entries whose apply started or finished after `close_storage()` had been called.
    applied_after_close: AtomicUsize,
}

#[async_trait]
impl StateMachine for SlowApplyProbe {
    async fn apply_chunk(
        &self,
        chunk: &[ApplyEntry],
    ) -> Result<Vec<ApplyResult>, Error> {
        self.apply_started.store(true, Ordering::SeqCst);
        let closed_at_start = self.storage_closed.load(Ordering::SeqCst);
        tokio::time::sleep(self.apply_delay).await;
        let result = self.inner.apply_chunk(chunk).await;
        if closed_at_start || self.storage_closed.load(Ordering::SeqCst) {
            self.applied_after_close.fetch_add(chunk.len(), Ordering::SeqCst);
        }
        result
    }

    fn close(&self) {
        self.storage_closed.store(true, Ordering::SeqCst);
        self.inner.close();
    }

    // Everything below delegates unchanged.
    async fn start(&self) -> Result<(), Error> {
        self.inner.start().await
    }
    fn is_running(&self) -> bool {
        self.inner.is_running()
    }
    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        self.inner.get(key_buffer)
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn update_last_applied(
        &self,
        last_applied: LogId,
    ) {
        self.inner.update_last_applied(last_applied)
    }
    fn last_applied(&self) -> LogId {
        self.inner.last_applied()
    }
    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<(), Error> {
        self.inner.persist_last_applied(last_applied)
    }
    fn update_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        self.inner.update_last_snapshot_metadata(snapshot_metadata)
    }
    fn snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.inner.snapshot_metadata()
    }
    fn persist_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        self.inner.persist_last_snapshot_metadata(snapshot_metadata)
    }
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> Result<SnapshotApplyResult, Error> {
        self.inner.apply_snapshot_from_file(metadata, snapshot_path).await
    }
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<Bytes, Error> {
        self.inner.generate_snapshot_data(new_snapshot_dir, last_included).await
    }
    fn save_hard_state(&self) -> Result<(), Error> {
        self.inner.save_hard_state()
    }
    fn flush(&self) -> Result<(), Error> {
        self.inner.flush()
    }
    async fn flush_async(&self) -> Result<(), Error> {
        self.inner.flush_async().await
    }
    async fn reset(&self) -> Result<(), Error> {
        self.inner.reset().await
    }
    fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<ScanResult, Error> {
        self.inner.scan_prefix(prefix)
    }
}

/// Business scenario: a single-node embedded service is busy writing when its operator stops it
/// (rolling restart). Commits the node already made are still being applied by the slow state
/// machine at that moment.
///
/// Purpose: `stop()` must let the worker finish applying those commits BEFORE it closes the
/// state machine's storage. Closing first means applies run on (or are cut off by) closed
/// storage, so committed writes do not reliably reach the state machine's disk.
#[tokio::test]
async fn test_stop_closes_state_machine_storage_only_after_in_flight_applies_finish() {
    unsafe {
        std::env::remove_var("CONFIG_PATH");
    }
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let storage =
        Arc::new(FileStorageEngine::new(temp_dir.path().join("storage")).expect("storage"));
    let probe = Arc::new(SlowApplyProbe {
        inner: FileStateMachine::new(temp_dir.path().join("sm")).await.expect("sm"),
        apply_delay: Duration::from_millis(300),
        apply_started: AtomicBool::new(false),
        storage_closed: AtomicBool::new(false),
        applied_after_close: AtomicUsize::new(0),
    });

    let engine = EmbeddedEngine::start_custom(temp_dir.path(), storage, probe.clone(), None)
        .await
        .expect("start engine");
    engine.wait_ready(Duration::from_secs(5)).await.expect("leader");

    // Writes in flight: not awaited, the operator does not wait for them.
    let client = engine.client();
    for i in 0..20u32 {
        let client = client.clone();
        tokio::spawn(async move {
            let _ = client.put(format!("key-{i}"), "value").await;
        });
    }
    // Wait until the state machine is busy applying, then stop the engine.
    while !probe.apply_started.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    engine.stop().await.expect("stop");

    assert_eq!(
        probe.applied_after_close.load(Ordering::SeqCst),
        0,
        "stop() closed the state machine's storage while committed entries were still being applied"
    );
}
