//! A state machine that forwards every call to an inner state machine shared through an `Arc`.
//!
//! The engine takes its state machine by value, so a test can no longer keep a second reference
//! to the very instance the engine uses. Tests that must reach that instance while the engine
//! runs (to read it directly, or to simulate a crash by closing its storage) wrap the shared
//! `Arc` in this type, hand the wrapper to the engine and keep their own `Arc`.
//!
//! This file is also compiled into the crate's unit tests, so it only names `d_engine_core` and
//! `d_engine_proto` paths.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_core::ApplyEntry;
use d_engine_core::ApplyResult;
use d_engine_core::Error;
use d_engine_core::ScanResult;
use d_engine_core::StateMachine;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;

/// Forwards the whole [`StateMachine`] trait to a shared inner state machine.
#[derive(Debug)]
pub struct SharedStateMachine<SM>(Arc<SM>);

impl<SM> SharedStateMachine<SM> {
    pub fn new(inner: Arc<SM>) -> Self {
        Self(inner)
    }
}

#[async_trait]
impl<SM: StateMachine + Debug> StateMachine for SharedStateMachine<SM> {
    fn close(&self) -> Result<(), Error> {
        self.0.close()
    }
    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        self.0.get(key_buffer)
    }
    fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, Error> {
        self.0.get_multi(keys)
    }
    async fn apply_chunk(
        &self,
        chunk: &[ApplyEntry],
    ) -> Result<Vec<ApplyResult>, Error> {
        self.0.apply_chunk(chunk).await
    }
    fn last_applied(&self) -> LogId {
        self.0.last_applied()
    }
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), Error> {
        self.0.apply_snapshot_from_file(metadata, snapshot_path).await
    }
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
    ) -> Result<LogId, Error> {
        self.0.generate_snapshot_data(new_snapshot_dir).await
    }
    async fn reset(&self) -> Result<(), Error> {
        self.0.reset().await
    }
    fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<ScanResult, Error> {
        self.0.scan_prefix(prefix)
    }
}
