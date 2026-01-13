//! Storage-level integration tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog integration with FileStorageEngine
//! at the storage layer, including compaction and storage-specific operations.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{FlushPolicy, PersistenceStrategy, RaftLog};
use d_engine_proto::common::{Entry, EntryPayload};

use super::TestContext;

// TODO: Extract 1 storage integration test from legacy_all_tests.rs:
// - test_log_compaction
