//! Crash recovery integration tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog behavior with real disk persistence
//! and crash recovery semantics using FileStorageEngine.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{FlushPolicy, PersistenceStrategy, RaftLog};
use d_engine_proto::common::{Entry, EntryPayload};
use tokio::time::sleep;

use super::TestContext;

// TODO: Extract 7 crash recovery tests from legacy_all_tests.rs:
// - test_crash_recovery (L1890)
// - test_crash_recovery_with_multiple_entries (L1934)
// - test_crash_recovery_mem_first (L1992)
// - test_partial_flush_after_crash (L2229)
// - test_recovery_under_different_scenarios (L3587)
// - test_memfirst_crash_recovery_durability (L3883)
// - test_diskfirst_crash_recovery_durability (L3935)
