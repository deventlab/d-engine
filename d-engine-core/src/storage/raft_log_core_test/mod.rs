//! RaftLogCore unit tests
//!
//! Ported from `buffered_raft_log_test` — same functional coverage, adapted
//! for `RaftLogCore`'s inline (no dedicated IO thread) execution model.
//!
//! These tests use `MockStorageEngine` to verify algorithm correctness without real disk I/O.
//! Integration tests with `FileStorageEngine` are in `d-engine-server/tests/integration/`.

mod basic_operations_test;
mod concurrent_fsync_test;
mod concurrent_operations_test;
mod content_validated_watermark_test;
mod durable_index_test;
mod durable_index_truncation_clamp_test;
mod edge_cases_test;
mod flush_strategy_test;
mod id_allocation_test;
mod performance_test;
mod pipeline_overlap_test;
mod prev_log_index_zero_idempotency_test;
mod quorum_durability_test;
mod raft_properties_test;
mod remove_range_test;
mod replace_range_fsync_test;
mod shutdown_test;
mod term_index_test;
mod term_segments_test;
mod truncation_fsync_fence_test;
