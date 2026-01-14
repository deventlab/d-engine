//! # Snapshot and Recovery Tests
//!
//! Tests snapshot generation and node recovery via snapshot installation.
//!
//! ## Test Coverage
//!
//! - `snapshot_generation_standalone.rs` - Snapshot creation with log compaction (gRPC mode)
//! - `snapshot_concurrent_writes_embedded.rs` - Writes during snapshot generation (embedded mode)
//! - `snapshot_concurrent_replication_embedded.rs` - Learner receives snapshot during replication (embedded mode)

mod snapshot_concurrent_replication_embedded;
mod snapshot_concurrent_writes_embedded;
mod snapshot_generation_standalone;
