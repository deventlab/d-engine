//! # Snapshot and Recovery Tests
//!
//! Tests snapshot generation and node recovery via snapshot installation.
//!
//! ## Test Coverage
//!
//! - `snapshot_generation_standalone.rs` - Snapshot creation with log compaction (gRPC mode)
//! - `snapshot_concurrent_writes_embedded.rs` - Writes during snapshot generation (embedded mode)
//! - `snapshot_concurrent_replication_embedded.rs` - Learner receives snapshot during replication (embedded mode)
//! - `snapshot_follower_generation_embedded.rs` - Follower independent snapshot trigger + replication safety (fix #270)
//! - `snapshot_interrupted_transfer_embedded.rs` - Learner recovers from stale temp file left by prior interrupted transfer
//! - `snapshot_leader_change_during_transfer_embedded.rs` - Learner catches up after snapshot source leader dies mid-transfer

mod snapshot_concurrent_replication_embedded;
mod snapshot_concurrent_writes_embedded;
mod snapshot_follower_generation_embedded;
mod snapshot_generation_standalone;
mod snapshot_interrupted_transfer_embedded;
mod snapshot_leader_change_during_transfer_embedded;
