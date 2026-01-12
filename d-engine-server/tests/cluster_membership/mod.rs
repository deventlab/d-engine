//! # Cluster Membership Tests
//!
//! Tests dynamic node joining and cluster membership changes.
//!
//! ## Test Coverage
//!
//! - `join_cluster_single_join_standalone.rs` - Single node joins existing cluster (standalone mode)
//! - `join_cluster_concurrent_joins_standalone.rs` - Multiple nodes join simultaneously (standalone mode)

mod join_cluster_concurrent_joins_standalone;
mod join_cluster_single_join_standalone;
