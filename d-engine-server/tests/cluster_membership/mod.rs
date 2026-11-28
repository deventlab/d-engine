//! # Cluster Membership Tests
//!
//! Tests dynamic node joining and cluster membership changes.
//!
//! ## Test Coverage
//!
//! - `join_cluster_concurrent_joins_embedded.rs` - Two nodes join at the same time, catch up
//!   from the leader's snapshot while the leader serves reads and writes

mod join_cluster_concurrent_joins_embedded;
