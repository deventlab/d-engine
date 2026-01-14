//! # Leader Election and State Verification Tests
//!
//! Tests leader election correctness and leader state consensus across cluster nodes.
//!
//! ## Test Coverage
//!
//! - `cluster_state_consensus_embedded.rs` - Leader uniqueness verification and all-nodes consensus on leader identity
//! - `metadata_api_standalone.rs` - Metadata API queries via gRPC

mod cluster_state_consensus_embedded;
mod metadata_api_standalone;
