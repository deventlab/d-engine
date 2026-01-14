//! # Leader Election Tests
//!
//! Tests leader election based on log recency (term and index).
//!
//! ## Test Coverage
//!
//! - `leader_election_log_term_index_standalone.rs` - Node with highest log term/index becomes leader (gRPC mode)

mod leader_election_log_term_index_standalone;
