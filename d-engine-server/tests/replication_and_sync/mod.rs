//! # Log Replication and Synchronization Tests
//!
//! Tests log replication correctness and peer synchronization with divergent logs.
//!
//! ## Test Coverage
//!
//! - `append_entries_out_of_sync_standalone.rs` - Leader handles followers with divergent logs via backtracking (gRPC mode)

mod append_entries_out_of_sync_standalone;
