//! BufferedRaftLog unit tests
//!
//! This module contains comprehensive unit tests for `BufferedRaftLog` organized by
//! functional domains for better maintainability.
//!
//! ## Test Organization
//!
//! - `basic_operations_test`: CRUD operations, range queries, conflict resolution
//! - `flush_strategy_test`: DiskFirst/MemFirst/Batched persistence strategies (Mock-based)
//! - `id_allocation_test`: ID pre-allocation logic and concurrency
//! - `term_index_test`: Term boundary tracking and calculation
//! - `concurrent_operations_test`: Thread-safety and race condition tests
//! - `crash_recovery_test`: Mock-based crash recovery simulation
//! - `performance_test`: Performance characteristics (not absolute throughput)
//! - `edge_cases_test`: Boundary conditions and unusual scenarios
//! - `helper_functions_test`: Internal utility function tests
//!
//! ## Test Strategy
//!
//! These tests use `MockStorageEngine` to verify algorithm correctness without real disk I/O.
//! Integration tests with `FileStorageEngine` are in `d-engine-server/tests/integration/`.

mod basic_operations_test;
mod concurrent_operations_test;
mod crash_recovery_test;
mod edge_cases_test;
mod flush_strategy_test;
mod helper_functions_test;
mod id_allocation_test;
mod performance_test;
mod raft_properties_test;
mod remove_range_test;
mod shutdown_test;
mod term_index_test;
