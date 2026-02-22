//! Drain-Based Batching Integration Tests
//!
//! Tests for the drain-based batch architecture that verify:
//! - Channel select fairness under high load
//! - No starvation of protocol messages
//! - Proper batch size enforcement

mod select_fairness_embedded;
