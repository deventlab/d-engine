//! # Watch and Event Subscriptions Tests
//!
//! Tests key-value change notifications and event streaming.
//!
//! ## Test Coverage
//!
//! - `watch_events_embedded.rs` - In-process key change subscriptions (embedded mode)
//! - `watch_events_grpc_standalone.rs` - gRPC streaming watch events (standalone mode)
//! - `watch_performance_gate_embedded.rs` - Watch latency and throughput benchmarks (embedded mode)

mod watch_events_embedded;
mod watch_events_grpc_standalone;
mod watch_performance_gate_embedded;
