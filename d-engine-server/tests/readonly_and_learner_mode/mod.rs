//! # Readonly and Learner Mode Tests
//!
//! Tests read-only learner nodes that replicate data but cannot vote in elections.
//!
//! ## Test Coverage
//!
//! - `learner_readonly_sync_embedded.rs` - Learner data synchronization and eventual consistency reads (embedded mode)
//! - `learner_readonly_sync_standalone.rs` - Learner operations via gRPC (standalone mode)

mod learner_readonly_sync_embedded;
mod learner_readonly_sync_standalone;
