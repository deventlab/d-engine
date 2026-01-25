//! # CompareAndSwap (CAS) Operations Tests
//!
//! This module verifies CAS atomic operations in d-engine clusters, ensuring
//! correctness of distributed coordination primitives under various scenarios.
//!
//! ## Business Scenarios Covered
//!
//! 1. **Distributed Lock (Embedded)**: Multiple clients compete for exclusive lock via CAS
//! 2. **Distributed Lock (Standalone)**: Same scenario via gRPC for remote clients
//! 3. **Snapshot Recovery (Embedded)**: CAS state survives snapshot and restart
//! 4. **Snapshot Recovery (Standalone)**: CAS state persists across gRPC restarts
//! 5. **Leader Failover + CAS (Embedded)**: CAS atomicity during leader crash
//! 6. **Leader Failover + CAS (Standalone)**: gRPC client retry after leader crash
//!
//! ## CAS Protocol Guarantees
//!
//! - **Atomicity**: Compare and swap is a single atomic operation
//! - **Linearizability**: CAS results reflect total order of operations
//! - **Exactly-Once Semantics**: Only one client succeeds when multiple compete
//! - **Persistence**: CAS results survive crashes and snapshots
//! - **Conflict Detection**: Failed CAS returns false (expected != actual)
//!
//! ## Developer API Coverage
//!
//! **Embedded Mode**:
//! - `EmbeddedClient::compare_and_swap(key, expected, new_value)` - Atomic CAS
//! - `EmbeddedClient::get(key)` - Read current value
//! - `EmbeddedClient::put(key, value)` - Initialize state
//!
//! **Standalone Mode (gRPC)**:
//! - `GrpcClient::compare_and_swap(key, expected, new_value)` - Remote CAS
//! - Retry logic on NOT_LEADER errors
//! - Leader discovery and connection management
//!
//! ## Integration Modes
//!
//! - ✅ **Embedded Mode**: In-process EmbeddedClient
//!   - `distributed_lock_embedded.rs` - Multi-client lock competition
//!   - `snapshot_recovery_embedded.rs` - CAS state persistence
//!   - `leader_failover_cas_embedded.rs` - CAS atomicity during leader crash
//!
//! - ✅ **Standalone Mode**: gRPC GrpcClient
//!   - `distributed_lock_standalone.rs` - Remote lock competition
//!   - `snapshot_recovery_standalone.rs` - Remote CAS persistence
//!   - `leader_failover_cas_standalone.rs` - gRPC retry logic during failover
//!
//! ## Test Files
//!
//! Each file contains ONE primary test scenario:
//! - File name matches test function name
//! - Clear 1:1 mapping between file and objective
//! - Self-contained setup and teardown
//!
//! ## CAS Use Cases
//!
//! ### Distributed Lock Pattern
//! ```rust,ignore
//! // Acquire lock: CAS(lock_key, None, client_id)
//! let acquired = client.compare_and_swap("lock", None, b"client1").await?;
//! if acquired {
//!     // Critical section
//!     client.compare_and_swap("lock", Some(b"client1"), None).await?; // Release
//! }
//! ```
//!
//! ### Leader Election Pattern
//! ```rust,ignore
//! // Compete for leadership: CAS(leader_key, None, node_id)
//! let is_leader = client.compare_and_swap("leader", None, b"node2").await?;
//! ```
//!
//! ### Optimistic Update Pattern
//! ```rust,ignore
//! loop {
//!     let current = client.get("counter").await?;
//!     let new_value = current + 1;
//!     if client.compare_and_swap("counter", Some(current), new_value).await? {
//!         break; // Success
//!     }
//!     // Retry on conflict
//! }
//! ```

mod distributed_lock_embedded;
mod distributed_lock_standalone;
mod leader_failover_cas_embedded;
mod leader_failover_cas_standalone;
mod snapshot_recovery_embedded;
mod snapshot_recovery_standalone;
