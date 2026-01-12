//! # Consistent Reads Tests
//!
//! This module verifies linearizable read operations and read optimization techniques.
//!
//! ## Business Scenarios Covered
//!
//! 1. **Linearizable Read Consistency (Embedded)**: Verify reads return most recent committed value
//! 2. **Linearizable Read with Concurrent Writes (Embedded)**: Ensure reads see all writes
//! 3. **Read Batching Integration (Embedded)**: Optimize read throughput with batching
//! 4. **Linearizable Read Consistency (Standalone)**: Same via gRPC

mod linearizable_read_batching_embedded;
mod linearizable_read_consistency_embedded;
