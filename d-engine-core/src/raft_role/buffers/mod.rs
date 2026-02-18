//! Batching buffers for the Leader's client request hot path.
//!
//! - [`BatchBuffer`] — generic single-type buffer backed by a contiguous `Vec`.
//!   Used for linearizable read requests.
//! - [`ProposeBatchBuffer`] — SoA (Struct-of-Arrays) buffer for write proposals.
//!   Stores payloads and senders in separate `Vec`s;
//!   `flush()` is O(1) via `mem::take`, no unzip scan.

pub mod batch_buffer;
pub mod propose_batch_buffer;

pub use batch_buffer::BatchBuffer;
pub use propose_batch_buffer::ProposeBatchBuffer;

#[cfg(test)]
mod batch_buffer_test;
#[cfg(test)]
mod propose_batch_buffer_test;
