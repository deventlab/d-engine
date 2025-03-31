//! # Commit Handler Module
//!
//! The `commit_handler` module is responsible for processing newly committed
//! log entries in a Raft cluster. It acts as a bridge between the Raft
//! consensus protocol and the application-specific business logic. When a new
//! log entry is committed by the Raft leader and replicated to a majority of
//! nodes, the commit handler ensures that the entry is applied to the state
//! machine in a consistent and thread-safe manner.
//!
//! ## Key Responsibilities
//! 1. **Receiving Commits**: Listens for newly committed log entries from the Raft log.
//! 2. **Batching**: Groups multiple log entries into batches for efficient processing.
//! 3. **Applying to State Machine**: Passes the committed entries to the state machine for
//!    application-specific processing.
//! 4. **Snapshotting**: Periodically triggers snapshots to reduce the size of the Raft log and
//!    improve recovery performance.
//!
//! ## Raft Protocol Integration
//! - The commit handler adheres to the Raft protocol by ensuring that committed entries are applied
//!   in the exact order they were committed.
//! - It guarantees linearizability by applying entries only after they are committed and replicated
//!   to a majority of nodes.
//!
//! ## Best Practices
//! - **Batching**: Use batching to reduce the overhead of applying individual log entries.
//! - **Concurrency**: Apply log entries concurrently (if safe) to improve throughput.
//! - **Snapshotting**: Regularly take snapshots to prevent the Raft log from growing indefinitely.
//! - **Error Handling**: Handle errors gracefully and ensure the state machine remains consistent
//!   even in the face of failures.
//!
//! ## Customization for Business Logic
//! - The actual implementation of how to use the committed log entries is left to the developer
//!   (business layer). The commit handler provides a framework for processing entries, but the
//!   business logic must define how to interpret and apply them.
//! - Developers should implement the `StateMachine` trait to define how log entries are applied to
//!   the application state.
//!
//! ## Example Workflow
//! 1. The Raft leader commits a new log entry and replicates it to a majority of nodes.
//! 2. The commit handler receives the committed entry and adds it to a batch.
//! 3. Once the batch reaches a certain size or a timeout occurs, the handler applies the batch to
//!    the state machine.
//! 4. The state machine processes the entries and updates the application state.
//! 5. Periodically, the commit handler triggers a snapshot(TODO: in future release) to compact the
//!    Raft log.
//!
//! ## Notes
//! - The commit handler is designed to be thread-safe and can be used in a multi-threaded or
//!   asynchronous environment.
//! - Developers must ensure that the state machine implementation is idempotent to handle potential
//!   retries or re-applications of log entries.
mod default_commit_handler;

pub use default_commit_handler::*;
#[cfg(test)]
mod default_commit_handler_test;

//--------------------------------
// CommitHandler trait definition
#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use crate::Result;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait CommitHandler: Send + Sync + 'static {
    async fn run(&mut self) -> Result<()>;
}
