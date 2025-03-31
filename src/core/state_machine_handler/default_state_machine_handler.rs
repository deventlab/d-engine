//! The `StateMachineHandler` module provides a core component for managing both write operations and read requests
//! against the `StateMachine`.
//!
//! ## Relationship Between `StateMachineHandler` and `StateMachine`
//! The `StateMachineHandler` serves as the primary interface for interacting with the `StateMachine`. Its dual responsibilities are:
//! 1. Applying committed log entries to the `StateMachine` to maintain state consistency
//! 2. Directly servicing client read requests through state machine queries
//!
//! While maintaining separation from the `StateMachine` itself, the handler leverages the `StateMachine` trait for both
//! state updates and read operations. This design centralizes all state access points while preserving separation of concerns.
//!
//! ## Design Recommendations
//! - **Customization Focus**: Developers should prioritize extending the `StateMachine` implementation rather than
//!   modifying the `StateMachineHandler`. The handler is intentionally generic and battle-tested, serving as:
//!   - Write coordinator for log application
//!   - Read router for direct state queries
//! - **State Access Unification**: All state access (both write and read) should flow through the handler to leverage:
//!   - Consistent concurrency control
//!   - Atomic visibility guarantees
//!   - Linearizable read optimizations
//!

use autometrics::autometrics;
use tonic::async_trait;

use super::StateMachineHandler;
use crate::{
    alias::{ROF, SMOF},
    grpc::rpc_service::{client_command::Command, ClientCommand, ClientResult},
    utils::cluster::error,
    RaftLog, Result, StateMachine, TypeConfig, API_SLO,
};
use log::error;
use std::{
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub struct DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    // last_applied, as an application progress indicator, may fall under the responsibility of the Handler, as it manages the application process.
    last_applied: AtomicU64,   // The last applied log index
    pending_commit: AtomicU64, // The highest pending commit index
    max_entries_per_chunk: usize,
    state_machine: Arc<SMOF<T>>, // Assume StateMachine is a trait
}

#[async_trait]
impl<T> StateMachineHandler<T> for DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    /// Update pending commit index
    fn update_pending(&self, new_commit: u64) {
        let mut current = self.pending_commit.load(Ordering::Acquire);
        while new_commit > current {
            match self.pending_commit.compare_exchange_weak(
                current,
                new_commit,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(e) => current = e,
            }
        }
    }

    /// Batch application log
    async fn apply_batch(&self, raft_log: Arc<ROF<T>>) -> Result<()> {
        if let Some(range) = self.pending_range() {
            // Read logs in batches
            let range_end = *range.end();
            let entries = raft_log.get_entries_between(range);

            // Apply in parallel
            let chunks = entries.into_iter().collect::<Vec<_>>();
            let handles: Vec<_> = chunks
                .chunks(self.max_entries_per_chunk)
                .map(|chunk| {
                    let sm = self.state_machine.clone();
                    let chunk = chunk.to_vec(); // Transfer ownership of chunk to the closure
                    tokio::spawn(async move { sm.apply_chunk(chunk) })
                })
                .collect();

            // Wait for all batches to complete
            for h in handles {
                if let Err(e) = h.await {
                    error("apply_batch", &e);
                }
            }

            // Atomic update last_applied
            self.last_applied.store(range_end, Ordering::Release);
        }
        Ok(())
    }

    /// TODO: decouple client related commands with RAFT internal logic
    ///
    #[autometrics(objective = API_SLO)]
    fn read_from_state_machine(
        &self,
        client_command: Vec<ClientCommand>,
    ) -> Option<Vec<ClientResult>> {
        let mut result = Vec::new();
        for c in client_command {
            match c.command {
                Some(Command::Get(key)) => {
                    if let Ok(Some(value)) = self.state_machine.get(&key) {
                        result.push(ClientResult { key: key, value });
                    }
                }
                _ => {
                    error!("might be a bug while receiving none GET command, ignore.")
                }
            }
        }

        if result.len() > 0 {
            Some(result)
        } else {
            None
        }
    }
}

impl<T> DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    pub fn new(
        last_applied: Option<u64>,
        max_entries_per_chunk: usize,
        state_machine: Arc<SMOF<T>>,
    ) -> Self {
        Self {
            last_applied: AtomicU64::new(last_applied.unwrap_or(0)),
            pending_commit: AtomicU64::new(0),
            max_entries_per_chunk,
            state_machine,
        }
    }

    /// Get the interval to be processed
    pub fn pending_range(&self) -> Option<RangeInclusive<u64>> {
        let last_applied = self.last_applied.load(Ordering::Acquire);
        let pending_commit = self.pending_commit.load(Ordering::Acquire);

        if pending_commit > last_applied {
            Some((last_applied + 1)..=pending_commit)
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn pending_commit(&self) -> u64 {
        self.pending_commit.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }
}
