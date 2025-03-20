//! Core model in Raft: StateStorage Definition, persistent state: e.g. current_term
//!
use crate::{HardState, Result};

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait StateStorage: Send + Sync + 'static {
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;
    fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    fn flush(&self) -> Result<usize>;

    /// When node restarts, check if there is stored state from disk
    fn load_hard_state(&self) -> Option<HardState>;
    /// Save role hard state into db
    fn save_hard_state(&self, hard_state: HardState) -> Result<()>;

    #[cfg(test)]
    fn len(&self) -> usize;
}
