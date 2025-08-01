//! Core model in Raft: StateStorage Definition, persistent state: e.g.
//! current_term

use bytes::Bytes;
#[cfg(test)]
use mockall::automock;

use crate::HardState;
use crate::Result;

#[cfg_attr(test, automock)]
pub trait StateStorage: Send + Sync + 'static {
    fn get(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<Bytes>>;

    fn insert(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>>;

    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    fn flush(&self) -> Result<()>;

    /// When node restarts, check if there is stored state from disk
    fn load_hard_state(&self) -> Option<HardState>;

    /// Save role hard state into db
    fn save_hard_state(
        &self,
        hard_state: HardState,
    ) -> Result<()>;

    #[cfg(test)]
    fn len(&self) -> usize;

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
