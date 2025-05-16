//! Snapshot policy aggregation module.
//! Defines interfaces and manages different snapshot triggering strategies.

mod composite;
mod log_size;
mod time_based;
pub(crate) use composite::*;
pub(crate) use log_size::*;
pub(crate) use time_based::*;

use crate::cluster::is_leader;

#[cfg(test)]
mod log_size_test;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait SnapshotPolicy: Send + Sync {
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool;
}

#[derive(Clone)]
pub struct SnapshotContext {
    pub role: i32,
    pub last_snapshot_index: u64,
    pub last_snapshot_term: u64,
    pub last_applied_index: u64,
    pub current_term: u64,
}

impl SnapshotContext {
    pub fn is_leader(&self) -> bool {
        is_leader(self.role)
    }
}
