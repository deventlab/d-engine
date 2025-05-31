//! Snapshot policy aggregation module.
//! Defines interfaces and manages different snapshot triggering strategies.

mod composite;
mod log_size;
mod time_based;
#[allow(unused)]
pub(crate) use composite::*;
pub(crate) use log_size::*;
#[allow(unused)]
pub(crate) use time_based::*;

use crate::cluster::is_leader;
use crate::proto::common::LogId;

#[cfg(test)]
mod composite_test;
#[cfg(test)]
mod log_size_test;
#[cfg(test)]
mod time_based_test;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait SnapshotPolicy: Send + Sync {
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool;

    /// Should be called after the snapshot is created successfully
    fn mark_snapshot_created(&mut self);
}

#[derive(Clone)]
pub struct SnapshotContext {
    pub role: i32,
    pub last_included: LogId,
    pub last_applied: LogId,
    pub current_term: u64,
}

impl SnapshotContext {
    #[allow(unused)]
    pub fn is_leader(&self) -> bool {
        is_leader(self.role)
    }
}
