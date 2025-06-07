use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use dashmap::DashSet;
use tracing::error;

use crate::proto::common::Entry;
use crate::CANDIDATE;
use crate::CLUSTER_FATAL_ERROR;
use crate::FOLLOWER;
use crate::LEADER;
use crate::LEARNER;

pub(crate) fn collect_ids(entries: &[Entry]) -> Vec<u64> {
    entries.iter().map(|e| e.index).collect()
}

pub(crate) fn is_majority(
    num: usize,
    total: usize,
) -> bool {
    num > total / 2
}

pub fn majority_count(total_nodes: usize) -> usize {
    (total_nodes / 2) + 1
}

pub(crate) fn find_nearest_lower_number(
    target_index: u64,
    set_of_index: Arc<DashSet<u64>>,
) -> Option<u64> {
    set_of_index
        .iter()
        .filter(|index| **index <= target_index) // Filter only numbers <= learner_next_index
        .max_by_key(|index| **index) // Find the maximum of the filtered numbers
        .map(|index| *index) // Convert from reference to value
}

/// record down cluster level error for debug and code optimization purpose.
pub(crate) fn record_down_cluster_error(event_id: u64) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs_f64();
    CLUSTER_FATAL_ERROR
        .with_label_values(&[&event_id.to_string()])
        .set(timestamp);
}

/// Format error logging
pub fn error(
    func_name: &str,
    e: &dyn std::fmt::Debug,
) {
    error!("{}::{} failed: {:?}", module_path!(), func_name, e);
}

#[inline]
pub fn is_follower(role_i32: i32) -> bool {
    role_i32 == FOLLOWER
}

#[inline]
pub fn is_candidate(role_i32: i32) -> bool {
    role_i32 == CANDIDATE
}

#[inline]
pub fn is_leader(role_i32: i32) -> bool {
    role_i32 == LEADER
}

#[inline]
pub fn is_learner(role_i32: i32) -> bool {
    role_i32 == LEARNER
}
