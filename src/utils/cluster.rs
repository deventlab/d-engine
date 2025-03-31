use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use dashmap::DashSet;
use log::error;

use crate::grpc::rpc_service::Entry;
use crate::CLUSTER_FATAL_ERROR;

pub(crate) fn collect_ids(entries: &Vec<Entry>) -> Vec<u64> {
    entries.into_iter().map(|e| e.index).collect()
}

pub(crate) fn is_majority(
    num: usize,
    count: usize,
) -> bool {
    num >= count / 2 + 1
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
