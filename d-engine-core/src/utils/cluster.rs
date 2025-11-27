use tracing::error;

use d_engine_proto::common::NodeRole::Candidate;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeRole::Learner;

pub(crate) fn is_majority(
    num: usize,
    total: usize,
) -> bool {
    num > (total / 2)
}

pub fn majority_count(total_nodes: usize) -> usize {
    (total_nodes / 2) + 1
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
    role_i32 == (Follower as i32)
}

#[inline]
pub fn is_candidate(role_i32: i32) -> bool {
    role_i32 == (Candidate as i32)
}

#[inline]
pub fn is_leader(role_i32: i32) -> bool {
    role_i32 == (Leader as i32)
}

#[inline]
pub fn is_learner(role_i32: i32) -> bool {
    role_i32 == (Learner as i32)
}
