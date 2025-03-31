mod commit_handler;
mod election;
mod event;
mod raft;
mod raft_context;
mod raft_role;
mod replication;
mod state_machine_handler;
mod timer;

pub use commit_handler::*;
pub use election::*;
pub use event::*;
pub use raft::*;
pub use raft_context::*;
pub use raft_role::*;
pub use replication::*;
pub use state_machine_handler::*;
pub use timer::*;

#[cfg(test)]
mod event_test;
#[cfg(test)]
mod raft_oneshot_test;
#[cfg(test)]
mod raft_test;

/// In raft, during any Leader to Peer communication,
///     if received response term is bigger than Leader's,
///     the current Leader need downgrade to follower
///     and update its term to higher one.
///
/// e.g. Append Entries RPC
/// e.g. Sync cluster membership configure

/// @return: true - found new leader; false - no new leader found;
pub(crate) fn if_new_leader_found(
    my_current_term: u64,
    term: u64,
    is_learner: bool,
) -> bool {
    //means I am fake leader or we should ignore learner's response
    if !is_learner && my_current_term < term {
        log::error!("my_current_term: {} < term: {} ?", my_current_term, term);
        return true;
    }

    return false;
}
