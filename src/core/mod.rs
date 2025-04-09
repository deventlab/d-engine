mod commit_handler;
mod election;
mod event;
mod raft;
mod raft_context;

mod raft_role;

mod replication;

mod state_machine_handler;

mod timer;

pub(crate) use commit_handler::*;
pub(crate) use election::*;
pub(crate) use event::*;
pub(crate) use raft::*;
pub(crate) use raft_context::*;
#[doc(hidden)]
pub use raft_role::*;
pub(crate) use replication::*;
pub(crate) use state_machine_handler::*;
pub(crate) use timer::*;

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
/// e.g. Election: receive VoteResponse
/// e.g. Sync cluster membership configure

/// @return: true - found higher term;
pub(crate) fn if_higher_term_found(
    my_current_term: u64,
    term: u64,
    is_learner: bool,
) -> bool {
    //means I am fake leader or we should ignore learner's response
    if !is_learner && my_current_term < term {
        log::error!("my_current_term: {} < term: {} ?", my_current_term, term);
        return true;
    }

    false
}

/// Raft paper: 5.4.1 Election restriction
///
/// Raft determines which of two logs is more up-to-date by comparing the index and term of the last
/// entries in the  logs. If the logs have last entries with different terms, then the log with the
/// later term is more up-to-date. If the logs end with the same term, then whichever log is longer
/// is more up-to-date.
pub(crate) fn is_target_log_more_recent(
    my_last_log_index: u64,
    my_last_log_term: u64,
    target_last_log_index: u64,
    target_last_log_term: u64,
) -> bool {
    (target_last_log_term > my_last_log_term)
        || (target_last_log_term == my_last_log_term && target_last_log_index >= my_last_log_index)
}
