//! At startup the Raft log is made to agree with the state machine's applied position.
//!
//! The case that matters: a node installs a leader's snapshot, so the state machine is at the
//! snapshot boundary, and stops before it purges its log. It restarts with the state machine at
//! (9, 4) and a log that still holds entries 1..=9 from term 2. The state machine is the single
//! source of truth: what the log holds beyond or against it is dropped, and a contradiction that
//! cannot be repaired stops the node.

use d_engine_proto::common::LogId;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;

fn log_id(
    index: u64,
    term: u64,
) -> LogId {
    LogId { index, term }
}

/// The log holds another term at the applied index: it is purged up to the applied position, so
/// the node no longer reports a last log position from the dead history, and the purge boundary
/// carries the state machine's term.
#[tokio::test]
async fn test_align_purges_a_log_from_before_a_snapshot_install_to_the_applied_position() {
    let ctx = RaftLogCoreTestContext::new("align_stale_before_install");
    ctx.append_entries(1, 9, 2).await;

    ctx.raft_log.align_with_state_machine(log_id(9, 4)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(9, 4)));
    assert_eq!(
        ctx.raft_log.entry(9).unwrap(),
        None,
        "the stale entry must be gone"
    );
    assert_eq!(
        ctx.raft_log.entry_term(9),
        Some(4),
        "the purge boundary must carry the state machine's term"
    );
}

/// Entries after the applied index of a stale log sit on the same dead branch and go too;
/// otherwise the node would still report (12, 2) as its last log position.
#[tokio::test]
async fn test_align_removes_the_entries_after_the_applied_position_of_a_stale_log() {
    let ctx = RaftLogCoreTestContext::new("align_stale_tail");
    ctx.append_entries(1, 12, 2).await;

    ctx.raft_log.align_with_state_machine(log_id(9, 4)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(9, 4)));
    for index in 10..=12 {
        assert_eq!(
            ctx.raft_log.entry(index).unwrap(),
            None,
            "entry {index} must be gone"
        );
    }
}

/// The log agrees with the state machine at the applied index: nothing is touched. Entries after
/// the applied index are the node's own un-applied tail and belong to the log.
#[tokio::test]
async fn test_align_leaves_a_log_that_agrees_with_the_state_machine_alone() {
    let ctx = RaftLogCoreTestContext::new("align_agrees");
    ctx.append_entries(1, 12, 2).await;

    ctx.raft_log.align_with_state_machine(log_id(9, 2)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(12, 2)));
    assert!(
        ctx.raft_log.entry(1).unwrap().is_some(),
        "no entry may be purged"
    );
}

/// The log ends before the applied position: it is purged up to that position, so the node
/// reports (9, 4) instead of an older position.
#[tokio::test]
async fn test_align_purges_a_log_that_ends_before_the_applied_position() {
    let ctx = RaftLogCoreTestContext::new("align_ends_early");
    ctx.append_entries(1, 5, 2).await;

    ctx.raft_log.align_with_state_machine(log_id(9, 4)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(9, 4)));
    assert_eq!(ctx.raft_log.entry(5).unwrap(), None);
}

/// An empty log and a state machine that has applied something: the purge boundary is set to the
/// applied position, so the leader numbers its next entries from it.
#[tokio::test]
async fn test_align_sets_the_boundary_on_an_empty_log() {
    let ctx = RaftLogCoreTestContext::new("align_empty_log");

    ctx.raft_log.align_with_state_machine(log_id(9, 4)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(9, 4)));
}

/// An applied position of 0, or one whose term is unknown (0), has nothing to compare against:
/// the log is left alone.
#[tokio::test]
async fn test_align_compares_nothing_when_the_applied_position_is_empty_or_has_no_term() {
    let ctx = RaftLogCoreTestContext::new("align_no_position");
    ctx.append_entries(1, 9, 2).await;

    ctx.raft_log.align_with_state_machine(log_id(0, 0)).await.unwrap();
    ctx.raft_log.align_with_state_machine(log_id(9, 0)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(9, 2)));
    assert!(ctx.raft_log.entry(1).unwrap().is_some());
}

/// Running it again changes nothing: a node that restarts twice before it serves anything must
/// not purge differently the second time.
#[tokio::test]
async fn test_align_twice_is_the_same_as_aligning_once() {
    let ctx = RaftLogCoreTestContext::new("align_twice");
    ctx.append_entries(1, 12, 2).await;

    ctx.raft_log.align_with_state_machine(log_id(9, 4)).await.unwrap();
    ctx.raft_log.align_with_state_machine(log_id(9, 4)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(9, 4)));
    assert_eq!(ctx.raft_log.entry_term(9), Some(4));
}

/// The purge boundary is exactly the applied position with the same term: the log after the
/// boundary is the node's own tail and stays.
#[tokio::test]
async fn test_align_keeps_the_tail_when_the_boundary_equals_the_applied_position() {
    let ctx = RaftLogCoreTestContext::new("align_boundary_equals_applied");
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.purge_logs_up_to(log_id(9, 2)).await.unwrap();

    ctx.raft_log.align_with_state_machine(log_id(9, 2)).await.unwrap();

    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(12, 2)));
    assert!(ctx.raft_log.entry(10).unwrap().is_some());
}

/// The state machine is behind the log's purge boundary: the two describe different histories
/// and nothing is safe to repair, so startup fails and the log is not touched.
#[tokio::test]
async fn test_align_fails_when_the_state_machine_is_behind_the_purge_boundary() {
    let ctx = RaftLogCoreTestContext::new("align_behind_boundary");
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.purge_logs_up_to(log_id(8, 2)).await.unwrap();

    let err = ctx
        .raft_log
        .align_with_state_machine(log_id(5, 2))
        .await
        .expect_err("a state machine behind the purge boundary must stop startup");

    assert!(err.is_fatal(), "expected a fatal error, got {err:?}");
    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(12, 2)));
    assert_eq!(ctx.raft_log.entry_term(8), Some(2));
}

/// The purge boundary and the state machine name different terms at the same index: both are
/// snapshots of different histories. Overwriting either would hide the conflict, so startup fails.
#[tokio::test]
async fn test_align_fails_when_the_purge_boundary_disagrees_with_the_state_machine() {
    let ctx = RaftLogCoreTestContext::new("align_boundary_disagrees");
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.purge_logs_up_to(log_id(9, 2)).await.unwrap();

    let err = ctx
        .raft_log
        .align_with_state_machine(log_id(9, 4))
        .await
        .expect_err("a boundary that disagrees with the state machine must stop startup");

    assert!(err.is_fatal(), "expected a fatal error, got {err:?}");
    assert_eq!(
        ctx.raft_log.entry_term(9),
        Some(2),
        "the boundary must be left as it was"
    );
    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(12, 2)));
}

/// The log was purged through (100, 5), and the state machine reports nothing applied: it did not
/// persist its position, or its data was lost. Entries 1..=100 exist nowhere on this node, so
/// startup fails, and the error tells the operator what to do. The log is not touched.
#[tokio::test]
async fn test_align_fails_when_nothing_is_applied_but_the_log_is_purged() {
    let ctx = RaftLogCoreTestContext::new("align_nothing_applied_log_purged");
    ctx.append_entries(1, 120, 5).await;
    ctx.raft_log.purge_logs_up_to(log_id(100, 5)).await.unwrap();

    let err = ctx
        .raft_log
        .align_with_state_machine(log_id(0, 0))
        .await
        .expect_err("an empty state machine behind a purged log must stop startup");

    assert!(err.is_fatal(), "expected a fatal error, got {err:?}");
    assert!(
        format!("{err:?}").contains("raft log directory"),
        "the error must say what to do: {err:?}"
    );
    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(120, 5)));
    assert_eq!(ctx.raft_log.entry_term(100), Some(5));
}

/// An applied position whose term is unknown is skipped only when it cannot be behind the purge
/// boundary. Index 50 is behind a boundary at 100 whatever its term, so startup still fails.
#[tokio::test]
async fn test_align_fails_when_an_applied_position_without_a_term_is_behind_the_purge_boundary() {
    let ctx = RaftLogCoreTestContext::new("align_no_term_behind_boundary");
    ctx.append_entries(1, 120, 5).await;
    ctx.raft_log.purge_logs_up_to(log_id(100, 5)).await.unwrap();

    let err = ctx
        .raft_log
        .align_with_state_machine(log_id(50, 0))
        .await
        .expect_err("a position behind the purge boundary must stop startup");

    assert!(err.is_fatal(), "expected a fatal error, got {err:?}");
    assert_eq!(ctx.raft_log.last_log_id(), Some(log_id(120, 5)));
}
