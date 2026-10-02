//! `replace_range_and_submit` (term-conflict truncation, see
//! `filter_out_conflicts_and_append`'s slow path) must submit fsync itself,
//! directly, as part of the same call — it must not depend on a subsequent
//! `append_entries()` call to separately trigger persistence.
//!
//! This test pins down that invariant: a term-conflict truncation with no
//! append afterward must still become durable on its own.

use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;

fn entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: None,
    }
}

/// A term-conflict truncation (`replace_range_and_submit`) must eventually
/// become durable even if no `append_entries()` call follows it.
#[tokio::test]
async fn test_replace_range_becomes_durable_without_a_following_append() {
    let mut ctx =
        RaftLogCoreTestContext::new("replace_range_becomes_durable_without_a_following_append");

    // Arrange: log [1,2,3] all term=1, explicitly flushed durable.
    ctx.append_entries(1, 3, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(ctx.raft_log.durable_index(), 3, "baseline must be durable");

    // Act: leader (term=2) sends entries that conflict at index=2 and extend
    // the log to index=4. filter_out_conflicts_and_append's slow path detects
    // the term mismatch at index=2, truncates [2,3], and replaces with
    // [2,3,4] (term=2) via `replace_range_and_submit` — with no append_entries()
    // call afterward.
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(1, 1, vec![entry(2, 2), entry(3, 2), entry(4, 2)])
        .await
        .unwrap();
    assert_eq!(result.unwrap().index, 4);
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        4,
        "memory must reflect the replace"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.drain_fsync_completions();

    assert_eq!(
        ctx.raft_log.durable_index(),
        4,
        "replace_range_and_submit must submit fsync itself, without needing a following append"
    );
}
