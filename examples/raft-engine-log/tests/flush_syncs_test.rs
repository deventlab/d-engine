//! `flush()` must reach the disk. This lives in its own test binary because the
//! counter it reads is process-wide: another test syncing in parallel could
//! otherwise satisfy the assertion and hide a store that never syncs.

use d_engine::common::Entry;
use d_engine::{LogStore, MetaStore, StorageEngine};
use raft_engine_log::RaftEngineStorage;
use tempfile::TempDir;

/// Samples recorded by raft-engine's own histogram of physical log syncs.
fn physical_syncs() -> u64 {
    prometheus::gather()
        .iter()
        .filter(|family| family.get_name() == "raft_engine_sync_log_duration_seconds")
        .flat_map(|family| family.get_metric().iter())
        .map(|metric| metric.get_histogram().get_sample_count())
        .sum()
}

fn entry(index: u64) -> Entry {
    Entry {
        index,
        term: 1,
        payload: None,
    }
}

/// Every `flush()` after a write must perform a physical sync, for the log and
/// for the hard state (acknowledged after `MetaStore::flush`). A store whose
/// `flush()` returns without syncing still passes restart tests (the page cache
/// survives a reopen) while acknowledging commits that a power loss would drop.
///
/// One test only: the counter is process-wide, so two tests running in parallel
/// in this binary could satisfy each other's assertions.
#[tokio::test]
async fn test_flush_after_write_performs_physical_sync() {
    use d_engine::HardState;

    let dir = TempDir::new().unwrap();
    let storage = RaftEngineStorage::open(dir.path().join("log")).unwrap();
    let log = storage.log_store();
    let meta = storage.meta_store();

    let mut previous = physical_syncs();
    for index in 1..=5 {
        log.persist_entries(vec![entry(index)]).await.unwrap();
        log.flush().unwrap();

        let now = physical_syncs();
        assert!(
            now > previous,
            "log flush #{index} did not sync (sync samples stayed at {previous})"
        );
        previous = now;
    }

    meta.save_hard_state(&HardState {
        current_term: 3,
        voted_for: None,
    })
    .unwrap();
    meta.flush().unwrap();
    assert!(
        physical_syncs() > previous,
        "meta flush did not sync (sync samples stayed at {previous})"
    );
}
