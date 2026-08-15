use std::sync::Arc;
use std::time::Duration;

use d_engine_server::RocksDBUnifiedEngine;
use d_engine_server::api::DefaultEmbeddedEngine;
use tracing::info;

use crate::common::create_node_config;
use crate::common::create_rejoin_node_config;
use crate::common::get_available_ports;
use crate::common::wait_for_new_leader;

/// Recursively copies `src` into `dst`, creating `dst` if needed.
///
/// Used to snapshot a RocksDB data directory right after its background
/// compaction/flush has been quiesced (see `close_db_for_crash_simulation`),
/// capturing exactly what would remain on disk after a real `kill -9` — without
/// ever reopening the original (still OS-locked, since we never release every
/// `Arc<DB>` clone) path. Opening the copy instead sidesteps the LOCK entirely:
/// it's a fresh directory the OS has never seen.
fn copy_dir_all(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

/// Verify that a follower's state machine recovers correctly after an abrupt crash
/// when `disableWAL=true` is active on the SM RocksDB.
///
/// With `disableWAL=true`, the SM does not journal writes to its own WAL.
/// On crash (kill -9), any SM MemTable data not yet flushed to SST is lost.
/// Recovery relies entirely on Raft log replay: the node restarts with a stale
/// `last_applied` and receives missing entries via AppendEntries from the leader.
///
/// Crash simulation: quiesce RocksDB background work (`close_db_for_crash_simulation`,
/// no flush) and snapshot the data directory via plain file copy — this captures
/// exactly what a real kill -9 would leave on disk (MemTable data not yet flushed
/// to SST is absent from the copy). The engine is then stopped gracefully so its
/// gRPC server/replication connections are torn down cleanly; by this point the
/// SM's db slot is already cleared, so stop()'s internal flush is a harmless no-op
/// and cannot resurrect the MemTable data we just excluded from the snapshot.
/// Restart opens the snapshot copy rather than the original path.
#[tokio::test]
async fn test_follower_sm_recovers_after_abrupt_crash() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    // ── Phase 1: Start a 3-node embedded cluster ──────────────────────────────
    // Allocate ports and bring up all three nodes with RocksDB storage
    // (which uses disableWAL=true on the SM side).
    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut engines = Vec::new();
    let mut configs = Vec::new();
    let mut db_paths = Vec::new();
    let mut sm_handles = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let node_db_root = db_root.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");
        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;
        let config_path = format!("/tmp/d-engine-test-sm-crash-follower-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        configs.push((config_str, config_path));
        db_paths.push(db_path);

        let sm_for_crash = Arc::new(state_machine);

        let engine = DefaultEmbeddedEngine::start_custom(
            db_paths.last().unwrap(),
            Arc::new(storage),
            Arc::clone(&sm_for_crash),
            Some(&configs[i].1),
        )
        .await?;
        engines.push(engine);
        sm_handles.push(sm_for_crash);
    }

    // ── Phase 2: Elect a leader and write initial entries ─────────────────────
    // Wait until the cluster has converged on a leader, then write 10 entries.
    // Allow time for AppendEntries to replicate to all followers before the crash.
    let leader_info = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("cluster failed to elect initial leader");
    info!(
        "Leader elected: node {} (term {})",
        leader_info.leader_id, leader_info.term
    );

    let leader_idx = (leader_info.leader_id - 1) as usize;

    for i in 0..10u8 {
        engines[leader_idx]
            .client()
            .put(
                format!("key-pre-crash-{i:02}").into_bytes(),
                format!("val-{i:02}").into_bytes(),
            )
            .await?;
    }

    // Give followers enough time to apply all 10 entries before we crash one.
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("Phase 2 complete: 10 entries written and replicated to all nodes");

    // ── Phase 3: Crash a follower — simulate kill -9 ──────────────────────────
    // close_db_for_crash_simulation() + the pre-stop snapshot below prevent any
    // graceful flush of SM MemTable to SST. This is the scenario where
    // disableWAL=true causes SM data to be at risk: the SM MemTable is lost;
    // only the Raft log WAL guarantees durability.
    let follower_idx = if leader_idx == 0 { 1 } else { 0 };
    let follower_id = (follower_idx + 1) as u64;
    info!(
        "Crashing follower node {} (SM MemTable loss simulated, network layer stopped cleanly)",
        follower_id
    );

    let crashed_engine = engines.remove(follower_idx);
    let crashed_config = configs.remove(follower_idx);
    let crashed_db_path = db_paths.remove(follower_idx);
    let crashed_sm = sm_handles.remove(follower_idx);

    // Quiesce RocksDB background compaction/flush (no data flush!) so the
    // directory is stable while we snapshot it, then copy what's on disk right
    // now — this is what a real kill -9 would leave behind. The SM's db slot
    // is cleared here (ArcSwapOption -> None), so the later stop() call's
    // internal close_db() finds nothing to flush (errors out harmlessly) —
    // MemTable data loss is already locked in by the time the snapshot is
    // taken, below.
    crashed_sm.close_db_for_crash_simulation();
    let crash_snapshot_path = crashed_db_path.with_file_name("db_crash_snapshot");
    copy_dir_all(&crashed_db_path, &crash_snapshot_path)?;
    // Graceful stop (not mem::forget) so the gRPC server/replication connections
    // are properly torn down — mem::forget leaked networking state, leaving
    // ghost tasks that confused peers about whether they were talking to the
    // old or the restarted node.
    crashed_engine.stop().await?;

    // Wait for background tokio tasks inside the forgotten engine to release
    // OS resources (TCP port binding, RocksDB file locks).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Phase 4: Write more entries while the follower is offline ─────────────
    // The cluster retains a majority (2/3 nodes), so writes continue to succeed.
    // These entries are unknown to the crashed follower and must be replayed
    // via Raft log on restart.
    let remaining_leader_idx =
        engines.iter().position(|e| e.node_id() == leader_info.leader_id).unwrap();

    for i in 0..10u8 {
        engines[remaining_leader_idx]
            .client()
            .put(
                format!("key-post-crash-{i:02}").into_bytes(),
                format!("val-{i:02}").into_bytes(),
            )
            .await?;
    }
    info!("Phase 4 complete: 10 more entries committed while follower was offline");

    // ── Phase 5: Restart the crashed follower ────────────────────────────────
    // Open the crash-snapshot copy (Phase 3), not the original path — it
    // reflects exactly what survived the simulated crash. The Raft log WAL on
    // disk is intact; the SM may be behind (last_applied stale or 0 if
    // MemTable was lost). The follower reconnects, discovers the gap, and
    // replays missing entries.
    info!("Restarting crashed follower node {}", follower_id);

    let (storage, state_machine) = RocksDBUnifiedEngine::open(&crash_snapshot_path)?;

    // Rejoining node needs a longer election timeout than a fresh node — must
    // outlast the replication worker's max reconnect backoff (1000ms default),
    // otherwise it times out and disrupts the cluster before the leader's first
    // heartbeat can reach it (see create_rejoin_node_config's doc comment).
    let peers: Vec<(u32, u16)> = (0..3).map(|i| ((i + 1) as u32, ports[i])).collect();
    let rejoin_config_str =
        create_rejoin_node_config(follower_id as u32, ports[follower_idx], &peers);
    tokio::fs::write(&crashed_config.1, &rejoin_config_str).await?;

    let restarted = DefaultEmbeddedEngine::start_custom(
        &crash_snapshot_path,
        Arc::new(storage),
        Arc::new(state_machine),
        Some(&crashed_config.1),
    )
    .await?;

    // ── Phase 6: Wait for the follower to catch up via Raft log replay ────────
    // The leader sends AppendEntries covering the gap. The SM re-applies all
    // missing entries — both the pre-crash entries (potentially lost from MemTable)
    // and the post-crash entries written while offline.
    restarted.wait_ready(Duration::from_secs(30)).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Phase 6 complete: restarted follower has rejoined the cluster");

    // ── Phase 7: Verify SM consistency ───────────────────────────────────────
    // All 20 entries must be present on the recovered follower. Missing entries
    // prove that the recovery path (Raft log replay) is correct and complete.
    for i in 0..10u8 {
        let key = format!("key-pre-crash-{i:02}").into_bytes();
        let val = restarted.client().get_eventual(key).await?;
        assert_eq!(
            val.as_deref(),
            Some(format!("val-{i:02}").as_bytes()),
            "key-pre-crash-{i:02} must be recovered via Raft log replay (was in SM MemTable at crash time)"
        );
    }
    for i in 0..10u8 {
        let key = format!("key-post-crash-{i:02}").into_bytes();
        let val = restarted.client().get_eventual(key).await?;
        assert_eq!(
            val.as_deref(),
            Some(format!("val-{i:02}").as_bytes()),
            "key-post-crash-{i:02} must be replicated to recovered follower (was committed while offline)"
        );
    }
    info!("Phase 7 complete: all 20 entries verified on the recovered follower");

    restarted.stop().await?;
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}

/// Verify that the old leader's state machine recovers after an abrupt crash,
/// a new leader is elected, and the old leader rejoins as a follower.
///
/// Leader crash is a more complex scenario than follower crash because:
/// 1. The cluster must run a new election before it can accept more writes.
/// 2. The old leader restarts with a potentially stale term and last_applied.
/// 3. It must accept the new leader's authority and replay any missing entries.
///
/// With `disableWAL=true`, entries applied by the leader's SM before the crash
/// may not have reached SST. Raft log WAL guarantees they can be replayed on restart.
#[tokio::test]
async fn test_leader_sm_recovers_after_abrupt_crash() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    // ── Phase 1: Start a 3-node embedded cluster ──────────────────────────────
    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut engines = Vec::new();
    let mut configs = Vec::new();
    let mut db_paths = Vec::new();
    let mut sm_handles = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let node_db_root = db_root.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");
        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;
        let config_path = format!("/tmp/d-engine-test-sm-crash-leader-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        configs.push((config_str, config_path));
        db_paths.push(db_path);

        let sm_for_crash = Arc::new(state_machine);

        let engine = DefaultEmbeddedEngine::start_custom(
            db_paths.last().unwrap(),
            Arc::new(storage),
            Arc::clone(&sm_for_crash),
            Some(&configs[i].1),
        )
        .await?;
        engines.push(engine);
        sm_handles.push(sm_for_crash);
    }

    // ── Phase 2: Elect a leader and write pre-crash entries ───────────────────
    // Write 10 committed entries. All are majority-replicated so they survive
    // any leader crash — Raft guarantees committed entries are never lost.
    let initial_leader_info = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("cluster failed to elect initial leader");
    info!(
        "Initial leader: node {} (term {})",
        initial_leader_info.leader_id, initial_leader_info.term
    );

    let leader_idx = (initial_leader_info.leader_id - 1) as usize;

    for i in 0..10u8 {
        engines[leader_idx]
            .client()
            .put(
                format!("key-pre-crash-{i:02}").into_bytes(),
                format!("val-{i:02}").into_bytes(),
            )
            .await?;
    }

    // Allow replication to reach followers so they can win an election immediately.
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("Phase 2 complete: 10 entries committed by leader and replicated");

    // ── Phase 3: Crash the leader — simulate kill -9 ──────────────────────────
    // close_db_for_crash_simulation() + the pre-stop snapshot below mean the
    // leader's SM MemTable (disableWAL=true) is not flushed to SST. The Raft
    // log WAL survives. The remaining two followers form a new quorum and
    // elect a new leader.
    info!(
        "Crashing leader node {} (SM MemTable loss simulated, network layer stopped cleanly)",
        initial_leader_info.leader_id
    );

    let crashed_leader_engine = engines.remove(leader_idx);
    let crashed_leader_config = configs.remove(leader_idx);
    let crashed_leader_db_path = db_paths.remove(leader_idx);
    let crashed_leader_sm = sm_handles.remove(leader_idx);

    // Quiesce RocksDB background compaction/flush (no data flush!) so the
    // directory is stable while we snapshot it, then copy what's on disk right
    // now — this is what a real kill -9 would leave behind. The SM's db slot
    // is cleared here, so the later stop() call's internal close_db() finds
    // nothing to flush (errors out harmlessly) — MemTable data loss is
    // already locked in by the time the snapshot is taken, below.
    crashed_leader_sm.close_db_for_crash_simulation();
    let crashed_leader_snapshot_path = crashed_leader_db_path.with_file_name("db_crash_snapshot");
    copy_dir_all(&crashed_leader_db_path, &crashed_leader_snapshot_path)?;
    // Graceful stop (not mem::forget) so the gRPC server/replication connections
    // are properly torn down — mem::forget leaked networking state, preventing
    // the surviving nodes from ever detecting the leader was gone.
    crashed_leader_engine.stop().await?;

    // Wait for background tasks to release the port so the remaining nodes can
    // detect the leader as gone (no heartbeat) and start an election.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Phase 4: Wait for new leader election ─────────────────────────────────
    // The two surviving followers detect the missing heartbeat and elect a new leader.
    // This may take up to 2× the election timeout to complete.
    info!("Waiting for surviving nodes to elect a new leader");
    let receivers = engines.iter().map(|e| e.leader_change_notifier()).collect();
    let new_leader_info = wait_for_new_leader(
        receivers,
        initial_leader_info.leader_id,
        Duration::from_secs(30),
    )
    .await;

    assert_ne!(
        new_leader_info.leader_id, initial_leader_info.leader_id,
        "new leader must differ from the crashed leader"
    );
    info!(
        "Phase 4 complete: new leader elected — node {} (term {})",
        new_leader_info.leader_id, new_leader_info.term
    );

    // ── Phase 5: Write post-crash entries under the new leader ────────────────
    // The cluster is operational with 2/3 nodes. Write 10 more entries.
    // The old (crashed) leader is unaware of these; it must receive them on rejoin.
    let new_leader_idx =
        engines.iter().position(|e| e.node_id() == new_leader_info.leader_id).unwrap();

    for i in 0..10u8 {
        engines[new_leader_idx]
            .client()
            .put(
                format!("key-post-crash-{i:02}").into_bytes(),
                format!("val-{i:02}").into_bytes(),
            )
            .await?;
    }
    info!("Phase 5 complete: 10 entries committed under the new leader");

    // ── Phase 6: Restart the old leader using its original data directory ─────
    // The old leader comes back as a plain follower (Raft always starts as Follower).
    // Its SM may have stale last_applied (MemTable was lost on crash).
    // The new leader sends AppendEntries to bring it up to date.
    info!(
        "Restarting old leader node {} as follower",
        initial_leader_info.leader_id
    );

    let (storage, state_machine) = RocksDBUnifiedEngine::open(&crashed_leader_snapshot_path)?;

    let rejoined = DefaultEmbeddedEngine::start_custom(
        &crashed_leader_snapshot_path,
        Arc::new(storage),
        Arc::new(state_machine),
        Some(&crashed_leader_config.1),
    )
    .await?;

    // ── Phase 7: Wait for the rejoined node to catch up ───────────────────────
    // The new leader replicates all missing entries. The old leader's SM applies
    // them in order, recovering any data lost from its MemTable at crash time.
    rejoined.wait_ready(Duration::from_secs(30)).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Phase 7 complete: old leader has rejoined and is catching up");

    // ── Phase 8: Verify SM consistency on the recovered node ─────────────────
    // All 20 entries must be present. Pre-crash entries verify MemTable recovery
    // via Raft log replay. Post-crash entries verify normal catch-up replication.
    for i in 0..10u8 {
        let key = format!("key-pre-crash-{i:02}").into_bytes();
        let val = rejoined.client().get_eventual(key).await?;
        assert_eq!(
            val.as_deref(),
            Some(format!("val-{i:02}").as_bytes()),
            "key-pre-crash-{i:02} must be present: recovered via Raft log replay on the old leader"
        );
    }
    for i in 0..10u8 {
        let key = format!("key-post-crash-{i:02}").into_bytes();
        let val = rejoined.client().get_eventual(key).await?;
        assert_eq!(
            val.as_deref(),
            Some(format!("val-{i:02}").as_bytes()),
            "key-post-crash-{i:02} must be present: replicated from new leader after rejoin"
        );
    }
    info!("Phase 8 complete: all 20 entries verified on the recovered old leader");

    rejoined.stop().await?;
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}
