//! Commit-path delivery tests, retargeted from `commit_handler/default_commit_handler_test.rs`
//! after merging the commit handler into the Raft main loop (#455).
//!
//! Committed entries are handed to the state machine worker by `Raft::process_committed()`, the
//! last stage of each loop iteration (`RaftRoleState::dispatch_committed`), driven by the role's
//! `commit_index` and the `delivered_index` cursor next to it. These tests keep every assertion
//! from the commit-handler tests; only the subject (the Raft loop instead of the commit handler
//! task) and the feed (advancing `commit_index` instead of sending a commit notification) changed.
//!
//! Two rigs:
//! - `Rig` advances `commit_index` and calls `Raft::process_committed()` directly, so a test can
//!   inspect the internal event channel (`MembershipApplied`, `StepDownSelfRemoved`) and the
//!   state-machine apply channel without the loop consuming those events first.
//! - `ShutdownRig` spawns `Raft::run()` to exercise the real shutdown hand-over.

use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::membership_change::Change;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;

use crate::Error;
use crate::InternalEvent;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockStateMachineHandler;
use crate::MockTypeConfig;
use crate::Raft;
use crate::Result;
use crate::StateMachineCommand;
use crate::StateMachineHandler;
use crate::test_utils::MockBuilder;
use crate::test_utils::snapshot_config;

const TEST_TERM: u64 = 1;

enum CommandType {
    Command(Bytes),
    Configuration(Change),
    Noop,
}

fn build_entries(
    commands: Vec<CommandType>,
    term: u64,
) -> Vec<Entry> {
    let mut r = vec![];
    let mut index = 1;
    for c in commands {
        let entry = match c {
            CommandType::Command(data) => Entry {
                index,
                term,
                payload: Some(EntryPayload::command(data)),
            },
            CommandType::Configuration(change) => Entry {
                index,
                term,
                payload: Some(EntryPayload::config(change)),
            },
            CommandType::Noop => Entry {
                index,
                term,
                payload: Some(EntryPayload::noop()),
            },
        };
        r.push(entry);
        index += 1;
    }
    r
}

// ------------------------------------------------------------------------------------------------
// exactly-once delivery rig
// ------------------------------------------------------------------------------------------------

/// Same arithmetic as `DefaultStateMachineHandler::{pending_range, update_pending}`.
struct Progress {
    /// Highest index the state machine has FINISHED applying.
    applied: AtomicU64,
    /// Highest commit index announced so far (only ever raised).
    pending: AtomicU64,
}

impl Progress {
    fn new(initial_applied: u64) -> Self {
        Self {
            applied: AtomicU64::new(initial_applied),
            pending: AtomicU64::new(0),
        }
    }
    fn range(&self) -> Option<RangeInclusive<u64>> {
        let applied = self.applied.load(Ordering::Acquire);
        let pending = self.pending.load(Ordering::Acquire);
        (pending > applied).then(|| (applied + 1)..=pending)
    }
    fn raise_pending(
        &self,
        new_commit: u64,
    ) {
        self.pending.fetch_max(new_commit, Ordering::AcqRel);
    }
}

/// `LOG_LEN` plain commands, optionally with `config_at` replaced by an AddNode change.
fn log_entries(config_at: Option<u64>) -> Vec<Entry> {
    let commands = (1..=200u64)
        .map(|i| {
            if Some(i) == config_at {
                CommandType::Configuration(Change::AddNode(d_engine_proto::common::AddNode {
                    node_id: 9,
                    address: "addr".into(),
                    status: d_engine_proto::common::NodeStatus::Promotable as i32,
                }))
            } else {
                CommandType::Command(Bytes::from(format!("cmd{i}")))
            }
        })
        .collect();
    build_entries(commands, TEST_TERM)
}

/// Mocks for the commit path: a state-machine handler whose `pending_range` mirrors `Progress`, a
/// raft log that serves `get_entries_range`, and a membership whose first config apply optionally
/// fails (to exercise the fatal path).
struct Parts {
    smh: MockStateMachineHandler<MockTypeConfig>,
    log: MockRaftLog,
    membership: MockMembership<MockTypeConfig>,
    progress: Arc<Progress>,
    config_applied: Arc<AtomicUsize>,
    /// How many times `RaftLog::close` was called.
    log_closed: Arc<AtomicUsize>,
}

fn build_parts(
    entries: Vec<Entry>,
    initial_applied: u64,
    first_config_apply_fails: bool,
) -> Parts {
    let progress = Arc::new(Progress::new(initial_applied));

    let mut smh = MockStateMachineHandler::new();
    let p = progress.clone();
    smh.expect_pending_range().returning(move || p.range());
    let p = progress.clone();
    smh.expect_update_pending().returning(move |n| p.raise_pending(n));
    let p = progress.clone();
    smh.expect_last_applied().returning(move || p.applied.load(Ordering::Acquire));

    let mut log = MockRaftLog::new();
    log.expect_load_hard_state().returning(|| Ok(None));
    log.expect_save_hard_state().returning(|_| Ok(()));
    log.expect_get_entries_range().returning(move |range| {
        Ok(entries.iter().filter(|e| range.contains(&e.index)).cloned().collect())
    });
    // `close` is only reached when `Raft::run()` ends; the direct-drive rig never calls it.
    let log_closed = Arc::new(AtomicUsize::new(0));
    let closes = log_closed.clone();
    log.expect_close().returning(move || {
        closes.fetch_add(1, Ordering::SeqCst);
    });

    let config_applied = Arc::new(AtomicUsize::new(0));
    let calls = config_applied.clone();
    let mut membership = MockMembership::new();
    membership.expect_notify_config_applied().returning(|_| {});
    membership.expect_apply_config_change().returning(move |_| {
        let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
        if first_config_apply_fails && n == 1 {
            Err(Error::Fatal(
                "config change could not be applied".to_string(),
            ))
        } else {
            Ok(())
        }
    });

    Parts {
        smh,
        log,
        membership,
        progress,
        config_applied,
        log_closed,
    }
}

/// Drives the commit path synchronously through `Raft::handle_internal_event`, leaving the
/// internal event channel readable so a test can assert on `MembershipApplied`/`FatalError`.
struct Rig {
    raft: Raft<MockTypeConfig>,
    progress: Arc<Progress>,
    /// One element per `ApplyEntries` batch the (fake) state machine worker received.
    delivered: Arc<std::sync::Mutex<Vec<Vec<u64>>>>,
    config_applied: Arc<AtomicUsize>,
}

impl Rig {
    fn start(
        entries: Vec<Entry>,
        initial_applied: u64,
        first_config_apply_fails: bool,
    ) -> Self {
        Self::build(entries, initial_applied, first_config_apply_fails, false)
    }

    /// Same as `start`, but the state-machine worker's receiving end is dropped so that
    /// `send_to_sm_worker` fails — simulates the worker task having already exited.
    fn start_with_closed_worker(
        entries: Vec<Entry>,
        initial_applied: u64,
        first_config_apply_fails: bool,
    ) -> Self {
        Self::build(entries, initial_applied, first_config_apply_fails, true)
    }

    fn build(
        entries: Vec<Entry>,
        initial_applied: u64,
        first_config_apply_fails: bool,
        closed_worker: bool,
    ) -> Self {
        let parts = build_parts(entries, initial_applied, first_config_apply_fails);

        let (sm_tx, sm_rx) = mpsc::unbounded_channel::<StateMachineCommand>();
        let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
        if closed_worker {
            drop(sm_rx);
        } else {
            let sink = delivered.clone();
            tokio::spawn(async move {
                let mut sm_rx = sm_rx;
                while let Some(cmd) = sm_rx.recv().await {
                    if let StateMachineCommand::ApplyEntries { entries, .. } = cmd {
                        sink.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
                    }
                }
            });
        }

        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let raft = MockBuilder::new(shutdown_rx)
            .with_state_machine_handler(parts.smh)
            .with_raft_log(parts.log)
            .with_membership(parts.membership)
            .with_state_machine_commands(crate::StateMachineCommandSender::new(sm_tx))
            .turn_on_election(false)
            .build_raft();

        Self {
            raft,
            progress: parts.progress,
            delivered,
            config_applied: parts.config_applied,
        }
    }

    /// Advance the role's `commit_index`, exactly as the role layer does, then run the stage of
    /// the loop iteration that hands newly committed entries to the state machine worker.
    async fn notify(
        &mut self,
        commit_index: u64,
    ) -> Result<()> {
        self.raft.role.state_mut().update_commit_index(commit_index)?;
        self.raft.process_committed().await
    }

    /// Wait until nothing more is being delivered.
    async fn settle(&self) {
        let mut stable = 0;
        let mut last = self.delivered_flat().len();
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = self.delivered_flat().len();
            stable = if now == last { stable + 1 } else { 0 };
            last = now;
            if stable >= 3 {
                return;
            }
        }
    }

    /// The state machine finished applying everything up to `index`.
    fn complete_apply(
        &self,
        index: u64,
    ) {
        self.progress.applied.fetch_max(index, Ordering::AcqRel);
    }

    /// A snapshot install moved the state machine to `index` without going through entries.
    fn jump_applied_to(
        &self,
        index: u64,
    ) {
        self.progress.applied.store(index, Ordering::Release);
    }

    fn delivered_flat(&self) -> Vec<u64> {
        self.delivered.lock().unwrap().iter().flatten().copied().collect()
    }

    fn assert_delivered(
        &self,
        expected: Vec<u64>,
        why: &str,
    ) {
        let batches = self.delivered.lock().unwrap().clone();
        assert_eq!(
            self.delivered_flat(),
            expected,
            "{why}\n  batches handed to the state machine, in order: {batches:?}"
        );
    }

    fn drain_events(&mut self) -> Vec<InternalEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = self.raft.internal_event_rx.try_recv() {
            out.push(ev);
        }
        out
    }

    /// Drop the internal-event receiver so sends to `internal_event_tx` fail — exercises the
    /// warn-only paths for `MembershipApplied` / `StepDownSelfRemoved`.
    fn drop_event_receiver(&mut self) {
        let (_dummy_tx, dummy_rx) = mpsc::unbounded_channel::<InternalEvent>();
        let old_rx = std::mem::replace(&mut self.raft.internal_event_rx, dummy_rx);
        drop(old_rx);
    }
}

fn range(r: RangeInclusive<u64>) -> Vec<u64> {
    r.collect()
}

/// Spawns `Raft::run()` to exercise the real shutdown hand-over; observes only the delivered
/// apply batches (the loop consumes its own internal events). The commit index is advanced before
/// the loop starts, so shutdown can be raced against commits the node already knows about.
struct ShutdownRig {
    raft: Option<Raft<MockTypeConfig>>,
    internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    delivered: Arc<std::sync::Mutex<Vec<Vec<u64>>>>,
    handle: Option<JoinHandle<Result<()>>>,
    shutdown_tx: watch::Sender<()>,
    log_closed: Arc<AtomicUsize>,
    /// Sender side of the inbound event channel, as gRPC stream handlers hold it.
    event_tx: mpsc::Sender<crate::InboundEvent>,
    /// Set by `spawn_loop_keeping_raft`: the loop's result together with the `Raft` itself.
    kept: Option<JoinHandle<(Result<()>, Raft<MockTypeConfig>)>>,
}

impl ShutdownRig {
    fn start(
        entries: Vec<Entry>,
        initial_applied: u64,
        first_config_apply_fails: bool,
    ) -> Self {
        let parts = build_parts(entries, initial_applied, first_config_apply_fails);

        let (sm_tx, sm_rx) = mpsc::unbounded_channel::<StateMachineCommand>();
        let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = delivered.clone();
        tokio::spawn(async move {
            let mut sm_rx = sm_rx;
            while let Some(cmd) = sm_rx.recv().await {
                if let StateMachineCommand::ApplyEntries { entries, .. } = cmd {
                    sink.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
                }
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let raft = MockBuilder::new(shutdown_rx)
            .with_state_machine_handler(parts.smh)
            .with_raft_log(parts.log)
            .with_membership(parts.membership)
            .with_state_machine_commands(crate::StateMachineCommandSender::new(sm_tx))
            .turn_on_election(false)
            .build_raft();
        let internal_event_tx = raft.internal_event_tx.clone();
        let event_tx = raft.event_sender();

        Self {
            raft: Some(raft),
            internal_event_tx,
            delivered,
            handle: None,
            shutdown_tx,
            log_closed: parts.log_closed,
            event_tx,
            kept: None,
        }
    }

    /// The node learns that `commit_index` is committed (before the loop starts).
    fn announce_commit(
        &mut self,
        commit_index: u64,
    ) {
        self.raft
            .as_mut()
            .expect("the loop has not started")
            .role
            .state_mut()
            .update_commit_index(commit_index)
            .unwrap();
    }

    /// One delivery stage of the loop, before the loop starts.
    async fn deliver_now(&mut self) {
        self.raft
            .as_mut()
            .expect("the loop has not started")
            .process_committed()
            .await
            .unwrap();
    }

    /// Like `spawn_loop`, but hands the `Raft` back when `run()` ends. In a real node the `Raft`
    /// outlives `run()` (it sits in the `Node`), so its state after `run()` is what matters.
    fn spawn_loop_keeping_raft(&mut self) {
        let mut raft = self.raft.take().expect("the loop has already started");
        self.kept = Some(tokio::spawn(async move {
            let result = raft.run().await;
            (result, raft)
        }));
    }

    async fn end_of_loop_keeping_raft(&mut self) -> (Result<()>, Raft<MockTypeConfig>) {
        time::timeout(
            Duration::from_secs(2),
            self.kept.as_mut().expect("loop not started"),
        )
        .await
        .expect("run() must end")
        .expect("the commit path must not panic")
    }

    fn spawn_loop(&mut self) {
        let mut raft = self.raft.take().expect("the loop has already started");
        self.handle = Some(tokio::spawn(async move { raft.run().await }));
    }

    /// Wait for `run()` to exit (accepting either Ok or Err) and for delivery to settle.
    async fn join_and_settle(&mut self) {
        let handle = self.handle.as_mut().expect("the loop has not started");
        let _ = time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("the commit path must exit after shutdown")
            .expect("the commit path must not panic");
        self.settle().await;
    }

    async fn settle(&self) {
        let mut stable = 0;
        let mut last = self.delivered_flat().len();
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = self.delivered_flat().len();
            stable = if now == last { stable + 1 } else { 0 };
            last = now;
            if stable >= 3 {
                return;
            }
        }
    }

    fn delivered_flat(&self) -> Vec<u64> {
        self.delivered.lock().unwrap().iter().flatten().copied().collect()
    }

    fn assert_delivered(
        &self,
        expected: Vec<u64>,
        why: &str,
    ) {
        let batches = self.delivered.lock().unwrap().clone();
        assert_eq!(
            self.delivered_flat(),
            expected,
            "{why}\n  batches handed to the state machine, in order: {batches:?}"
        );
    }
}

// ================================================================================================
// Exactly-once delivery contract (#455)
// ================================================================================================
//
// CONTRACT: every committed log entry is handed to the state machine worker EXACTLY ONCE, in log
// order, with no gaps — no matter how the commit notifications arrive (back to back, repeated,
// stale) or how far behind the state machine is.

/// A second commit notification arrives while the state machine has not applied the first batch
/// yet (the normal case under load). Entries 1..=5 must not be delivered again.
#[tokio::test]
async fn test_commit_notifications_while_apply_is_pending_deliver_each_entry_once() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    rig.notify(5).await.unwrap();
    rig.settle().await;
    rig.notify(10).await.unwrap(); // state machine still has not finished anything
    rig.settle().await;
    rig.notify(15).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(
        range(1..=15),
        "each committed entry must be handed to the state machine exactly once",
    );
}

/// The commit index advances several times before the delivery stage runs (e.g. several
/// AppendResults in one loop iteration). Only the latest value matters: one delivery covers the
/// whole range, as a single batch, with nothing queued in between.
#[tokio::test]
async fn test_several_commit_advances_before_one_delivery_are_delivered_as_one_batch() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    for commit_index in [3, 5, 8, 12] {
        rig.raft.role.state_mut().update_commit_index(commit_index).unwrap();
    }
    rig.raft.process_committed().await.unwrap();
    rig.settle().await;

    let batches = rig.delivered.lock().unwrap().clone();
    assert_eq!(
        batches,
        vec![range(1..=12)],
        "one delivery must cover everything up to the latest commit index, as a single batch"
    );
}

/// The same notification repeated (leader re-announces the same commit index, e.g. on every ack)
/// must not make the commit path re-read and re-send anything.
#[tokio::test]
async fn test_repeated_identical_notification_does_not_redeliver() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    for _ in 0..3 {
        rig.notify(8).await.unwrap();
        rig.settle().await;
    }
    rig.assert_delivered(
        range(1..=8),
        "a repeated commit index must deliver nothing new",
    );
}

/// A stale (lower) notification arriving after a higher one must be ignored.
#[tokio::test]
async fn test_stale_lower_notification_does_not_redeliver() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    rig.notify(10).await.unwrap();
    rig.settle().await;
    rig.notify(4).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(1..=10), "a lower commit index must deliver nothing");
}

/// 100 single-step notifications in a row while nothing is ever reported as applied: the state
/// machine must still see 1..=100 once each, contiguous and ordered.
#[tokio::test]
async fn test_burst_of_single_step_notifications_delivers_each_entry_once() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    for i in 1..=100 {
        rig.notify(i).await.unwrap();
    }
    rig.settle().await;
    rig.assert_delivered(
        range(1..=100),
        "a burst of notifications must neither duplicate nor skip entries",
    );
}

/// Guard: when the state machine reports progress between notifications, only the NEW entries are
/// delivered.
#[tokio::test]
async fn test_apply_progress_between_notifications_delivers_only_new_entries() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    rig.notify(5).await.unwrap();
    rig.settle().await;
    rig.complete_apply(5);
    rig.notify(9).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(
        range(1..=9),
        "entries 1..=5 were applied; only 6..=9 are new",
    );
}

/// Guard: announcing a commit index the state machine already reached delivers nothing.
#[tokio::test]
async fn test_notification_for_already_applied_index_delivers_nothing() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    rig.notify(5).await.unwrap();
    rig.settle().await;
    rig.complete_apply(5);
    rig.notify(5).await.unwrap();
    rig.notify(3).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(1..=5), "nothing above index 5 was committed");
}

/// Guard: after a restart the state machine is already at index 20. Entries up to 20 are never
/// delivered, whatever commit index is announced first.
#[tokio::test]
async fn test_entries_at_or_below_initial_applied_index_are_never_delivered() {
    let mut rig = Rig::start(log_entries(None), 20, false);
    rig.notify(10).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(
        vec![],
        "commit index 10 is below what is already applied (20)",
    );
    rig.notify(25).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(21..=25), "only 21..=25 are new");
}

/// Guard: a snapshot install moves the state machine past entries it never saw. Delivery resumes
/// after the snapshot point and never goes back for the covered entries.
#[tokio::test]
async fn test_snapshot_install_jump_skips_covered_entries() {
    let mut rig = Rig::start(log_entries(None), 0, false);
    rig.notify(10).await.unwrap();
    rig.settle().await;
    rig.complete_apply(10);
    rig.notify(30).await.unwrap();
    rig.settle().await; // 11..=30 delivered, not yet reported applied
    rig.jump_applied_to(50); // snapshot at index 50 installed
    rig.notify(60).await.unwrap();
    rig.settle().await;
    let mut expected = range(1..=30);
    expected.extend(range(51..=60));
    rig.assert_delivered(expected, "after the snapshot at 50 only 51..=60 are new");
}

/// A committed membership change is applied to the membership exactly once, even when the commit
/// path is notified again before the state machine reports the config entry applied.
#[tokio::test]
async fn test_config_change_is_applied_exactly_once_when_commits_are_announced_again() {
    let mut rig = Rig::start(log_entries(Some(3)), 0, false);
    rig.notify(3).await.unwrap(); // contains the AddNode at index 3
    rig.settle().await;
    rig.notify(5).await.unwrap(); // state machine has not reported anything applied
    rig.settle().await;
    assert_eq!(
        rig.config_applied.load(Ordering::SeqCst),
        1,
        "the AddNode at index 3 must change the membership once"
    );
    let applied_events = rig
        .drain_events()
        .into_iter()
        .filter(|e| matches!(e, InternalEvent::MembershipApplied))
        .count();
    assert_eq!(
        applied_events, 1,
        "one committed change => one MembershipApplied event"
    );
    rig.assert_delivered(range(1..=5), "entries are still delivered exactly once");
}

/// A committed membership change that cannot be applied is FATAL for the node (owner decision,
/// #455): the cluster has already agreed on it, so a node that skips it holds a membership view
/// that no longer matches its own log, and nothing can repair that. The commit path returns a
/// fatal error (which stops the loop), and no `MembershipApplied` is announced for a change that
/// failed (#209).
#[tokio::test]
async fn test_failed_config_change_raises_fatal_error_and_announces_nothing() {
    let mut rig = Rig::start(log_entries(Some(3)), 0, true);
    let result = rig.notify(3).await; // the AddNode at index 3 fails to apply
    rig.settle().await;
    rig.complete_apply(3);

    assert!(
        matches!(result, Err(ref e) if e.is_fatal()),
        "a committed membership change that cannot be applied must be fatal"
    );
    let events = rig.drain_events();
    assert!(
        !events.iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "MembershipApplied must not be announced for a change that failed"
    );
}

/// An entry whose payload kind is missing (corrupt / hostile peer) must never panic the commit
/// path (no panics in embedded core paths). Later entries are still delivered.
#[tokio::test]
async fn test_entry_with_missing_payload_kind_does_not_panic_the_commit_path() {
    let mut entries = log_entries(None);
    entries[1].payload = Some(EntryPayload { payload: None }); // index 2: oneof not set
    let mut rig = Rig::start(entries, 0, false);
    let result = rig.notify(3).await;
    rig.settle().await;

    assert!(
        result.is_ok(),
        "the commit path must not return an error (panic) on an entry with an empty payload kind"
    );
    let fatal = rig.drain_events().iter().any(|e| matches!(e, InternalEvent::FatalError { .. }));
    assert!(
        fatal || rig.delivered_flat().contains(&3),
        "after the malformed entry, later entries were not delivered and no FatalError was raised \
         (delivered: {:?})",
        rig.delivered_flat()
    );
}

/// An entry with no payload at all (not just an unset payload kind) is skipped the same way:
/// no panic, no error, and the entries around it are still delivered.
#[tokio::test]
async fn test_entry_without_any_payload_is_skipped_and_later_entries_are_delivered() {
    let mut entries = log_entries(None);
    entries[1].payload = None; // index 2: no payload at all
    let mut rig = Rig::start(entries, 0, false);
    let result = rig.notify(3).await;
    rig.settle().await;

    assert!(
        result.is_ok(),
        "an entry with no payload must not fail the commit path"
    );
    rig.assert_delivered(
        vec![1, 3],
        "the malformed entry is skipped, its neighbours delivered",
    );
}

// ------------------------------------------------------------------------------------------------
// graceful shutdown
// ------------------------------------------------------------------------------------------------

/// CONTRACT (owner decision, #455): on graceful shutdown, every commit the node already knows
/// about is handed to the state machine worker before the commit path exits. 30 rounds make a
/// race between shutdown and the loop fail reliably rather than by luck.
#[tokio::test]
async fn test_graceful_shutdown_hands_all_announced_commits_to_the_state_machine() {
    for round in 0..30 {
        let mut rig = ShutdownRig::start(log_entries(None), 0, false);
        // The commit and the shutdown are both known before the loop gets to run at all.
        rig.announce_commit(10);
        rig.spawn_loop();
        rig.shutdown_tx.send(()).unwrap();

        rig.join_and_settle().await;
        rig.assert_delivered(
            range(1..=10),
            &format!("round {round}: shutdown dropped an announced commit"),
        );
    }
}

/// Runs the real `Raft::run()` and the real `StateMachineWorker`, with commit 10 announced before
/// the loop starts, and returns the indices the state machine applied.
///
/// It shuts down the way `Node::run` does: signal the Raft loop, wait until it has ended, and only
/// then put `Shutdown` into the worker's queue. The runtime is single-threaded, so spawn order is
/// poll order and both starting orders are reproduced exactly.
///
/// Why this exists: the tests above stand in a fake worker, so they cannot see whether the loop's
/// final hand-over really reaches a worker and is applied before it stops.
async fn applied_after_shutdown_with_real_worker(worker_first: bool) -> Vec<u64> {
    let parts = build_parts(log_entries(None), 0, false);

    let applied = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = applied.clone();
    let mut writer = crate::MockStateMachineWriterOps::new();
    writer.expect_apply_chunk().returning(move |entries| {
        sink.lock().unwrap().extend(entries.iter().map(|e| e.index));
        Ok(entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect())
    });
    // The worker closes the state machine as its last act.
    writer.expect_close().returning(|| ());

    let (sm_tx, sm_rx) = mpsc::unbounded_channel::<StateMachineCommand>();
    // The worker reports apply results here; the receiver must stay open or the worker fails.
    let (worker_events_tx, _worker_events_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let worker = crate::StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(writer),
        sm_tx.clone(),
        sm_rx,
        worker_events_tx,
    );
    let owner_tx = sm_tx.clone();
    let mut raft = MockBuilder::new(shutdown_rx)
        .with_state_machine_handler(parts.smh)
        .with_raft_log(parts.log)
        .with_membership(parts.membership)
        .with_state_machine_commands(crate::StateMachineCommandSender::new(sm_tx))
        .turn_on_election(false)
        .build_raft();
    raft.role.state_mut().update_commit_index(10).unwrap();

    let (worker_handle, raft_handle);
    if worker_first {
        worker_handle = tokio::spawn(worker.run());
        raft_handle = tokio::spawn(async move { raft.run().await });
    } else {
        raft_handle = tokio::spawn(async move { raft.run().await });
        worker_handle = tokio::spawn(worker.run());
    }
    shutdown_tx.send(()).unwrap();

    let _ = time::timeout(Duration::from_secs(2), raft_handle)
        .await
        .expect("the raft loop must exit after shutdown")
        .expect("the raft loop must not panic");
    // The loop has ended, so everything it will ever hand over is already queued.
    owner_tx.send(StateMachineCommand::Shutdown).unwrap();
    let _ = time::timeout(Duration::from_secs(2), worker_handle)
        .await
        .expect("the state machine worker must exit after shutdown")
        .expect("the state machine worker must not panic");
    applied.lock().unwrap().clone()
}

/// CONTRACT (owner decision, #455): graceful shutdown must not finish with a commit the node
/// already knew about still unapplied in the state machine. Here the worker is spawned first and
/// is already waiting on its queue when the loop makes its final hand-over.
#[tokio::test]
async fn test_graceful_shutdown_applies_announced_commits_when_worker_is_spawned_first() {
    assert_eq!(
        applied_after_shutdown_with_real_worker(true).await,
        range(1..=10),
        "announced commits were not applied after a graceful shutdown"
    );
}

/// Same contract, other starting order: the loop is spawned first, so its final hand-over is
/// queued before the worker has run at all.
#[tokio::test]
async fn test_graceful_shutdown_applies_announced_commits_when_loop_is_spawned_first() {
    assert_eq!(
        applied_after_shutdown_with_real_worker(false).await,
        range(1..=10),
        "announced commits were not applied after a graceful shutdown"
    );
}

/// The commit advanced in several steps and only the first steps were delivered before shutdown:
/// shutdown delivers the rest, and every entry is delivered exactly once.
#[tokio::test]
async fn test_graceful_shutdown_with_partly_delivered_commits_delivers_everything_once() {
    for round in 0..30 {
        let mut rig = ShutdownRig::start(log_entries(None), 0, false);
        rig.announce_commit(2);
        rig.deliver_now().await;
        rig.announce_commit(6);
        rig.deliver_now().await;
        rig.announce_commit(10); // known, not yet delivered
        rig.spawn_loop();
        rig.shutdown_tx.send(()).unwrap();

        rig.join_and_settle().await;
        rig.assert_delivered(
            range(1..=10),
            &format!("round {round}: commits were lost or duplicated at shutdown"),
        );
    }
}

/// Guard: shutdown with nothing pending exits promptly and delivers nothing.
#[tokio::test]
async fn test_graceful_shutdown_with_no_pending_commit_exits_cleanly() {
    let mut rig = ShutdownRig::start(log_entries(None), 0, false);
    rig.spawn_loop();
    rig.shutdown_tx.send(()).unwrap();
    rig.join_and_settle().await;
    rig.assert_delivered(vec![], "nothing was committed");
}

/// A failed membership change is fatal and stops the loop; a shutdown that follows must not
/// deliver the same entries a second time.
#[tokio::test]
async fn test_shutdown_after_failed_config_change_does_not_redeliver_entries() {
    let mut rig = ShutdownRig::start(log_entries(Some(3)), 0, true);
    rig.announce_commit(5); // AddNode at index 3 fails to apply
    rig.spawn_loop();
    // Wake the loop so its delivery stage runs and hits the failing config change.
    rig.internal_event_tx.send(InternalEvent::LeaderDiscovered(1, 1)).unwrap();
    rig.settle().await;
    // The loop has already stopped after the fatal error: nobody listens then.
    let _ = rig.shutdown_tx.send(());
    rig.join_and_settle().await;

    let flat = rig.delivered_flat();
    let mut unique = flat.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        flat.len(),
        unique.len(),
        "entries delivered twice around fatal shutdown: {:?}",
        rig.delivered.lock().unwrap().clone()
    );
}

/// Business scenario: the operator stops a node. `Raft` is the only writer of its log, so before
/// `run()` returns it must have waited for in-flight log writes (`RaftLog::close`), exactly once.
#[tokio::test]
async fn test_raft_log_is_closed_once_when_run_ends_by_shutdown() {
    let mut rig = ShutdownRig::start(log_entries(None), 0, false);
    rig.spawn_loop();
    rig.shutdown_tx.send(()).unwrap();
    rig.join_and_settle().await;

    assert_eq!(
        rig.log_closed.load(Ordering::SeqCst),
        1,
        "the log must be closed exactly once when run() ends by shutdown"
    );
}

/// Business scenario: a committed membership change cannot be applied, which is fatal and ends
/// `run()` with an error; nobody sent a shutdown. The log's in-flight writes must still be waited
/// for before `run()` returns: closing is not something only a clean shutdown gets to do.
#[tokio::test]
async fn test_raft_log_is_closed_when_run_ends_by_fatal_error() {
    let mut rig = ShutdownRig::start(log_entries(Some(3)), 0, true);
    rig.announce_commit(5); // AddNode at index 3 fails to apply
    rig.spawn_loop();
    // Wake the loop so its delivery stage runs and hits the failing config change.
    rig.internal_event_tx.send(InternalEvent::LeaderDiscovered(1, 1)).unwrap();

    let result = time::timeout(Duration::from_secs(2), rig.handle.as_mut().unwrap())
        .await
        .expect("the fatal error must end run()")
        .expect("the commit path must not panic");
    assert!(
        result.is_err(),
        "the failed config change must end run() with an error"
    );

    assert_eq!(
        rig.log_closed.load(Ordering::SeqCst),
        1,
        "the log must be closed even though run() ended by error, not by shutdown"
    );
}

/// Business scenario: the operator stops a node while gRPC stream handlers are feeding it events.
/// Once `run()` has ended nobody consumes events, so the channel must be closed: a handler that
/// is blocked sending, or sends later, then gets an error instead of waiting forever (which would
/// keep `serve_with_shutdown`, and with it the whole node, from ever finishing).
#[tokio::test]
async fn test_event_channel_is_closed_when_run_ends_by_shutdown() {
    let mut rig = ShutdownRig::start(log_entries(None), 0, false);
    rig.spawn_loop_keeping_raft();
    rig.shutdown_tx.send(()).unwrap();
    let (result, _raft) = rig.end_of_loop_keeping_raft().await;

    assert!(result.is_ok());
    assert!(
        rig.event_tx.is_closed(),
        "the event channel must be closed once run() ended"
    );
}

/// Business scenario: same, but `run()` ends because a committed membership change could not be
/// applied (fatal). The stream handlers need the same release; nobody sent a shutdown.
#[tokio::test]
async fn test_event_channel_is_closed_when_run_ends_by_fatal_error() {
    let mut rig = ShutdownRig::start(log_entries(Some(3)), 0, true);
    rig.announce_commit(5); // AddNode at index 3 fails to apply
    rig.spawn_loop_keeping_raft();
    rig.internal_event_tx.send(InternalEvent::LeaderDiscovered(1, 1)).unwrap();
    let (result, _raft) = rig.end_of_loop_keeping_raft().await;

    assert!(
        result.is_err(),
        "the failed config change must end run() with an error"
    );
    assert!(
        rig.event_tx.is_closed(),
        "the event channel must be closed even though run() ended by error, not by shutdown"
    );
}

// ------------------------------------------------------------------------------------------------
// the rig itself
// ------------------------------------------------------------------------------------------------

/// The rig's `Progress` must behave exactly like the real handler's, otherwise the tests above
/// would prove nothing about production. Same inputs => same `pending_range()`.
#[test]
fn test_emulated_progress_matches_real_state_machine_handler() {
    for initial_applied in [0u64, 5, 40] {
        let dir = PathBuf::from(format!("/tmp/test_emulated_progress_{initial_applied}"));
        let real = crate::DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            initial_applied,
            Arc::new(crate::MockStateMachine::new()),
            dir.clone(),
            snapshot_config(dir),
            crate::MockSnapshotPolicy::new(),
        );
        let emulated = Progress::new(initial_applied);
        assert_eq!(real.pending_range(), emulated.range(), "before any commit");
        for commit in [3, 7, 7, 2, 12, 12, 50, 49] {
            real.update_pending(commit);
            emulated.raise_pending(commit);
            assert_eq!(
                real.pending_range(),
                emulated.range(),
                "after update_pending({commit}) with last_applied={initial_applied}"
            );
        }
    }
}

// ------------------------------------------------------------------------------------------------
// process_batch behaviour (config ordering, membership events, self-removal, worker unavailable)
// ------------------------------------------------------------------------------------------------

/// The happy path with all entry types is delivered without error.
#[tokio::test]
async fn test_full_processing_flow() {
    let entries = build_entries(
        vec![
            CommandType::Command(Bytes::from(b"cmd1".to_vec())),
            CommandType::Configuration(Change::AddNode(d_engine_proto::common::AddNode {
                node_id: 1,
                address: "addr".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            })),
            CommandType::Noop,
            CommandType::Command(Bytes::from(b"cmd2".to_vec())),
        ],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(4).await.unwrap();
    rig.settle().await;

    rig.assert_delivered(range(1..=4), "all four committed entries must be delivered");
    assert!(
        rig.drain_events().iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "a successful membership change must announce MembershipApplied"
    );
}

/// A config change removing a non-self node applies cleanly.
#[tokio::test]
async fn test_config_remove_node() {
    let entries = build_entries(
        vec![CommandType::Configuration(Change::RemoveNode(
            d_engine_proto::common::RemoveNode { node_id: 2 }, // not self
        ))],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1).await.unwrap();
    rig.settle().await;

    rig.assert_delivered(
        range(1..=1),
        "the config entry must reach the state machine",
    );
    assert!(
        rig.drain_events().iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "a successful removal must announce MembershipApplied"
    );
}

/// Batch processing under load: 1000 plain commands delivered without error.
#[tokio::test]
async fn test_high_throughput_processing() {
    let mut commands = Vec::new();
    for i in 1..=1000 {
        commands.push(CommandType::Command(Bytes::from(format!("cmd{i}"))));
    }
    let entries = build_entries(commands, 1);

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1000).await.unwrap();
    rig.settle().await;

    rig.assert_delivered(
        range(1..=1000),
        "every command must be delivered exactly once",
    );
}

/// An empty pending range delivers nothing and is not an error.
#[tokio::test]
async fn processes_empty_batch_successfully() {
    let mut rig = Rig::start(vec![], 0, false);
    let result = rig.notify(1).await;
    assert!(result.is_ok());
    rig.assert_delivered(vec![], "an empty batch must deliver nothing");
}

/// A closed state-machine worker channel is reported the same way every other Worker-unavailable
/// error is (non-fatal), not as `Error::Fatal` (reserved for conditions requiring node shutdown).
#[tokio::test]
async fn test_process_batch_reports_worker_unavailable_not_fatal_when_channel_closed() {
    let entries = build_entries(vec![CommandType::Command(Bytes::from(b"cmd1".to_vec()))], 1);
    let mut rig = Rig::start_with_closed_worker(entries, 0, false);

    let err = rig
        .notify(1)
        .await
        .expect_err("process_batch must fail when the SM Worker channel is closed");
    assert!(
        !err.is_fatal(),
        "a closed SM Worker channel must be reported as non-fatal, not Error::Fatal"
    );
}

/// Consecutive commands are handed to the state machine as a single batch.
#[tokio::test]
async fn batches_consecutive_commands() {
    let entries = build_entries(
        vec![
            CommandType::Command(Bytes::from(b"cmd1".to_vec())),
            CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            CommandType::Command(Bytes::from(b"cmd3".to_vec())),
        ],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(3).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(1..=3), "three consecutive commands must be delivered");
}

/// A config change flushes the preceding command batch before it is applied.
#[tokio::test]
async fn flushes_commands_at_config_change() {
    let entries = build_entries(
        vec![
            CommandType::Command(Bytes::from(b"cmd1".to_vec())),
            CommandType::Configuration(Change::AddNode(d_engine_proto::common::AddNode {
                node_id: 1,
                address: "addr".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            })),
            CommandType::Command(Bytes::from(b"cmd2".to_vec())),
        ],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(3).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(
        range(1..=3),
        "commands around a config change must all be delivered",
    );
}

/// A noop flushes the preceding command batch.
#[tokio::test]
async fn flushes_commands_at_noop() {
    let entries = build_entries(
        vec![
            CommandType::Command(Bytes::from(b"cmd1".to_vec())),
            CommandType::Noop,
            CommandType::Command(Bytes::from(b"cmd2".to_vec())),
        ],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(3).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(1..=3), "commands around a noop must all be delivered");
}

/// A config change that fails to apply makes the commit path return a fatal error.
#[tokio::test]
async fn handles_config_failure_properly() {
    let entries = build_entries(
        vec![
            CommandType::Configuration(Change::AddNode(d_engine_proto::common::AddNode {
                node_id: 1,
                address: "addr".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            })),
            CommandType::Command(Bytes::from(b"cmd1".to_vec())),
        ],
        1,
    );

    let mut rig = Rig::start(entries, 0, true);
    let result = rig.notify(2).await;
    assert!(
        result.is_err(),
        "a failed config change must fail the commit path"
    );
}

/// Mixed command/config/noop entries are processed in log order.
#[tokio::test]
async fn processes_mixed_entries_correctly() {
    let entries = build_entries(
        vec![
            CommandType::Command(Bytes::from(b"cmd1".to_vec())),
            CommandType::Configuration(Change::AddNode(d_engine_proto::common::AddNode {
                node_id: 1,
                address: "addr".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            })),
            CommandType::Noop,
            CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            CommandType::Command(Bytes::from(b"cmd3".to_vec())),
        ],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(5).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(1..=5), "mixed entries must all be delivered in order");
}

/// A large batch of commands is delivered in one apply call.
#[tokio::test]
async fn handles_large_command_batches() {
    let mut commands = Vec::new();
    for _ in 1..=1000 {
        commands.push(CommandType::Command(Bytes::from(b"cmd".to_vec())));
    }
    let entries = build_entries(commands, 1);

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1000).await.unwrap();
    rig.settle().await;
    rig.assert_delivered(range(1..=1000), "1000 commands must all be delivered");
}

/// MembershipApplied event MUST be sent after a successful config change (#209).
#[tokio::test]
async fn membership_applied_event_sent_on_success() {
    let entries = build_entries(
        vec![CommandType::Configuration(Change::AddNode(
            d_engine_proto::common::AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            },
        ))],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1).await.unwrap();
    assert!(
        rig.drain_events().iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "a successful config change must send MembershipApplied"
    );
}

/// Event order: MembershipApplied is sent AFTER apply_config_change (#209).
#[tokio::test]
async fn membership_applied_event_sent_after_apply() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));

    let entries = build_entries(
        vec![CommandType::Configuration(Change::AddNode(
            d_engine_proto::common::AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            },
        ))],
        1,
    );

    // Build the rig manually so `apply_config_change` records into `order` before succeeding.
    let order_clone = order.clone();
    let progress = Arc::new(Progress::new(0));
    let mut smh = MockStateMachineHandler::new();
    let p = progress.clone();
    smh.expect_pending_range().returning(move || p.range());
    let p = progress.clone();
    smh.expect_update_pending().returning(move |n| p.raise_pending(n));
    smh.expect_last_applied().returning(|| 0);

    let mut log = MockRaftLog::new();
    log.expect_load_hard_state().returning(|| Ok(None));
    log.expect_save_hard_state().returning(|_| Ok(()));
    let e = entries.clone();
    log.expect_get_entries_range().returning(move |range| {
        Ok(e.iter().filter(|x| range.contains(&x.index)).cloned().collect())
    });
    log.expect_close().returning(|| ());

    let mut membership = MockMembership::new();
    membership.expect_notify_config_applied().returning(|_| {});
    membership.expect_apply_config_change().returning(move |_| {
        order_clone.lock().unwrap().push("apply_config_change");
        Ok(())
    });

    let (sm_tx, sm_rx) = mpsc::unbounded_channel::<StateMachineCommand>();
    tokio::spawn(async move {
        let mut sm_rx = sm_rx;
        while sm_rx.recv().await.is_some() {}
    });

    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut raft = MockBuilder::new(shutdown_rx)
        .with_state_machine_handler(smh)
        .with_raft_log(log)
        .with_membership(membership)
        .with_state_machine_commands(crate::StateMachineCommandSender::new(sm_tx))
        .turn_on_election(false)
        .build_raft();
    raft.role.state_mut().update_commit_index(1).unwrap();
    raft.process_committed().await.unwrap();

    let events: Vec<InternalEvent> = {
        let mut out = Vec::new();
        while let Ok(ev) = raft.internal_event_rx.try_recv() {
            out.push(ev);
        }
        out
    };
    assert!(
        events.iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "a successful config change must send MembershipApplied"
    );
    order.lock().unwrap().push("MembershipApplied_event");

    assert_eq!(
        *order.lock().unwrap(),
        vec!["apply_config_change", "MembershipApplied_event"],
        "Event must be sent AFTER apply_config_change"
    );
}

/// NO MembershipApplied event is sent when a config change fails (#209). A FatalError is allowed
/// and expected (#455).
#[tokio::test]
async fn no_membership_applied_event_on_config_failure() {
    let entries = build_entries(
        vec![CommandType::Configuration(Change::AddNode(
            d_engine_proto::common::AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            },
        ))],
        1,
    );

    let mut rig = Rig::start(entries, 0, true);
    let result = rig.notify(1).await;
    assert!(result.is_err(), "config change should fail");

    let events = rig.drain_events();
    assert!(
        !events.iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "MembershipApplied sent although the membership change failed"
    );
}

/// AddNode, RemoveNode, and Promote all trigger MembershipApplied (#209).
#[tokio::test]
async fn all_config_change_types_send_membership_applied() {
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::common::PromoteLearner;

    // AddNode
    let entries = build_entries(
        vec![CommandType::Configuration(Change::AddNode(
            d_engine_proto::common::AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            },
        ))],
        1,
    );
    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1).await.unwrap();
    assert!(
        rig.drain_events().iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "AddNode should send MembershipApplied"
    );

    // RemoveNode
    let entries = build_entries(
        vec![CommandType::Configuration(Change::RemoveNode(
            d_engine_proto::common::RemoveNode { node_id: 2 },
        ))],
        1,
    );
    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1).await.unwrap();
    assert!(
        rig.drain_events().iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "RemoveNode should send MembershipApplied"
    );

    // PromoteLearner
    let entries = build_entries(
        vec![CommandType::Configuration(Change::Promote(
            PromoteLearner {
                node_id: 2,
                status: NodeStatus::Active as i32,
            },
        ))],
        1,
    );
    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1).await.unwrap();
    assert!(
        rig.drain_events().iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
        "PromoteLearner should send MembershipApplied"
    );
}

/// A self-removal (RemoveNode(my_id)) sends both MembershipApplied and StepDownSelfRemoved, in
/// that order (#209, #200).
#[tokio::test]
async fn self_removal_sends_both_events() {
    let entries = build_entries(
        vec![CommandType::Configuration(Change::RemoveNode(
            d_engine_proto::common::RemoveNode { node_id: 1 }, // self (my_id = 1)
        ))],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.notify(1).await.unwrap();

    let events = rig.drain_events();
    let first = events.iter().position(|e| matches!(e, InternalEvent::MembershipApplied));
    let second = events.iter().position(|e| matches!(e, InternalEvent::StepDownSelfRemoved));
    assert!(
        first.is_some(),
        "self-removal must announce MembershipApplied"
    );
    assert!(
        second.is_some(),
        "self-removal must announce StepDownSelfRemoved"
    );
    assert!(
        first.unwrap() < second.unwrap(),
        "MembershipApplied must precede StepDownSelfRemoved"
    );
}

/// Dropping the event receiver makes the MembershipApplied send fail. The warn path must run
/// without propagating an error.
#[tokio::test]
async fn membership_applied_send_fails_gracefully_when_receiver_dropped() {
    let entries = build_entries(
        vec![CommandType::Configuration(Change::AddNode(
            d_engine_proto::common::AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            },
        ))],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.drop_event_receiver();
    let result = rig.notify(1).await;
    assert!(
        result.is_ok(),
        "send failure on MembershipApplied must not propagate as Err"
    );
}

/// Dropping the event receiver makes the StepDownSelfRemoved send fail. The error path inside the
/// self-removal branch must run without panicking or returning an error.
#[tokio::test]
async fn step_down_send_fails_gracefully_when_receiver_dropped() {
    let entries = build_entries(
        vec![CommandType::Configuration(Change::RemoveNode(
            d_engine_proto::common::RemoveNode { node_id: 1 }, // self (my_id = 1)
        ))],
        1,
    );

    let mut rig = Rig::start(entries, 0, false);
    rig.drop_event_receiver();
    let result = rig.notify(1).await;
    assert!(
        result.is_ok(),
        "send failure on StepDownSelfRemoved must not propagate as Err"
    );
}

// ------------------------------------------------------------------------------------------------
// delivery timing
// ------------------------------------------------------------------------------------------------

/// CONTRACT: a commit that advances in ANY stage of a loop iteration is handed to the state
/// machine worker before that iteration ends — not one wake-up later.
///
/// A follower learns of a commit only from an inbound `AppendEntries` (the LAST stage of an
/// iteration, after the internal-event and client-command stages). The first RPC from a new leader
/// makes the follower emit `LeaderDiscovered`, which wakes the loop a second time and would hide a
/// late delivery; so the test first lets that settle, then sends a second RPC from the same
/// leader that advances the commit. Election is off and nothing else is sent, so nothing can wake
/// the loop again: if delivery ran in an earlier stage, the commit would sit undelivered and the
/// test times out.
#[tokio::test]
async fn test_commit_advanced_by_inbound_append_entries_is_delivered_in_the_same_iteration() {
    use d_engine_proto::server::replication::AppendEntriesRequest;
    use d_engine_proto::server::replication::AppendEntriesResponse;

    use crate::AppendResponseWithUpdates;
    use crate::InboundEvent;
    use crate::MockReplicationCore;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::maybe_clone_oneshot::RaftOneshot;

    let new_commit = 5;
    let mut parts = build_parts(log_entries(None), 0, false);
    parts.log.expect_durable_index().returning(|| 0);
    parts.log.expect_last_entry_id().returning(|| 0);
    parts.log.expect_last_log_id().returning(|| None);
    // The follower workflow looks its own node up in the membership.
    parts.membership.expect_retrieve_node_meta().returning(|_| None);

    // Only the second RPC advances the commit.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut replication = MockReplicationCore::new();
    replication.expect_handle_append_entries().returning(move |_, _, _| {
        let second_call = calls.fetch_add(1, Ordering::SeqCst) == 1;
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(1, 1, None),
            commit_index_update: second_call.then_some(new_commit),
        })
    });

    let (sm_tx, mut sm_rx) = mpsc::unbounded_channel::<StateMachineCommand>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut raft = MockBuilder::new(shutdown_rx)
        .with_state_machine_handler(parts.smh)
        .with_raft_log(parts.log)
        .with_membership(parts.membership)
        .with_replication_handler(replication)
        .with_state_machine_commands(crate::StateMachineCommandSender::new(sm_tx))
        .turn_on_election(false)
        .build_raft();
    let event_tx = raft.event_sender();
    let _loop = tokio::spawn(async move { raft.run().await });

    let append_entries = || {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        InboundEvent::AppendEntries(
            AppendEntriesRequest {
                term: 1,
                leader_id: 5,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit_index: 0,
            },
            vec![resp_tx],
        )
    };

    event_tx.send(append_entries()).await.unwrap();
    time::sleep(Duration::from_millis(100)).await; // leader discovery settles; the loop goes idle
    event_tx.send(append_entries()).await.unwrap();

    // Far below the election timeout (min 500 ms): only the next tick could deliver it late.
    let delivered = time::timeout(Duration::from_millis(300), async {
        loop {
            if let Some(StateMachineCommand::ApplyEntries { entries, .. }) = sm_rx.recv().await {
                return entries.iter().map(|e| e.index).collect::<Vec<_>>();
            }
        }
    })
    .await
    .expect("the commit advanced by AppendEntries was not delivered in the same loop iteration");
    assert_eq!(delivered, range(1..=new_commit));
}
