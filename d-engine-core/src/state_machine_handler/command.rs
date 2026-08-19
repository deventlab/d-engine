//! Commands accepted by `StateMachineWorker` — the single execution point for
//! all state-machine mutations.
//! one serial writer.

use std::path::PathBuf;

use crate::Result;
use crate::SnapshotApplyResult;
use crate::SnapshotError;
use d_engine_proto::common::Entry;
use d_engine_proto::server::storage::SnapshotMetadata;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// A snapshot stream already fully received and decompressed to a local temp
/// directory. Owns the directory's lifetime: dropping it (e.g. once the
/// Worker finishes consuming it) cleans up the files automatically.
#[derive(Debug)]
pub struct PreparedSnapshot {
    pub metadata: SnapshotMetadata,
    pub temp_dir: TempDir,
}

/// A locally-captured snapshot: `generate_snapshot_data` has already run and already
/// committed `SnapshotMetadata` as the state machine's current snapshot (each
/// `StateMachine` adapter does this internally as part of that call) — nothing here
/// still touches the live state machine. `temp_dir` is a path under `snapshots_dir`,
/// not an OS tempdir, so cleanup is the caller's job (mirrors the pre-#436
/// `create_snapshot` behavior).
#[derive(Debug)]
pub struct CapturedLocalSnapshot {
    pub metadata: SnapshotMetadata,
    pub temp_dir: PathBuf,
}

/// The only channel through which anything may mutate the state machine.
/// `StateMachineWorker` is the sole consumer
pub enum StateMachineCommand {
    /// Fire-and-forget by design: completion is reported via
    /// `InternalEvent::ApplyCompleted`, not through this variant, to keep
    /// `CommitHandler` decoupled from apply I/O (see worker.rs doc comment).
    ApplyEntries { entries: Vec<Entry> },

    /// Carries a response channel because the caller (Raft main loop) must
    /// block until the install genuinely completes before replying to the
    /// leader (Raft §7).
    InstallSnapshot {
        snapshot: PreparedSnapshot,
        response: oneshot::Sender<Result<SnapshotApplyResult>>,
    },

    /// Local snapshot capture (#436). Must run through the Worker so it's
    /// ordered against `ApplyEntries`/`InstallSnapshot` — not because it mutates
    /// anything itself (it's read-only against the state machine), but because it
    /// reads `last_applied` and exports/copies the state machine's current content,
    /// and that read must not race a concurrent `apply_chunk`/`apply_snapshot_from_file`.
    CaptureLocalSnapshot {
        response: oneshot::Sender<Result<CapturedLocalSnapshot>>,
    },

    /// Internal-only: sent by CaptureLocalSnapshot's own background task back to the
    /// Worker after the slow export finishes, so "is this still fresh" is decided on
    /// Worker's own single-threaded loop, in the same FIFO order as InstallSnapshot —
    /// never constructed outside `state_machine_handler` (#436).
    LocalSnapshotReady {
        captured: Result<CapturedLocalSnapshot>,
        epoch_before: u64,
        response: oneshot::Sender<Result<CapturedLocalSnapshot>>,
    },
}

/// The only way role code (follower/learner) may reach `StateMachineWorker` — hides the
/// raw command channel so callers never touch `mpsc`/`oneshot` directly and never need to
/// know `WorkerUnavailable` can come from either a closed send or a dropped response.
#[derive(Clone, Debug)]
pub struct StateMachineCommandSender {
    tx: mpsc::UnboundedSender<StateMachineCommand>,
}

impl StateMachineCommandSender {
    pub fn new(tx: mpsc::UnboundedSender<StateMachineCommand>) -> Self {
        Self { tx }
    }

    /// Sends entries to the Worker without waiting for apply to complete — completion is
    /// reported separately via `InternalEvent::ApplyCompleted` (see worker.rs doc comment).
    pub(crate) fn apply_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        self.tx
            .send(StateMachineCommand::ApplyEntries { entries })
            .map_err(|_| SnapshotError::WorkerUnavailable.into())
    }

    /// Sends a prepared snapshot to the Worker and waits for the authoritative install
    /// result. Any failure to reach the Worker or receive its response is reported
    /// uniformly as `WorkerUnavailable` — callers don't need to distinguish "couldn't
    /// send" from "worker died before replying".
    pub(crate) async fn install_snapshot(
        &self,
        snapshot: PreparedSnapshot,
    ) -> Result<SnapshotApplyResult> {
        let (response, response_rx) = oneshot::channel();
        self.tx
            .send(StateMachineCommand::InstallSnapshot { snapshot, response })
            .map_err(|_| SnapshotError::WorkerUnavailable)?;
        response_rx.await.map_err(|_| SnapshotError::WorkerUnavailable)?
    }

    /// Sends a capture request to the Worker and waits for the result. Same
    /// `WorkerUnavailable` uniform-error treatment as `install_snapshot`.
    pub(crate) async fn capture_local_snapshot(&self) -> Result<CapturedLocalSnapshot> {
        let (response, response_rx) = oneshot::channel();
        self.tx
            .send(StateMachineCommand::CaptureLocalSnapshot { response })
            .map_err(|_| SnapshotError::WorkerUnavailable)?;
        response_rx.await.map_err(|_| SnapshotError::WorkerUnavailable)?
    }
}
