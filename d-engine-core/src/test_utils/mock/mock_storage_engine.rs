use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::HardState;
use crate::MockStorageEngine;

// Process-global storage so data persists across threads (e.g. raft-io std::thread vs test thread).
// Tests are isolated by unique instance_id keys; nextest runs each test in its own process.
static MOCK_STORAGE_DATA: LazyLock<Mutex<HashMap<String, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Preset configurations of the mockall-generated `MockStorageEngine`.
///
/// Every preset behaves like a real store: what is written can be read back, and two
/// instances built with the same `id` share the same data (a restarted node).
/// Expectations for the same method are matched in registration order, so a preset that
/// fails once registers its `.once()` expectation before the always-succeeds fallback.
impl MockStorageEngine {
    pub fn with_id(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        Self::configure_flush_success(&mut mock);

        mock
    }

    /// Create a MockStorageEngine whose `flush()` calls are counted.
    ///
    /// Returns the engine and a shared counter incremented on every `flush()` call.
    /// Use this to test the write/flush separation and batch-flush efficiency.
    pub fn with_flush_counter(id: String) -> (Self, Arc<AtomicU64>) {
        let flush_count = Arc::new(AtomicU64::new(0));
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        // configure_flush_counted is called instead of configure_flush_success so its
        // expectation is the only one for flush — no FIFO conflict between the two.
        Self::configure_flush_counted(&mut mock, flush_count.clone());

        (mock, flush_count)
    }

    fn configure_mocks(
        engine: &mut MockStorageEngine,
        instance_id: &str,
    ) {
        let instance_id = instance_id.to_string();

        // Last index implementation with persistence simulation
        engine.expect_last_index().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let key = format!("{instance_id_ref}_last_index");
                data.get(&key)
                    .map(|v| {
                        let bytes: [u8; 8] = v.as_slice().try_into().unwrap();
                        u64::from_be_bytes(bytes)
                    })
                    .unwrap_or(0)
            }
        });

        // persist_entries is NOT configured here — extracted into
        // configure_persist_entries_success() so failure-variants (e.g.
        // first_persist_fails) can install their own expectation
        // first without a mockall FIFO conflict (an always-succeeds catch-all
        // registered here would silently absorb every call before a
        // stricter, later expectation ever got a turn).

        // Get entry implementation with persistence simulation
        engine.expect_entry().returning({
            let instance_id_ref = instance_id.clone();
            move |index| {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let key = format!("{instance_id_ref}_entry_{index}");
                Ok(data.get(&key).map(|v| bincode::deserialize(v).unwrap()))
            }
        });

        // Get entries range implementation with persistence simulation
        engine.expect_get_entries().returning({
            let instance_id_ref = instance_id.clone();
            move |range| {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let mut result = Vec::new();
                for index in *range.start()..=*range.end() {
                    let key = format!("{instance_id_ref}_entry_{index}");
                    if let Some(value) = data.get(&key) {
                        result.push(bincode::deserialize(value).unwrap());
                    }
                }
                Ok(result)
            }
        });

        // purge is NOT configured here — extracted into
        // configure_purge_success() for the same FIFO-ordering reason as
        // persist_entries above.
        engine.expect_load_purge_boundary().returning(|| Ok(None));
        engine.expect_truncate().returning({
            let instance_id_ref = instance_id.clone();
            move |from_index| {
                let mut data = MOCK_STORAGE_DATA.lock().unwrap();
                let prefix = format!("{instance_id_ref}_entry_");
                let keys_to_delete: Vec<String> = data
                    .keys()
                    .filter(|k| {
                        k.strip_prefix(&prefix)
                            .and_then(|s| s.parse::<u64>().ok())
                            .is_some_and(|idx| idx >= from_index)
                    })
                    .cloned()
                    .collect();
                for key in keys_to_delete {
                    data.remove(&key);
                }
                let new_last = from_index.saturating_sub(1);
                let last_key = format!("{instance_id_ref}_last_index");
                if new_last == 0 {
                    data.remove(&last_key);
                } else {
                    data.insert(last_key, new_last.to_be_bytes().to_vec());
                }
                Ok(())
            }
        });
        // save_hard_state is NOT configured here — extracted into
        // configure_save_hard_state_success() for the same FIFO-ordering
        // reason as persist_entries/purge above.
        engine.expect_load_hard_state().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let key = format!("{instance_id_ref}_meta_hard_state");
                Ok(data.get(&key).map(|bytes| {
                    bincode::deserialize::<HardState>(bytes).expect("deserialize hard_state")
                }))
            }
        });
    }

    /// Default `persist_entries()`: always succeeds, writes into
    /// `MOCK_STORAGE_DATA`. Extracted out of `configure_mocks` so that
    /// failure-variants (e.g. `first_persist_fails`) can install
    /// their own stricter expectation *first* — see the comment left in
    /// `configure_mocks` for why ordering here matters (mockall FIFO
    /// matching).
    fn configure_persist_entries_success(
        engine: &mut MockStorageEngine,
        instance_id: &str,
    ) {
        let instance_id_ref = instance_id.to_string();
        engine.expect_persist_entries().returning(move |entries| {
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            for entry in &entries {
                let key = format!("{}_entry_{}", instance_id_ref, entry.index);
                let value = bincode::serialize(entry).unwrap();
                data.insert(key, value);
            }
            if let Some(last_entry) = entries.last() {
                let key = format!("{instance_id_ref}_last_index");
                data.insert(key, last_entry.index.to_be_bytes().to_vec());
            }
            Ok(())
        });
    }

    /// Default `purge()`: always succeeds. Extracted out of `configure_mocks`
    /// for the same reason as persist_entries.
    fn configure_purge_success(engine: &mut MockStorageEngine) {
        engine.expect_purge().returning(|_| Ok(()));
    }

    /// Default `save_hard_state()`: always succeeds, writes into
    /// `MOCK_STORAGE_DATA`. Extracted out of `configure_mocks` for the same
    /// reason as persist_entries/purge.
    fn configure_save_hard_state_success(
        engine: &mut MockStorageEngine,
        instance_id: &str,
    ) {
        let instance_id_ref = instance_id.to_string();
        engine.expect_save_hard_state().returning(move |state| {
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            let key = format!("{instance_id_ref}_meta_hard_state");
            let value = bincode::serialize(state).expect("serialize hard_state");
            data.insert(key, value);
            Ok(())
        });
    }

    /// Create a MockStorageEngine where the **first**
    /// `flush()` call returns an error, simulating a transient fsync failure.
    ///
    /// A failed fsync poisons the log permanently (see `FsyncWorker`). This is the
    /// deterministic way to reach the poisoned state through a real fsync failure.
    pub fn first_flush_fails(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        // First flush fails — leaves pending_max non-zero (only success path zeros it).
        mock.expect_flush()
            .once()
            .returning(|| Err(crate::Error::Fatal("simulated fsync failure".into())));
        // All subsequent flushes succeed.
        mock.expect_flush().returning(|| Ok(()));

        mock
    }

    /// Create a MockStorageEngine where the **first**
    /// `persist_entries()` call (page-cache write) returns an error, simulating a
    /// transient write failure (e.g. disk full, backend rejects the write batch).
    ///
    /// Distinct from `first_flush_fails`, which only fails the later
    /// fsync step — `persist_entries()` and `flush()` are two independent failure
    /// surfaces in `RaftLogCore` (`persist_from_memory` vs
    /// `FsyncWorker::run_until_caught_up`). `flush()` itself always succeeds
    /// here, keeping the two surfaces isolated. All subsequent `persist_entries()`
    /// calls succeed.
    pub fn first_persist_fails(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        mock.expect_flush().returning(|| Ok(()));

        // First persist_entries() call fails. Registered before the
        // always-succeeds fallback below — mockall tries expectations in
        // registration order, so the stricter `.once()` one must come first.
        mock.expect_persist_entries().once().returning(|_| {
            Err(crate::Error::Fatal(
                "simulated persist_entries failure".into(),
            ))
        });
        // All subsequent persist_entries() calls succeed.
        Self::configure_persist_entries_success(&mut mock, &id);

        mock
    }

    /// Create a MockStorageEngine where `truncate()` always fails, simulating a
    /// fatal storage error while a term conflict removes the divergent tail.
    /// `truncate_and_persist_tail` treats this as unrecoverable and poisons the
    /// log — disk state is now uncertain.
    pub fn truncate_fails(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        // Registered before `configure_mocks`' always-succeeds `truncate()` —
        // mockall tries expectations in registration order, so this one always wins.
        mock.expect_truncate()
            .returning(|_| Err(crate::Error::Fatal("simulated truncate failure".into())));
        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        mock.expect_flush().returning(|| Ok(()));

        mock
    }

    /// Create a MockStorageEngine where `purge()` always fails, simulating a
    /// fatal storage error during log compaction.
    pub fn purge_fails(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_save_hard_state_success(&mut mock, &id);
        mock.expect_flush().returning(|| Ok(()));
        mock.expect_purge()
            .returning(|_| Err(crate::Error::Fatal("simulated purge failure".into())));

        mock
    }

    /// Create a MockStorageEngine where `save_hard_state()` always fails,
    /// simulating a fatal storage error while persisting current_term/voted_for.
    pub fn save_hard_state_fails(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        mock.expect_flush().returning(|| Ok(()));
        mock.expect_save_hard_state().returning(|_| {
            Err(crate::Error::Fatal(
                "simulated save_hard_state failure".into(),
            ))
        });

        mock
    }

    /// Create a MockStorageEngine where every
    /// `flush()` call returns an error, simulating a persistent IO device failure.
    ///
    /// Use this to verify that callers (e.g. `flush()`) propagate the error back
    /// rather than hanging forever waiting for a `durable_index` that never advances.
    pub fn always_failing_flush(id: String) -> Self {
        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        mock.expect_flush()
            .returning(|| Err(crate::Error::Fatal("simulated fsync failure".into())));

        mock
    }

    /// Create a MockStorageEngine where the FIRST
    /// `flush()` call blocks until the returned sender is signalled — every call
    /// after that returns `Ok(())` immediately.
    ///
    /// Use this to deterministically test behavior *while* a physical fsync is
    /// in flight (e.g. `durable_index` must not advance yet), without relying on
    /// a fixed `sleep()` and hoping the assertion runs inside the window.
    pub fn gated_flush(id: String) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));

        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        mock.expect_flush().returning(move || {
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            Ok(())
        });

        (mock, tx)
    }

    /// Same as `gated_flush`, but also returns a shared counter of how
    /// many times `flush()` was physically invoked — for asserting how many fsync
    /// rounds a batch of concurrent `flush()` callers actually triggered.
    pub fn gated_flush_counted(id: String) -> (Self, std::sync::mpsc::Sender<()>, Arc<AtomicU64>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));
        let flush_count = Arc::new(AtomicU64::new(0));

        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        let counter = flush_count.clone();
        mock.expect_flush().returning(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            Ok(())
        });

        (mock, tx, flush_count)
    }

    /// Same as `gated_flush`, but every `flush()` call — the first
    /// gated one and any that follow — returns `Err` once unblocked, simulating
    /// a disk failure discovered right as the fsync finally runs.
    pub fn gated_flush_failing(id: String) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));

        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        Self::configure_persist_entries_success(&mut mock, &id);
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        mock.expect_flush().returning(move || {
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            Err(crate::Error::Fatal("simulated fsync failure".into()))
        });

        (mock, tx)
    }

    /// Create a MockStorageEngine where the first `persist_entries()` call blocks
    /// until the returned sender fires. `flush()` is left at
    /// its always-succeeds default (`configure_flush_success`) — this gate is only
    /// about the write-to-storage-engine step, not fsync.
    ///
    /// Use this to freeze the IO thread mid-persist so a concurrent truncation
    /// can be driven deterministically — see
    /// `durable_index_truncation_clamp_test.rs`.
    pub fn gated_persist(id: String) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));

        let mut mock = MockStorageEngine::new();

        Self::configure_mocks(&mut mock, &id);
        // persist_entries is gated below instead of via configure_persist_entries_success.
        Self::configure_purge_success(&mut mock);
        Self::configure_save_hard_state_success(&mut mock, &id);
        Self::configure_flush_success(&mut mock);

        let instance_id_ref = id.clone();
        mock.expect_persist_entries().returning(move |entries| {
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            for entry in &entries {
                let key = format!("{instance_id_ref}_entry_{}", entry.index);
                let value = bincode::serialize(entry).unwrap();
                data.insert(key, value);
            }
            if let Some(last_entry) = entries.last() {
                let key = format!("{instance_id_ref}_last_index");
                data.insert(key, last_entry.index.to_be_bytes().to_vec());
            }
            Ok(())
        });

        (mock, tx)
    }

    /// Configure a `flush()` that always succeeds.
    fn configure_flush_success(engine: &mut MockStorageEngine) {
        engine.expect_flush().returning(|| Ok(()));
    }

    /// Configure a `flush()` that always succeeds and counts its calls.
    fn configure_flush_counted(
        engine: &mut MockStorageEngine,
        flush_count: Arc<AtomicU64>,
    ) {
        let counter = flush_count.clone();
        engine.expect_flush().returning(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
    }
}
