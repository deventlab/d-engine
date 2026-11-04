use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use tempfile::TempDir;

use crate::ApplyEntry;
use crate::Command;
use crate::Error;
use crate::storage::StateMachine;

/// Test suite for StateMachine implementations
///
/// This suite provides comprehensive tests that can be used to validate
/// any StateMachine implementation. Developers should implement the
/// `StateMachineBuilder` trait and then call `run_all_state_machine_tests`
/// with their builder.
pub struct StateMachineTestSuite;

/// Builder trait for creating StateMachine instances for testing
#[async_trait]
pub trait StateMachineBuilder: Send + Sync {
    /// Creates a state machine on new, empty storage. Every call starts from nothing: whatever an
    /// earlier call wrote is not visible, so a test never has to wipe a state machine first.
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error>;

    /// Opens a state machine on the storage the last `build()` or `reopen()` used, with whatever
    /// that storage holds now: a restart. The instance returned by that call has been closed or
    /// dropped first.
    async fn reopen(&self) -> Result<Arc<dyn StateMachine>, Error>;

    /// Opens a state machine on a copy of the storage the last `build()` or `reopen()` used,
    /// exactly as it is on disk right now, without closing or dropping the live one: what a restart
    /// after a crash (the process killed, nothing flushed, no `close()`, no `Drop`) would find.
    /// Return `None` if the builder cannot do this; the crash test is then skipped.
    /// [`copy_storage_dir`] copies a directory.
    async fn build_from_crash_image(&self) -> Result<Option<Arc<dyn StateMachine>>, Error> {
        Ok(None)
    }

    /// Clean up any resources after testing
    async fn cleanup(&self) -> Result<(), Error>;
}

/// Copies `from` and everything under it to `to`; for implementing
/// [`StateMachineBuilder::build_from_crash_image`] on a directory-based storage.
pub fn copy_storage_dir(
    from: &std::path::Path,
    to: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_storage_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

impl StateMachineTestSuite {
    /// Run all state machine tests
    pub async fn run_all_tests<B: StateMachineBuilder>(builder: B) -> Result<(), Error> {
        Self::test_close_may_be_called_twice(builder.build().await?).await?;
        Self::test_basic_kv_operations(builder.build().await?).await?;
        Self::test_apply_chunk_functionality(builder.build().await?).await?;
        Self::test_cas_operations(builder.build().await?).await?;
        Self::test_batch_operations(builder.build().await?).await?;
        Self::test_last_applied_detection(builder.build().await?).await?;
        Self::test_snapshot_operations(&builder).await?;
        Self::test_reads_during_snapshot_install_see_old_new_or_fail(&builder).await?;
        Self::test_snapshot_taken_during_writes_never_skips_entries_after_restart(&builder).await?;
        Self::test_restart_after_crash_finds_position_and_data_of_the_same_entry(&builder).await?;

        Self::test_drop_flushes_data(&builder).await?;
        Self::test_drop_persists_last_applied(&builder).await?;
        Self::test_last_applied_survives_reopen_after_snapshot_install(&builder).await?;
        Self::test_data_survives_reopen(&builder).await?;
        Self::test_batch_survives_reopen(&builder).await?;
        Self::test_ungraceful_shutdown_recovery(&builder).await?;
        Self::test_close_persists_without_drop(&builder).await?;

        Self::test_get_multi_returns_values_in_key_order(builder.build().await?).await?;
        Self::test_get_multi_absent_keys_return_none(builder.build().await?).await?;
        Self::test_get_multi_empty_keys_returns_empty(builder.build().await?).await?;
        Self::test_get_multi_partial_hit_some_keys_missing(builder.build().await?).await?;
        Self::test_get_multi_service_registry_coherence(builder.build().await?).await?;
        Self::test_get_multi_election_state_coherence(builder.build().await?).await?;
        Self::test_get_multi_quota_management_coherence(builder.build().await?).await?;

        Self::test_generate_snapshot_data_is_atomic_under_concurrent_apply(&builder).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Run performance tests (optional, not included in run_all_tests)
    pub async fn run_performance_tests<B: StateMachineBuilder>(builder: B) -> Result<(), Error> {
        Self::test_apply_chunk_performance_smoke(builder.build().await?).await?;
        Self::test_apply_chunk_scalability(builder.build().await?).await?;

        builder.cleanup().await?;
        Ok(())
    }

    /// Test close lifecycle: a freshly built state machine already serves, and `close` may be
    /// called more than once. The engine calls nothing on a state machine after `close`, so what a
    /// closed one answers is not part of the contract.
    pub async fn test_close_may_be_called_twice(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Serving as soon as it is built: there is no separate start step
        assert!(state_machine.get(b"lifecycle-probe").is_ok());

        state_machine.close()?;
        state_machine.close()?; // a second call returns Ok

        Ok(())
    }

    /// Test basic key-value operations
    pub async fn test_basic_kv_operations(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let test_key = b"test_key";
        let test_value = Bytes::from(b"test_value".to_vec());

        // Create an insert entry
        let entries = vec![create_insert_entry(
            1,
            Bytes::from(test_key.to_vec()),
            test_value.clone(),
        )];
        state_machine.apply_chunk(&entries).await?;

        // Verify the value was inserted
        match state_machine.get(test_key)? {
            Some(value) => assert_eq!(value, test_value),
            None => panic!("Value not found after insert"),
        }

        // Create a delete entry
        let entries = vec![create_delete_entry(2, Bytes::from(test_key.to_vec()))];
        state_machine.apply_chunk(&entries).await?;

        // Verify the value was deleted
        assert!(state_machine.get(test_key)?.is_none());

        Ok(())
    }

    /// Test CAS operations
    pub async fn test_cas_operations(sm: Arc<dyn StateMachine>) -> Result<(), Error> {
        // Test 1: CAS on non-existent key (expected None)
        let entry1 = create_cas_entry(1, b"lock".to_vec().into(), None, b"owner1".to_vec().into());
        sm.apply_chunk(&[entry1]).await?;
        assert_eq!(sm.get(b"lock")?, Some(b"owner1".to_vec().into()));

        // Test 2: CAS success (expected matches)
        let entry2 = create_cas_entry(
            2,
            b"lock".to_vec().into(),
            Some(b"owner1".to_vec().into()),
            b"owner2".to_vec().into(),
        );
        sm.apply_chunk(&[entry2]).await?;
        assert_eq!(sm.get(b"lock")?, Some(b"owner2".to_vec().into()));

        // Test 3: CAS failure (expected mismatch) - value should not change
        let entry3 = create_cas_entry(
            3,
            b"lock".to_vec().into(),
            Some(b"wrong".to_vec().into()),
            b"owner3".to_vec().into(),
        );
        sm.apply_chunk(&[entry3]).await?;
        assert_eq!(sm.get(b"lock")?, Some(b"owner2".to_vec().into())); // Still owner2

        sm.close()?;
        Ok(())
    }

    /// Verify that Command::Batch applies all ops atomically — inserts and deletes
    /// within a single entry must all take effect.
    pub async fn test_batch_operations(sm: Arc<dyn StateMachine>) -> Result<(), Error> {
        use crate::BatchOp;

        let entry = create_batch_entry(1, vec![
            BatchOp::Insert {
                key: b"k1".to_vec().into(),
                value: b"v1".to_vec().into(),
            },
            BatchOp::Insert {
                key: b"k2".to_vec().into(),
                value: b"v2".to_vec().into(),
            },
            BatchOp::Delete {
                key: b"k1".to_vec().into(),
            },
        ]);
        sm.apply_chunk(&[entry]).await?;

        // k1 was deleted after insert, k2 survived
        assert_eq!(
            sm.get(b"k1")?,
            None,
            "k1 inserted then deleted in batch must be gone"
        );
        assert_eq!(
            sm.get(b"k2")?,
            Some(b"v2".to_vec().into()),
            "k2 inserted in batch must survive"
        );
        assert_eq!(sm.last_applied().index, 1);

        sm.close()?;
        Ok(())
    }

    /// Test chunk application functionality
    pub async fn test_apply_chunk_functionality(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Create a mix of insert and delete operations
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_delete_entry(3, Bytes::from(b"key1".to_vec())),
            create_insert_entry(
                4,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];

        state_machine.apply_chunk(&entries).await?;

        // Verify the final state
        assert!(state_machine.get(b"key1")?.is_none());
        assert_eq!(
            state_machine.get(b"key2")?,
            Some(Bytes::from(b"value2".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key3")?,
            Some(Bytes::from(b"value3".to_vec()))
        );
        assert_eq!(state_machine.last_applied(), LogId { index: 4, term: 1 });

        Ok(())
    }

    /// A lagging node installs a snapshot while clients keep reading from it.
    ///
    /// Scenario: the node holds old values for keys 1..=50 and receives a snapshot in which every
    /// key holds a new value. Reads that bypass Raft are served straight from the state machine,
    /// so they run while the install replaces the state.
    ///
    /// Expected: every read returns the old value, the new value, or fails. It never returns
    /// "not found" or any other value: that would show a client a state that never existed. After
    /// the install every key holds the new value.
    pub async fn test_reads_during_snapshot_install_see_old_new_or_fail<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        const KEYS: u64 = 300;
        const READ_KEYS: u64 = 50;
        let key = |i: u64| Bytes::from(format!("key-{i}"));
        let entries = |range: std::ops::RangeInclusive<u64>, value: &'static [u8]| {
            range
                .map(|i| create_insert_entry(i, key(i), Bytes::from_static(value)))
                .collect::<Vec<_>>()
        };

        // 1. A snapshot of the state in which every key holds the new value.
        let source = builder.build().await?;
        source.apply_chunk(&entries(1..=KEYS, b"new")).await?;
        let temp_dir = TempDir::new()?;
        let snapshot_dir = temp_dir.path().join("snapshot");
        let last_included = source.generate_snapshot_data(snapshot_dir.clone()).await?;
        drop(source);
        assert_eq!(
            last_included,
            LogId {
                index: KEYS,
                term: 1
            },
            "the exported position must be the last entry applied before the export"
        );
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::from(vec![0; 32]),
        };

        // 2. The node is behind: it only has the old values for the first keys.
        let sm = builder.build().await?;
        sm.apply_chunk(&entries(1..=READ_KEYS, b"old")).await?;

        // 3. Readers keep reading while the snapshot is installed.
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicUsize::new(0));
        let violations = Arc::new(Mutex::new(Vec::<String>::new()));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (sm, stop, reads, violations) =
                    (sm.clone(), stop.clone(), reads.clone(), violations.clone());
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        for i in 1..=READ_KEYS {
                            let seen = match sm.get(&key(i)) {
                                Ok(Some(v)) if v == "old" || v == "new" => None,
                                Ok(Some(other)) => {
                                    Some(format!("key {i}: unexpected value {other:?}"))
                                }
                                Ok(None) => Some(format!("key {i}: reported as not found")),
                                // A failed read is allowed; it shows nothing wrong.
                                Err(_) => None,
                            };
                            reads.fetch_add(1, Ordering::Relaxed);
                            if let Some(violation) = seen {
                                violations.lock().unwrap().push(violation);
                            }
                        }
                    }
                })
            })
            .collect();
        while reads.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }

        let installed = sm.apply_snapshot_from_file(&metadata, snapshot_dir).await;

        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().expect("a reader thread panicked");
        }
        installed?;

        let violations = violations.lock().unwrap();
        assert!(
            violations.is_empty(),
            "reads during the snapshot install saw a state that never existed, for example: {:?} \
             ({} reads, {} violations)",
            &violations[..violations.len().min(3)],
            reads.load(Ordering::Relaxed),
            violations.len()
        );
        for i in 1..=KEYS {
            assert_eq!(
                sm.get(&key(i))?,
                Some(Bytes::from_static(b"new")),
                "key {i} must hold the new value after the install"
            );
        }
        Ok(())
    }

    /// A snapshot taken while orders keep arriving must not make a restarted node skip orders.
    ///
    /// Scenario: an order service keeps writing. Order number `n` is log entry `n`, and the key
    /// `orders` always holds the number of the latest order. The leader takes a snapshot while the
    /// writes continue and a lagging node installs it. The lagging node then restarts.
    ///
    /// Expected: after the restart the node's position is not ahead of its data. A position ahead
    /// of the data (say "applied up to order 17197" while the data stops at order 15993) makes
    /// the node continue from order 17198, so orders 15994 to 17197 are never replayed and are
    /// lost without any error.
    ///
    /// The position may be behind the data: the engine then replays a few orders that the data
    /// already contains, which is harmless for a key-value store.
    pub async fn test_snapshot_taken_during_writes_never_skips_entries_after_restart<
        B: StateMachineBuilder,
    >(
        builder: &B
    ) -> Result<(), Error> {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        for attempt in 0..3 {
            // 1. Orders keep arriving: entry n sets `orders` to n, one entry at a time.
            let sm = builder.build().await?;
            let stop = Arc::new(AtomicBool::new(false));
            let writer = {
                let (sm, stop) = (sm.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut n = 1u64;
                    while !stop.load(Ordering::Relaxed) {
                        let entry = create_insert_entry(
                            n,
                            Bytes::from_static(b"orders"),
                            Bytes::from(n.to_string()),
                        );
                        sm.apply_chunk(&[entry]).await.expect("apply");
                        n += 1;
                        tokio::task::yield_now().await;
                    }
                })
            };
            tokio::time::sleep(Duration::from_millis(100)).await;

            // 2. A snapshot is taken in the middle of the writes.
            let temp_dir = TempDir::new()?;
            let snapshot_dir = temp_dir.path().join("snapshot");
            let exported = sm.generate_snapshot_data(snapshot_dir.clone()).await?;
            stop.store(true, Ordering::Relaxed);
            writer.await.expect("the writer must not panic");

            // 3. A lagging node (an empty one) installs it and restarts.
            drop(sm);
            let lagging = builder.build().await?;
            let metadata = SnapshotMetadata {
                // The engine labels the snapshot with the position the export reports, and the
                // installing state machine adopts that label as its `last_applied`, so a position
                // ahead of the data shows up after the restart.
                last_included: Some(exported),
                checksum: Bytes::from(vec![0; 32]),
            };
            lagging.apply_snapshot_from_file(&metadata, snapshot_dir).await?;
            lagging.close()?;
            drop(lagging);
            let restarted = builder.reopen().await?;

            let position = restarted.last_applied().index;
            let data: u64 = restarted
                .get(b"orders")?
                .map(|v| String::from_utf8_lossy(&v).parse().expect("a number"))
                .unwrap_or(0);
            assert!(
                position <= data,
                "attempt {attempt}: after the restart the node believes it applied up to {position}, \
                 but its data only reaches {data}: entries {}..={position} would never be replayed",
                data + 1
            );
        }
        Ok(())
    }

    /// A node is killed while orders keep arriving: nothing is flushed, `close()` and `Drop` never
    /// run. After the restart the position it reports must be exactly the entry its data has
    /// reached.
    ///
    /// Orders service: entry `n` sets `orders` to `n`, so the value of `orders` is the entry the
    /// data has reached. The engine starts replaying the log right after the reported position. A
    /// position behind the data (typically: the position is only written at `close()`) makes it
    /// replay entries the data already contains, which gives a different result for operations
    /// such as compare-and-swap; a position ahead of the data skips entries for good. Both are
    /// avoided by committing the position in the same transaction as the data. Storage that loses
    /// both (no write-ahead log, nothing flushed yet) reports position 0 and an empty state,
    /// which is fine: the engine replays everything.
    ///
    /// Skipped, with a message, when the builder does not implement
    /// [`StateMachineBuilder::build_from_crash_image`].
    pub async fn test_restart_after_crash_finds_position_and_data_of_the_same_entry<
        B: StateMachineBuilder,
    >(
        builder: &B
    ) -> Result<(), Error> {
        let sm = builder.build().await?;
        for n in 1..=200u64 {
            sm.apply_chunk(&[create_insert_entry(
                n,
                Bytes::from_static(b"orders"),
                Bytes::from(n.to_string()),
            )])
            .await?;
        }

        // The node is killed here: no close(), no drop.
        let Some(restarted) = builder.build_from_crash_image().await? else {
            eprintln!("crash image not supported by this builder; crash test skipped");
            return Ok(());
        };
        let position = restarted.last_applied().index;
        let data: u64 = restarted
            .get(b"orders")?
            .map(|v| String::from_utf8_lossy(&v).parse().expect("a number"))
            .unwrap_or(0);

        assert_eq!(
            position, data,
            "after a crash the reported position {position} and the data (entry {data}) must \
             describe the same entry: the position has to be committed with the data, not only \
             at close()"
        );
        Ok(())
    }

    /// Test last applied index detection
    pub async fn test_last_applied_detection(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Initial state
        assert_eq!(state_machine.last_applied(), LogId { index: 0, term: 0 });

        // Apply entries with different terms
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_insert_entry(
                3,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];

        state_machine.apply_chunk(&entries).await?;
        assert_eq!(state_machine.last_applied(), LogId { index: 3, term: 1 });

        Ok(())
    }

    /// Test snapshot operations
    pub async fn test_snapshot_operations<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        let state_machine = builder.build().await?;

        // Add some test data
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"key2".to_vec()),
                Bytes::from(b"value2".to_vec()),
            ),
            create_insert_entry(
                3,
                Bytes::from(b"key3".to_vec()),
                Bytes::from(b"value3".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&entries).await?;

        // Create a temporary directory for the snapshot
        let temp_dir = TempDir::new()?;
        let snapshot_dir = temp_dir.path().join("snapshot");

        // Generate snapshot
        let last_included = state_machine.generate_snapshot_data(snapshot_dir.clone()).await?;
        assert_eq!(
            last_included,
            LogId { index: 3, term: 1 },
            "the exported position must be the last entry applied before the export"
        );
        let checksum = crate::file_io::compute_checksum_from_folder_path(&snapshot_dir).await?;

        // Apply snapshot (simulate receiving from leader)
        // let snapshot_path = snapshot_dir.join("snapshot.bin");
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::from(checksum.to_vec()),
        };

        // The node that receives the snapshot starts empty.
        drop(state_machine);
        let state_machine = builder.build().await?;
        assert_eq!(state_machine.get(b"key1")?, None);
        assert_eq!(state_machine.get(b"key2")?, None);
        assert_eq!(state_machine.get(b"key3")?, None);
        assert_eq!(state_machine.last_applied(), LogId::default());

        // Assume there were some entries. After applying snapshot, all the old ones should be
        // cleared.
        //
        // Deliberately stop this replay BEFORE last_included.index (3): the snapshot's
        // authoritative stale/duplicate check (#436) compares only the single
        // (index, term) boundary, not per-entry content, so as long as
        // current.index < incoming.index the install proceeds and does a full replace
        // regardless of what garbage sat below the boundary. Replaying all the way to
        // index 3 would make current == incoming's boundary, which the classification
        // (correctly) treats as an idempotent duplicate and skips — reaching that state
        // with *different* content at the same (index, term) can't happen in a real Raft
        // cluster (Log Matching Property), so doing it here would only be testing an
        // impossible scenario, not a real one.
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"key1".to_vec()),
                Bytes::from(b"old_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"old_key2".to_vec()),
                Bytes::from(b"vold_alue2".to_vec()),
            ),
        ];
        state_machine.apply_chunk(&entries).await?;

        state_machine.apply_snapshot_from_file(&metadata, snapshot_dir).await?;

        // Verify state was preserved after snapshot application
        assert_eq!(
            state_machine.get(b"key1")?,
            Some(Bytes::from(b"value1".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key2")?,
            Some(Bytes::from(b"value2".to_vec()))
        );
        assert_eq!(
            state_machine.get(b"key3")?,
            Some(Bytes::from(b"value3".to_vec()))
        );
        assert_eq!(state_machine.get(b"old_key2")?, None);
        assert_eq!(state_machine.get(b"old_key3")?, None);
        assert_eq!(state_machine.last_applied(), last_included);

        Ok(())
    }

    /// Performance smoke test: apply_chunk baseline
    ///
    /// Ensures apply_chunk doesn't have catastrophic performance issues.
    /// Threshold: 100 entries in < 1 second (generous for CI stability).
    pub async fn test_apply_chunk_performance_smoke(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let entries: Vec<_> = (1..=100)
            .map(|i| {
                create_insert_entry(
                    i,
                    Bytes::from(format!("perf_key_{i}")),
                    Bytes::from(format!("perf_value_{i}")),
                )
            })
            .collect();

        let start = Instant::now();
        state_machine.apply_chunk(&entries).await?;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "Performance regression: apply_chunk(100) took {elapsed:?} (expected < 1s)",
        );

        Ok(())
    }

    /// Performance scalability test: verify O(N) complexity
    ///
    /// Tests that 1000 entries take ~10x longer than 100 entries (not 100x).
    /// Detects algorithmic issues (e.g., O(N²) instead of O(N)).
    pub async fn test_apply_chunk_scalability(
        state_machine: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Baseline: 100 entries
        let small_entries: Vec<_> = (1..=100)
            .map(|i| {
                create_insert_entry(
                    i,
                    Bytes::from(format!("scale_key_{i}")),
                    Bytes::from(format!("scale_value_{i}")),
                )
            })
            .collect();

        let start_small = Instant::now();
        state_machine.apply_chunk(&small_entries).await?;
        let elapsed_small = start_small.elapsed();

        // Large batch: 1000 entries
        let large_entries: Vec<_> = (101..=1100)
            .map(|i| {
                create_insert_entry(
                    i,
                    Bytes::from(format!("scale_key_{i}")),
                    Bytes::from(format!("scale_value_{i}")),
                )
            })
            .collect();

        let start_large = Instant::now();
        state_machine.apply_chunk(&large_entries).await?;
        let elapsed_large = start_large.elapsed();

        // Verify linear scalability: 1000 entries should be 5x-15x slower (not 100x)
        let ratio = elapsed_large.as_micros() as f64 / elapsed_small.as_micros().max(1) as f64;

        // Relax threshold in CI environment (resource-constrained)
        let threshold = if std::env::var("CI").is_ok() {
            100.0 // CI: Allow up to 100x (disk I/O can be slow)
        } else {
            20.0 // Local: Expect near-linear scalability
        };

        assert!(
            ratio < threshold,
            "Scalability issue: 1000 entries took {ratio:.1}x longer than 100 entries (expected ~10x). \
             Possible O(N²) complexity. Threshold: {threshold}x"
        );

        Ok(())
    }

    /// Test that stop() persists data correctly
    ///
    /// This test verifies that:
    /// 1. Calling stop() flushes all data to disk
    /// 2. Data survives across StateMachine instances (reopen from same path)
    /// 3. Stop triggers proper cleanup including flush
    ///
    /// This catches bugs where:
    /// - stop() doesn't call flush
    /// - Data lives only in memory buffers
    ///
    /// Test that stop() properly persists data
    ///
    /// This test verifies that calling stop() explicitly will persist both
    /// data and metadata. This is the "graceful shutdown" path.
    pub async fn test_drop_flushes_data<B: StateMachineBuilder>(builder: &B) -> Result<(), Error> {
        // Step 1: Create state machine, write data, call stop()
        let sm = builder.build().await?;

        // Write test data
        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"drop_test_key1".to_vec()),
                Bytes::from(b"drop_test_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"drop_test_key2".to_vec()),
                Bytes::from(b"drop_test_value2".to_vec()),
            ),
        ];
        sm.apply_chunk(&entries).await?;

        // Verify data exists in memory
        assert_eq!(
            sm.get(b"drop_test_key1")?,
            Some(Bytes::from(b"drop_test_value1".to_vec())),
            "Data should exist before stop"
        );

        // Call close() for graceful shutdown - close() must persist data
        sm.close()?;

        // Explicitly drop Arc to release database locks (critical for RocksDB)
        drop(sm);
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Step 2: Reopen state machine from same path
        let sm2 = builder.reopen().await?;

        // Step 3: Verify data survived
        let value1 = sm2.get(b"drop_test_key1")?;
        let value2 = sm2.get(b"drop_test_key2")?;

        assert_eq!(
            value1,
            Some(Bytes::from(b"drop_test_value1".to_vec())),
            "BUG: Data lost after close! StateMachine.close() must persist data."
        );
        assert_eq!(
            value2,
            Some(Bytes::from(b"drop_test_value2".to_vec())),
            "BUG: Data lost after close! StateMachine.close() must persist data."
        );

        Ok(())
    }

    /// Test that Drop ALONE persists last_applied metadata
    ///
    /// This is a critical test that verifies Drop implementation correctness.
    /// When a StateMachine is dropped (without explicit stop/flush), it must
    /// still persist last_applied index/term to prevent replay of already
    /// applied entries on restart.
    ///
    /// Bug scenario if Drop is incorrect:
    /// 1. Apply entries 1-10
    /// 2. Crash before persisting last_applied=10
    /// 3. On restart, last_applied reads as 0
    /// 4. Raft replays entries 1-10 again -> data corruption
    pub async fn test_drop_persists_last_applied<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        // Step 1: Write data - NO explicit stop() or flush() before drop
        {
            let sm = builder.build().await?;

            let entries = vec![
                create_insert_entry(
                    1,
                    Bytes::from(b"drop_meta_key1".to_vec()),
                    Bytes::from(b"drop_meta_value1".to_vec()),
                ),
                create_insert_entry(
                    2,
                    Bytes::from(b"drop_meta_key2".to_vec()),
                    Bytes::from(b"drop_meta_value2".to_vec()),
                ),
                create_insert_entry(
                    3,
                    Bytes::from(b"drop_meta_key3".to_vec()),
                    Bytes::from(b"drop_meta_value3".to_vec()),
                ),
            ];
            sm.apply_chunk(&entries).await?;

            // Verify last_applied in memory
            assert_eq!(
                sm.last_applied().index,
                3,
                "last_applied should be 3 after applying 3 entries"
            );

            // ⚠️ NO stop() call here!
            // ⚠️ NO flush() call here!
            // Just let Drop run when sm goes out of scope
        } // <- Drop runs here

        // Small delay to ensure file system sync (especially for macOS)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Reopen
        let sm2 = builder.reopen().await?;

        // Step 3: Critical assertion - last_applied must be persisted by Drop
        assert_eq!(
            sm2.last_applied().index,
            3,
            "BUG: last_applied not persisted by Drop! \
             Drop implementation must call save_hard_state() (not just flush()) \
             to persist last_applied index/term. Without this, Raft will replay \
             already-applied entries after restart, causing data corruption."
        );

        // Also verify the data itself survived
        assert_eq!(
            sm2.get(b"drop_meta_key1")?,
            Some(Bytes::from(b"drop_meta_value1".to_vec())),
            "Data should also be persisted by Drop"
        );
        assert_eq!(
            sm2.get(b"drop_meta_key3")?,
            Some(Bytes::from(b"drop_meta_value3".to_vec())),
            "Data should also be persisted by Drop"
        );

        Ok(())
    }

    /// `last_applied` must survive a crash/restart right after a snapshot install, with
    /// no further `apply_chunk` — every other crash/reopen scenario in this suite drives
    /// its data through `apply_chunk` first, so an adapter that only updates
    /// `last_applied` in memory on install (never persists it) slipped through.
    pub async fn test_last_applied_survives_reopen_after_snapshot_install<
        B: StateMachineBuilder,
    >(
        builder: &B
    ) -> Result<(), Error> {
        let last_included = LogId { index: 3, term: 1 };

        {
            let sm = builder.build().await?;

            let entries = vec![
                create_insert_entry(
                    1,
                    Bytes::from(b"snap_survive_key1".to_vec()),
                    Bytes::from(b"v1".to_vec()),
                ),
                create_insert_entry(
                    2,
                    Bytes::from(b"snap_survive_key2".to_vec()),
                    Bytes::from(b"v2".to_vec()),
                ),
                create_insert_entry(
                    3,
                    Bytes::from(b"snap_survive_key3".to_vec()),
                    Bytes::from(b"v3".to_vec()),
                ),
            ];
            sm.apply_chunk(&entries).await?;

            let temp_dir = TempDir::new()?;
            let snapshot_dir = temp_dir.path().join("snapshot");
            let exported = sm.generate_snapshot_data(snapshot_dir.clone()).await?;
            assert_eq!(
                exported, last_included,
                "the exported position must be the last entry applied before the export"
            );
            let checksum = crate::file_io::compute_checksum_from_folder_path(&snapshot_dir).await?;

            // Fresh node receiving this snapshot for the first time (current == default),
            // so the install is strictly "Greater" — eligible, not a stale/duplicate no-op.
            drop(sm);
            let sm = builder.build().await?;
            assert_eq!(sm.last_applied(), LogId::default());

            let metadata = SnapshotMetadata {
                last_included: Some(last_included),
                checksum: Bytes::from(checksum.to_vec()),
            };
            sm.apply_snapshot_from_file(&metadata, snapshot_dir).await?;
            assert_eq!(
                sm.last_applied(),
                last_included,
                "last_applied must reflect the install immediately, in memory"
            );

            // NO apply_chunk, NO stop(), NO flush() below — only Drop (or whatever the
            // install itself already persisted) may carry last_applied across restart.
        } // <- Drop runs here

        tokio::time::sleep(Duration::from_millis(50)).await;

        let sm2 = builder.reopen().await?;

        assert_eq!(
            sm2.last_applied(),
            last_included,
            "BUG: last_applied not persisted after a snapshot install! If an adapter's \
             apply_snapshot_from_file updates last_applied only in memory (e.g. via \
             update_last_applied) without also durably persisting it (e.g. via \
             persist_last_applied) before returning, this is exactly where it shows up: \
             the process restarts with no record the install ever happened, and Raft \
             replays the entire log on top of already-installed snapshot content."
        );

        assert_eq!(
            sm2.get(b"snap_survive_key1")?,
            Some(Bytes::from(b"v1".to_vec())),
            "snapshot content must also survive the restart"
        );

        Ok(())
    }

    /// Test that data survives across stop() and reopen
    ///
    /// This test verifies:
    /// 1. Graceful stop() persists all data
    /// 2. Reopening from same path recovers all data
    /// 3. last_applied index is correctly restored
    pub async fn test_data_survives_reopen<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        let last_applied_before: LogId;

        // Step 1: Create, write, stop gracefully
        {
            let sm = builder.build().await?;

            let entries = vec![
                create_insert_entry(
                    1,
                    Bytes::from(b"reopen_key1".to_vec()),
                    Bytes::from(b"reopen_value1".to_vec()),
                ),
                create_insert_entry(
                    2,
                    Bytes::from(b"reopen_key2".to_vec()),
                    Bytes::from(b"reopen_value2".to_vec()),
                ),
                create_insert_entry(
                    3,
                    Bytes::from(b"reopen_key3".to_vec()),
                    Bytes::from(b"reopen_value3".to_vec()),
                ),
            ];
            sm.apply_chunk(&entries).await?;

            last_applied_before = sm.last_applied();
            assert_eq!(last_applied_before.index, 3);

            // Graceful close
            sm.close()?;
        }

        // Step 2: Reopen and verify
        let sm2 = builder.reopen().await?;

        // Verify all data
        assert_eq!(
            sm2.get(b"reopen_key1")?,
            Some(Bytes::from(b"reopen_value1".to_vec())),
            "Data should survive graceful stop"
        );
        assert_eq!(
            sm2.get(b"reopen_key2")?,
            Some(Bytes::from(b"reopen_value2".to_vec()))
        );
        assert_eq!(
            sm2.get(b"reopen_key3")?,
            Some(Bytes::from(b"reopen_value3".to_vec()))
        );

        // Verify last_applied is restored
        assert_eq!(
            sm2.last_applied(),
            last_applied_before,
            "last_applied index should be restored after reopen"
        );

        Ok(())
    }

    /// Batch ops must survive WAL encode → crash → decode round-trip.
    ///
    /// Covers the #415 fix: Command::Batch encoding in file-based state machines
    /// must produce records that replay correctly after an ungraceful shutdown.
    pub async fn test_batch_survives_reopen<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        use crate::BatchOp;

        // Step 1: apply a batch with mixed Insert + Delete, then drop without stop()
        {
            let sm = builder.build().await?;

            let entry = create_batch_entry(1, vec![
                BatchOp::Insert {
                    key: b"bk1".to_vec().into(),
                    value: b"bv1".to_vec().into(),
                },
                BatchOp::Insert {
                    key: b"bk2".to_vec().into(),
                    value: b"bv2".to_vec().into(),
                },
                BatchOp::Delete {
                    key: b"bk1".to_vec().into(),
                },
            ]);
            sm.apply_chunk(&[entry]).await?;

            assert_eq!(sm.get(b"bk1")?, None);
            assert_eq!(sm.get(b"bk2")?, Some(b"bv2".to_vec().into()));
            assert_eq!(sm.last_applied().index, 1);
        } // drop → crash simulation

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Step 2: reopen from same path — WAL must replay correctly
        let sm2 = builder.reopen().await?;

        assert_eq!(
            sm2.get(b"bk1")?,
            None,
            "deleted key must be absent after recovery"
        );
        assert_eq!(
            sm2.get(b"bk2")?,
            Some(b"bv2".to_vec().into()),
            "inserted key must survive recovery"
        );
        assert_eq!(sm2.last_applied().index, 1, "last_applied must be restored");

        sm2.close()?;
        Ok(())
    }

    /// Test recovery from ungraceful shutdown (crash simulation)
    ///
    /// This test simulates a crash by:
    /// 1. Writing data
    /// 2. Dropping without stop() - simulates SIGKILL/power loss
    /// 3. Verifying data recovery on reopen
    ///
    /// This is critical for production scenarios where:
    /// - Process receives SIGKILL
    /// - Server crashes/panics
    /// - Power loss occurs
    ///
    /// Test ungraceful shutdown recovery (crash simulation)
    ///
    /// This test simulates a crash by dropping the state machine without
    /// calling stop(). The Drop implementation must ensure that both data
    /// and metadata (especially last_applied) are persisted.
    ///
    /// This is different from test_drop_persists_last_applied because:
    /// - This test focuses on the "crash and recover" narrative
    /// - This test verifies both data AND metadata recovery
    /// - This explicitly models the ungraceful shutdown scenario
    pub async fn test_ungraceful_shutdown_recovery<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        // Step 1: Write data and drop ungracefully (no stop())
        let sm = builder.build().await?;

        let entries = vec![
            create_insert_entry(
                1,
                Bytes::from(b"crash_key1".to_vec()),
                Bytes::from(b"crash_value1".to_vec()),
            ),
            create_insert_entry(
                2,
                Bytes::from(b"crash_key2".to_vec()),
                Bytes::from(b"crash_value2".to_vec()),
            ),
        ];
        sm.apply_chunk(&entries).await?;

        // Verify data exists in memory
        assert!(sm.get(b"crash_key1")?.is_some());
        assert_eq!(sm.last_applied().index, 2);

        // ⚠️ NO flush() call here!
        // ⚠️ NO stop() call here!
        // 🔥 Simulate crash: drop without graceful shutdown
        // Drop implementation MUST handle persistence
        drop(sm);

        // Small delay to ensure file system sync
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 2: Simulate restart - create new instance from same path
        let sm2 = builder.reopen().await?;

        // Step 3: Verify crash recovery - both data and metadata
        let value1 = sm2.get(b"crash_key1")?;
        let value2 = sm2.get(b"crash_key2")?;

        assert_eq!(
            value1,
            Some(Bytes::from(b"crash_value1".to_vec())),
            "BUG: Data lost after ungraceful shutdown! \
             Drop implementation must persist data. Consider: \
             1) Implement Drop to call save_hard_state() \
             2) Use sync writes for critical data"
        );
        assert_eq!(
            value2,
            Some(Bytes::from(b"crash_value2".to_vec())),
            "BUG: Data lost after ungraceful shutdown!"
        );

        // Verify last_applied is recovered
        assert_eq!(
            sm2.last_applied().index,
            2,
            "BUG: last_applied not recovered after crash! \
             Drop must persist last_applied to prevent entry replay."
        );

        Ok(())
    }

    /// `close()` alone must persist everything a restart needs, even while the developer still
    /// holds the state machine.
    ///
    /// Business scenario: the node shuts down cleanly, but the application keeps its own `Arc`
    /// to the state machine, so `Drop` never runs. A restart must still see every applied key
    /// and the applied position; otherwise Raft replays entries that were already applied.
    /// Every other reopen test in this suite drops the state machine first, which lets a `Drop`
    /// impl hide a `close()` that forgot to persist.
    pub async fn test_close_persists_without_drop<B: StateMachineBuilder>(
        builder: &B
    ) -> Result<(), Error> {
        let sm = builder.build().await?;
        sm.apply_chunk(&[
            create_insert_entry(
                1,
                Bytes::from_static(b"close_key1"),
                Bytes::from_static(b"v1"),
            ),
            create_insert_entry(
                2,
                Bytes::from_static(b"close_key2"),
                Bytes::from_static(b"v2"),
            ),
            create_insert_entry(
                3,
                Bytes::from_static(b"close_key3"),
                Bytes::from_static(b"v3"),
            ),
        ])
        .await?;
        assert_eq!(sm.last_applied().index, 3);

        sm.close()?;
        // `sm` stays alive until the end of the test: Drop must not be what saves the state.

        let sm2 = builder.reopen().await?;

        assert_eq!(
            sm2.last_applied().index,
            3,
            "close() must persist the applied position without relying on Drop"
        );
        assert_eq!(
            sm2.get(b"close_key1")?,
            Some(Bytes::from_static(b"v1")),
            "close() must persist applied data without relying on Drop"
        );
        assert_eq!(sm2.get(b"close_key3")?, Some(Bytes::from_static(b"v3")));

        drop(sm);
        Ok(())
    }

    // ── get_multi tests ───────────────────────────────────────────────────────

    /// Verify that get_multi preserves input key order in its output.
    ///
    /// The caller maps result[i] back to keys[i] without a key lookup.
    /// Any reordering silently breaks that mapping.
    pub async fn test_get_multi_returns_values_in_key_order(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        sm.apply_chunk(&[
            create_insert_entry(1, Bytes::from("key_a"), Bytes::from("value_a")),
            create_insert_entry(2, Bytes::from("key_b"), Bytes::from("value_b")),
            create_insert_entry(3, Bytes::from("key_c"), Bytes::from("value_c")),
        ])
        .await?;

        // Request in reverse alphabetical order to catch any accidental sorting.
        let keys = vec![
            Bytes::from("key_c"),
            Bytes::from("key_a"),
            Bytes::from("key_b"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3, "result length must match key count");
        assert_eq!(
            values[0],
            Some(Bytes::from("value_c")),
            "position 0 = key_c"
        );
        assert_eq!(
            values[1],
            Some(Bytes::from("value_a")),
            "position 1 = key_a"
        );
        assert_eq!(
            values[2],
            Some(Bytes::from("value_b")),
            "position 2 = key_b"
        );
        Ok(())
    }

    /// Verify that absent keys produce None entries, not errors or panics.
    ///
    /// Callers rely on None to distinguish "key not found" from a storage failure.
    /// Returning Err for a missing key would force callers to treat a normal business
    /// state (no registration yet) as an error.
    pub async fn test_get_multi_absent_keys_return_none(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let keys = vec![Bytes::from("ghost_1"), Bytes::from("ghost_2")];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 2);
        assert!(values[0].is_none(), "absent key must return None, not Err");
        assert!(values[1].is_none(), "absent key must return None, not Err");
        Ok(())
    }

    /// Verify that an empty key slice returns an empty result without error.
    ///
    /// Callers may pass an empty slice when the coordinator has no keys to fetch
    /// (e.g., an empty service namespace). The implementation must not panic or
    /// return an error in this case.
    pub async fn test_get_multi_empty_keys_returns_empty(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        let values = sm.get_multi(&[])?;
        assert!(
            values.is_empty(),
            "empty key list must produce empty result"
        );
        Ok(())
    }

    /// Verify that a mix of present and absent keys returns Some/None correctly.
    ///
    /// The position contract must hold even when some slots are None.
    /// A naive implementation that skips missing keys and compacts the result
    /// would shift positions, silently misrouting values to wrong fields.
    pub async fn test_get_multi_partial_hit_some_keys_missing(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        sm.apply_chunk(&[create_insert_entry(
            1,
            Bytes::from("present"),
            Bytes::from("v"),
        )])
        .await?;

        let keys = vec![
            Bytes::from("missing_before"),
            Bytes::from("present"),
            Bytes::from("missing_after"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(
            values.len(),
            3,
            "result length must equal key count even with gaps"
        );
        assert!(
            values[0].is_none(),
            "missing_before must be None at position 0"
        );
        assert_eq!(
            values[1],
            Some(Bytes::from("v")),
            "present must have value at position 1"
        );
        assert!(
            values[2].is_none(),
            "missing_after must be None at position 2"
        );
        Ok(())
    }

    /// Service registry coordinator scenario: batch read must not produce a torn state.
    ///
    /// In distributed service routing, (addr, version, health) must all come from
    /// the same applied state. A torn read — addr from apply index 100, version from
    /// index 105 — would cause traffic to route to the old instance while the client
    /// believes it runs the new version, breaking gray-release correctness.
    ///
    /// This test verifies that after a full v2 deployment, get_multi returns all three
    /// fields from v2 with no v1 values mixed in.
    pub async fn test_get_multi_service_registry_coherence(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // v1: initial deployment
        sm.apply_chunk(&[
            create_insert_entry(
                1,
                Bytes::from("/service/payment/addr"),
                Bytes::from("10.0.0.1:8080"),
            ),
            create_insert_entry(
                2,
                Bytes::from("/service/payment/version"),
                Bytes::from("v2.2"),
            ),
            create_insert_entry(
                3,
                Bytes::from("/service/payment/health"),
                Bytes::from("healthy"),
            ),
        ])
        .await?;

        // v2: new deployment — all three fields transition together
        sm.apply_chunk(&[
            create_insert_entry(
                4,
                Bytes::from("/service/payment/addr"),
                Bytes::from("10.0.0.2:8080"),
            ),
            create_insert_entry(
                5,
                Bytes::from("/service/payment/version"),
                Bytes::from("v2.3"),
            ),
            create_insert_entry(
                6,
                Bytes::from("/service/payment/health"),
                Bytes::from("starting"),
            ),
        ])
        .await?;

        let keys = vec![
            Bytes::from("/service/payment/addr"),
            Bytes::from("/service/payment/version"),
            Bytes::from("/service/payment/health"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3);
        // All three must reflect the v2 state consistently.
        // Any mix (e.g., addr=v2 + version=v2.2) is a torn read.
        assert_eq!(
            values[0],
            Some(Bytes::from("10.0.0.2:8080")),
            "addr must be v2.3 instance"
        );
        assert_eq!(values[1], Some(Bytes::from("v2.3")), "version must be v2.3");
        assert_eq!(
            values[2],
            Some(Bytes::from("starting")),
            "health must be v2.3 startup value"
        );
        Ok(())
    }

    /// Leader election coordinator scenario: batch read must not produce a phantom state.
    ///
    /// (leader, term, lease_expire) must be internally consistent. A torn read can
    /// produce a combination that never existed — e.g., leader=node-3 (old term) with
    /// term=43 (new term). A coordinator observing this phantom state may incorrectly
    /// conclude that node-3 holds a lease in term 43 and take split-brain actions.
    ///
    /// This test verifies that after a term transition, get_multi returns all three
    /// fields from the new term with no old-term values mixed in.
    pub async fn test_get_multi_election_state_coherence(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Term 42: node-3 is leader
        sm.apply_chunk(&[
            create_insert_entry(1, Bytes::from("/election/leader"), Bytes::from("node-3")),
            create_insert_entry(2, Bytes::from("/election/term"), Bytes::from("42")),
            create_insert_entry(
                3,
                Bytes::from("/election/lease_expire"),
                Bytes::from("1748600000"),
            ),
        ])
        .await?;

        // Term 43: node-5 wins new election
        sm.apply_chunk(&[
            create_insert_entry(4, Bytes::from("/election/leader"), Bytes::from("node-5")),
            create_insert_entry(5, Bytes::from("/election/term"), Bytes::from("43")),
            create_insert_entry(
                6,
                Bytes::from("/election/lease_expire"),
                Bytes::from("1748603600"),
            ),
        ])
        .await?;

        let keys = vec![
            Bytes::from("/election/leader"),
            Bytes::from("/election/term"),
            Bytes::from("/election/lease_expire"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3);
        // Must reflect term-43 state consistently.
        // leader=node-3 + term=43 is a phantom state that never existed.
        assert_eq!(
            values[0],
            Some(Bytes::from("node-5")),
            "leader must be node-5 (term 43)"
        );
        assert_eq!(values[1], Some(Bytes::from("43")), "term must be 43");
        assert_eq!(
            values[2],
            Some(Bytes::from("1748603600")),
            "lease_expire must be term-43 value"
        );
        Ok(())
    }

    /// Quota management coordinator scenario: batch read must return coherent counters.
    ///
    /// (limit, used, window_start) from different apply indexes produce incorrect
    /// rate-limit calculations — a pure business correctness failure that is independent
    /// of Linearizable semantics. Reading used=850 with a new window_start makes the
    /// rate limiter reject requests that should be allowed in the new window.
    ///
    /// This test verifies that after a window reset, get_multi returns all three
    /// counters from the new window with no old-window values mixed in.
    pub async fn test_get_multi_quota_management_coherence(
        sm: Arc<dyn StateMachine>
    ) -> Result<(), Error> {
        // Window 1: tenant-A has consumed 850 of their 1000 quota
        sm.apply_chunk(&[
            create_insert_entry(1, Bytes::from("/quota/tenant-A/limit"), Bytes::from("1000")),
            create_insert_entry(2, Bytes::from("/quota/tenant-A/used"), Bytes::from("850")),
            create_insert_entry(
                3,
                Bytes::from("/quota/tenant-A/window_start"),
                Bytes::from("1748599900"),
            ),
        ])
        .await?;

        // Window 2: quota window resets — used counter goes back to 0
        sm.apply_chunk(&[
            create_insert_entry(4, Bytes::from("/quota/tenant-A/limit"), Bytes::from("1000")),
            create_insert_entry(5, Bytes::from("/quota/tenant-A/used"), Bytes::from("0")),
            create_insert_entry(
                6,
                Bytes::from("/quota/tenant-A/window_start"),
                Bytes::from("1748603500"),
            ),
        ])
        .await?;

        let keys = vec![
            Bytes::from("/quota/tenant-A/limit"),
            Bytes::from("/quota/tenant-A/used"),
            Bytes::from("/quota/tenant-A/window_start"),
        ];
        let values = sm.get_multi(&keys)?;

        assert_eq!(values.len(), 3);
        // Must read window-2 consistently.
        // used=850 + window_start from window-2 is a torn read that wrongly
        // blocks requests that should be free in the new window.
        assert_eq!(
            values[0],
            Some(Bytes::from("1000")),
            "limit is unchanged across windows"
        );
        assert_eq!(
            values[1],
            Some(Bytes::from("0")),
            "used must be 0 after window reset"
        );
        assert_eq!(
            values[2],
            Some(Bytes::from("1748603500")),
            "window_start must be from window-2"
        );
        Ok(())
    }

    /// `generate_snapshot_data` must present a single, atomically-pinned view of the state
    /// machine (#436): if `apply_chunk` runs concurrently with an in-flight
    /// `generate_snapshot_data` call, the exported snapshot must reflect either the state
    /// strictly before or strictly after that `apply_chunk` — never a mix of both. See the
    /// `generate_snapshot_data` doc comment on the `StateMachine` trait for the contract
    /// this enforces on implementors.
    ///
    /// Races `generate_snapshot_data` against a two-key `apply_chunk` (mirroring the
    /// service-registry addr/version coherence scenario used by `get_multi` above) many
    /// times, since a single interleaving is timing-dependent and may not hit an unsafe
    /// window even on a broken implementation.
    pub async fn test_generate_snapshot_data_is_atomic_under_concurrent_apply<
        B: StateMachineBuilder,
    >(
        builder: &B
    ) -> Result<(), Error> {
        struct Round {
            _temp_dir: TempDir, // kept alive so snapshot_dir survives to the verification pass
            snapshot_dir: PathBuf,
            last_included: LogId,
            checksum: Bytes,
            before: (Bytes, Bytes),
            after: (Bytes, Bytes),
        }

        let live = builder.build().await?;

        let addr_key = Bytes::from_static(b"/service/payment/addr");
        let version_key = Bytes::from_static(b"/service/payment/version");

        let mut prev_addr = Bytes::from_static(b"10.0.0.1:8080");
        let mut prev_version = Bytes::from_static(b"v1");
        live.apply_chunk(&[
            create_insert_entry(1, addr_key.clone(), prev_addr.clone()),
            create_insert_entry(2, version_key.clone(), prev_version.clone()),
        ])
        .await?;

        // Racing phase: keep every round's exported directory around (don't verify yet) —
        // `StateMachineBuilder` implementations (e.g. RocksDB) only support one live handle
        // per path, so verifying on `live` here would clobber it mid-race.
        let mut rounds = Vec::with_capacity(200);
        let mut next_index = 3u64;
        for round_num in 0u32..200 {
            let last_included = LogId {
                index: next_index - 1,
                term: 1,
            };
            let temp_dir = TempDir::new()?;
            let snapshot_dir = temp_dir.path().join("snapshot");

            let new_addr = Bytes::from(format!("10.0.0.{}:8080", round_num + 2));
            let new_version = Bytes::from(format!("v{}", round_num + 2));

            let export_fut = {
                let live = live.clone();
                let dir = snapshot_dir.clone();
                async move { live.generate_snapshot_data(dir).await }
            };
            let apply_fut = {
                let live = live.clone();
                let (addr_key, version_key) = (addr_key.clone(), version_key.clone());
                let (new_addr, new_version) = (new_addr.clone(), new_version.clone());
                let idx = next_index;
                async move {
                    live.apply_chunk(&[
                        create_insert_entry(idx, addr_key, new_addr),
                        create_insert_entry(idx + 1, version_key, new_version),
                    ])
                    .await
                }
            };

            let (export_result, apply_result) = tokio::join!(export_fut, apply_fut);
            export_result?;
            let checksum = crate::file_io::compute_checksum_from_folder_path(&snapshot_dir).await?;
            apply_result?;
            next_index += 2;

            rounds.push(Round {
                _temp_dir: temp_dir,
                snapshot_dir,
                last_included,
                checksum: Bytes::from(checksum.to_vec()),
                before: (prev_addr.clone(), prev_version.clone()),
                after: (new_addr.clone(), new_version.clone()),
            });

            prev_addr = new_addr;
            prev_version = new_version;
        }

        // Verification phase: now that racing is done, install every round's export, oldest first,
        // into a separate empty state machine and inspect what the export actually captured. Each
        // snapshot is newer than the previous one and replaces the whole state, so the rounds do
        // not affect each other.
        let target = builder.build().await?;
        for r in &rounds {
            let metadata = SnapshotMetadata {
                last_included: Some(r.last_included),
                checksum: r.checksum.clone(),
            };
            target.apply_snapshot_from_file(&metadata, r.snapshot_dir.clone()).await?;

            let got_addr = target.get(&addr_key)?;
            let got_version = target.get(&version_key)?;

            let matches_before =
                got_addr.as_ref() == Some(&r.before.0) && got_version.as_ref() == Some(&r.before.1);
            let matches_after =
                got_addr.as_ref() == Some(&r.after.0) && got_version.as_ref() == Some(&r.after.1);

            assert!(
                matches_before || matches_after,
                "torn snapshot at last_included={:?}: addr={got_addr:?} version={got_version:?}, \
                 expected either before={:?} or after={:?}",
                r.last_included,
                r.before,
                r.after
            );
        }

        Ok(())
    }
}

/// Helper function to create an Insert ApplyEntry
fn create_insert_entry(
    index: u64,
    key: Bytes,
    value: Bytes,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Insert { key, value },
    }
}

/// Helper function to create a Delete ApplyEntry
fn create_delete_entry(
    index: u64,
    key: Bytes,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Delete { key },
    }
}

/// Helper function to create a CAS ApplyEntry
fn create_cas_entry(
    index: u64,
    key: Bytes,
    expected: Option<Bytes>,
    value: Bytes,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::CompareAndSwap {
            key,
            expected,
            value,
        },
    }
}

/// Helper: create a single Batch entry with the given ops.
fn create_batch_entry(
    index: u64,
    ops: Vec<crate::BatchOp>,
) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Batch { ops },
    }
}

// ============================================================================
// Default scan_prefix implementation test
// ============================================================================
//
// The default scan_prefix on the trait returns an error because not all state
// machines support prefix scans.  This test exercises that code path through a
// minimal concrete type that delegates everything else but does NOT override
// scan_prefix, ensuring the default arm is reachable and exercised.
#[cfg(test)]
mod default_scan_prefix_tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use async_trait::async_trait;
    use bytes::Bytes;
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::storage::SnapshotMetadata;

    use crate::ApplyEntry;
    use crate::storage::state_machine::ApplyResult;
    use crate::storage::state_machine::StateMachine;
    use crate::storage::state_machine::StateMachineError;

    struct MinimalSm {
        running: AtomicBool,
    }

    #[async_trait]
    impl StateMachine for MinimalSm {
        fn close(&self) -> Result<(), StateMachineError> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }

        fn get(
            &self,
            _key_buffer: &[u8],
        ) -> Result<Option<Bytes>, StateMachineError> {
            Ok(None)
        }

        async fn apply_chunk(
            &self,
            _chunk: &[ApplyEntry],
        ) -> Result<Vec<ApplyResult>, StateMachineError> {
            Ok(vec![])
        }

        fn last_applied(&self) -> LogId {
            LogId::default()
        }

        async fn apply_snapshot_from_file(
            &self,
            _metadata: &SnapshotMetadata,
            _snapshot_path: std::path::PathBuf,
        ) -> Result<(), StateMachineError> {
            Ok(())
        }

        async fn generate_snapshot_data(
            &self,
            _new_snapshot_dir: std::path::PathBuf,
        ) -> Result<LogId, StateMachineError> {
            Ok(LogId::default())
        }

        // scan_prefix intentionally omitted → exercises the trait default implementation
    }

    /// Test that the default scan_prefix returns an error.
    ///
    /// State machines that do not support prefix scans must return an error so
    /// callers can surface a clear message instead of panicking.
    #[tokio::test]
    async fn test_scan_prefix_default_returns_error() {
        let sm = MinimalSm {
            running: AtomicBool::new(true),
        };

        let result = sm.scan_prefix(b"/test/prefix/");

        assert!(
            result.is_err(),
            "default scan_prefix must return Err; got Ok unexpectedly"
        );
        // The error type is StateMachineError — check the Debug representation which
        // includes the nested message (Display only shows the outermost variant).
        let err_debug = format!("{:?}", result.unwrap_err());
        assert!(
            err_debug.contains("scan_prefix not supported"),
            "error should mention 'scan_prefix not supported', got: {err_debug}"
        );
    }
}

/// The suite tests for a snapshot install and for a snapshot taken during writes must be able to
/// fail.
///
/// `InMemorySm` is a small state machine with three ways to go wrong, each switched on by a flag:
/// - `faulty`: it installs a snapshot by emptying its state, pausing, and refilling it, so a read
///   in between sees an empty state.
/// - `late_position`: it records the position of a snapshot after it has copied the data, so a
///   write in between leaves the position ahead of the data. Without a flag it replaces its state
///   in one step and exports data and position together.
#[cfg(test)]
mod install_reader_suite_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::storage::SnapshotMetadata;

    use super::StateMachineBuilder;
    use super::StateMachineTestSuite;
    use crate::ApplyEntry;
    use crate::Command;
    use crate::Error;
    use crate::storage::StateMachine;
    use crate::storage::StateMachineError;
    use crate::storage::state_machine::ApplyResult;

    /// What survives a restart: shared by every state machine the builder creates.
    #[derive(Default)]
    struct Durable {
        data: RwLock<HashMap<Bytes, Bytes>>,
        applied: RwLock<LogId>,
    }

    impl Durable {
        /// What a crash leaves on disk: a copy of what is durable right now.
        fn copy(&self) -> Durable {
            Durable {
                data: RwLock::new(self.data.read().unwrap().clone()),
                applied: RwLock::new(*self.applied.read().unwrap()),
            }
        }
    }

    struct InMemorySm {
        durable: Arc<Durable>,
        faulty: bool,
        late_position: bool,
        /// Wrong: the position is kept in memory and only made durable by `close()`.
        position_on_close_only: bool,
        pending_position: RwLock<LogId>,
    }

    impl InMemorySm {
        fn new(
            durable: Arc<Durable>,
            faulty: bool,
            late_position: bool,
            position_on_close_only: bool,
        ) -> Self {
            let pending_position = RwLock::new(*durable.applied.read().unwrap());
            Self {
                durable,
                faulty,
                late_position,
                position_on_close_only,
                pending_position,
            }
        }
    }

    #[async_trait]
    impl StateMachine for InMemorySm {
        fn get(
            &self,
            key: &[u8],
        ) -> Result<Option<Bytes>, StateMachineError> {
            Ok(self.durable.data.read().unwrap().get(key).cloned())
        }

        async fn apply_chunk(
            &self,
            chunk: &[ApplyEntry],
        ) -> Result<Vec<ApplyResult>, StateMachineError> {
            // Data and position change under one lock, like a database transaction.
            let mut data = self.durable.data.write().unwrap();
            for entry in chunk {
                if let Command::Insert { key, value } = &entry.command {
                    data.insert(key.clone(), value.clone());
                }
                let at = LogId {
                    index: entry.index,
                    term: entry.term,
                };
                if self.position_on_close_only {
                    *self.pending_position.write().unwrap() = at;
                } else {
                    *self.durable.applied.write().unwrap() = at;
                }
            }
            Ok(chunk.iter().map(|e| ApplyResult::success(e.index)).collect())
        }

        fn last_applied(&self) -> LogId {
            if self.position_on_close_only {
                *self.pending_position.read().unwrap()
            } else {
                *self.durable.applied.read().unwrap()
            }
        }

        fn close(&self) -> Result<(), StateMachineError> {
            if self.position_on_close_only {
                *self.durable.applied.write().unwrap() = *self.pending_position.read().unwrap();
            }
            Ok(())
        }

        async fn generate_snapshot_data(
            &self,
            new_snapshot_dir: PathBuf,
        ) -> Result<LogId, StateMachineError> {
            std::fs::create_dir_all(&new_snapshot_dir)?;
            // Right: copy the data and read the position under the same lock.
            let (lines, mut position) = {
                let data = self.durable.data.read().unwrap();
                let lines: Vec<String> = data
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{}={}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(v)
                        )
                    })
                    .collect();
                (lines, *self.durable.applied.read().unwrap())
            };
            if self.late_position {
                // Wrong: the position is read after the data was copied, and writes may have
                // happened in between.
                tokio::time::sleep(Duration::from_millis(30)).await;
                position = *self.durable.applied.read().unwrap();
            }
            std::fs::write(new_snapshot_dir.join("data.txt"), lines.join("\n"))?;
            std::fs::write(
                new_snapshot_dir.join("position.txt"),
                format!("{} {}", position.index, position.term),
            )?;
            Ok(position)
        }

        async fn apply_snapshot_from_file(
            &self,
            _metadata: &SnapshotMetadata,
            snapshot_path: PathBuf,
        ) -> Result<(), StateMachineError> {
            let text = std::fs::read_to_string(snapshot_path.join("data.txt"))?;
            let new_data: HashMap<Bytes, Bytes> = text
                .lines()
                .filter_map(|l| l.split_once('='))
                .map(|(k, v)| (Bytes::from(k.to_string()), Bytes::from(v.to_string())))
                .collect();
            let position = std::fs::read_to_string(snapshot_path.join("position.txt"))?;
            let (index, term) = position.split_once(' ').expect("index and term");
            let position = LogId {
                index: index.parse().unwrap(),
                term: term.parse().unwrap(),
            };
            if self.faulty {
                // Wrong: readers can run between the clear and the refill.
                self.durable.data.write().unwrap().clear();
                tokio::time::sleep(Duration::from_millis(20)).await;
                *self.durable.data.write().unwrap() = new_data;
            } else {
                // Right: the whole state is replaced in one step.
                *self.durable.data.write().unwrap() = new_data;
            }
            *self.durable.applied.write().unwrap() = position;
            *self.pending_position.write().unwrap() = position;
            Ok(())
        }
    }

    #[derive(Default)]
    struct InMemoryBuilder {
        durable: std::sync::Mutex<Arc<Durable>>,
        faulty: bool,
        late_position: bool,
        position_on_close_only: bool,
    }

    #[async_trait]
    impl StateMachineBuilder for InMemoryBuilder {
        async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
            let fresh = Arc::new(Durable::default());
            *self.durable.lock().unwrap() = fresh.clone();
            Ok(Arc::new(InMemorySm::new(
                fresh,
                self.faulty,
                self.late_position,
                self.position_on_close_only,
            )))
        }

        async fn reopen(&self) -> Result<Arc<dyn StateMachine>, Error> {
            Ok(Arc::new(InMemorySm::new(
                self.durable.lock().unwrap().clone(),
                self.faulty,
                self.late_position,
                self.position_on_close_only,
            )))
        }

        async fn build_from_crash_image(&self) -> Result<Option<Arc<dyn StateMachine>>, Error> {
            Ok(Some(Arc::new(InMemorySm::new(
                Arc::new(self.durable.lock().unwrap().copy()),
                self.faulty,
                self.late_position,
                self.position_on_close_only,
            ))))
        }

        async fn cleanup(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Purpose: a state machine that replaces its state in one step passes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_reader_suite_accepts_a_state_machine_that_swaps_its_state_atomically() {
        StateMachineTestSuite::test_reads_during_snapshot_install_see_old_new_or_fail(
            &InMemoryBuilder::default(),
        )
        .await
        .expect("an atomic install must pass");
    }

    /// Purpose: a state machine that empties its state and then refills it is caught, because a
    /// reader that runs in between sees keys as not found.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[should_panic(expected = "saw a state that never existed")]
    async fn test_reader_suite_rejects_a_state_machine_that_empties_its_state_during_install() {
        let _ = StateMachineTestSuite::test_reads_during_snapshot_install_see_old_new_or_fail(
            &InMemoryBuilder {
                faulty: true,
                ..Default::default()
            },
        )
        .await;
    }

    /// Purpose: a state machine that exports its data and position together passes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_snapshot_suite_accepts_a_state_machine_that_exports_data_and_position_together() {
        StateMachineTestSuite::test_snapshot_taken_during_writes_never_skips_entries_after_restart(
            &InMemoryBuilder::default(),
        )
        .await
        .expect("a consistent export must pass");
    }

    /// Purpose: a state machine that records the position after copying the data is caught: after
    /// the restart its position is ahead of its data.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[should_panic(expected = "would never be replayed")]
    async fn test_snapshot_suite_rejects_a_state_machine_whose_exported_position_runs_ahead() {
        let _ = StateMachineTestSuite::test_snapshot_taken_during_writes_never_skips_entries_after_restart(
            &InMemoryBuilder {
                late_position: true,
                ..Default::default()
            },
        )
        .await;
    }

    /// Purpose: a state machine that commits the position with the data passes the crash test.
    #[tokio::test]
    async fn test_crash_suite_accepts_a_state_machine_that_commits_position_with_data() {
        StateMachineTestSuite::test_restart_after_crash_finds_position_and_data_of_the_same_entry(
            &InMemoryBuilder::default(),
        )
        .await
        .expect("position committed with the data must pass");
    }

    /// Purpose: a state machine that only makes the position durable at `close()` is caught: after
    /// a crash its data is there but its position is not.
    #[tokio::test]
    #[should_panic(expected = "must describe the same entry")]
    async fn test_crash_suite_rejects_a_state_machine_that_persists_position_only_at_close() {
        let _ = StateMachineTestSuite::test_restart_after_crash_finds_position_and_data_of_the_same_entry(
            &InMemoryBuilder {
                position_on_close_only: true,
                ..Default::default()
            },
        )
        .await;
    }
}
