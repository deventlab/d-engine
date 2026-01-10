//! Watch Mechanism Performance Benchmarks
//!
//! This benchmark suite measures the performance impact of the Watch mechanism
//! on different aspects of the system.
//!
//! Performance Targets:
//! - Watch overhead on apply_chunk: < 1% with 100 watchers
//! - Embedded mode overhead: < 5% with 100 watchers
//! - Standalone mode overhead: < 10% with 100 watchers
//! - Notification latency: < 100µs (p99)

#![cfg(all(feature = "watch", feature = "rocksdb"))]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use d_engine_core::StateMachine;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBStorageEngine;
use d_engine_server::api::EmbeddedEngine;
use prost::Message;
use tempfile::TempDir;
use tokio::time::sleep;

//=============================================================================
// Helper Functions
//=============================================================================

/// Create test entries for benchmarking
fn create_test_entries(
    count: usize,
    start_index: u64,
) -> Vec<Entry> {
    let mut entries = Vec::new();
    for i in 0..count {
        let key = format!("key_{i}");
        let value = format!("value_{i}");

        let write_cmd = WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::from(key.into_bytes()),
                value: Bytes::from(value.into_bytes()),
                ttl_secs: 0,
            })),
        };

        let mut buf = Vec::new();
        write_cmd.encode(&mut buf).unwrap();

        entries.push(Entry {
            index: start_index + i as u64,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(Bytes::from(buf))),
            }),
        });
    }
    entries
}

/// Create a temporary EmbeddedEngine for benchmarking
async fn create_embedded_engine() -> Result<(EmbeddedEngine, TempDir), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");

    // Create config with watch enabled
    let config_path = temp_dir.path().join("d-engine.toml");
    let port = 50000 + (std::process::id() % 10000);
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{port}"
db_root_dir = "{}"
single_node = true

[raft.watch]
event_queue_size = 10000
watcher_buffer_size = 100
"#,
        db_path.display()
    );
    std::fs::write(&config_path, config_content)?;

    // Create storage and state machine
    let storage_path = db_path.join("storage");
    let sm_path = db_path.join("state_machine");
    tokio::fs::create_dir_all(&storage_path).await?;
    tokio::fs::create_dir_all(&sm_path).await?;

    let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
    let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

    let engine =
        EmbeddedEngine::start_custom(storage, state_machine, Some(config_path.to_str().unwrap()))
            .await?;

    // Wait for ready
    engine.wait_ready(Duration::from_secs(5)).await?;

    Ok((engine, temp_dir))
}

//=============================================================================
// Benchmark 1: Embedded Mode Performance Impact
//=============================================================================

fn bench_embedded_mode_with_watchers(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("embedded_mode_put");
    group.sample_size(10); // Reduce sample size due to engine startup cost

    for &watcher_count in &[0, 10, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{watcher_count}_watchers")),
            &watcher_count,
            |b, &count| {
                // Setup: Create engine once, reuse across iterations
                let (engine, _temp_dir) = runtime.block_on(async {
                    create_embedded_engine().await.expect("Failed to create engine")
                });

                // Register watchers once
                let mut _watchers = Vec::new();
                for i in 0..count {
                    let key = format!("watch_key_{i}").into_bytes();
                    let watcher = engine.watch(&key).expect("Failed to register watcher");
                    _watchers.push(watcher);
                }

                runtime.block_on(async {
                    sleep(Duration::from_millis(100)).await;
                });

                // Benchmark: Only measure PUT operations
                b.to_async(&runtime).iter(|| async {
                    for i in 0..100 {
                        let key = format!("key_{i}").into_bytes();
                        let value = format!("value_{i}").into_bytes();
                        engine.client().put(&key, &value).await.expect("PUT failed");
                    }
                });

                // Cleanup after all iterations
                runtime.block_on(async {
                    engine.stop().await.expect("Failed to stop engine");
                });
            },
        );
    }

    group.finish();
}

//=============================================================================
// Benchmark 2: Watch Notification Latency
//=============================================================================

fn bench_watch_notification_latency(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Setup: Create engine once outside the benchmark loop
    let (engine, _temp_dir) = runtime
        .block_on(async { create_embedded_engine().await.expect("Failed to create engine") });

    c.bench_function("watch_notification_latency", |b| {
        b.to_async(&runtime).iter(|| async {
            let key = b"latency_test_key";
            let mut watcher = engine.watch(key).expect("Failed to register watcher");

            // Give watcher time to register
            sleep(Duration::from_millis(50)).await;

            // Spawn receiver task
            let recv_handle = tokio::spawn(async move {
                let start = std::time::Instant::now();
                watcher.receiver_mut().recv().await;
                start.elapsed()
            });

            // Perform PUT
            engine.client().put(key, b"test_value").await.expect("PUT failed");

            // Wait for notification and measure latency
            let latency = recv_handle.await.expect("Receiver task failed");

            black_box(latency);
        });
    });

    // Cleanup after all iterations
    runtime.block_on(async {
        engine.stop().await.expect("Failed to stop engine");
    });
}

//=============================================================================
// Benchmark 3: Multiple Watchers Same Key
//=============================================================================

fn bench_multiple_watchers_same_key(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("multiple_watchers_same_key");

    // Setup: Create engine once outside the benchmark loop
    let (engine, _temp_dir) = runtime
        .block_on(async { create_embedded_engine().await.expect("Failed to create engine") });

    for &watcher_count in &[1, 10, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{watcher_count}_watchers")),
            &watcher_count,
            |b, &count| {
                // Register watchers once per parameter
                let key = b"shared_key";
                let mut _watchers = Vec::new();
                for _ in 0..count {
                    let watcher = engine.watch(key).expect("Failed to register watcher");
                    _watchers.push(watcher);
                }

                runtime.block_on(async {
                    sleep(Duration::from_millis(50)).await;
                });

                b.to_async(&runtime).iter(|| async {
                    // Only measure PUT broadcast time
                    let start = std::time::Instant::now();
                    engine.client().put(key, b"broadcast_value").await.expect("PUT failed");
                    let elapsed = start.elapsed();

                    black_box(elapsed);
                });
            },
        );
    }

    group.finish();

    // Cleanup after all iterations
    runtime.block_on(async {
        engine.stop().await.expect("Failed to stop engine");
    });
}

//=============================================================================
// Benchmark 4: Watcher Cleanup Performance
//=============================================================================

fn bench_watcher_cleanup(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("watcher_cleanup");

    // Setup: Create engine once outside the benchmark loop
    let (engine, _temp_dir) = runtime
        .block_on(async { create_embedded_engine().await.expect("Failed to create engine") });

    for &watcher_count in &[10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{watcher_count}_watchers")),
            &watcher_count,
            |b, &count| {
                b.iter_with_setup(
                    || {
                        // Setup: Register watchers (not measured)
                        let mut watchers = Vec::new();
                        for i in 0..count {
                            let key = format!("cleanup_key_{i}").into_bytes();
                            let watcher = engine.watch(&key).expect("Failed to register watcher");
                            watchers.push(watcher);
                        }
                        runtime.block_on(async {
                            sleep(Duration::from_millis(100)).await;
                        });
                        watchers
                    },
                    |watchers| {
                        // Only measure cleanup time
                        let start = std::time::Instant::now();
                        drop(watchers);
                        let elapsed = start.elapsed();
                        black_box(elapsed);
                    },
                );
            },
        );
    }

    group.finish();

    // Cleanup after all iterations
    runtime.block_on(async {
        engine.stop().await.expect("Failed to stop engine");
    });
}

//=============================================================================
// Benchmark 6: Apply Chunk Watch Overhead
//=============================================================================
//
// Note: This benchmark tests StateMachine.apply_chunk() directly.
// Watch integration happens at StateMachineHandler level, which requires
// complex TypeConfig setup. For now, this measures pure apply_chunk performance
// as a baseline. Full watch overhead is measured by embedded_mode_put benchmark.

fn bench_apply_chunk_baseline(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("apply_chunk_baseline");

    group.bench_function("100_entries", |b| {
        b.to_async(&runtime).iter(|| async {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let sm_path = temp_dir.path().join("state_machine");
            tokio::fs::create_dir_all(&sm_path).await.unwrap();

            // Create state machine
            let state_machine = Arc::new(
                RocksDBStateMachine::new(&sm_path).expect("Failed to create state machine"),
            );

            // Create test entries
            let entries = create_test_entries(100, 1);

            // Benchmark: apply 100 entries
            let start = std::time::Instant::now();
            state_machine.apply_chunk(entries).await.expect("apply_chunk failed");
            let elapsed = start.elapsed();

            black_box(elapsed);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_embedded_mode_with_watchers,
    bench_watch_notification_latency,
    bench_multiple_watchers_same_key,
    bench_watcher_cleanup,
    // bench_standalone_mode_with_watchers, // Removed: duplicate of embedded_mode
    bench_apply_chunk_baseline,
);

criterion_main!(benches);
