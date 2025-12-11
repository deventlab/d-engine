//! State Machine Performance Benchmarks
//!
//! This benchmark suite measures the core performance characteristics of the state machine,
//! focusing on the overhead introduced by TTL functionality and Watch mechanism.
//!
//! Performance Targets:
//! - Without TTL: < 10ns overhead per operation
//! - With TTL passive check: < 50ns overhead per read
//! - Watch overhead on Apply path: < 0.01% (< 10ns per watcher)
//! - End-to-end Watch notification latency: < 100µs
//! - Batch operations: Linear scaling

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use d_engine_core::StateMachine;
use d_engine_core::config::WatchConfig;
use d_engine_core::watch::{WatchManager, WatcherHandle};
use d_engine_proto::client::{
    WriteCommand,
    write_command::{Insert, Operation},
};
use d_engine_proto::common::{Entry, EntryPayload, entry_payload::Payload};
use d_engine_server::storage::FileStateMachine;
use prost::Message;
use tempfile::TempDir;

/// Helper to create a temporary state machine for benchmarking
async fn create_test_state_machine() -> (FileStateMachine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create state machine");
    (sm, temp_dir)
}

/// Helper to create a WatchManager for benchmarking
fn create_watch_manager(
    event_queue_size: usize,
    watcher_buffer_size: usize,
) -> Arc<WatchManager> {
    let config = WatchConfig {
        enabled: true,
        event_queue_size,
        watcher_buffer_size,
        enable_metrics: false,
    };
    let manager = WatchManager::new(config);
    manager.start();
    Arc::new(manager)
}

/// Helper to register multiple watchers
///
/// Returns the watcher handles to keep watchers alive during benchmarks.
/// Handles must be kept in scope or watchers will be immediately unregistered.
async fn register_watchers(
    manager: &WatchManager,
    count: usize,
    key_prefix: &str,
) -> Vec<WatcherHandle> {
    let mut handles = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("{key_prefix}{i}");
        handles.push(manager.register(key.into()).await);
    }
    handles
}

/// Helper to create write entries without TTL
fn create_entries_without_ttl(
    count: usize,
    start_index: u64,
) -> Vec<Entry> {
    (0..count)
        .map(|i| {
            let key = format!("key_{}", start_index + i as u64);
            let value = format!("value_{}", start_index + i as u64);

            let insert = Insert {
                key: Bytes::from(key),
                value: Bytes::from(value),
                ttl_secs: 0,
            };
            let write_cmd = WriteCommand {
                operation: Some(Operation::Insert(insert)),
            };
            let payload = Payload::Command(write_cmd.encode_to_vec().into());

            Entry {
                index: start_index + i as u64,
                term: 1,
                payload: Some(EntryPayload {
                    payload: Some(payload),
                }),
            }
        })
        .collect()
}

/// Helper to create write entries with TTL
fn create_entries_with_ttl(
    count: usize,
    start_index: u64,
    ttl_secs: u64,
) -> Vec<Entry> {
    (0..count)
        .map(|i| {
            let key = format!("key_ttl_{}", start_index + i as u64);
            let value = format!("value_ttl_{}", start_index + i as u64);

            let insert = Insert {
                key: Bytes::from(key),
                value: Bytes::from(value),
                ttl_secs,
            };
            let write_cmd = WriteCommand {
                operation: Some(Operation::Insert(insert)),
            };
            let payload = Payload::Command(write_cmd.encode_to_vec().into());

            Entry {
                index: start_index + i as u64,
                term: 1,
                payload: Some(EntryPayload {
                    payload: Some(payload),
                }),
            }
        })
        .collect()
}

/// Benchmark: Apply operations WITHOUT TTL
/// Target: < 10ns overhead per operation
fn bench_apply_without_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("apply_without_ttl", |b| {
        b.to_async(&runtime).iter(|| async {
            let (sm, _temp_dir) = create_test_state_machine().await;
            let entries = create_entries_without_ttl(1, 1);

            // Measure pure apply performance
            sm.apply_chunk(entries).await.unwrap();
            black_box(());
        });
    });
}

/// Benchmark: Apply operations WITH TTL
/// This measures the overhead of registering TTL entries
fn bench_apply_with_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("apply_with_ttl", |b| {
        b.to_async(&runtime).iter(|| async {
            let (sm, _temp_dir) = create_test_state_machine().await;
            let entries = create_entries_with_ttl(1, 1, 3600); // 1 hour TTL

            // Measure apply with TTL registration
            sm.apply_chunk(entries).await.unwrap();
            black_box(());
        });
    });
}

/// Benchmark: Get operation WITHOUT TTL data
/// Baseline for read performance
fn bench_get_without_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Setup state machine once before benchmark
    let (sm, _temp_dir) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;
        let entries = create_entries_without_ttl(100, 1);
        sm.apply_chunk(entries).await.unwrap();
        (sm, temp_dir)
    });

    c.bench_function("get_without_ttl", |b| {
        b.iter(|| {
            // Measure pure read performance (synchronous get)
            let key = b"key_50";
            black_box(sm.get(key).unwrap());
        });
    });
}

/// Benchmark: Get operation WITH TTL passive check
/// Target: < 50ns overhead compared to non-TTL reads
fn bench_get_with_ttl_check(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Setup state machine once before benchmark
    let (sm, _temp_dir) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;
        let entries = create_entries_with_ttl(100, 1, 3600); // Long TTL
        sm.apply_chunk(entries).await.unwrap();
        (sm, temp_dir)
    });

    c.bench_function("get_with_ttl_check", |b| {
        b.iter(|| {
            // Measure read with TTL check (synchronous get)
            let key = b"key_ttl_50";
            black_box(sm.get(key).unwrap());
        });
    });
}

/// Benchmark: Get operation with EXPIRED TTL entry
/// This measures the cost of passive deletion
fn bench_get_expired_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Setup state machine once before benchmark
    let (sm, _temp_dir) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;
        let entries = create_entries_with_ttl(100, 1, 1); // 1 second TTL
        sm.apply_chunk(entries).await.unwrap();

        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;

        (sm, temp_dir)
    });

    c.bench_function("get_expired_ttl", |b| {
        b.iter(|| {
            // Measure read with expired entry (should trigger passive deletion)
            let key = b"key_ttl_50";
            black_box(sm.get(key).unwrap());
        });
    });
}

/// Benchmark: Batch apply operations (scaling test)
/// Verify that performance scales linearly with batch size
fn bench_batch_apply(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("batch_apply");

    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let (sm, _temp_dir) = create_test_state_machine().await;
                let entries = create_entries_without_ttl(size, 1);

                sm.apply_chunk(entries).await.unwrap();
                black_box(());
            });
        });
    }

    group.finish();
}

/// Benchmark: Batch apply with TTL (scaling test)
fn bench_batch_apply_with_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("batch_apply_with_ttl");

    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let (sm, _temp_dir) = create_test_state_machine().await;
                let entries = create_entries_with_ttl(size, 1, 3600);

                sm.apply_chunk(entries).await.unwrap();
                black_box(());
            });
        });
    }

    group.finish();
}

/// Benchmark: Apply operations WITHOUT Watch (baseline)
/// Target: Establish baseline performance
fn bench_apply_without_watch(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("apply_without_watch", |b| {
        b.to_async(&runtime).iter(|| async {
            let (sm, _temp_dir) = create_test_state_machine().await;
            let entries = create_entries_without_ttl(100, 1);

            // Measure pure apply performance without watch
            sm.apply_chunk(entries).await.unwrap();
            black_box(());
        });
    });
}

/// Benchmark: Apply operations WITH 1 watcher
/// Target: < 10ns overhead compared to baseline
fn bench_apply_with_1_watcher(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("apply_with_1_watcher", |b| {
        b.to_async(&runtime).iter(|| async {
            let (sm, _temp_dir) = create_test_state_machine().await;
            let watch_manager = create_watch_manager(1000, 10);

            // Register 1 watcher (keep handle alive to prevent unregistration)
            let _watchers = register_watchers(&watch_manager, 1, "key_").await;

            let entries = create_entries_without_ttl(100, 1);

            // Simulate notify_watchers call for each entry
            for entry in &entries {
                if let Some(payload) = &entry.payload {
                    if let Some(Payload::Command(cmd_bytes)) = &payload.payload {
                        if let Ok(write_cmd) = WriteCommand::decode(cmd_bytes.as_ref()) {
                            if let Some(op) = write_cmd.operation {
                                match op {
                                    Operation::Insert(insert) => {
                                        watch_manager
                                            .notify_put(insert.key.clone(), insert.value.clone());
                                    }
                                    Operation::Delete(delete) => {
                                        watch_manager.notify_delete(delete.key.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            sm.apply_chunk(entries).await.unwrap();
            black_box(());
        });
    });
}

/// Benchmark: Apply operations WITH 10 watchers
/// Target: < 100ns overhead compared to baseline
fn bench_apply_with_10_watchers(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("apply_with_10_watchers", |b| {
        b.to_async(&runtime).iter(|| async {
            let (sm, _temp_dir) = create_test_state_machine().await;
            let watch_manager = create_watch_manager(1000, 10);

            // Register 10 watchers (keep handles alive to prevent unregistration)
            let _watchers = register_watchers(&watch_manager, 10, "key_").await;

            let entries = create_entries_without_ttl(100, 1);

            // Simulate notify_watchers call for each entry
            for entry in &entries {
                if let Some(payload) = &entry.payload {
                    if let Some(Payload::Command(cmd_bytes)) = &payload.payload {
                        if let Ok(write_cmd) = WriteCommand::decode(cmd_bytes.as_ref()) {
                            if let Some(op) = write_cmd.operation {
                                match op {
                                    Operation::Insert(insert) => {
                                        watch_manager
                                            .notify_put(insert.key.clone(), insert.value.clone());
                                    }
                                    Operation::Delete(delete) => {
                                        watch_manager.notify_delete(delete.key.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            sm.apply_chunk(entries).await.unwrap();
            black_box(());
        });
    });
}

/// Benchmark: Apply operations WITH 100 watchers
/// Target: < 1µs overhead compared to baseline
fn bench_apply_with_100_watchers(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("apply_with_100_watchers", |b| {
        b.to_async(&runtime).iter(|| async {
            let (sm, _temp_dir) = create_test_state_machine().await;
            let watch_manager = create_watch_manager(1000, 10);

            // Register 100 watchers (keep handles alive to prevent unregistration)
            let _watchers = register_watchers(&watch_manager, 100, "key_").await;

            let entries = create_entries_without_ttl(100, 1);

            // Simulate notify_watchers call for each entry
            for entry in &entries {
                if let Some(payload) = &entry.payload {
                    if let Some(Payload::Command(cmd_bytes)) = &payload.payload {
                        if let Ok(write_cmd) = WriteCommand::decode(cmd_bytes.as_ref()) {
                            if let Some(op) = write_cmd.operation {
                                match op {
                                    Operation::Insert(insert) => {
                                        watch_manager
                                            .notify_put(insert.key.clone(), insert.value.clone());
                                    }
                                    Operation::Delete(delete) => {
                                        watch_manager.notify_delete(delete.key.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            sm.apply_chunk(entries).await.unwrap();
            black_box(());
        });
    });
}

/// Benchmark: End-to-end Watch notification latency
/// Target: < 100µs from notify to receiver
fn bench_watch_e2e_latency(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("watch_e2e_latency", |b| {
        b.to_async(&runtime).iter(|| async {
            let watch_manager = create_watch_manager(1000, 10);

            let key = Bytes::from("test_key");
            let value = Bytes::from("test_value");

            // Register a watcher
            let handle = watch_manager.register(key.clone()).await;
            let (_id, _key, mut receiver, _guard) = handle.into_receiver();

            // Measure time from notify to receive
            let start = tokio::time::Instant::now();
            watch_manager.notify_put(key.clone(), value.clone());

            // Wait for event to arrive
            if let Some(_event) = receiver.recv().await {
                let latency = start.elapsed();
                black_box(latency);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_apply_without_ttl,
    bench_apply_with_ttl,
    bench_get_without_ttl,
    bench_get_with_ttl_check,
    bench_get_expired_ttl,
    bench_batch_apply,
    bench_batch_apply_with_ttl,
    bench_apply_without_watch,
    bench_apply_with_1_watcher,
    bench_apply_with_10_watchers,
    bench_apply_with_100_watchers,
    bench_watch_e2e_latency,
);

criterion_main!(benches);
