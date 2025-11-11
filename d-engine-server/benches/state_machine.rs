//! State Machine Performance Benchmarks
//!
//! This benchmark suite measures the core performance characteristics of the state machine,
//! focusing on the overhead introduced by TTL functionality.
//!
//! Performance Targets:
//! - Without TTL: < 10ns overhead per operation
//! - With TTL passive check: < 50ns overhead per read
//! - Batch operations: Linear scaling

use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use d_engine_core::StateMachine;
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
                ttl_secs: None,
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
                ttl_secs: Some(ttl_secs),
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
            black_box(sm.apply_chunk(entries).await.unwrap());
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
            black_box(sm.apply_chunk(entries).await.unwrap());
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

                black_box(sm.apply_chunk(entries).await.unwrap());
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

                black_box(sm.apply_chunk(entries).await.unwrap());
            });
        });
    }

    group.finish();
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
);

criterion_main!(benches);
