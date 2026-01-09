//! Lease Performance Benchmarks - Simplified Configuration
//!
//! This benchmark validates the performance of the simplified lease configuration:
//! - get() method: Background mode (no on-read overhead)
//! - apply_chunk TTL registration: optimized with cached flags
//!
//! These benchmarks compare:
//! 1. Background strategy vs No Lease overhead in get()
//! 2. TTL registration performance in apply_chunk()
//! 3. Batch operations with TTL

use std::sync::Arc;

use bytes::Bytes;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use d_engine_core::StateMachine;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_server::storage::DefaultLease;
use d_engine_server::storage::FileStateMachine;
use prost::Message;
use tempfile::TempDir;

/// Create FileStateMachine with Background strategy (lease enabled)
async fn create_sm_background() -> (FileStateMachine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let lease_config = d_engine_core::config::LeaseConfig {
        enabled: true,
        interval_ms: 1000,
        max_cleanup_duration_ms: 1,
    };

    let mut sm = FileStateMachine::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create state machine");

    let lease = Arc::new(DefaultLease::new(lease_config));
    sm.set_lease(lease);

    (sm, temp_dir)
}

/// Create FileStateMachine without lease (no TTL overhead)
async fn create_sm_no_lease() -> (FileStateMachine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let sm = FileStateMachine::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create state machine");

    (sm, temp_dir)
}

/// Helper to create entry with TTL
fn create_entry_with_ttl(
    index: u64,
    term: u64,
    key: &[u8],
    value: &[u8],
    ttl_secs: u64,
) -> Entry {
    let insert = d_engine_proto::client::write_command::Insert {
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
        ttl_secs,
    };

    let write_cmd = WriteCommand {
        operation: Some(Operation::Insert(insert)),
    };

    let mut buf = Vec::new();
    write_cmd.encode(&mut buf).unwrap();

    Entry {
        index,
        term,
        payload: Some(EntryPayload {
            payload: Some(Payload::Command(Bytes::from(buf))),
        }),
    }
}

/// Benchmark: get() performance with different strategies
fn bench_get_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_performance");

    // Scenario 1: No lease (baseline)
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let (sm, _temp) = rt.block_on(create_sm_no_lease());

    // Insert key without TTL
    rt.block_on(async {
        let entry = create_entry_with_ttl(1, 1, b"bench_key", b"bench_value", 0);
        sm.apply_chunk(vec![entry]).await.unwrap();
    });

    group.bench_function("no_lease_baseline", |b| {
        b.iter(|| {
            let result = sm.get(black_box(b"bench_key")).unwrap();
            black_box(result);
        });
    });

    drop(sm);
    drop(_temp);

    // Scenario 2: Background strategy (lease enabled, no on-read checks)
    let (sm_bg, _temp_bg) = rt.block_on(create_sm_background());

    rt.block_on(async {
        let entry = create_entry_with_ttl(1, 1, b"bg_key", b"bg_value", 3600);
        sm_bg.apply_chunk(vec![entry]).await.unwrap();
    });

    group.bench_function("background_strategy", |b| {
        b.iter(|| {
            let result = sm_bg.get(black_box(b"bg_key")).unwrap();
            black_box(result);
        });
    });

    drop(sm_bg);
    drop(_temp_bg);

    group.finish();
}

/// Benchmark: apply_chunk TTL registration performance
fn bench_ttl_registration(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_registration");

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Scenario 1: No TTL (baseline)
    let (sm_no_ttl, _temp1) = rt.block_on(create_sm_background());

    group.bench_function("no_ttl_baseline", |b| {
        b.to_async(&rt).iter(|| async {
            let entry = create_entry_with_ttl(1, 1, b"key", b"value", 0);
            sm_no_ttl.apply_chunk(vec![entry]).await.unwrap();
        });
    });

    drop(sm_no_ttl);
    drop(_temp1);

    // Scenario 2: With TTL (lease enabled)
    let (sm_ttl, _temp2) = rt.block_on(create_sm_background());

    group.bench_function("with_ttl_background", |b| {
        b.to_async(&rt).iter(|| async {
            let entry = create_entry_with_ttl(1, 1, b"key", b"value", 3600);
            sm_ttl.apply_chunk(vec![entry]).await.unwrap();
        });
    });

    drop(sm_ttl);
    drop(_temp2);

    group.finish();
}

/// Benchmark: Batch operations with TTL
fn bench_batch_ttl_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_ttl_operations");

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Create StateMachine once for all batch sizes
    let (sm_bg, _temp_bg) = rt.block_on(create_sm_background());

    for batch_size in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("background", batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let entries: Vec<Entry> = (0..size)
                        .map(|i| {
                            create_entry_with_ttl(
                                1 + i as u64,
                                1,
                                format!("key_{i}").as_bytes(),
                                b"value",
                                3600,
                            )
                        })
                        .collect();
                    sm_bg.apply_chunk(entries).await.unwrap();
                });
            },
        );
    }

    drop(sm_bg);
    drop(_temp_bg);

    group.finish();
}

criterion_group!(
    benches,
    bench_get_performance,
    bench_ttl_registration,
    bench_batch_ttl_operations
);
criterion_main!(benches);
