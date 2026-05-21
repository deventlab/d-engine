//! TTL Manager Performance Benchmarks
//!
//! This benchmark suite focuses specifically on TTL management operations,
//! particularly background cleanup and TTL registration overhead.
//!
//! Performance Targets:
//! - Background cleanup: < 1ms for typical workloads
//! - TTL registration: < 100ns per entry
//! - Expired check: < 50ns per key

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use d_engine_core::{ApplyEntry, Command, Lease, StateMachine};
use d_engine_server::storage::FileStateMachine;
use tempfile::TempDir;

/// Helper to create a temporary state machine for benchmarking
async fn create_test_state_machine() -> (FileStateMachine, TempDir) {
    use d_engine_server::storage::DefaultLease;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    // For TTL benchmarks, we need lease enabled
    let lease_config = d_engine_core::config::LeaseConfig {
        enabled: true,
        cleanup_interval_ms: 1000,
        max_cleanup_duration_ms: 1,
    };

    let mut sm = FileStateMachine::new(temp_dir.path().to_path_buf())
        .await
        .expect("Failed to create state machine");

    // Manually inject lease for benchmarking
    let lease = std::sync::Arc::new(DefaultLease::new(lease_config));
    sm.set_lease(lease);
    sm.load_lease_data().await.expect("Failed to load lease data");

    (sm, temp_dir)
}

/// Helper to create write entries with TTL
fn create_entries_with_ttl(
    count: usize,
    start_index: u64,
    ttl_secs: u64,
) -> Vec<ApplyEntry> {
    (0..count)
        .map(|i| {
            let key = format!("key_ttl_{}", start_index + i as u64);
            let value = format!("value_ttl_{}", start_index + i as u64);
            ApplyEntry {
                index: start_index + i as u64,
                term: 1,
                command: Command::Insert {
                    key: Bytes::from(key),
                    value: Bytes::from(value),
                    ttl_secs: if ttl_secs > 0 { Some(ttl_secs) } else { None },
                },
            }
        })
        .collect()
}

/// Helper to create a no-op entry (for triggering piggyback cleanup)
fn create_noop_entry(index: u64) -> ApplyEntry {
    ApplyEntry {
        index,
        term: 1,
        command: Command::Noop,
    }
}

/// Benchmark: Piggyback cleanup with varying numbers of expired keys
/// Target: < 1ms for typical workload (100 expired keys)
/// This directly benchmarks lease.on_apply() to isolate cleanup overhead
fn bench_piggyback_cleanup(c: &mut Criterion) {
    use d_engine_server::storage::DefaultLease;

    let mut group = c.benchmark_group("piggyback_cleanup");

    // Test with different numbers of expired keys
    for expired_count in [10, 50, 100, 500].iter() {
        let lease_config = d_engine_core::config::LeaseConfig {
            enabled: true,
            cleanup_interval_ms: 1000,
            max_cleanup_duration_ms: 1,
        };
        let lease = DefaultLease::new(lease_config);

        // Register keys with very short TTL
        for i in 0..*expired_count {
            let key = format!("key_ttl_{i}");
            lease.register(bytes::Bytes::from(key), 1); // 1 second TTL
        }

        // Wait for expiration
        std::thread::sleep(Duration::from_secs(2));

        group.bench_with_input(
            BenchmarkId::from_parameter(expired_count),
            expired_count,
            |b, _| {
                b.iter(|| {
                    // Directly measure cleanup logic without apply_chunk overhead
                    let expired_keys = lease.on_apply();
                    black_box(expired_keys);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: TTL registration overhead
/// Measures the cost of registering a TTL entry in the TTL manager
/// Target: < 100ns per registration
fn bench_ttl_registration(c: &mut Criterion) {
    use d_engine_server::storage::DefaultLease;

    let lease_config = d_engine_core::config::LeaseConfig {
        enabled: true,
        cleanup_interval_ms: 1000,
        max_cleanup_duration_ms: 1,
    };
    let lease = DefaultLease::new(lease_config);

    c.bench_function("ttl_registration", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            // Directly measure registration overhead
            let key = format!("key_{counter}");
            lease.register(bytes::Bytes::from(key), 3600);
            counter += 1;
            black_box(());
        });
    });
}

/// Benchmark: Batch TTL registration
/// Verify that TTL registration scales well with batch size
fn bench_batch_ttl_registration(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("batch_ttl_registration");

    let (sm, _temp_dir) = runtime.block_on(async { create_test_state_machine().await });

    let idx = AtomicU64::new(1);
    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let start = idx.fetch_add(size as u64, Ordering::Relaxed);
                let entries = create_entries_with_ttl(size, start, 3600);
                sm.apply_chunk(&entries).await.unwrap();
                black_box(());
            });
        });
    }

    group.finish();
}

/// Benchmark: Mixed workload - active and expired TTL entries
/// Simulates realistic scenario with both active and expired entries
fn bench_mixed_ttl_workload(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("mixed_ttl_workload");
    group.sample_size(10);

    // Setup: Create StateMachine with mixed entries (expired + active)
    let (sm, _temp_dir) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;

        // Insert 50 entries with short TTL (will expire)
        let expired_entries = create_entries_with_ttl(50, 1, 1);
        sm.apply_chunk(&expired_entries).await.unwrap();

        // Insert 50 entries with long TTL (will remain active)
        let active_entries = create_entries_with_ttl(50, 51, 3600);
        sm.apply_chunk(&active_entries).await.unwrap();

        // Wait for first batch to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        (sm, temp_dir)
    });

    let noop_idx = AtomicU64::new(101); // setup applied 1..=100
    group.bench_function("cleanup_mixed", |b| {
        b.to_async(&runtime).iter(|| async {
            let i = noop_idx.fetch_add(1, Ordering::Relaxed);
            let noop = create_noop_entry(i);
            sm.apply_chunk(&[noop]).await.unwrap();
            black_box(());
        });
    });

    group.finish();
}

/// Benchmark: Piggyback cleanup with expired entries
/// Measures the actual cleanup overhead when expired entries exist
fn bench_piggyback_high_frequency(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("piggyback_high_frequency");
    group.sample_size(10);

    let (_temp_dir, sm) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;
        (temp_dir, sm)
    });

    let idx = AtomicU64::new(1);
    group.bench_function("cleanup_with_expired_entries", |b| {
        b.to_async(&runtime).iter(|| async {
            let start = idx.fetch_add(101, Ordering::Relaxed);
            let entries = create_entries_with_ttl(100, start, 1);
            sm.apply_chunk(&entries).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;

            let noop = create_noop_entry(start + 100);
            sm.apply_chunk(&[noop]).await.unwrap();
            black_box(());
        });
    });

    group.finish();
}

/// Benchmark: TTL with varying expiration times
/// Tests if different TTL durations affect performance
fn bench_varying_ttl_durations(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("varying_ttl_durations");

    let (sm, _temp_dir) = runtime.block_on(async { create_test_state_machine().await });

    // Test with different TTL durations
    let idx = AtomicU64::new(1);
    for ttl_secs in [60, 3600, 86400].iter() {
        // 1 min, 1 hour, 1 day
        group.bench_with_input(
            BenchmarkId::from_parameter(ttl_secs),
            ttl_secs,
            |b, &ttl_secs| {
                b.to_async(&runtime).iter(|| async {
                    let start = idx.fetch_add(100, Ordering::Relaxed);
                    let entries = create_entries_with_ttl(100, start, ttl_secs);
                    sm.apply_chunk(&entries).await.unwrap();
                    black_box(());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Worst case - all entries expired
/// Tests cleanup performance when all entries need to be removed
fn bench_worst_case_all_expired(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("worst_case_all_expired");
    group.sample_size(10);

    // Setup: Create state machine with all expired entries
    let (sm, _temp_dir) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;

        // Insert 1000 entries with very short TTL
        let entries = create_entries_with_ttl(1000, 1, 1);
        sm.apply_chunk(&entries).await.unwrap();

        // Wait for all to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        (sm, temp_dir)
    });

    let idx = AtomicU64::new(1001); // setup applied 1..=1000
    group.bench_function("cleanup_all_expired", |b| {
        b.to_async(&runtime).iter(|| async {
            let start = idx.fetch_add(1001, Ordering::Relaxed);
            let entries = create_entries_with_ttl(1000, start, 1);
            sm.apply_chunk(&entries).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;

            let noop = create_noop_entry(start + 1000);
            sm.apply_chunk(&[noop]).await.unwrap();
            black_box(());
        });
    });

    group.finish();
}

/// Benchmark: Best case - no expired entries
/// Tests that cleanup is fast when nothing needs to be cleaned
fn bench_best_case_no_expired(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("best_case_no_expired");
    group.sample_size(10);

    // Setup: Create state machine with non-expired entries
    let (sm, _temp_dir) = runtime.block_on(async {
        let (sm, temp_dir) = create_test_state_machine().await;

        // Insert 1000 entries with very long TTL
        let entries = create_entries_with_ttl(1000, 1, 86400); // 1 day
        sm.apply_chunk(&entries).await.unwrap();

        (sm, temp_dir)
    });

    let noop_idx = AtomicU64::new(1001); // setup applied 1..=1000
    group.bench_function("cleanup_no_expired", |b| {
        b.to_async(&runtime).iter(|| async {
            let i = noop_idx.fetch_add(1, Ordering::Relaxed);
            let noop = create_noop_entry(i);
            sm.apply_chunk(&[noop]).await.unwrap();
            black_box(());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_piggyback_cleanup,
    bench_ttl_registration,
    bench_batch_ttl_registration,
    bench_mixed_ttl_workload,
    bench_piggyback_high_frequency,
    bench_varying_ttl_durations,
    bench_worst_case_all_expired,
    bench_best_case_no_expired,
);
criterion_main!(benches);
