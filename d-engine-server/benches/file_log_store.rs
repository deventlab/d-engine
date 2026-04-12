//! FileLogStore Performance Benchmarks
//!
//! Measures persist_entries throughput before and after flush-per-entry fix.
//!
//! Performance Targets:
//! - persist_entries 1k entries:  < 50ms
//! - persist_entries 10k entries: < 500ms

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use d_engine_core::LogStore;
use d_engine_core::StorageEngine;
use d_engine_proto::common::Entry;
use d_engine_server::FileStorageEngine;
use tempfile::TempDir;

fn make_entries(count: u64) -> Vec<Entry> {
    (1..=count)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect()
}

fn bench_persist_entries(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("file_log_store/persist_entries");

    for count in [1_000u64, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let temp = TempDir::new().unwrap();
                let engine = FileStorageEngine::new(temp.path().to_path_buf()).unwrap();
                let log_store = engine.log_store();
                let entries = make_entries(count);
                rt.block_on(async { log_store.persist_entries(entries).await.unwrap() });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_persist_entries);
criterion_main!(benches);
