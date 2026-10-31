# D-Engine Performance Benchmarks

This directory contains Criterion-based performance benchmarks for the d-engine state machine and Watch mechanism.

## Overview

We use [Criterion.rs](https://github.com/bheisler/criterion.rs) for benchmarking, which provides:
- Statistical analysis with confidence intervals
- Outlier detection
- Beautiful HTML reports
- Performance regression detection
- Works on stable Rust

## Benchmark Suites

### 1. State Machine Benchmarks (`state_machine.rs`)

Tests core state machine performance and Watch overhead:

- **apply** - Baseline apply performance
- **get** - Baseline read performance
- **batch_apply** - Scaling test for batch operations (10, 100, 1000 entries)
- **apply_without_watch**, **apply_with_1_watcher**, **apply_with_10_watchers**, **apply_with_100_watchers** - Watch overhead on the apply path
- **watch_e2e_latency** - Watch notification latency

## Running Benchmarks

### Run all benchmarks
```bash
cargo bench --package d-engine-server
```

### Run specific benchmark suite
```bash
cargo bench --package d-engine-server --bench state_machine
```

### Run specific test
```bash
cargo bench --package d-engine-server --bench state_machine apply
```

### Quick run (fewer samples)
```bash
cargo bench --package d-engine-server -- --quick
```

## Viewing Results

After running benchmarks, detailed HTML reports are available at:
```
target/criterion/report/index.html
```

Open this file in your browser to see:
- Performance graphs
- Statistical analysis
- Historical comparisons
- Detailed timing distributions

## Performance Targets

Based on our design goals:

| Metric | Target | Benchmark |
|--------|--------|-----------|
| Watch overhead on apply path | < 10ns per watcher | `apply_with_*_watcher*` vs `apply_without_watch` |
| Batch apply scaling | Linear | `batch_apply/*` |

## CI Integration

### Continuous Integration
The benchmarks are integrated into CI in two ways:

1. **Compilation Check** (every PR)
   - `.github/workflows/ci.yml` includes `cargo bench --no-run`
   - Ensures benchmark code doesn't break
   - Fast (~30 seconds)

2. **Performance Monitoring** (weekly + main branch)
   - `.github/workflows/benchmark.yml` runs full benchmarks
   - Stores results in gh-pages branch
   - Alerts on >10% performance regression
   - Manual trigger available for important PRs

### Viewing Historical Trends
Performance trends are available at:
```
https://[your-org].github.io/d-engine/dev/bench/
```

## Best Practices

1. **Run on isolated hardware** - Close other applications for accurate results
2. **Run multiple times** - Criterion handles this automatically
3. **Warm up your system** - First run may be slower
4. **Compare against baseline** - Use git branches to compare performance
5. **Check for regressions** - Review HTML reports after changes

## Troubleshooting

### Benchmarks fail to compile
```bash
# Clean and rebuild
cargo clean
cargo bench --no-run --package d-engine-server
```

### Results seem noisy
```bash
# Increase sample size
cargo bench --package d-engine-server -- --sample-size 1000
```

### Need faster iteration
```bash
# Run with fewer samples
cargo bench --package d-engine-server -- --quick
```

## Adding New Benchmarks

To add a new benchmark:

1. Add your benchmark function to the appropriate file
2. Add it to the `criterion_group!` macro at the bottom
3. Ensure it follows the naming convention: `bench_<what_you_test>`
4. Include clear comments about what you're measuring
5. Run `cargo bench --no-run` to check it compiles

Example:
```rust
/// Benchmark: Description of what this tests
/// Target: < XYZ performance goal
fn bench_my_new_test(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    
    c.bench_function("my_new_test", |b| {
        b.to_async(&runtime).iter(|| async {
            // Your benchmark code here
            black_box(/* operation to measure */);
        });
    });
}
```

## References

- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
