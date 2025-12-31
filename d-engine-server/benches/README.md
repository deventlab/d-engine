# D-Engine Performance Benchmarks

This directory contains Criterion-based performance benchmarks for the d-engine state machine and TTL functionality.

## Overview

We use [Criterion.rs](https://github.com/bheisler/criterion.rs) for benchmarking, which provides:
- Statistical analysis with confidence intervals
- Outlier detection
- Beautiful HTML reports
- Performance regression detection
- Works on stable Rust

## Benchmark Suites

### 1. State Machine Benchmarks (`state_machine.rs`)

Tests core state machine performance with focus on TTL overhead:

- **apply_without_ttl** - Baseline apply performance (target: < 10ns overhead)
- **apply_with_ttl** - Apply with TTL registration overhead
- **get_without_ttl** - Baseline read performance
- **get_with_ttl_check** - Read with TTL passive check (target: < 50ns overhead)
- **get_expired_ttl** - Cost of passive deletion on read
- **batch_apply** - Scaling test for batch operations (10, 100, 1000 entries)
- **batch_apply_with_ttl** - Batch operations with TTL

### 2. TTL Benchmarks (`ttl.rs`)

Tests TTL-specific functionality:

- **piggyback_cleanup** - Cleanup performance with varying expired key counts (target: < 1ms for 100 keys)
- **ttl_registration** - Cost of registering TTL entries
- **batch_ttl_registration** - TTL registration scaling (10, 100, 1000 entries)
- **mixed_ttl_workload** - Realistic mix of active and expired entries
- **piggyback_high_frequency** - Cost of frequent cleanup triggers
- **varying_ttl_durations** - Performance with different TTL values (1min, 1hr, 1day)
- **worst_case_all_expired** - All 1000 entries expired
- **best_case_no_expired** - No cleanup needed

## Running Benchmarks

### Run all benchmarks
```bash
cargo bench --package d-engine-server
```

### Run specific benchmark suite
```bash
cargo bench --package d-engine-server --bench state_machine
cargo bench --package d-engine-server --bench ttl
```

### Run specific test
```bash
cargo bench --package d-engine-server --bench state_machine apply_without_ttl
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
| No TTL overhead | < 10ns | `apply_without_ttl` vs `apply_with_ttl` |
| TTL passive check | < 50ns | `get_with_ttl_check` vs `get_without_ttl` |
| Piggyback cleanup (100 keys) | < 1ms | `piggyback_cleanup/100` |
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
- [d-engine TTL Design Doc](../../d-engine-product-design/20251107/)
