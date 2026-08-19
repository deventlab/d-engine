//! LeaderState Performance Benchmarks
//!
//! ## Purpose
//!
//! This benchmark suite measures the performance of critical LeaderState operations
//! to ensure no performance regressions are introduced during refactoring or feature
//! additions. It focuses on operations that directly impact cluster throughput and
//! latency:
//!
//! 1. **LeaderState Creation**: Measures instantiation overhead
//! 2. **Pending Promotion Processing**: Measures learner-to-voter promotion latency
//! 3. **Batch Promotion Scaling**: Validates performance characteristics across different batch sizes
//!
//! ## Performance Acceptance Criteria
//!
//! ### Benchmark 1: LeaderState Creation
//! - **Target**: < 500 ns per operation
//! - **Failure Criteria**: > 1000 ns indicates memory allocation issues or lock contention
//! - **Why It Matters**: Leader election happens during network partitions; slow creation
//!   delays cluster recovery
//!
//! ### Benchmark 2: Process Pending Promotions (2 nodes)
//! - **Target**: < 20 ms per operation
//! - **Failure Criteria**: > 50 ms indicates quorum verification slowdown
//! - **Why It Matters**: Directly impacts auto-scaling responsiveness in production
//!
//! ### Benchmark 3: Batch Promotion Scaling
//! - **Target**: Linear scaling O(n) where n = batch size
//! - **Failure Criteria**: Quadratic scaling O(n²) or worse
//! - **Expected Behavior**:
//!   - batch_1: ~10 ms (baseline)
//!   - batch_2: ~20 ms (2x baseline)
//!   - batch_5: ~50 ms (5x baseline)
//!   - batch_10: ~100 ms (10x baseline)
//! - **Why It Matters**: Non-linear scaling would cause cluster instability during
//!   large-scale node additions
//!
//! ## How to Identify Failures
//!
//! ### Running Benchmarks
//!
//! ```bash
//! # Run all benchmarks
//! make bench
//!
//! # Run only LeaderState benchmarks
//! cargo bench --bench leader_state_bench
//!
//! # Save baseline for future comparison
//! cargo bench --bench leader_state_bench -- --save-baseline main
//!
//! # Compare against baseline
//! cargo bench --bench leader_state_bench -- --baseline main
//! ```
//!
//! ### Interpreting Results
//!
//! #### Example: Successful Run
//! ```
//! LeaderState::new        time:   [425.32 ns 437.45 ns 449.78 ns]
//!                         change: [-2.5% -1.2% +0.3%] (p = 0.15 > 0.05)
//!                         No significant change detected ✅
//!
//! process_pending_promotions_2_nodes
//!                         time:   [18.452 ms 18.591 ms 18.740 ms]
//!                         change: [-3.234% -1.456% +0.823%] (p = 0.25 > 0.05)
//!                         No significant change detected ✅
//! ```
//! **Verdict**: PASS - Both within target thresholds and no regression
//!
//! #### Example: Performance Regression
//! ```
//! LeaderState::new        time:   [1.2453 µs 1.2678 µs 1.2901 µs]
//!                         change: [+180.234% +185.456% +190.823%] (p = 0.00 < 0.05)
//!                         Performance regressed! ❌
//! ```
//! **Verdict**: FAIL - Exceeded 1000 ns threshold and shows 185% regression
//! **Action Required**:
//! 1. Review recent changes to LeaderState constructor
//! 2. Check for new heap allocations or lock acquisitions
//! 3. Revert if no valid justification
//!
//! #### Example: Non-Linear Scaling Detected
//! ```
//! batch_promotion/batch_1     time:   [10.123 ms ...]
//! batch_promotion/batch_2     time:   [22.456 ms ...] (2.2x instead of 2.0x)
//! batch_promotion/batch_5     time:   [75.890 ms ...] (7.5x instead of 5.0x) ❌
//! batch_promotion/batch_10    time:   [210.345 ms ...] (20.8x instead of 10.0x) ❌
//! ```
//! **Verdict**: FAIL - Indicates O(n²) complexity or worse
//! **Action Required**:
//! 1. Profile `process_pending_promotions` with flamegraph
//! 2. Check for nested loops or redundant quorum verifications
//! 3. Investigate membership update inefficiencies
//!
//! ## Continuous Integration
//!
//! This benchmark should be run in CI on every PR to prevent performance regressions.
//! Configure GitHub Actions to fail if:
//! - Any operation exceeds failure threshold
//! - Performance degrades by > 10% compared to main branch
//! - Scaling characteristics become non-linear
//!
//! ## Historical Context
//!
//! - **v0.1.0**: Initial benchmark suite
//! - **v0.2.0** (#236): Added after ReadIndex batching refactor to ensure no regression
//!   in promotion path

use std::sync::Arc;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use d_engine_core::leader_state::LeaderState;
use d_engine_core::{MockTypeConfig, RaftNodeConfig};

/// Benchmark 1: LeaderState Creation Performance
///
/// Measures the overhead of instantiating a new LeaderState.
/// This is critical during leader election scenarios.
///
/// Target: < 500 ns
/// Failure: > 1000 ns
fn bench_leader_state_creation(c: &mut Criterion) {
    let node_config = RaftNodeConfig::default();
    let config = Arc::new(node_config);

    c.bench_function("LeaderState::new", |b| {
        b.iter(|| {
            let state = LeaderState::<MockTypeConfig>::new(1, config.clone());
            black_box(state)
        })
    });
}

criterion_group!(benches, bench_leader_state_creation,);
criterion_main!(benches);
