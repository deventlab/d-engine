# Benchmark Guide

## Background
This benchmark suite is designed to evaluate d-engine's performance against etcd's official benchmarks. Refer to [etcd's performance documentation](https://etcd.io/docs/v3.5/op-guide/performance/) for comparison methodology and baseline metrics.

## Getting Started

### 1. Dependency Setup
Ensure you're using the correct d-engine version in your `Cargo.toml`:
```toml
[dependencies]
d-engine = "0.1.2"  # Verify latest version
```

### 2. Start Minimum Valid d-engine Cluster
The benchmark test requires a minimum cluster size of three nodes.

**Why 3 Nodes Matter for Testing?**

1. **Leader Election**: Raft requires majority quorum (2/3 nodes)
2. **Log Replication**: Realistic network roundtrips between nodes
3. **Failure Simulation**: Can test with 1 node down
4. **Performance Characteristics**: Actual consensus overhead

### 3. Build Benchmark Tool
```bash
cargo build --release  # Optimized build for accurate measurements
```

## Running Tests

### Write Test

**Basic test (1 connection, 1 client):**
```bash
./target/release/d-engine-bench  \
    --endpoints http://127.0.0.1:9081 --endpoints  http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 1 \
    --clients 1 \
    --sequential-keys \
    --total 10000 \
    --key-size 8 \
    --value-size 256 \
    put --prefix ''

```
**High-concurrency test (10 connections, 100 clients):**
```bash
./target/release/d-engine-bench  \
    --endpoints http://127.0.0.1:9081 --endpoints  http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 10 \
    --clients 100 \
    --sequential-keys \
    --total 10000 \
    --key-size 8 \
    --value-size 256 \
    put --prefix foo
```

### Read Tests
**Linearizable Read (Strong consistency):**
```bash
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 \
    --endpoints http://127.0.0.1:9082 \
    --endpoints http://127.0.0.1:9083 \
    --conns 10 \
    --clients 100 \
    --sequential-keys \
    --total 10000 \
    --key-size 8 \
    --value-size 256 \
    range --consistency l
```

**Sequential Read (Eventual consistency):**
```bash
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 \
    --endpoints http://127.0.0.1:9082 \
    --endpoints http://127.0.0.1:9083 \
    --clients 100 \
    --conns 10 \
    --total 10000 \
    range --key foo --consistency s
```

## Key Parameters

| **Parameter** | **Description** | **Typical Values** |
| --- | --- | --- |
| `--conns` | Simultaneous TCP connections | 1-100 |
| `--clients` | Concurrent client workers | 1-1000 |
| `--total` | Total operations to execute | ≥10,000 for stability |
| `--key-size` | Byte size of keys (8-1024) | 8 (matching etcd) |
| `--value-size` | Byte size of values (256-65536) | 256 (matching etcd) |

## Understanding Results

Sample output structure:
```text
Total time:    1.27 s
Throughput:    7941.31 ops/sec

Latency distribution (μs):
Avg    1255.94
p50    1245
p90    1319
p99    1687
```

**Key Metrics:**

- **Throughput**: Operations per second (higher is better)
- **p99 Latency**: 99th percentile latency (lower is better)
- **Latency Distribution**: Shows performance consistency

## Reference Reports

Pre-generated test reports are available in the `reports/` directory at project root:
```bash
ls reports/
```

## Optimization Tips

1. Start with single-node tests before cluster tests
2. Gradually increase `-conns` and `-clients` to find saturation point
3. Compare linear vs sequential reads to understand consistency tradeoffs
4. Use `RUST_LOG=d-engine-bench=debug` for detailed diagnostics

