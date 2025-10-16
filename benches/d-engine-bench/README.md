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

### 2. Setup Cluster

Start cluster from `/examples/three-nodes-cluster/`

```bash
# Start 3-node cluster (required for accurate benchmarking)
cd /examples/three-nodes-cluster/
make start-cluster
```

### 2. Run Test

```bash
make
```

## Test Scenarios

### Write Throughput

```bash
# Basic write (1 connection, 1 client)
./target/release/d-engine-bench \
    --endpoints localhost:9081,localhost:9082,localhost:9083 \
    --conns 1 --clients 1 --total 10000 \
    put

# High-concurrency write (10 connections, 100 clients)
./target/release/d-engine-bench \
    --endpoints localhost:9081,localhost:9082,localhost:9083 \
    --conns 10 --clients 100 --total 100000 \
    put
```

### Read Consistency

```bash
# Strong consistency (linearizable)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 10000 --key-size 8 \
    range --consistency l

# Lease-based reads (better performance with still strong consistency)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 --key-size 8 \
    range --consistency s

# Eventual consistency (highest performance, may return stale data)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 --key-size 8 \
    range --consistency e
```

## Key Parameters

| **Parameter**  | **Description**                      | **Recommended Range**                                             |
| -------------- | ------------------------------------ | ----------------------------------------------------------------- |
| `--endpoints`  | Endpoints for client to connect      | http://127.0.0.1:9081,http://127.0.0.1:9082,http://127.0.0.1:9083 |
| `--conns`      | Concurrent TCP connections           | 1-100                                                             |
| `--clients`    | Parallel client workers              | 1-1000                                                            |
| `--total`      | Total operations (≥10k for accuracy) | 10,000-1M                                                         |
| `--key-size`   | Key size in bytes (default: 8)       | 8-1024                                                            |
| `--value-size` | Value size in bytes (default: 256)   | 256-65536                                                         |

## Understanding Output

```text
Total time:    1.27s    Throughput: 7941 ops/s

Latency (μs):
Average   1255
p50       1245
p90       1319
p99       1687
```

**Key Metrics:**

- **Throughput**: Ops/second (higher better)
- **p99 Latency**: 99th percentile (lower better)
- **Latency Distribution**: Performance consistency

## Optimization Guide

1. **Baseline First**

   Start with single connection/client before scaling

2. **Find Saturation**

   Gradually increase `--conns` and `--clients` until latency spikes

3. **Consistency Tradeoffs**

   Compare linearizable vs lease-based vs eventual consistency reads

4. **Debugging**

   Enable detailed logs:

   `RUST_LOG=d-engine-bench=debug ./target/release/d-engine-bench ...`

## Reference Data

Pre-generated reports in `reports/`:

```bash
# View latest reportcat reports/$(ls -t reports/ | head -1)
```

---

**Why 3 Nodes?**

- Required for Raft leader election (2/3 quorum)
- Simulates real network conditions
- Allows failure tolerance testing
