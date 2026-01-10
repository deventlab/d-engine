# standalone-bench

## Background

Standalone mode benchmark using gRPC client (comparable with [etcd benchmarks](https://etcd.io/docs/v3.5/op-guide/performance/)).

## Getting Started

### 1. Dependency Setup

Ensure you're using the correct d-engine version in your `Cargo.toml`:

```toml
[dependencies]
d-engine = "0.2.0"  # Verify latest version
```

### 2. Setup Cluster

Start cluster from `/examples/three-nodes-standalone/`

```bash
# Start 3-node cluster (required for accurate benchmarking)
cd /examples/three-nodes-standalone/
make start-cluster
```

## Quick Start

```bash
# Start 3-node cluster
cd examples/three-nodes-standalone
make start-cluster

# Run benchmark
cd benches/standalone-bench
cargo run --release -- \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --total 100000 --key-size 8 --value-size 256 --sequential-keys \
    put
```

## Request Routing Behavior

| Operation        | Target Nodes              | Notes                               |
| ---------------- | ------------------------- | ----------------------------------- |
| **Write (put)**  | Leader only               | Raft protocol requirement           |
| **Linearizable** | Leader only               | Ensures strong consistency          |
| **LeaseRead**    | Leader only               | Leader lease-based optimization     |
| **Eventual**     | All nodes (load balanced) | Round-robin across Leader+Followers |

gRPC client automatically routes requests based on consistency policy.

## Test Commands

See [v0.2.0 report](reports/v0.2.0/report_v0.2.0_final.md#reproduce-results) for full benchmark suite.

## Parameters

| Parameter       | Description                | Example         |
| --------------- | -------------------------- | --------------- |
| `--endpoints`   | Cluster node addresses     | See commands    |
| `--conns`       | TCP connections            | 1-200           |
| `--clients`     | Concurrent workers         | 1-1000          |
| `--total`       | Total operations           | 10K-100K        |
| `--key-size`    | Key bytes (default: 8)     | 8-1024          |
| `--value-size`  | Value bytes (default: 256) | 256-65536       |
| `--consistency` | Read policy: `l`/`s`/`e`   | See table above |

## Output

```
Total time:     1.27s
Requests:       10000
Throughput:     7941 ops/sec

Latency (μs):
 Avg    1255
 p50    1245
 p99    1687
```

**Key metrics**: Throughput (higher better), p99 latency (lower better).
