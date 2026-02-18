# Benchmarking Guide

Two benchmark modes are available: **Embedded** (in-process, zero network overhead)
and **Standalone** (gRPC, comparable to etcd benchmarks).

---

## Embedded Bench

Tests d-engine's `EmbeddedEngine` mode. Three nodes run in separate processes on
the same machine, forming a Raft cluster via in-process communication.

**Location**: `benches/embedded-bench/`

### Prerequisites

- Rust toolchain (see `rust-toolchain.toml`)
- `make`

### Quick Start

Before each run, clean up data and logs from any previous run:

```bash
cd benches/embedded-bench
make clean
```

Then open **three terminal tabs** and run one command per tab:

```bash
# Tab 1
cd benches/embedded-bench
make all-tests NODE=n1

# Tab 2
cd benches/embedded-bench
make all-tests NODE=n2

# Tab 3
cd benches/embedded-bench
make all-tests NODE=n3
```

All three nodes must be running simultaneously to form a quorum.
The node that wins leader election will print the benchmark results.
The other two follower tabs will auto-shutdown once the benchmark completes.

### Expected Output

The leader node prints a full benchmark report:

```text
╔════════════════════════════════════════╗
║  Running Batch Benchmark Tests        ║
╚════════════════════════════════════════╝

========================================
Single Client Write (10K requests)
========================================
Summary:
Total time:     0.99 s
 Requests:      10000
Throughput:     10128.74 ops/sec

Latency distribution (μs):
 Avg    98.12
 Min    65
 Max    3559
 p50    95
 p90    112
 p99    137
 p99.9  356

========================================
High Concurrency Write (100K requests)
========================================
...

╔════════════════════════════════════════╗
║  All Batch Tests Completed!           ║
╚════════════════════════════════════════╝
```

### Available Tests

```bash
make test-single-write        # Single client write (10K requests)
make test-high-conc-write     # High concurrency write (100K requests)
make test-linearizable-read   # Linearizable read (100K requests)
make test-lease-read          # Lease-based read (100K requests)
make test-eventual-read       # Eventual consistency read (100K requests)
make test-hot-key             # Hot-key test (100K requests, 10 keys)
make all-tests                # Run all tests above in sequence
```

---

## Standalone Bench

Tests d-engine in standalone server mode via gRPC client, using the same
methodology as [etcd benchmarks](https://etcd.io/docs/v3.5/op-guide/performance/).

**Location**: `benches/standalone-bench/`

### Prerequisites

- Rust toolchain (see `rust-toolchain.toml`)
- `make`, `protobuf-compiler`

### Step 1: Start the Cluster

```bash
cd examples/three-nodes-standalone
make clean
make build
make start-cluster
```

Wait until leader election completes. You will see log lines similar to:

```text
[Node1] Follower → Candidate (term 1)
[Node3] [Cluster] All 2 peer(s) health check passed
[Node1] Candidate → Leader (term 3)
```

Once a node reports `Candidate → Leader`, the cluster is ready.

### Step 2: Build the Bench Tool

Open a second terminal tab:

```bash
cd benches/standalone-bench
make clean && make build
```

### Step 3: Run Benchmarks

```bash
# Single client write (10K requests)
./target/release/standalone-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 1 --clients 1 --sequential-keys --total 10000 \
    --key-size 8 --value-size 256 put

# High concurrency write (100K requests)
./target/release/standalone-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 --value-size 256 put

# Linearizable read (100K requests)
./target/release/standalone-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 range --consistency l

# Lease-based read (100K requests)
./target/release/standalone-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 range --consistency s

# Eventual consistency read (100K requests)
./target/release/standalone-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 range --consistency e

# Hot-key test (100K requests, 10 keys)
./target/release/standalone-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --total 100000 --key-size 8 \
    --key-space 10 \
    range --consistency l
```

### Expected Output

Each command prints a result summary:

```text
Initializing client connection with: ["http://127.0.0.1:9081", ...]
Summary:
Total time:     1.58 s
 Requests:      10000
Throughput:     6347.57 ops/sec

Latency distribution (μs):
 Avg    156.91
 Min    119
 Max    1689
 p50    155
 p90    172
 p99    205
 p99.9  283
```

### Reset for a Fresh Run

To repeat benchmarks with a clean state:

```bash
# Stop the cluster first (Ctrl+C in the cluster terminal), then:
cd examples/three-nodes-standalone
make clean-log-db

# Restart
make start-cluster
```

---

## Performance Reports

Historical benchmark reports are available for reference:

- [Embedded v0.2.2](../../../benches/embedded-bench/reports/v0.2.2/embedded_bench_report_v0.2.2.md)
- [Standalone v0.2.2](../../../benches/standalone-bench/reports/v0.2.2/standalone_bench_report_v0.2.2.md)
