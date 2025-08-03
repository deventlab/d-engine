## Starting a Three-Node Cluster

### Quick Start with Makefile (Recommended)

```bash
# First build (ensure cargo is installed)
make build

# Start full cluster (automatically in parallel)
make start-cluster

# Individual node control
make start-node1  # Launch node 1
make start-node2  # Launch node 2
make start-node3  # Launch node 3
make clean        # Clean all build artifacts and logs

### Manual Startup (Advanced)

For manual control, run these in separate terminal sessions:

### Node 1

```bash
CONFIG_PATH=config/n1 \
LOG_DIR="./logs/1" \
RUST_LOG=demo=debug,d_engine=debug \
RUST_BACKTRACE=1 \
target/release/demo
```

### Node 2

```bash
CONFIG_PATH=config/n2 \
LOG_DIR="./logs/2" \
RUST_LOG=demo=debug,d_engine=debug \
target/release/demo
```

### Node 3

```bash
CONFIG_PATH=config/n3 \
LOG_DIR="./logs/3" \
RUST_LOG=demo=debug,d_engine=debug \
target/release/demo
```

## Prerequisites

### 1. Build Executable

```bash
# Using Makefile (recommended)make build

# Or directly with cargocargo build --release
```

### 2. Verify Configuration Files

Ensure these config files exist:

```bash
config/
├── n1
├── n2
└── n3
```

## Operations Guide

### Key Makefile Commands

| **Command**          | **Description**                 |
| -------------------- | ------------------------------- |
| `make build`         | Build release binary            |
| `make start-cluster` | Start 3-node cluster (parallel) |
| `make start-nodeN`   | Start individual node (N=1,2,3) |
| `make clean`         | Clean build artifacts & logs    |
| `make help`          | Show all available commands     |

### Log Monitoring

```bash
tail -f logs/1/demo.log# Watch node 1 logstail -f logs/2/demo.log# Watch node 2 logstail -f logs/3/demo.log# Watch node 3 logs
```

## Important Notes

1. Node communication depends on addresses in config files
2. Default log levels:
   - `debug`: Engine internals
   - `info`: Application logs
3. When using Makefile:
   - Auto-creates log directories
   - Verifies binary existence
   - Use Ctrl+C to terminate all nodes
4. Cluster requires all 3 nodes to be operational
5. Nodes must run on different ports (pre-configured in example)

## Recommended Directory Structure

```bash
project-root/
├── Makefile
├── examples/
│   └── three-node-cluster/
│       ├── README.md
│       ├── config/
│       │   ├── n1
│       │   ├── n2
│       │   └── n3
│       └── scripts/
├── src/
└── target/
```

This version maintains technical accuracy while improving:

- Clear hierarchy between recommended/advanced methods
- Consistent command formatting
- Visual separation of code blocks
- Table-based command reference
- Path structure visualization
- Cross-platform considerations (Linux/macOS/WSL2)

## Profiling with Samply

## Overview

This guide explains how to profile a three-node DEngine cluster using [Samply](https://github.com/mstange/samply) to identify CPU bottlenecks and generate flame graphs.

Use this when:

1. Investigating **performance bottlenecks** in Raft replication or log persistence
2. Comparing **different Raft log strategies** (DiskFirst, MemFirst, Batched)
3. Analyzing **CPU hot paths** under real-world load

---

## Prerequisites

1. **Rust Release Build**

```bash
make build
```

2. **Install Samply**

```bash
cargo install samply
```

3. **Ensure Profiling Directory Exists**

```bash
mkdir -p samply_reports
```

Samply will generate .profile.json files here.

### Running Profiling

#### 1. Profile a Single Node

```bash
make perf-node1
```

This runs Node 1 under samply record and outputs:

```text
samply_reports/1.profile.json
```

You can open this file in Firefox’s Profiler (about:profiling) for analysis.

#### 2. Profile Full Cluster

```bash
make perf-cluster
```

- Launches all 3 nodes in parallel under samply
- Generates:

```text
samply_reports/
├── 1.profile.json
├── 2.profile.json
└── 3.profile.json
```

### Interpreting Results

1. Open \*.profile.json in Firefox Profiler

2. Use Flame Graph View to identify CPU hotspots

3. Look for:

- raft module hot paths
- Disk I/O or serialization overhead
- Lock contention in BufferedRaftLog

### Cleanup

```bash
make clean
```

Removes:

- target/ build artifacts
- logs/ cluster logs
- db/ state storage
- samply_reports/ profiling outputs
