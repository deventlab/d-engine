# Quick Start: Standalone Mode Example

Go client example for connecting to d-engine standalone cluster.

## Prerequisites

- Go 1.20+
- Running d-engine 3-node cluster (see `../three-nodes-cluster`)

## Run

```bash
# Start cluster first
cd ../three-nodes-cluster
make start-cluster

# Run this example
cd ../quick-start-standalone
make run
```

**Expected output:**

```
Write success (leader at 127.0.0.1:9081)
Value: world
```

## What This Does

1. Tries all 3 nodes to find the leader
2. Writes `hello=world` to the leader
3. Reads back the value with LinearizableRead consistency

## Full Documentation

See [Quick Start: Standalone Mode](https://docs.rs/d-engine/latest/d_engine/docs/quick_start_standalone/index.html)
