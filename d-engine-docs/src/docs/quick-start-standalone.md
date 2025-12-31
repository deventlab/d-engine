# Quick Start: Standalone Mode in 5 Minutes

**Goal**: Start 3-node d-engine cluster and verify it works with gRPC.

> d-engine uses **standard gRPC** - works with any language (Go, Python, Java, Node.js, Rust, etc.)

---

## Prerequisites

- Rust stable (to build d-engine)
- Go 1.20+ (for client example)
- 5 minutes

---

## Step 1: Start Demo Cluster (1 minute)

> **What is this?**  
> `examples/three-nodes-cluster` is a demo project included in d-engine repository.  
> It simulates a production-like 3-node cluster for quick testing.

```bash
cd examples/three-nodes-cluster
make start-cluster
```

**What happens**:

- Starts 3 d-engine server processes (Node 1/2/3)
- Auto-elects leader (takes ~2s)
- Exposes gRPC endpoints: `127.0.0.1:9081-9083`
- Persists data to `./db/1/`, `./db/2/`, `./db/3/`

**Verify cluster is ready**:

```bash
# Check processes
ps aux | grep "target/release/demo"  # Should see 3 processes

# Check logs for leader election
tail -f logs/1/demo.log  # Look for ">>> switch to Leader"
```

---

## Step 2: Run Go Client (1 minute)

```bash
cd examples/quick-start-standalone
make run
```

**What this does:**

1. Connects to any node (tries 127.0.0.1:9081 first)
2. Sends write request
3. If `NOT_LEADER` error, follows `metadata.LeaderAddress` to redirect
4. Writes `hello=world` to leader
5. Reads back with LinearizableRead consistency

**Expected output**:

```text
Not leader, redirecting to 0.0.0.0:9082
Write success (leader at 0.0.0.0:9082)
Value: world
```

> **Note**: Leader may be at any node. The client uses Leader Hint from error response to redirect automatically.

**Want to see the code?** Check the [quick-start-standalone example](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-standalone)

---

## What's Next?

**Try other languages:**

- Proto files: `d-engine-proto/proto/*.proto`
- Language guides: <https://grpc.io/docs/languages/>

**Advanced topics:**

- [Monitor cluster and test failover](#monitor-cluster)
- [Embedded vs Standalone comparison](#embedded-vs-standalone)
- [Production deployment example](https://github.com/deventlab/d-engine/tree/main/examples/three-nodes-cluster)

**Clean up:**

```bash
make stop-cluster && make clean
```

---

## Advanced Topics

### Monitor Cluster

```bash
# Watch node 1 logs
tail -f logs/1/demo.log

# Test failover (kill leader, observe re-election)
pkill -f "demo.*node-id=1"
tail -f logs/2/demo.log  # Watch new leader election
```

### Embedded vs Standalone

| Feature           | Embedded (Rust)                | Standalone (gRPC)      |
| ----------------- | ------------------------------ | ---------------------- |
| **Latency**       | <0.1ms                         | 1-2ms                  |
| **Serialization** | None                           | Protobuf               |
| **Language**      | Rust only                      | Any (Go, Python, Java) |
| **Deployment**    | 1 binary                       | 3 server processes     |
| **Setup**         | `EmbeddedEngine::start_with()` | `make start-cluster`   |

---

**Created**: 2025-12-04  
**Updated**: 2025-12-30
