# Quick Start: Standalone Mode in 5 Minutes

**Goal**: Start 3-node d-engine cluster, connect with Go gRPC client.

---

## Prerequisites

- Rust stable
- Go 1.20+ (optional, for client example)
- 5 minutes

---

## Step 1: Start 3-Node Cluster (1 minute)

```bash
cd examples/three-nodes-cluster
make start-cluster
```

This command:

- Builds release binary
- Starts 3 nodes in parallel
- Auto-elects leader
- Ready for gRPC connections on `127.0.0.1:9081`, `9082`, `9083`

---

## Step 2: Go Client Hello World (2 minutes)

Create `main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	// When Go client available: "github.com/deventlab/d-engine-go-client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO: When Go client ready
	// client := dengine.NewClient("127.0.0.1:9081")
	// client.Put(ctx, []byte("hello"), []byte("world"))
	// val, _ := client.Get(ctx, []byte("hello"))
	// fmt.Printf("Value: %s\n", val)

	fmt.Println("Go client coming soon")
}
```

---

## What Just Happened?

| Component            | What it does                               |
| -------------------- | ------------------------------------------ |
| `make start-cluster` | Builds + starts Node 1, 2, 3 in parallel   |
| Auto-election        | Majority (2/3) nodes elected leader in <2s |
| RocksDB              | Each node persists logs/state to `./db/N/` |
| gRPC server          | Each node listens on port 908N             |

---

## Monitor Cluster

```bash
# Watch node 1 logs
tail -f examples/three-nodes-cluster/logs/1/demo.log

# Kill leader (observe failover)
pkill -f "target/release/demo"

# Restart cluster
make start-cluster
```

---

## Key Differences: Embedded vs Standalone

| Feature           | Embedded (Rust)                  | Standalone (gRPC)      |
| ----------------- | -------------------------------- | ---------------------- |
| **Latency**       | <0.1ms                           | 1-2ms                  |
| **Serialization** | None                             | Protobuf               |
| **Language**      | Rust only                        | Any (Go, Python, Java) |
| **Deployment**    | 1 binary                         | 3 server processes     |
| **Setup**         | `EmbeddedEngine::with_rocksdb()` | `make start-cluster`   |

---

## Next: Scale & Monitor

- **Add node**: `make join-node4` (learner mode)
- **Logs**: `logs/1/`, `logs/2/`, `logs/3/`
- **Metrics**: Prometheus on ports 8081-8083
- **Profiling**: `make perf-cluster` (generates flame graphs)

See `examples/three-nodes-cluster/README.md` for advanced usage.

---

**Created**: 2025-12-04  
**Updated**: 2025-12-04
