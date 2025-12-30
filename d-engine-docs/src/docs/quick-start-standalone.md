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

## Step 2: Go Client Example (1 minute)

> **Note**: This example uses raw gRPC. Official Go SDK coming soon.

### 2.1 Setup Project

```bash
# Create project directory
mkdir my-d-engine-client && cd my-d-engine-client
go mod init my-d-engine-client

# For local testing (before d-engine is published)
export D_ENGINE_REPO="/path/to/d-engine"  # ← Replace with actual path
echo "replace github.com/deventlab/d-engine/proto => $D_ENGINE_REPO/d-engine-proto/go" >> go.mod
```

> **Note**: After d-engine is published to GitHub, remove the `replace` directive and use `go get github.com/deventlab/d-engine/proto/client` directly.

### 2.2 Write Client Code

Create `main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    pb "github.com/deventlab/d-engine/proto/client"
)

func main() {
    conn, err := grpc.NewClient(
        "127.0.0.1:9081",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewRaftClientServiceClient(conn)

    // Write
    writeResp, err := client.HandleClientWrite(context.Background(), &pb.ClientWriteRequest{
        Commands: []*pb.WriteCommand{{
            Operation: &pb.WriteCommand_Insert_{
                Insert: &pb.WriteCommand_Insert{
                    Key:   []byte("hello"),
                    Value: []byte("world"),
                },
            },
        }},
    })
    if err != nil {
        log.Fatalf("Write failed: %v", err)
    }
    fmt.Printf("Write success: %v\n", writeResp.GetWriteAck())

    // Read
    resp, err := client.HandleClientRead(context.Background(), &pb.ClientReadRequest{
        Keys: [][]byte{[]byte("hello")},
    })
    if err != nil {
        log.Fatalf("Read failed: %v", err)
    }

    if len(resp.GetReadData().GetResults()) == 0 {
        log.Fatal("Key not found")
    }

    fmt.Printf("Value: %s\n", resp.GetReadData().Results[0].Value)
}
```

**Install dependencies and run**:

```bash
go mod tidy
go run main.go
```

**Expected output**:

```
Write success: true
Value: world
```

---

## What's Next?

**Try other languages:**

- Proto files: `d-engine-proto/proto/*.proto`
- Language guides: https://grpc.io/docs/languages/

**Advanced topics:**

- [Monitor cluster and test failover](#monitor-cluster)
- [Embedded vs Standalone comparison](#embedded-vs-standalone)
- [Production deployment](../../examples/three-nodes-cluster/README.md)

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
