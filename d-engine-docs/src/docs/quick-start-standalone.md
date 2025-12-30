# Quick Start: Standalone Mode in 5 Minutes

**Goal**: Start 3-node d-engine cluster and verify it works with gRPC.

---

## Prerequisites

- Rust stable (to build d-engine)
- `grpcurl` (for testing: `brew install grpcurl` or `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`)
- Go 1.20+ (optional, for client example)
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

## Step 2: Quick Test with grpcurl (1 minute)

```bash
# List available services
grpcurl -plaintext 127.0.0.1:9081 list
```

Expected output:

```text
RaftClientService
ClusterManagementService
```

**Write data** (Put):

```bash
grpcurl -plaintext -d '{
  "requests": [{"put": {"key": "aGVsbG8=", "value": "d29ybGQ="}}]
}' 127.0.0.1:9081 RaftClientService.Write
```

**Read data** (Get):

```bash
grpcurl -plaintext -d '{
  "requests": [{"get": {"key": "aGVsbG8="}}]
}' 127.0.0.1:9081 RaftClientService.Read
```

Expected output:

```json,ignore
{
  "results": [
    {
      "value": "d29ybGQ=" // "world" in base64
    }
  ]
}
```

---

## Step 3: Go Client Example (Optional, 2 minutes)

> **Note**: This example uses raw gRPC. Official Go SDK coming soon.

**Install dependencies**:

```bash
go mod init myapp
go get google.golang.org/grpc
# Copy proto files from d-engine-proto/proto/ and generate Go code
```

**Simple client** (`main.go`):

```go,ignore
package main

import (
    "context"
    "fmt"
    "log"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    // TODO: Replace with your generated proto package
    pb "path/to/generated/proto"
)

func main() {
    // Connect to any cluster node
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
    _, err = client.Write(context.Background(), &pb.ClientWriteRequest{
        Requests: []*pb.WriteCommand{{
            Put: &pb.PutCommand{
                Key:   []byte("hello"),
                Value: []byte("world"),
            },
        }},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Read
    resp, err := client.Read(context.Background(), &pb.ClientReadRequest{
        Requests: []*pb.ReadRequest{{
            Get: &pb.GetRequest{Key: []byte("hello")},
        }},
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Value: %s\n", resp.Results[0].GetValue())
}
```

**Run**:

```bash
go run main.go  # Output: Value: world
```

---

## Other Languages

d-engine uses **standard gRPC** - works with Python, Java, Node.js, Rust, etc.

**Proto files**: `d-engine-proto/proto/*.proto`  
**Language guides**: https://grpc.io/docs/languages/

---

## Monitor Cluster

```bash
# Watch node 1 logs
tail -f logs/1/demo.log

# Check cluster status
grpcurl -plaintext 127.0.0.1:9081 ClusterManagementService.GetMetadata

# Test failover (kill leader, observe re-election)
pkill -f "demo.*node-id=1"
tail -f logs/2/demo.log  # Watch new leader election
```

---

## Key Differences: Embedded vs Standalone

| Feature           | Embedded (Rust)                | Standalone (gRPC)      |
| ----------------- | ------------------------------ | ---------------------- |
| **Latency**       | <0.1ms                         | 1-2ms                  |
| **Serialization** | None                           | Protobuf               |
| **Language**      | Rust only                      | Any (Go, Python, Java) |
| **Deployment**    | 1 binary                       | 3 server processes     |
| **Setup**         | `EmbeddedEngine::start_with()` | `make start-cluster`   |

---

## Next Steps

- **Production setup**: See `examples/three-nodes-cluster/README.md`
- **Add node**: `make join-node4` (learner auto-promotion)
- **Metrics**: Prometheus endpoints on ports 8081-8083

---

## Cleanup

```bash
make stop-cluster  # Stop all nodes
make clean         # Remove db/ and logs/
```

---

**Created**: 2025-12-04  
**Updated**: 2025-12-30
