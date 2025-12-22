# Building Go Applications with d-engine

d-engine provides Protocol Buffer definitions and gRPC APIs for Go client integration. This guide shows how to integrate d-engine as a distributed consensus engine in your Go applications.

## Reference Implementation

**d-queue** is a production-ready distributed message queue built with d-engine in Go.

📦 **Repository**: https://github.com/deventlab/d-queue

Use d-queue as a reference for:

- How to integrate d-engine in Go applications
- Production-ready error handling patterns
- Connection management and retry logic
- Real-world usage of consistency policies

Study the d-queue source code to see these patterns in action.

## Quick Start

### Generate Proto Definitions

#### Prerequisites

- Go 1.20+
- protoc compiler
- protoc-gen-go and protoc-gen-go-grpc plugins

```bash
# Install protoc generation tools
go install github.com/grpc/grpc-go/cmd/protoc-gen-go-grpc@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

#### Generation Steps

1. **Clone d-engine repository**

```bash
git clone https://github.com/deventlab/d-engine.git
cd d-engine/d-engine-proto
```

2. **Generate proto code in your project**

```bash
#!/bin/bash
# your-project/scripts/generate_dengine_proto.sh

DENGINE_PROTO_DIR="${DENGINE_PROTO_DIR:-.}"
OUTPUT_DIR="${OUTPUT_DIR:-./pkg/rpc/proto}"

mkdir -p "$OUTPUT_DIR"

# Ensure we're in the right directory for proto imports
cd "$(dirname "$DENGINE_PROTO_DIR")"

protoc \
    --go_out="$OUTPUT_DIR" \
    --go_opt=paths=source_relative \
    --go-grpc_out="$OUTPUT_DIR" \
    --go-grpc_opt=paths=source_relative \
    --experimental_allow_proto3_optional \
    -I=. \
    proto/common.proto \
    proto/error.proto \
    proto/client/client_api.proto \
    proto/server/election.proto \
    proto/server/replication.proto \
    proto/server/cluster.proto \
    proto/server/storage.proto
```

3. **Integrate with your build system**

For Makefile:

```makefile
.PHONY: generate-proto

generate-proto:
	@DENGINE_PROTO_DIR=../d-engine/d-engine-proto/proto ./scripts/generate_dengine_proto.sh
	@echo "✅ d-engine proto code generated"

build: generate-proto
	go build ./...
```

For other build systems, run the script as part of your build process.

4. **Version Management**

Pin the d-engine version in `go.mod`:

```go
require github.com/deventlab/d-engine v0.2.0
// Comment: proto code generated from d-engine v0.2.0
// When updating d-engine, regenerate proto code
```

## Core Concepts

### Connection Management

```go
import "google.golang.org/grpc"
import pb "github.com/deventlab/d-engine/proto/client"

// Connect to d-engine cluster node
conn, err := grpc.Dial(
    "localhost:9081",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
    return err
}
defer conn.Close()

client := pb.NewRaftClientServiceClient(conn)
```

### Consistency Policies

d-engine supports three read consistency policies. Choose based on your requirements:

```go
import pb "github.com/deventlab/d-engine/proto/client"

// EventualConsistency: Fastest, works on any node (leader/follower)
// May return slightly stale data (<100ms)
eventualPolicy := pb.ReadConsistencyPolicy_READ_CONSISTENCY_POLICY_EVENTUAL_CONSISTENCY

// LinearizableRead: Strongest consistency (default)
// Always returns the latest committed value
linearizablePolicy := pb.ReadConsistencyPolicy_READ_CONSISTENCY_POLICY_LINEARIZABLE_READ

// LeaseRead: Balanced - 7-20x faster than LinearizableRead
// Strong consistency with bounded staleness (~500ms)
leasePolicy := pb.ReadConsistencyPolicy_READ_CONSISTENCY_POLICY_LEASE_READ
```

**Selection guide**:

| Scenario                                  | Policy              | Reason                                           |
| ----------------------------------------- | ------------------- | ------------------------------------------------ |
| Financial transactions, critical metadata | LinearizableRead    | Strongest consistency, no stale reads            |
| General purpose, session management       | LeaseRead           | Balance performance & consistency (7-20x faster) |
| Monitoring, dashboards, analytics         | EventualConsistency | Highest throughput (10-20x faster)               |
| System startup (no leader yet)            | EventualConsistency | Works without leader election                    |

_See [Read Consistency Guide](../server_guide/read-consistency.md) for detailed performance benchmarks and trade-offs._

### Example: KV Store Client

```go
package main

import (
    "context"
    "fmt"
    "google.golang.org/grpc"
    pb "github.com/deventlab/d-engine/proto/client"
    error_pb "github.com/deventlab/d-engine/proto/error"
    "google.golang.org/protobuf/types/known/wrapperspb"
)

type DEngineClient struct {
    client pb.RaftClientServiceClient
    conn   *grpc.ClientConn
}

// Create new client connected to d-engine
func NewDEngineClient(addr string) (*DEngineClient, error) {
    conn, err := grpc.Dial(
        addr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        return nil, err
    }

    return &DEngineClient{
        client: pb.NewRaftClientServiceClient(conn),
        conn:   conn,
    }, nil
}

// Put stores a key-value pair
func (c *DEngineClient) Put(ctx context.Context, key, value string) error {
    cmd := &pb.WriteCommand{
        Operation: &pb.WriteCommand_Insert_{
            Insert: &pb.WriteCommand_Insert{
                Key:   []byte(key),
                Value: []byte(value),
            },
        },
    }

    resp, err := c.client.HandleClientWrite(ctx, cmd)
    if err != nil {
        return fmt.Errorf("RPC error: %w", err)
    }

    if resp.Error != error_pb.ErrorCode_SUCCESS {
        return fmt.Errorf("write failed: %v", resp.Error)
    }

    return nil
}

// Get retrieves a value by key with LinearizableRead consistency
func (c *DEngineClient) Get(ctx context.Context, key string) (string, error) {
    linearizablePolicy := pb.ReadConsistencyPolicy_READ_CONSISTENCY_POLICY_LINEARIZABLE_READ

    req := &pb.ClientReadRequest{
        ClientId: "my-client",
        Keys:     [][]byte{[]byte(key)},
        ConsistencyPolicy: &linearizablePolicy,
    }

    resp, err := c.client.HandleClientRead(ctx, req)
    if err != nil {
        return "", fmt.Errorf("RPC error: %w", err)
    }

    if resp.Error != error_pb.ErrorCode_SUCCESS {
        return "", fmt.Errorf("read failed: %v", resp.Error)
    }

    readData := resp.GetReadData()
    if readData == nil || len(readData.Results) == 0 {
        return "", fmt.Errorf("key not found")
    }

    return string(readData.Results[0].Value), nil
}

// Close closes the connection
func (c *DEngineClient) Close() error {
    return c.conn.Close()
}
```

## Best Practices

### 1. Study d-queue for Integration Patterns

Before building your own integration, review d-queue's implementation:

```bash
git clone https://github.com/deventlab/d-queue.git
cd d-queue

# Look at client integration patterns
# - Connection management
# - Error handling
# - Retry logic
# - Consistency policy selection
```

✅ **Recommended approach**: Use d-queue patterns as a template for your integration.

### 2. Version Pinning

Always explicitly pin the d-engine version in `go.mod`:

```go
module github.com/yourorg/your-app

require (
    github.com/deventlab/d-engine v0.2.0
)

// Your proto code was generated from d-engine v0.2.0
// Update this version when you upgrade d-engine
// and regenerate proto
```

### 3. Context and Timeouts

Always use context with appropriate timeouts:

```go
// Good: context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

resp, err := client.HandleClientRead(ctx, req)
```

```go
// Also acceptable: context from request
func (h *Handler) Handle(ctx context.Context, req *Request) {
    resp, err := client.HandleClientRead(ctx, req)
}
```

### 4. Error Handling

d-engine uses specific error codes. Always check both RPC errors and operation errors:

```go
resp, rpcErr := client.HandleClientRead(ctx, req)

// Check RPC layer errors
if rpcErr != nil {
    log.Errorf("RPC error: %v", rpcErr)
    return rpcErr
}

// Check application layer errors
if resp.Error != error_pb.ErrorCode_SUCCESS {
    log.Errorf("Operation failed: %v", resp.Error)
    return fmt.Errorf("operation failed: %v", resp.Error)
}

// Process successful response
readData := resp.GetReadData()
```

### 5. Retry Logic

Network operations can fail. Implement appropriate retry logic:

```go
func (c *DEngineClient) GetWithRetry(ctx context.Context, key string, maxRetries int) (string, error) {
    var lastErr error

    for attempt := 0; attempt < maxRetries; attempt++ {
        value, err := c.Get(ctx, key)
        if err == nil {
            return value, nil
        }

        lastErr = err

        // Exponential backoff
        backoff := time.Duration(1<<uint(attempt)) * 100*time.Millisecond
        time.Sleep(backoff)
    }

    return "", fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}
```

### 6. Connection Pooling

For production systems, use connection pooling or load balancing:

```go
// Simple round-robin across multiple nodes
var clients []*DEngineClient
addrs := []string{"node1:9081", "node2:9081", "node3:9081"}

for _, addr := range addrs {
    client, err := NewDEngineClient(addr)
    if err != nil {
        return err
    }
    clients = append(clients, client)
}

// Use load balancer to select client
lb := NewRoundRobinLB(clients)
```

## Testing

When testing applications using d-engine:

```go
// Unit test with mock client
func TestMyApp(t *testing.T) {
    // Create mock d-engine client
    // Test your application logic
}

// Integration test with real d-engine
func TestMyAppIntegration(t *testing.T) {
    // Start d-engine test cluster
    // Test against real d-engine
    // Verify behavior
}
```

## Migrating from etcd to d-engine

If migrating from etcd:

1. **Define abstraction layer**

   ```go
   type DistributedStore interface {
       Get(ctx context.Context, key string) (string, error)
       Put(ctx context.Context, key, value string) error
       Delete(ctx context.Context, key string) error
   }
   ```

2. **Implement adapters for both systems**

   ```go
   type EtcdStore struct { /* ... */ }
   type DEngineStore struct { /* ... */ }
   ```

3. **Switch via configuration**

   ```go
   var store DistributedStore
   if config.UseEtcd {
       store = NewEtcdStore(etcdClient)
   } else {
       store = NewDEngineStore(dengineClient)
   }
   ```

4. **Gradually migrate data** before switching to d-engine

## Troubleshooting

### Connection Refused

- Verify d-engine is running on the specified address
- Check firewall rules and port accessibility
- Ensure correct node address (not internal RPC port)

### Timeout Errors

- Increase context timeout if operations are slow
- Check d-engine cluster health
- Verify network connectivity

### "Leadership verification failed" Error

**Symptom**: LinearizableRead or LeaseRead requests fail with `FailedPrecondition` error.

**Cause**: No leader elected yet (during startup), or leader lost quorum (network partition).

**Solution**:

- Use EventualConsistency during system startup (works without leader)
- Switch to LinearizableRead or LeaseRead after cluster stabilizes
- For production, use LeaseRead for general operations (balanced performance)
- Reserve LinearizableRead for critical transactions only

### Key Not Found

- Verify key was written successfully
- Check consistency policy (eventual vs linearizable)
- Ensure reading from correct cluster

## Monitoring and Observability

d-engine exposes metrics and logs. Monitor:

- RPC latency and error rates
- Cluster state changes
- Data consistency issues

Example monitoring:

```go
// Log all operations
func (c *DEngineClient) GetLogged(ctx context.Context, key string) (string, error) {
    start := time.Now()

    value, err := c.Get(ctx, key)

    duration := time.Since(start)
    if err != nil {
        log.Warnf("GET %s failed after %v: %v", key, duration, err)
    } else {
        log.Debugf("GET %s succeeded in %v", key, duration)
    }

    return value, err
}
```

## API Reference

For complete API documentation and examples, see:

- **d-queue Source Code**: https://github.com/deventlab/d-queue (recommended reference)
- [d-engine Proto Definitions](https://github.com/deventlab/d-engine/tree/main/d-engine-proto/proto)
- [Read Consistency Guide](../server_guide/read-consistency.md) - Performance and trade-offs

## Resources

- **d-queue (Go Reference)**: https://github.com/deventlab/d-queue
- **d-engine GitHub**: https://github.com/deventlab/d-engine
- **Proto Definitions**: https://github.com/deventlab/d-engine/tree/main/d-engine-proto/proto
- **Community Discussions**: https://github.com/deventlab/d-engine/discussions
- **Issue Tracker**: https://github.com/deventlab/d-engine/issues

## Contributing

Found a bug or have suggestions for the Go client integration?

- Open an issue: https://github.com/deventlab/d-engine/issues
- Start a discussion: https://github.com/deventlab/d-engine/discussions
- Submit a pull request with improvements

We welcome community feedback and contributions!
