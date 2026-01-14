# embedded-bench

Performance benchmark tool for d-engine's `EmbeddedEngine` mode.

## Quick Start

### Run Benchmarks (Makefile)

```bash
# Build benchmark binary
make build

# Run individual tests (comparable to Standalone report)
make test-single-write        # Single client write (10K requests)
make test-high-conc-write     # High concurrency write (100K requests)
make test-linearizable-read   # Linearizable read (100K requests)
make test-lease-read          # Lease-based read (100K requests)
make test-eventual-read       # Eventual consistency read (100K requests)
make test-hot-key             # Hot-key test (100K requests, 10 keys)

# Run all tests
make all-tests
```

**Compare with Standalone mode:**

- Standalone report: `../../benches/standalone-bench/reports/v0.2.2/report_v0.2.2.md`
- Same test parameters (key-size=8, value-size=256, sequential-keys)
- Direct performance comparison (Embedded vs Standalone)

## Overview

`embedded-bench` tests d-engine in embedded mode with three operation modes:

- **`local`**: Direct in-process benchmark using `LocalKvClient` (zero-copy, baseline performance)
- **`server`**: HTTP server exposing KV operations via REST API
- **`client`**: HTTP client for testing with load balancer (e.g., HAProxy)

## Node Behavior Matrix

The following table describes which nodes participate in benchmarks based on operation type, matching the behavior of `standalone-bench` (Standalone mode):

| Operation Type        | Leader Node      | Follower Nodes        | Notes                                      |
| --------------------- | ---------------- | --------------------- | ------------------------------------------ |
| **Write (put)**       | ✅ Run benchmark | ⏸️ Idle (wait Ctrl+C) | Only Leader accepts writes (Raft protocol) |
| **Linearizable Read** | ✅ Run benchmark | ⏸️ Idle (wait Ctrl+C) | Requires Leader to ensure linearizability  |
| **LeaseRead**         | ✅ Run benchmark | ⏸️ Idle (wait Ctrl+C) | Requires Leader for lease-based reads      |
| **Eventual Read**     | ✅ Run benchmark | ✅ Run benchmark      | Simulates load balancing across all nodes  |

**Key Points**:

- **Write Operations**: Only the Leader node runs the benchmark. Follower nodes keep the cluster alive but remain idle.
- **Linearizable/Lease Reads**: Only the Leader node runs the benchmark (matches gRPC client behavior in Standalone mode).
- **Eventual Reads**: All nodes (Leader + Followers) run the benchmark concurrently, simulating load-balanced read traffic across the cluster.
- **Idle Nodes**: Non-participating nodes maintain cluster membership and wait for Ctrl+C to shutdown gracefully.

This design mirrors how `standalone-bench` (Standalone) distributes requests via gRPC client load balancing.

## Architecture

### Test A: Embedded Pure Mode (Baseline)

Direct in-process benchmark measuring `LocalKvClient` performance without network overhead.

```

┌────────────────────────────────────────────────────────────┐
│ Single　Server (127.0.0.1) │
├────────────────────────────────────────────────────────────┤
│ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ embedded- │ │ embedded- │ │ embedded- │ │
│ │ bench n1 │ │ bench n2 │ │ bench n3 │ │
│ ├──────────────┤ ├──────────────┤ ├──────────────┤ │
│ │ LocalKvClient│ │ LocalKvClient│ │ LocalKvClient│ │
│ │ (internal │ │ (internal │ │ (internal │ │
│ │ benchmark) │ │ benchmark) │ │ benchmark) │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
│ │
└────────────────────────────────────────────────────────────┘

```

### Test B: Embedded + HAProxy Mode

End-to-end benchmark through HAProxy load balancer with automatic request routing.

```

┌────────────────────────────────────────────────────────────┐
│ Single　Server (127.0.0.1) │
├────────────────────────────────────────────────────────────┤
│ │
│ Benchmark Client (--mode client) │
│ ┌──────────────────────────────────────────────────┐ │
│ │ POST /kv → Write operations │ │
│ │ GET /kv/:key → Read operations │ │
│ └────────────────────┬─────────────────────────────┘ │
│ ↓ │
│ ┌─────────────────────────────────────────────────┐ │
│ │ HAProxy :8080 │ │
│ ├─────────────────────────────────────────────────┤ │
│ │ ACL: POST/PUT/DELETE → write_backend │ │
│ │ ACL: GET → read_backend │ │
│ └───┬──────────────────────────┬──────────────────┘ │
│ │ (health check) │ │
│ ↓ ↓ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ embedded- │ │ embedded- │ │ embedded- │ │
│ │ bench n1 │ │ bench n2 │ │ bench n3 │ │
│ ├──────────────┤ ├──────────────┤ ├──────────────┤ │
│ │ :8008 │ │ :8009 │ │ :8010 │ │ ← Health check
│ │ /primary │ │ /primary │ │ /primary │ │
│ │ /replica │ │ /replica │ │ /replica │ │
│ ├──────────────┤ ├──────────────┤ ├──────────────┤ │
│ │ :9001 │ │ :9002 │ │ :9003 │ │ ← Business API
│ │ POST /kv │ │ POST /kv │ │ POST /kv │ │
│ │ GET /kv/:key │ │ GET /kv/:key │ │ GET /kv/:key │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
│ │
└────────────────────────────────────────────────────────────┘

```

## HAProxy Request Routing

HAProxy automatically routes requests based on HTTP method:

- **Write Backend** (`/primary` health check):
  - Methods: `POST`, `PUT`, `DELETE`
  - Routes to: Leader node only
- **Read Backend** (`/replica` health check):
  - Methods: `GET`
  - Routes to: All nodes (round-robin)

### Health Check Endpoints

Each node provides health check endpoints:

- `GET /primary`: Returns `200 OK` if node is Leader, `503 Service Unavailable` otherwise
- `GET /replica`: Returns `200 OK` if node is Follower, `503 Service Unavailable` otherwise

## Manual Usage (Advanced)

### Test A: Pure Embedded Mode Benchmark

Measure baseline performance with zero network overhead:

```bash
# Node 1 (Leader only - writes require Leader)
CONFIG_PATH=./config/n1.toml \
./target/release/embedded-bench \
    --mode local \
    --total 100000 --clients 1000 \
    --key-size 8 --value-size 256 \
    --sequential-keys put

# Node 2
CONFIG_PATH=./config/n2.toml \
./target/release/embedded-bench \
    --mode local \
    --total 100000 --clients 1000 \
    --key-size 8 --value-size 256 \
    --sequential-keys put

# Node 3
CONFIG_PATH=./config/n3.toml \
./target/release/embedded-bench \
    --mode local \
    --total 100000 --clients 1000 \
    --key-size 8 --value-size 256 \
    --sequential-keys put
```

### Test B: Embedded + HAProxy Benchmark

Measure end-to-end performance with load balancer:

#### 1. Start HTTP Servers

```bash
# Node 1 (port 9001, health check 8008)
CONFIG_PATH=./config/n1.toml \
./target/release/embedded-bench --mode server --port 9001 --health-port 8008 &

# Node 2 (port 9002, health check 8009)
CONFIG_PATH=./config/n2.toml \
./target/release/embedded-bench --mode server --port 9002 --health-port 8009 &

# Node 3 (port 9003, health check 8010)
CONFIG_PATH=./config/n3.toml \
./target/release/embedded-bench --mode server --port 9003 --health-port 8010 &
```

#### 2. Configure HAProxy

Create `haproxy.cfg`:

```haproxy
global
    maxconn 4096

defaults
    mode http
    timeout connect 5s
    timeout client 30s
    timeout server 30s

frontend kv_frontend
    bind *:8080

    # Route writes to leader
    acl is_write method POST PUT DELETE
    use_backend write_backend if is_write

    # Route reads to all nodes
    default_backend read_backend

backend write_backend
    option httpchk GET /primary
    http-check expect status 200

    server n1 127.0.0.1:9001 check port 8008
    server n2 127.0.0.1:9002 check port 8009
    server n3 127.0.0.1:9003 check port 8010

backend read_backend
    balance roundrobin

    server n1 127.0.0.1:9001 check port 8008
    server n2 127.0.0.1:9002 check port 8009
    server n3 127.0.0.1:9003 check port 8010
```

Start HAProxy:

```bash
haproxy -f haproxy.cfg
```

#### 3. Run Benchmark Client

```bash
./target/release/embedded-bench \
    --mode client \
    --endpoint http://127.0.0.1:8080 \
    --total 100000 --clients 1000 \
    --key-size 8 --value-size 256 \
    --sequential-keys put
```

## CLI Options

```
embedded-bench [OPTIONS] <COMMAND>

Options:
  --mode <MODE>              Operation mode: local, server, client [default: local]
  --config-path <PATH>       Path to config file (or set CONFIG_PATH env var)
  --port <PORT>              HTTP server port (server mode only) [default: 9001]
  --health-port <PORT>       Health check port (server mode only) [default: 8008]
  --endpoint <URL>           HAProxy endpoint (client mode only) [default: http://127.0.0.1:8080]
  --key-size <SIZE>          Key size in bytes [default: 8]
  --value-size <SIZE>        Value size in bytes [default: 256]
  --total <NUM>              Total number of requests [default: 10000]
  --clients <NUM>            Number of concurrent clients [default: 1]
  --sequential-keys          Use sequential keys instead of random
  --key-space <NUM>          Limit key space (enables hot-key testing)

Commands:
  put                        Benchmark write operations
  get                        Benchmark read operations
    --consistency <POLICY>   Read consistency: l (linearizable), s (lease), e (eventual) [default: l]
```

## Performance Comparison

Expected performance comparison chain:

```
Standalone < Embedded+HAProxy < Embedded Pure Mode
   (gRPC)    (HTTP+HAProxy)      (LocalKvClient)
```

| Mode               | Overhead                     | Use Case                    |
| ------------------ | ---------------------------- | --------------------------- |
| Standalone         | gRPC serialization + network | Multi-language clients      |
| Embedded + HAProxy | HTTP serialization + HAProxy | Load-balanced deployments   |
| Embedded Pure      | Zero (in-process)            | Rust applications (optimal) |

## API Reference

### HTTP Endpoints (Server Mode)

#### Write Operations

```http
POST /kv
Content-Type: application/json

{
  "key": "my-key",
  "value": "my-value"
}

Response: 200 OK | 500 Internal Server Error
```

#### Read Operations

```http
GET /kv/:key

Response: 200 OK
{
  "value": "my-value"
}

Response: 404 Not Found (key does not exist)
```

#### Health Check Endpoints

```http
GET /primary
Response: 200 OK (if Leader) | 503 Service Unavailable (if Follower)

GET /replica
Response: 200 OK (if Follower) | 503 Service Unavailable (if Leader)
```

## Implementation Notes

### Why HTTP/JSON?

1. **HAProxy ACL Support**: HTTP method-based routing (`POST` → write, `GET` → read)
2. **Simplicity**: Easy to test with `curl` or any HTTP client
3. **Universality**: Language-agnostic (compare with gRPC in Standalone mode)
4. **Industry Standard**: etcd also provides HTTP API (`/v3/kv/put`, `/v3/kv/range`)

### Why Three Modes?

- **`local`**: Establishes performance baseline (zero overhead)
- **`server`**: Enables load balancer integration testing
- **`client`**: Measures end-to-end latency through HAProxy

## Troubleshooting

### HAProxy reports all nodes as DOWN

Check health check endpoints:

```bash
curl http://127.0.0.1:8008/primary  # Should return 200 on Leader
curl http://127.0.0.1:8009/replica  # Should return 200 on Follower
```

### Writes fail with "Not Leader" error

HAProxy may be routing to follower. Verify:

1. Health check path is correct (`/primary` for write backend)
2. Leader election completed (`engine.wait_ready()`)
3. HAProxy ACL is configured (`method POST PUT DELETE`)

### Benchmark client cannot connect

Ensure HAProxy is listening:

```bash
curl http://127.0.0.1:8080/kv/test  # Should route to nodes
```

---

**Created**: 2025-01-04  
**Updated**: 2025-01-04
