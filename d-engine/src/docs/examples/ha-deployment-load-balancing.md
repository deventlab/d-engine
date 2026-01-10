# HA Deployment with Load Balancing

**Build production-ready high-availability deployments with embedded d-engine and load balancers.**

---

## What You'll Build

A 3-node cluster with intelligent request routing:

- **Write requests** → Automatically routed to leader node
- **Read requests** → Load-balanced across all nodes
- **Health-based routing** → Failed nodes excluded automatically
- **Sub-second failover** → New leader elected when current leader fails

**Time to deploy: 5 minutes**

---

## Architecture

```text
                    ┌─────────────────────┐
                    │   Load Balancer     │
                    │  (HAProxy/Nginx/    │
                    │   Envoy/Custom)     │
                    └──────────┬──────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
   Writes (POST/PUT/DELETE)    │         Reads (GET)  │
   Route to leader only        │         Round-robin  │
        │                      │                      │
        ▼                      ▼                      ▼
   ┌─────────┐            ┌─────────┐          ┌─────────┐
   │ Node 1  │            │ Node 2  │          │ Node 3  │
   │ Leader  │◄──Raft────►│Follower │◄──Raft──►│Follower │
   │         │            │         │          │         │
   │HTTP:8081│            │HTTP:8082│          │HTTP:8083│
   │Health:  │            │Health:  │          │Health:  │
   │ 10001   │            │ 10002   │          │ 10003   │
   └─────────┘            └─────────┘          └─────────┘
        │                      │                      │
   /primary → 200         /primary → 503        /primary → 503
   /replica → 503         /replica → 200        /replica → 200
```

**Key principle**: Load balancer queries health endpoints to detect node roles and route requests accordingly.

---

## Quick Start

### Step 1: Start the Cluster

```bash
cd examples/three-nodes-embedded
make start-cluster
```

Wait for leader election (~2 seconds). Verify leader via health checks:

```bash
for port in 10001 10002 10003; do
  echo -n "Port $port: "
  curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary
  echo
done
# One port returns 200 (leader), others return 503 (followers)
```

### Step 2: Configure Load Balancer

**HAProxy example** (`haproxy.cfg`):

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
    server n1 127.0.0.1:8081 check port 10001
    server n2 127.0.0.1:8082 check port 10002
    server n3 127.0.0.1:8083 check port 10003

backend read_backend
    balance roundrobin
    server n1 127.0.0.1:8081 check port 10001
    server n2 127.0.0.1:8082 check port 10002
    server n3 127.0.0.1:8083 check port 10003
```

**Start HAProxy**:

```bash
haproxy -f haproxy.cfg
```

### Step 3: Test the Setup

**Write through load balancer** (routes to leader automatically):

```bash
curl -i -X POST http://localhost:8080/kv \
  -H "Content-Type: application/json" \
  -d '{"key": "username", "value": "alice"}'
# HTTP/1.1 200 OK
```

**Read through load balancer** (load-balanced):

```bash
curl http://localhost:8080/kv/username
# {"value":"alice"}
```

**Verify data consistency** (read from any node directly):

```bash
curl http://localhost:8082/kv/username
# {"value":"alice"}
```

---

## How It Works

### Health Check Mechanism

Each node exposes two health endpoints:

**`/primary` endpoint**:

- Returns `200 OK` if node is the leader
- Returns `503 Service Unavailable` if node is a follower
- Load balancer uses this to find the leader

**`/replica` endpoint**:

- Returns `200 OK` if node is a follower
- Returns `503 Service Unavailable` if node is the leader
- Optional: use for read-only backends

**Implementation** (from example code):

```rust,ignore
async fn health_primary(State(engine): State<Arc<EmbeddedEngine>>) -> StatusCode {
    if engine.is_leader() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
```

### Request Routing Strategy

**Write backend**:

1. Load balancer checks `/primary` on all nodes
2. Only the node returning `200` receives write traffic
3. Health checks run every 3 seconds (configurable)
4. On leader failure, new leader detected within 1-2 seconds

**Read backend**:

1. Round-robin across all healthy nodes
2. All nodes can serve reads (LeaseRead consistency)
3. Sub-millisecond local reads via embedded engine

---

## Load Balancer Options

This pattern works with any HTTP load balancer:

| Load Balancer | Health Check Support           | Configuration Complexity |
| ------------- | ------------------------------ | ------------------------ |
| **HAProxy**   | ✅ HTTP status codes           | Low (reference above)    |
| **Nginx**     | ✅ Active health checks (Plus) | Medium                   |
| **Envoy**     | ✅ HTTP health checks          | Medium (YAML config)     |
| **Custom**    | ✅ Any HTTP client             | Varies                   |

**Choosing a load balancer**:

- HAProxy: Industry standard, battle-tested, free
- Nginx Plus: If already using Nginx ecosystem
- Envoy: For service mesh environments
- Custom: Use any HTTP client to poll health endpoints

---

## Failover Behavior

**Scenario**: Leader node fails

```text
Time 0s: Node 1 (leader) crashes
   ↓
Time 1s: HAProxy health check fails on Node 1
   ↓
Time 2s: Raft election timeout triggers
   ↓
Time 3s: Node 2 elected as new leader
   ↓
Time 4s: HAProxy detects Node 2 /primary → 200
   ↓
Result: Write traffic automatically routes to Node 2
```

**Downtime**: ~3-5 seconds (configurable via Raft election timeout)

**Data safety**: All committed writes replicated to at least 2 nodes before acknowledgment

---

## Production Considerations

### Scaling

**Add more nodes**:

- 5-node cluster tolerates 2 failures
- 7-node cluster tolerates 3 failures
- Update `initial_cluster` in config files
- Add servers to load balancer config

### Monitoring

**Essential metrics**:

- Health endpoint response times
- Leader election frequency
- Read/write latency distribution
- Raft replication lag

### Network Partitions

**Split-brain prevention**:

- Raft requires majority quorum (2/3 for 3-node)
- Minority partition cannot elect leader
- Minority partition rejects writes
- Load balancer routes around failed partition

---

## When to Use This Pattern

**✅ Use embedded mode + load balancer when**:

- Your application is written in Rust
- You need custom HTTP/gRPC API (not just KV)
- You want sub-millisecond local reads
- You're deploying multiple application instances

**⚠️ Consider standalone mode when**:

- Your application is not in Rust
- You need language-agnostic deployment
- 1-2ms gRPC latency is acceptable

See [Integration Modes](crate::docs::integration_modes) for comparison.

---

## See Also

- **[three-nodes-embedded example](https://github.com/deventlab/d-engine/tree/main/examples/three-nodes-embedded)** - Complete working implementation
- **[Integration Modes](crate::docs::integration_modes)** - Embedded vs Standalone
- **[Read Consistency](crate::docs::client_guide::read_consistency)** - Tuning read behavior
- **[Single Node Expansion](crate::docs::examples::single_node_expansion)** - Scale from 1 to 3 nodes

---

**Key Takeaway**: This pattern gives you production-grade HA with minimal complexity. The load balancer handles routing, d-engine handles consistency. Copy the config, run `make start-cluster`, and you're done.
