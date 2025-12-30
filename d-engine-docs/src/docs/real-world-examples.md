# d-engine Use Cases

**Purpose**: Help developers identify if d-engine fits their needs and choose the right integration mode.

---

## Overview

d-engine serves three main application types, each with different integration requirements:

1. **Control Plane Services** ⭐⭐⭐ (Embedded Mode recommended)
2. **DNS/Service Discovery** ⭐⭐ (Embedded or Standalone)
3. **Distributed Orchestration** ⭐ (Embedded Mode)

---

## Use Case 1: Control Plane Services ⭐⭐⭐

### What It Is

Applications that coordinate distributed systems: database HA managers, distributed schedulers, workflow engines, API gateways.

### Architecture Pattern

```text
┌─────────────────────────────────────────┐
│    Frontend (multi-region/global)       │
│  Dashboard/CLI/API Clients (anywhere)   │
└────────────┬────────────────────────────┘
             │
             │ HTTP/gRPC (connect any backend node)
             ▼
┌──────────────────────────────────────────┐
│   Backend Service Cluster (single DC)    │
│  ┌──────┐  ┌──────┐  ┌──────┐           │
│  │App A │  │App B │  │App C │           │
│  │(embed│  │(embed│  │(embed│           │
│  │ded)  │  │ded)  │  │ded)  │           │
│  │d-    │  │d-    │  │d-    │           │
│  │engine│  │engine│  │engine│           │
│  └──────┘  └──────┘  └──────┘           │
│     ↓          ↓          ↓              │
│  [Manage PostgreSQL/MySQL/state machine] │
└──────────────────────────────────────────┘
```

### Key Requirements

1. **Frontend doesn't care who is leader**
   - Users/clients connect to any node randomly
   - Application layer forwards writes to leader
   - Reads can be served from any node (with stale read option)

2. **Embedded for performance**
   - Need <0.1ms local access (LocalKvClient)
   - Avoid network serialization overhead
   - Single binary deployment

3. **Application-layer forwarding**
   - Detect `NotLeader` errors
   - Redirect/retry to actual leader
   - HTTP 307 or internal retry logic

### Real-World Examples

**Patroni (PostgreSQL HA)**:

- 3-node Patroni cluster manages 1 PostgreSQL primary
- Patroni embeds distributed consensus for leader election
- REST API accepts requests on any node, forwards to leader internally

**Apache APISIX (API Gateway)**:

- Embeds d-engine for configuration synchronization
- Any gateway node can receive config updates
- Leader writes, followers read locally

### Integration Mode

**Recommended**: Embedded Mode (Rust only)

```toml
[dependencies]
d-engine = "0.2"  # default = server + rocksdb
```

**Example Code**:

```rust,ignore
use d_engine::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = EmbeddedEngine::start_with("d-engine.toml").await?;
    engine.wait_ready(Duration::from_secs(5)).await?;

    let client = engine.client();  // <0.1ms local access

    // Application logic: forward writes to leader if needed
    match client.put(key, value).await {
        Err(e) if e.is_not_leader() => {
            let leader_addr = e.leader_address();
            // HTTP 307 redirect or internal retry
        }
        Ok(_) => { /* success */ }
        Err(e) => { /* other errors */ }
    }

    Ok(())
}
```

### See Also

- [Quick Start Guide](quick-start-5min.md)
- [Service Discovery Pattern](client_guide/service-discovery-pattern.md)
- [Error Handling](client_guide/error-handling.md)

---

## Use Case 2: DNS/Service Discovery ⭐⭐

### What It Is

DNS servers, service registries, configuration centers that serve read-heavy workloads.

### Architecture Pattern

```text
┌──────────────────────────────────────────┐
│  Clients (Pods/Apps in cluster)          │
│         ↓ DNS queries                    │
└─────────┼────────────────────────────────┘
          │ (random DNS server selection)
          ▼
┌──────────────────────────────────────────┐
│  DNS Server Replicas (read-only)         │
│  ┌──────┐  ┌──────┐  ┌──────┐           │
│  │DNS A │  │DNS B │  │DNS C │           │
│  │(watch│  │(watch│  │(watch│           │
│  │ +    │  │ +    │  │ +    │           │
│  │stale │  │stale │  │stale │           │
│  │read) │  │read) │  │read) │           │
│  └──────┘  └──────┘  └──────┘           │
│              ↓                           │
│     Shared d-engine cluster              │
│     (writes from external controller)    │
└──────────────────────────────────────────┘
```

### Key Requirements

1. **Stale read acceptable**
   - DNS allows 50-100ms consistency lag
   - EventualConsistency policy: <0.1ms local reads
   - Watch API for real-time updates

2. **Read-write separation**
   - DNS servers only read
   - Separate controller writes (e.g., Kubernetes Controller)

3. **Watch-based updates**
   - No polling, event-driven updates
   - Automatic cache invalidation

### Real-World Examples

**CoreDNS (Kubernetes)**:

- Watches Kubernetes Service/Endpoints resources
- Updates local DNS cache on changes
- Serves DNS queries from local cache

**Consul (Service Discovery)**:

- Agents watch service registry
- Three consistency modes: stale/default/consistent
- Balances performance vs consistency

### Integration Modes

#### Option A: Embedded Mode (Rust DNS server)

```toml
[dependencies]
d-engine = "0.2"
```

**Code**:

```rust,ignore
use d_engine::prelude::*;

// DNS server with embedded d-engine
let engine = EmbeddedEngine::start_with("d-engine.toml").await?;
let client = engine.client();

// Watch for service record changes
let mut watch_rx = client.watch(b"services/nginx".to_vec()).await?;

tokio::spawn(async move {
    while let Some(event) = watch_rx.recv().await {
        match event {
            WatchEvent::Put(key, value) => {
                // Update local DNS cache
                update_dns_cache(key, value);
            }
            WatchEvent::Delete(key) => {
                // Remove from cache
                remove_from_cache(key);
            }
        }
    }
});

// Serve DNS queries from local cache (EventualConsistency)
let value = client.get_eventual(b"services/nginx".to_vec()).await?;
```

#### Option B: Standalone Mode (Go/Python DNS server)

```toml
d-engine = { version = "0.2", features = ["client"], default-features = false }
```

Connect Go/Python DNS server to d-engine cluster via gRPC.

### See Also

- [Watch Feature Guide](server_guide/watch-feature.md)
- [Read Consistency Policies](client_guide/read_consistency.md)
- [Service Discovery Pattern](client_guide/service-discovery-pattern.md)

---

## Use Case 3: Distributed Orchestration ⭐

### What It Is

Kubernetes Operators, storage orchestrators, task schedulers that require leader-only execution.

### Architecture Pattern

```text
┌──────────────────────────────────────────┐
│  Operator Replicas (Leader Election)     │
│  ┌──────┐  ┌──────┐  ┌──────┐           │
│  │Op A  │  │Op B  │  │Op C  │           │
│  │leader│  │follow│  │follow│           │
│  │(embed│  │(embed│  │(embed│           │
│  │ded)  │  │ded)  │  │ded)  │           │
│  └──────┘  └──────┘  └──────┘           │
│              ↓                           │
│     Only leader executes operations      │
└──────────────────────────────────────────┘
```

### Key Requirements

1. **Leader-only work mode**
   - Only leader executes orchestration tasks
   - Followers standby, ready to take over

2. **Fast failover**
   - Leader failure → Follower election in <5s
   - Minimal business interruption

3. **Kubernetes integration**
   - Leader Election with K8s Service
   - Health checks + auto routing

### Real-World Examples

**Rook Ceph Operator**:

- Uses etcd for leader election
- Only leader pod executes storage orchestration
- Kubernetes Service routes to active leader

**Vitess (MySQL Orchestrator)**:

- Leader-only shard management
- Followers watch for leader changes
- Automatic failover

### Integration Mode

**Recommended**: Embedded Mode

```rust,ignore
use d_engine::prelude::*;
use std::time::Duration;

// Get node_id from config file (production recommended)
let config = RaftNodeConfig::new()?.with_override_config("node1.toml")?;
let my_node_id = config.cluster.node_id;

let engine = EmbeddedEngine::start_with("./data", Some("node1.toml")).await?;

// Wait for initial leader election
engine.wait_ready(Duration::from_secs(5)).await?;

// Subscribe to leader changes
let mut leader_rx = engine.leader_change_notifier();

tokio::spawn(async move {
    while leader_rx.changed().await.is_ok() {
        match leader_rx.borrow().as_ref() {
            Some(info) if info.leader_id == my_node_id => {
                // I became leader, start orchestration tasks
                start_orchestration().await;
            }
            Some(_) => {
                // I'm a follower, stop tasks
                stop_orchestration().await;
            }
            None => {
                // Election in progress
            }
        }
    }
});
```

### See Also

- [Leader Election Example](examples/three-nodes-cluster.md)
- [Lifecycle APIs](quick-start-5min.md#lifecycle-management)

---

## Comparison Table

| Use Case                  | Read/Write Ratio | Consistency Needs     | Recommended Mode       | Key APIs                           |
| ------------------------- | ---------------- | --------------------- | ---------------------- | ---------------------------------- |
| Control Plane Services    | 50/50            | Strong (LeaseRead)    | Embedded (Rust)        | LocalKvClient, error handling      |
| DNS/Service Discovery     | 99/1             | Eventual (<100ms lag) | Embedded or Standalone | Watch, EventualConsistency         |
| Distributed Orchestration | 10/90            | Strong (leader-only)  | Embedded (Rust)        | leader_change_notifier, wait_ready |

---

## Choosing Integration Mode

### Decision Tree

```text
Is your application written in Rust?
├─ YES → Use Embedded Mode
│         - Zero overhead (<0.1ms)
│         - Single binary
│         - Type-safe
│
└─ NO → Use Standalone Mode
          - Language-agnostic (gRPC)
          - 51% higher write throughput vs etcd*
          - Deploy d-engine as separate service

*Test environment benchmarks. Production varies by hardware.
```

### When NOT to Use d-engine

❌ **Don't use d-engine for**:

- Large-scale data storage (use TiKV, CockroachDB)
- Multi-datacenter geo-replication (use CockroachDB, YugabyteDB)
- Weak consistency is acceptable (use Redis, Memcached)
- SQL query interface required (use PostgreSQL, MySQL)

✅ **d-engine is for**:

- Coordination metadata (<100MB)
- Strong consistency within single datacenter
- Embedded Rust applications
- Replacing etcd with better performance

---

## Next Steps

1. **Quick Start**: [5-Minute Tutorial](quick-start-5min.md)
2. **Examples**: [Single-Node Expansion](examples/single-node-expansion.md)
3. **Production**: [3-Node Cluster Setup](examples/three-nodes-cluster.md)
4. **Custom Integration**: [Server Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/index.html)

---

**Created**: 2025-12-13  
**Updated**: 2025-12-13
