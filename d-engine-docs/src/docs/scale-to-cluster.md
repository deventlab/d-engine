# Scale from Single-Node to 3-Node Cluster

**Goal**: Expand your d-engine deployment from 1 node to a 3-node cluster for high availability.

---

## Prerequisites

- Completed [quick-start-5min.md](./quick-start-5min.md)
- Understanding of single-node embedded mode
- 3 servers or 3 terminal windows (for local testing)

---

## Why Scale to 3 Nodes?

### Single-Node Limitations

```
1 node = No fault tolerance
- If node crashes → data unavailable
- If disk fails → potential data loss
- No protection against hardware failure
```

### 3-Node Benefits

```
3 nodes = Fault-tolerant cluster
- Any 1 node can fail → cluster still works
- Automatic leader re-election
- Data replicated across nodes
- Zero downtime during node replacement
```

**Key principle**: 3 nodes = tolerate 1 failure (majority = 2 out of 3).

---

## Configuration Changes Only

**No code changes required.** Same binary, same application logic.

### Single-Node Config

```toml
# config/single-node.toml
[cluster]
node_id = 1
# Empty initial_cluster = single node
```

### 3-Node Cluster Config

**Node 1** (`config/node1.toml`):

```toml
[cluster]
node_id = 1
listen_address = "127.0.0.1:9081"
initial_cluster = [
    { id = 1, address = "127.0.0.1:9081" },
    { id = 2, address = "127.0.0.1:9082" },
    { id = 3, address = "127.0.0.1:9083" }
]
```

**Node 2** (`config/node2.toml`):

```toml
[cluster]
node_id = 2
listen_address = "127.0.0.1:9082"
initial_cluster = [
    { id = 1, address = "127.0.0.1:9081" },
    { id = 2, address = "127.0.0.1:9082" },
    { id = 3, address = "127.0.0.1:9083" }
]
```

**Node 3** (`config/node3.toml`):

```toml
[cluster]
node_id = 3
listen_address = "127.0.0.1:9083"
initial_cluster = [
    { id = 1, address = "127.0.0.1:9081" },
    { id = 2, address = "127.0.0.1:9082" },
    { id = 3, address = "127.0.0.1:9083" }
]
```

**Key difference**: `initial_cluster` lists all 3 nodes.

---

## Update Application Code (1 line)

### Before (single-node)

```rust
let engine = EmbeddedEngine::with_rocksdb("./data").await?;
```

### After (cluster-aware)

```rust
use std::path::PathBuf;
use std::sync::Arc;

// Load config file
let config_path = std::env::var("CONFIG_PATH")
    .unwrap_or_else(|_| "config/node1.toml".to_string());

// Create storage with node-specific paths
let node_id = std::env::var("NODE_ID")
    .unwrap_or_else(|_| "1".to_string());
let data_dir = format!("./data/node{}", node_id);

let storage = Arc::new(RocksDBStorageEngine::new(
    PathBuf::from(format!("{}/storage", data_dir))
)?);
let state_machine = Arc::new(RocksDBStateMachine::new(
    PathBuf::from(format!("{}/state_machine", data_dir))
)?);

// Start with config file
let engine = EmbeddedEngine::start(
    Some(&config_path),
    storage,
    state_machine
).await?;
```

---

## Launch 3-Node Cluster (Local Testing)

### Terminal 1: Start Node 1

```bash
CONFIG_PATH=config/node1.toml NODE_ID=1 cargo run
```

**Expected output**:

```
Starting d-engine...
✓ Node initialized
⏳ Waiting for leader election...
```

**Note**: Node 1 will wait for majority (need 2 out of 3 nodes).

---

### Terminal 2: Start Node 2

```bash
CONFIG_PATH=config/node2.toml NODE_ID=2 cargo run
```

**Expected output** (on both Node 1 and Node 2):

```
✓ Leader elected:
 node 1 (term 1)
```

**Why Node 1 becomes leader**: First node to start typically wins election (lowest node_id tie-breaker).

---

### Terminal 3: Start Node 3

```bash
CONFIG_PATH=config/node3.toml NODE_ID=3 cargo run
```

Node 3 joins the cluster and syncs existing data from the leader.

---

## Verify Cluster Works

### Test 1: Write on Node 1

In your application code
(or via client):

```rust
// On Node 1
client.put(b"test-key".to_vec(), b"test-value".to_vec()).await?;
println!("✓ Written on Node 1");
```

### Test 2: Read from Node 2

```rust
// On Node 2
if let Some(value) = client.get(b"test-key".to_vec()).await? {
    println!("✓ Read from Node 2: {}", String::from_utf8_lossy(&value));
}
```

**Expected**: Node 2 sees the data written on Node 1 (Raft replication).

---

## Test Failover

### Kill Node 1 (Current Leader)

Press `Ctrl+C` in Terminal 1.

**What happens**:

1. Node 2 an
   d Node 3 detect leader failure (~1s heartbeat timeout)
2. Node 2 or Node 3 starts election
3. New leader elected (term increases
   to 2)
4. Cluster continues serving requests

**Expected output** (on Node 2 or Node 3):

```
Leader elected: node 2 (term 2)
```

### Verify Cluster Still Works

Write new data on Node 2 or Node 3:

```rust
// On Node 2 (new leader)
client.put(b"after-failover".to_vec(), b"still-works
".to_vec()).await?;
println!("✓ Cluster still operational after Node 1 failure");
```

---

## Restart Node 1 (Rejoin Cluster)

Restart Terminal 1:

```bash
CONFIG_PATH=config/node1.toml NODE_ID=1 cargo run
```

**What happens**:

1. Node 1 starts as Follower (not leader anymore)
2. Discovers current leader is Node 2 (term 2)
3. Syn
   cs missing data from leader
4. Joins cluster as replica

**Expected output**:

```
✓ Node initialized
✓ Leader elected: node 2 (term 2)  ← Node 2 is now leader
```

**Verify sync**: Read `after-failover` key on Node 1 → should return `still-works`.

---

## Performance

Comparison

| Metric              | Single-Node | 3-Node Cluster |
| ------------------- | ----------- | -------------- |
| **Write latency**   | <0.1ms      | 1-5ms          |
| **Read latency**    | <0.1ms      | <0.1ms         |
| **Fault tolerance** | 0 failures  | 1 failure      |
| **Leader election** | Instant     | 1-2s           |
| **Disk usage**      | 1x          | 3x             |
| **Cost**            | $100/mo     | $300/mo        |

**Tradeoff
**: 3x cost for fault tolerance and availability.

---

## Production Deployment

### Use Separate Servers

**Server 1** (192.168.1.10):

```toml
[cluster]
node_id = 1
listen_address = "192.168.1.10:9081"
initial_cluster = [
    { id = 1, address = "192.168.1.10:9081" },
    { id = 2, address = "192.168.1.11:9082" },
    { id = 3, address = "192.168.1.12:9083" }
]
```

**Server 2** (192.168.1.11):

```toml
[cluster]
node_id = 2
listen_address = "192.168.1.11:9082"
initial_cluster = [
    { id = 1, address = "192.168.1.10:9081" },
    { id = 2, address = "192.168.1.11:9082" },
    { id = 3, address = "192.168.1.12:9083" }
]
```

**Server 3** (192.168.1.12):

```toml
[cluster]
node_id = 3
listen_address = "192.168.1.12:9083"
initial_cluster = [
    { id = 1, address = "192.168.1.10:9081" },
    { id = 2, address = "192.168.1.11:9082" },
    { id = 3, address = "192.168.1.12:9083" }
]
```

### Network Requirements

**Firewall rules**:

- Allow inbound TCP on port 9081-9083 (Raft protocol)
- Allow outbound TCP to all peer nodes

**Network latency**:

- <10ms between nodes (recommended)
- > 100ms may cause election timeouts (adjust config)

---

## Common Issues

### "Cluster stuck, no leader elected"

**Cause**: Less than majority (2 out of 3) nodes online.

**Fix**:

- Ensure at least 2 nodes are running
- Check network connectivity between nodes
- Verify `initial_cluster` is identical on all nodes

### "Node won't join cluster"

**Cause**: Mismatched `initial_cluster` configuration.

**Fix**:

```bash
# Verify configs are identical
diff config/node1.toml config/node2.toml
# Only node_id and listen_address should differ
```

### "Data not syncing between nodes"

**Cause**: Network partition or slow replication.

**Fix**:

- Check network latency: `ping <peer-ip>`
- Verify firewall allows Raft ports
- Check logs for replication errors

---

## Best Practices

### 1. Always Use 3 or 5 Nodes (Odd Numbers)

```
1 node → 0 failures tolerated
3 nodes → 1 failure tolerated (recommended)
5 nodes → 2 failures tolerated (high availability)
```

**Never use 2 or 4 nodes**: No benefit (same fault tolerance as 1 or 3).

### 2. Deploy Across Availability Zones

```
Node 1 → Zone A (us-east-1a)
Node 2 → Zone B (us-east-1b)
Node 3 → Zone C (us-east-1c)
```

Protects against datacenter failures.

### 3. Monitor Leader Changes

```rust
let mut leader_rx = engine.leader_notifier();

tokio::spawn(async move {
    while leader_rx.changed().await.is_ok() {
        if let Some(info) = leader_rx.borrow().as_ref() {
            // Alert if leader changes frequently (network instability)
            warn!("Leader changed: {} (term {})", info.leader_id, info.term);
        }
    }
});
```

Frequent leader changes indicate network problems.

### 4. Plan for Rolling Upgrades

```
1. Stop Node 3 → upgrade binary → restart
2. Wait for Node 3 to sync
3. Stop Node 2 → upgrade → restart
4. Wait for Node 2 to sync
5. Stop Node 1 → upgrade → restart
```

Always maintain majority (2 nodes) during upgrades.

---

## Next Steps

### Production Deployment Guide

See [deployment-guide.md](./deployment-guide.md):

- Systemd service files
- Health checks and monitoring
- Backup and restore
- Security hardening

### Configuration Reference

See [configuration.md](./configuration.md):

- All available config options
- Performance tuning parameters
- Network and timeout settings

### Monitoring and Observability

See [monitoring-guide.md](./monitoring-guide.md):

- Key metrics to track
- Prometheus integration
- Alerting rules
- Log analysis

---

## Key Takeaways

- ✅ **Zero code changes**: Only config file updates needed
- ✅ **Gradual rollout**: Start 2 nodes first, add 3rd later
- ✅ **Automatic failover**: Cluster re-elects leader in 1-2s
- ✅ **Same API**: `client.put()` works identically on all nodes
- ✅ **Cost-effective**: 3x cost for production-grade fault tolerance

**This is what "scale smart" means**: complexity stays in d-engine, your code stays simple.

---

**Created**: 2025-11-28  
**Updated**: 2025-11-28
