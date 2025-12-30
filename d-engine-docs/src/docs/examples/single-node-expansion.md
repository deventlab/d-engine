# Scale from Single-Node to 3-Node Cluster

**Goal**: Dynamically expand your running d-engine from 1 node to 3 nodes without downtime.

---

## Why Scale to 3 Nodes?

| Single Node                        | 3-Node Cluster               |
| ---------------------------------- | ---------------------------- |
| No fault tolerance                 | Tolerates 1 node failure     |
| $100/month                         | $300/month                   |
| If node crashes → data unavailable | Auto leader re-election      |
| No replication                     | Data replicated across nodes |

**Key principle**: 3 nodes = tolerate 1 failure (quorum = 2 out of 3).

---

## Dynamic Expansion (Zero Downtime)

**Target**: 1 node → 3 nodes (skip 2-node, no fault tolerance)

**What happens**:

1. Node 1 running as single-node cluster (already has data)
2. Start Node 2 → joins as Learner → syncs data → auto-promotes to Voter
3. Start Node 3 immediately → joins and syncs (do NOT stop at 2 nodes)
4. Result: 3-node cluster, Node 1 never restarted

**Why not 2 nodes?** 2-node cluster has zero fault tolerance (quorum = 2, any failure = cluster down). Always use odd numbers: 1, 3, or 5 nodes.

**Example**: `examples/single-node-expansion/`

---

## Prerequisites

- Node 1 running in single-node mode
- 2 additional servers (or terminals for local testing)
- Network connectivity between nodes

---

## Step 1: Start Node 1 (Single-Node)

**Node 1 config** (`config/n1.toml`):

```toml
[cluster]
node_id = 1
listen_address = "0.0.0.0:9081"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 }
]
initial_cluster_size = 1  # Single-node mode
```

**Config field reference**:

- `role = 2`: Leader (NodeRole: 0=Follower, 1=Candidate, 2=Leader, 3=Learner)
- `status = 2`: ACTIVE (NodeStatus: 0=JOINING, 1=SYNCING, 2=ACTIVE)

**Start**:

```bash
cd examples/single-node-expansion
make build
make start-node1
```

**Expected log**:

```text
[1<2>] >>> switch to Leader now.
```

Node 1 is now leader, accepting writes.

---

## Step 2: Join Node 2

**Node 2 config** (`config/n2.toml`):

```toml,ignore
[cluster]
node_id = 2
listen_address = "0.0.0.0:9082"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Existing leader
    { id = 2, address = "0.0.0.0:9082", role = 3, status = 0 },  # Self: Learner
]
initial_cluster_size = 1  # Must match Node 1
```

**Key fields**:

- `role = 3`: Learner (will auto-promote to Voter)
- `status = 0`: JOINING (new node catching up with logs)

> **Note**: `status = 0` (JOINING) means this node is new and needs to sync data.  
> `status = 2` (ACTIVE) means the node is already a formal member (like Node 1).

**Why join as Learner (not Follower)?**

| Join Method              | Safety                                     | Quorum Impact                           |
| ------------------------ | ------------------------------------------ | --------------------------------------- |
| **Learner** (role=3) ✅  | Safe - doesn't affect quorum during sync   | None - promotes after catching up       |
| **Follower** (role=2) ⚠️ | Risky - immediately participates in quorum | High - can slow down writes if unstable |

**IMPORTANT**: Always join new nodes as Learner. Joining as Follower can impact cluster availability if the new node is slow or unstable.  
📖 Details: See [Node Join & Learner Promotion Architecture](../../architecture/node-join-architecture.md)

**Start**:

```bash
make join-node2
```

**Expected log**:

```text
✅ NODE 2 SUCCESSFULLY JOINED CLUSTER
Role: 🎓 Learner → Syncing data from Leader 1
```

**Sync mechanism**: InstallSnapshot (bulk data) + AppendEntries (incremental logs), then auto-promotes to Voter.

---

## Step 3: Join Node 3 (Immediately After Node 2)

**IMPORTANT**: Do NOT stop at 2 nodes. Start Node 3 right after Node 2.

**Node 3 config** (`config/n3.toml`):

```toml,ignore
[cluster]
node_id = 3
listen_address = "0.0.0.0:9083"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Leader, ACTIVE
    { id = 2, address = "0.0.0.0:9082", role = 1, status = 2 },  # Follower (promoted), ACTIVE
    { id = 3, address = "0.0.0.0:9083", role = 3, status = 0 },  # Self: Learner, JOINING
]
initial_cluster_size = 1
```

**Key**: Node 2 listed as `role = 1, status = 2` assumes it's already promoted to Follower and ACTIVE.

> **Alternative (safer)**: If unsure about Node 2's promotion status, use conservative config:  
> `{ id = 2, ..., role = 3, status = 0 }` - System will auto-correct if Node 2 is already promoted.

**Start**:

```bash
make join-node3
```

**Result**: 3-node cluster with 1-failure tolerance. Node 1 never restarted.

---

## Verify Cluster

**Check cluster status**:

```bash
# All 3 nodes should be running
ps aux | grep demo
```

**Test replication**:

1. Write data via Node 1 (leader)
2. Read from Node 2 or Node 3
3. Data should be replicated

---

## Test Failover (Optional)

**Kill current leader**:

```bash
# Find leader process (check which node is leader)
ps aux | grep demo | grep 908[1-3]
kill <PID>
```

**Expected behavior** (Raft guarantees):

- Remaining 2 nodes detect leader failure (~1s)
- New leader elected via majority vote (2/3 quorum)
- Cluster continues accepting writes
- ~1-2s downtime during re-election

**Restart killed node**:

```bash
# If killed Node 1
make start-node1
# If killed Node 2/3, they will auto-rejoin
```

Node rejoins as follower, syncs missing data from new leader.

---

## Troubleshooting

**"Node won't join"**:

- Check `initial_cluster_size = 1` matches across all configs
- Verify Node 1 is running and leader
- Check network connectivity: `nc -zv 0.0.0.0 9081`

**"No leader elected"**:

- Ensure at least 2 nodes running (quorum)
- Check logs for errors
- Verify addresses in configs match actual IPs

---

## Production Deployment

**For production servers**, update addresses:

```toml
# Node 2 on server 192.168.1.11
[cluster]
node_id = 2
listen_address = "192.168.1.11:9082"
initial_cluster = [
    { id = 1, address = "192.168.1.10:9081", role = 2, status = 2 },
    { id = 2, address = "192.168.1.11:9082", role = 3, status = 0 },
]
initial_cluster_size = 1
```

**Network requirements**:

- < 10ms latency between nodes
- Allow TCP ports 9081-9083
- Deploy across availability zones for fault tolerance

---

## Next Steps

- See `examples/single-node-expansion/README.md` for detailed architecture
- Review [quick-start-5min.md](./quick-start-5min.md) for embedded mode basics
- Check `examples/three-nodes-cluster/` for direct 3-node deployment

---

**Created**: 2025-12-03  
**Updated**: 2025-12-25  
**Example**: `examples/single-node-expansion/`
