# Single-Node to 3-Node Cluster Expansion

Demonstrates **zero-downtime expansion** from 1 node to 3 nodes.

## What This Tests

```
Initial State:                After Node 2 Joins:          After Node 3 Joins:
┌─────────┐                   ┌─────────┐                  ┌─────────┐
│ Node 1  │                   │ Node 1  │                  │ Node 1  │
│ Leader  │                   │ Leader  │                  │ Leader  │
│         │                   │         │                  │         │
└─────────┘                   └────┬────┘                  └────┬────┘
                                   │                            │
Single node cluster                │                       ┌────┴────┐
(initial_cluster_size=1)      ┌────┴────┐                 │         │
                              │ Node 2  │            ┌────┴────┐ ┌──┴──────┐
                              │ Voter   │            │ Node 2  │ │ Node 3  │
                              │         │            │ Voter   │ │ Voter   │
                              └─────────┘            └─────────┘ └─────────┘

                              2-node cluster          3-node cluster
                              (quorum = 2)            (quorum = 2)
```

```
Step 1: Node 1 starts as leader (single-node cluster)
Step 2: Node 2 joins as Learner → auto-promotes to Voter
Step 3: Node 3 joins as Learner → auto-promotes to Voter

Result: 3-node cluster with fault tolerance (Node 1 never restarted)
```

---

## Quick Start

### Build

```bash
make build
```

### Run (3 terminals)

**Terminal 1: Start Node 1**

```bash
make start-node1
```

Expected output:

```
[Node 1] Candidate → Leader (term 2)
```

**Terminal 2: Join Node 2**

```bash
make join-node2
```

Expected output:

```
✅ NODE 2 SUCCESSFULLY JOINED CLUSTER
Role: 🎓 Learner → Syncing data from Leader 1
[Node 2] Learner → Follower (term 2)
🎊 NODE 2 PROMOTED TO VOTER!
```

**Terminal 3: Join Node 3**

```bash
make join-node3
```

Expected output:

```
✅ NODE 3 SUCCESSFULLY JOINED CLUSTER
[Node 3] Learner → Follower (term 2)
🎊 NODE 3 PROMOTED TO VOTER!
```

---

## Key Observations

**What to verify:**

- ✅ Node 1 becomes leader immediately (no election)
- ✅ Node 2 joins as Learner, syncs data, promotes to Voter
- ✅ Node 3 joins as Learner, syncs data, promotes to Voter
- ✅ Final cluster: 1 Leader + 2 Followers (3 voters total)
- ✅ Node 1 never restarted during expansion

**How nodes sync:**

- Learner syncs via `AppendEntries` (raft log replication)
- Leader monitors learner's `match_index` progress
- Auto-promotion when learner catches up to leader's `commit_index`

---

## Configuration Overview

**Node 1** (`config/n1.toml`):

```toml
[cluster]
node_id = 1
listen_address = "0.0.0.0:9081"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 }  # Leader
]
db_root_dir = "./db"
```

**Node 2** (`config/n2.toml`):

```toml
[cluster]
node_id = 2
listen_address = "0.0.0.0:9082"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Existing leader
    { id = 2, address = "0.0.0.0:9082", role = 3, status = 0 },  # Self: Learner
]
db_root_dir = "./db"
```

**Node 3** (`config/n3.toml`):

```toml
[cluster]
node_id = 3
listen_address = "0.0.0.0:9083"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Leader
    { id = 2, address = "0.0.0.0:9082", role = 1, status = 2 },  # Follower (promoted)
    { id = 3, address = "0.0.0.0:9083", role = 3, status = 0 },  # Self: Learner
]
db_root_dir = "./db"
```

**Role values:** `1=Follower, 2=Leader, 3=Learner`  
**Status values:** `0=JOINING, 2=ACTIVE`

---

## Cleanup

```bash
make clean        # Remove all artifacts
make clean-logs   # Remove logs and DB only
```

---

## Troubleshooting

**Node 2/3 can't join:**

- Verify Node 1 is running: `ps aux | grep demo`
- Check addresses in config match actual IPs

**Learner never promotes:**

- Check Node 1 logs for "PROMOTING LEARNER" messages
- Verify data is syncing (match_index increasing)

---

## Further Reading

- [Single-Node Expansion Guide](../../d-engine-docs/src/docs/examples/single-node-expansion.md) - Detailed explanation

---

**Test Status:** ✅ Verified working (2025-12-30)
