# Node Join & Learner Promotion Architecture

## The Problem: Why Learner?

**Scenario**: Single node cluster (Node 1 as Leader). You want to add Node 2.

### ❌ Wrong: Add Node 2 as Follower immediately

- Node 2 becomes voter → quorum = 2 (both must agree)
- If Node 2 is slow syncing → Node 1 waits for replies
- **Result**: Cluster write latency increases. Developers see slowdown.

### ✅ Right: Add Node 2 as Learner first

- Node 2 doesn't vote → quorum stays 1 (Node 1 alone)
- Node 2 syncs independently, doesn't block Node 1
- After sync completes → auto-promote to voter
- **Result**: Zero impact on leader. Safe scaling.

---

## When to Use Each Role

| Scenario          | Role     | Status  | Auto-Promote?       |
| ----------------- | -------- | ------- | ------------------- |
| Expanding cluster | Learner  | Joining | Yes, after catch-up |
| Analytics node    | Learner  | Active  | Never (read-only)   |
| Regular member    | Follower | Active  | N/A                 |

---

## Why Odd Node Count? (1, 3, 5...)

Quorum = `⌈(N+1)/2⌉`

| Nodes | Quorum | Fault Tolerance | Status         |
| ----- | ------ | --------------- | -------------- |
| 1     | 1      | 0 failures      | Development ✅ |
| 2     | 2      | **0 failures**  | Don't use ❌   |
| 3     | 2      | 1 failure       | Production ✅  |
| 4     | 3      | 1 failure       | Wasteful ⚠️    |
| 5     | 3      | 2 failures      | High-HA ✅     |

**Rule**: Odd only. Even-count adds cost without improving fault tolerance.

---

## Configuration Example: Joining Node 2

```toml
[cluster]
node_id = 2
listen_address = "0.0.0.0:9082"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Leader, Active
    { id = 2, address = "0.0.0.0:9082", role = 3, status = 0 },  # ← Self: Learner, Joining
]
initial_cluster_size = 1
```

**Key fields**:

- `role = 3`: Learner (non-voting)
- `status = 0`: Joining (new node, syncing)
- `initial_cluster_size = 1`: Matches Node 1 (immutable)

See [Single-Node Expansion Example](../examples/single-node-expansion.md) for full walkthrough.

---

## How It Works

1. **Node 2 sends JoinRequest** → Leader accepts
2. **Leader commits AddNode** config change via Raft
3. **Node 2 syncs** logs from Leader (via AppendEntries + optional Snapshot)
4. **Leader monitors progress**: `match_index ≈ commit_index`?
5. **Auto-promote**: When caught up, Leader promotes to Voter
6. **Node 2 transitions**: Learner → Follower, now voting

**No manual steps needed. Automatic.**

---

## What to Expect in Logs

| Event       | Node 1 (Leader)                              | Node 2 (Learner)                        |
| ----------- | -------------------------------------------- | --------------------------------------- |
| Join starts | -                                            | `Sending JoinRequest to Node 1`         |
| Accepted    | `Node 2 added as learner`                    | `Join successful`                       |
| Syncing     | `Learner progress: node_id=2, match_index=X` | `Syncing logs via AppendEntries`        |
| Caught up   | `Promoting learner 2 to voter (caught up)`   | -                                       |
| Complete    | -                                            | `Transitioned from Learner to Follower` |

---

## Understanding Voter Count

When d-engine calculates cluster size for quorum checks, **the leader always includes itself**:

```
total_voters = membership.voters().len() + 1  (for self/leader)
```

This is crucial for `calculate_safe_batch_size()`: it ensures the final cluster size remains odd after promotion.

**Example**:

- 1 existing voter (peer) + 1 leader (self) = 2 total
- 2 pending promotions available
- `calculate_safe_batch_size(2, 2)` → 1 (because 2+1=3 is odd)
- After promoting 1: 3 total voters ✅

---

## FAQ

**Q: How long does sync take?**  
A: Depends on log size. Usually seconds.

**Q: What if Node 2 crashes during sync?**  
A: No impact on Node 1. Just restart Node 2; it rejoins automatically.

**Q: Can I add multiple nodes at once?**  
A: Yes, add all as Learners. They promote independently when ready.

**Q: Why not skip Learner and go straight to Follower?**  
A: You can, but it risks slowing Node 1 during the sync phase. Not recommended.

---

## Design Principles

- **Safety First**: Learners don't affect cluster quorum → safe to add
- **Automatic**: No manual promotion steps required
- **Observable**: Logs show clear progress (join → sync → promote)
- **Recoverable**: Node failure during sync doesn't block cluster

---
