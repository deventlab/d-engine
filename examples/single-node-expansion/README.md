# Single-Node Dynamic Expansion Example

This example demonstrates **dynamic cluster expansion** from a single-node cluster to a three-node cluster without downtime, testing the implementation of [Issue #179](https://github.com/your-repo/d-engine/issues/179).

## Overview

This test validates the following workflow:

1. **Phase 1**: Start a single node that bootstraps as leader (no election)
2. **Phase 2**: Dynamically add a second node (Learner → Voter promotion)
3. **Phase 3**: Dynamically add a third node (Learner → Voter promotion)

The entire process happens **without restarting the original node** and **without data loss**.

## Architecture

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

## Key Technical Concepts

### Single-Node Bootstrap

- Node 1 starts with `initial_cluster_size = 1`
- Automatically becomes leader **without election**
- Can accept writes immediately
- Designed for development or budget-constrained deployments

### Dynamic Node Join

- New nodes (2 and 3) start with `role = 3` (Learner), `status = 0` (Joining)
- Learner discovery flow:
  1. Learner discovers leader (via `initial_cluster` config or broadcast)
  2. Learner sends `JoinRequest` to leader via gRPC
  3. Leader commits `AddNode` configuration change via Raft
  4. Learner syncs data via `AppendEntries` RPC (raft log replication)
  5. Learner may fetch snapshot (state machine state) if needed
  6. Leader monitors learner's `match_index` progress
  7. When `match_index` catches up to `commit_index`, leader auto-promotes learner to Voter

### Snapshot vs Log Replication

- **Raft Log Replication**: New nodes sync via `AppendEntries` RPC
- **Snapshot Transfer**: For state machine data (NOT raft log)
  - Helps new nodes catch up without replaying all historical log entries
  - Transferred via streaming RPC
  - Applied to state machine, not raft log

### Learner Promotion

- Automatic promotion triggered by leader when learner catches up
- Promotion requires committing a `PromoteToVoter` configuration change
- After promotion, the node participates in voting and leader election

## Configuration Files

### n1.toml - Single Node Bootstrap

```toml
[cluster]
node_id = 1
listen_address = "0.0.0.0:9081"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Self as Leader
]
initial_cluster_size = 1  # KEY: Triggers single-node mode
```

**Key Points**:
- `initial_cluster_size = 1` is **IMMUTABLE** and triggers single-node bootstrap
- `role = 2` (Leader) - node expects to become leader immediately
- `status = 2` (Active) - node is active from the start

### n2.toml - Joining as Learner

```toml
[cluster]
node_id = 2
listen_address = "0.0.0.0:9082"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Existing leader
    { id = 2, address = "0.0.0.0:9082", role = 3, status = 0 },  # Self: Learner, Joining
]
initial_cluster_size = 1  # Must match Node 1
```

**Key Points**:
- Lists ALL nodes (existing + self)
- Self has `role = 3` (Learner), `status = 0` (Joining)
- `initial_cluster_size = 1` matches Node 1 (immutable)

### n3.toml - Joining After Node 2

```toml
[cluster]
node_id = 3
listen_address = "0.0.0.0:9083"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 2, status = 2 },  # Leader
    { id = 2, address = "0.0.0.0:9082", role = 1, status = 2 },  # Promoted voter
    { id = 3, address = "0.0.0.0:9083", role = 3, status = 0 },  # Self: Learner
]
initial_cluster_size = 1  # Must match Node 1
```

**Note**: Node 2 is listed as `role = 1` (Follower) assuming it's already promoted.

## Testing Instructions

### Prerequisites

```bash
# Ensure you have Rust and cargo installed
rustc --version

# Navigate to example directory
cd examples/single-node-expansion
```

### Build

```bash
make build
```

### Testing Steps

#### Terminal 1: Start Node 1 (Single-Node Bootstrap)

```bash
make start-node1
```

**Expected logs**:
```
Single-node mode detected (initial_cluster_size=1)
Node 1 transitioning directly to Leader role
Leader state initialized successfully
```

**What to observe**:
- Node 1 becomes leader immediately (no election)
- Can accept writes immediately

#### Terminal 2: Join Node 2

Wait for Node 1 to be fully initialized, then:

```bash
make join-node2
```

**Expected logs**:

Node 2:
```
Learner state initialized
Discovering leader from initial_cluster
Sending JoinRequest to Node 1 (0.0.0.0:9081)
Join successful, snapshot_metadata: ...
Syncing raft logs via AppendEntries
```

Node 1:
```
Received JoinRequest from Node 2
Committing AddNode configuration change
Node 2 (0.0.0.0:9082) successfully added as learner
Learner progress: node_id=2, match_index=X, commit_index=Y
Promoting learner 2 to voter (caught up)
```

Node 2:
```
Received promotion to Voter
Transitioned from Learner to Follower
```

**What to observe**:
- Node 2 joins as Learner
- Node 2 syncs data via AppendEntries
- Node 1 monitors Node 2's progress
- Node 2 auto-promotes to Voter when caught up

#### Terminal 3: Join Node 3

Wait for Node 2 to be promoted, then:

```bash
make join-node3
```

**Expected logs**: Similar to Node 2's join flow.

**What to observe**:
- Node 3 joins as Learner
- Configuration change now requires 2/3 quorum (Node 1 + Node 2)
- Node 3 syncs and promotes to Voter

### Log Monitoring

In additional terminal windows, you can monitor logs:

```bash
# Watch Node 1 logs
make tail-node1

# Watch Node 2 logs
make tail-node2

# Watch Node 3 logs
make tail-node3
```

### Cleanup

```bash
# Clean all build artifacts and data
make clean

# Or just clean logs and database
make clean-logs
```

## Key Log Messages to Watch

### Successful Join Flow

| Stage | Node | Log Message |
|-------|------|-------------|
| 1. Join Request | Node 2/3 | `Sending JoinRequest to Node 1` |
| 2. Leader Accepts | Node 1 | `Node X successfully added as learner` |
| 3. Config Change | Node 1 | `Committing AddNode configuration change` |
| 4. Learner Syncing | Node 2/3 | `Syncing raft logs via AppendEntries` |
| 5. Leader Monitors | Node 1 | `Learner progress: node_id=X, match_index=Y` |
| 6. Promotion Triggered | Node 1 | `Promoting learner X to voter` |
| 7. Learner Promoted | Node 2/3 | `Transitioned from Learner to Follower` |

### Error Scenarios

| Error | Possible Cause | Solution |
|-------|----------------|----------|
| `NoLeader` | Node 1 not started | Start Node 1 first |
| `NodeAlreadyExists` | Duplicate node_id | Use unique node_ids |
| `CommitTimeout` | Quorum not available | Ensure majority of voters are running |
| `JoinClusterFailed` | Network issue | Check addresses in config |

## Testing Checklist

- [ ] Node 1 starts as leader without election
- [ ] Node 1 can accept writes before any joins
- [ ] Node 2 successfully joins as Learner
- [ ] Node 2 syncs data from Node 1
- [ ] Node 2 auto-promotes to Voter
- [ ] Node 3 successfully joins as Learner
- [ ] Config change for Node 3 requires 2/3 quorum
- [ ] Node 3 auto-promotes to Voter
- [ ] Final cluster has 3 active voters
- [ ] Original Node 1 never restarted
- [ ] No data loss during expansion

## Troubleshooting

### Node 2/3 Cannot Find Leader

**Symptom**: `NoLeader` error in logs

**Solution**:
1. Verify Node 1 is running: `ps aux | grep demo`
2. Check Node 1 logs for "Leader state initialized"
3. Verify addresses in config files match

### Join Request Times Out

**Symptom**: `CommitTimeout` or `JoinClusterFailed`

**Solution**:
1. Check network connectivity: `curl http://0.0.0.0:9081` (should fail but proves port is open)
2. Verify `initial_cluster` in joining node's config lists correct leader address
3. Check Node 1 logs for error messages

### Learner Never Promotes

**Symptom**: Node stays in Learner role indefinitely

**Solution**:
1. Check Node 1 logs for "Learner progress" messages
2. Verify `match_index` is increasing (data is syncing)
3. Check for errors in AppendEntries replication
4. Verify `learner_catchup_threshold` is reasonable (default)

### Promotion Requires Quorum But Node 2 Not Voting Yet

**Symptom**: Node 3 join fails because only 1 voter exists

**Solution**:
1. Wait for Node 2 to promote to Voter first
2. Check Node 2's role: should see "Transitioned from Learner to Follower"
3. Retry Node 3 join after Node 2 is promoted

## Advanced Topics

### Manual Testing Without Makefile

**Node 1**:
```bash
CONFIG_PATH=config/n1 \
DB_PATH="./db/1" \
LOG_DIR="./logs/1" \
METRICS_PORT=8081 \
RUST_LOG=demo=debug,d_engine=debug \
RUST_BACKTRACE=1 \
./target/release/demo
```

**Node 2**:
```bash
CONFIG_PATH=config/n2 \
DB_PATH="./db/2" \
LOG_DIR="./logs/2" \
METRICS_PORT=8082 \
RUST_LOG=demo=debug,d_engine=debug \
RUST_BACKTRACE=1 \
./target/release/demo
```

### Testing with Writes During Expansion

You can send write requests to Node 1 while nodes are joining to verify:
- Leader continues accepting writes
- New nodes sync all committed entries
- No data loss during expansion

### Testing Failure Scenarios

1. **Kill Node 1 during Node 2 join**: Node 2 should handle gracefully
2. **Kill Node 2 during sync**: Should be able to restart and rejoin
3. **Network partition**: Test split-brain prevention

## References

- [Issue #179: Single-node cluster support](https://github.com/your-repo/d-engine/issues/179)
- [Raft Paper: Section 6 - Cluster Membership Changes](https://raft.github.io/raft.pdf)
- [etcd Learner Documentation](https://etcd.io/docs/v3.5/learning/design-learner/)
- [openraft Membership Changes](https://docs.rs/openraft/latest/openraft/docs/cluster_control/index.html)

## Next Steps

After successful testing, consider:

1. **Performance Testing**: Measure join latency and sync throughput
2. **Failure Testing**: Test node failures during expansion
3. **Multi-Stage Expansion**: Test adding more than 3 nodes
4. **Snapshot Testing**: Enable snapshots and test join with large state
5. **Production Validation**: Test with real workloads

## Contributing

If you find issues during testing:

1. Check logs in `logs/1/`, `logs/2/`, `logs/3/`
2. Open an issue with:
   - Configuration files used
   - Full log output
   - Steps to reproduce
3. Link to [Issue #179](https://github.com/your-repo/d-engine/issues/179)
