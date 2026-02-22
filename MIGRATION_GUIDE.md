# Migration Guide for d-engine

## 🎯 For New Users

**Starting fresh with the latest version?** No migration needed - skip this guide and go to [Quick Start](./examples/quick-start/).

---

## 🚨 For v0.1.x Users: WAL Format Change

### What Changed

Starting from **v0.2.0**, the WAL (Write-Ahead Log) format for file-based state machines has changed to support **absolute expiration time semantics**.

**Old Format (pre-v0.2.0):**

```
Entry fields: ..., ttl_secs: u32 (4 bytes, relative TTL)
```

**New Format (v0.2.0+):**

```
Entry fields: ..., expire_at_secs: u64 (8 bytes, absolute expiration time in UNIX seconds)
```

### Why This Change?

- **Crash Safety**: Absolute expiration time ensures TTL correctness across restarts
- **Deterministic Semantics**: Matches industry-standard lease semantics (absolute expiry)
- **No TTL Reset**: TTL no longer resets on node restart

### Impact

⚠️ **WAL files from pre-v0.2.0 are NOT compatible with v0.2.0+**

- Reading old WAL files will cause deserialization errors
- Node startup will fail if old WAL files are present

---

## Migration Strategies

### Option 1: Clean Start (Recommended for Development)

**Best for**: Development, testing, or non-production environments

1. **Backup your data** (optional, if you need to preserve state)
2. **Stop the node** gracefully
3. **Delete old WAL directory**:
   ```bash
   rm -rf /path/to/storage/wal/*
   ```
4. **Start with v0.2.0**

⚠️ **Warning**: This will lose all uncommitted/unreplicated data in the WAL.

---

### Option 2: Rolling Upgrade (Production Cluster)

**Best for**: Production clusters with replication (3+ nodes)

Since d-engine uses Raft consensus, you can perform a rolling upgrade:

1. **Ensure cluster is healthy** (all nodes synchronized)
2. **For each node**:
   - Stop the node gracefully (ensure data is persisted)
   - Upgrade to v0.2.0
   - Clear WAL directory: `rm -rf /path/to/storage/wal/*`
   - Start the node (it will catch up from other nodes)
3. **Repeat** for all nodes one by one

The cluster will remain available during the upgrade (assuming you have 3+ nodes).

---

### Option 3: Snapshot-based Migration

**Best for**: Large WAL files or single-node setups

1. **On old version (pre-v0.2.0)**:
   - Trigger a snapshot to persist current state
   - Wait for snapshot to complete
   - Verify snapshot file exists: `/path/to/storage/snapshots/`
2. **Upgrade to v0.2.0**
3. **Clear WAL**: `rm -rf /path/to/storage/wal/*`
4. **Start node** - it will restore from the snapshot

---

## Verification After Migration

After upgrading, verify:

```bash
# Check node starts without errors
journalctl -u d-engine -f

# Verify TTL entries expire correctly
# (create a key with TTL and wait for expiration)

# Check logs for WAL-related errors
grep "WAL" /var/log/d-engine.log
```

---

## TTL Behavior Changes

| Aspect               | Old (pre-v0.2.0)            | New (v0.2.0+)              |
| -------------------- | --------------------------- | -------------------------- |
| TTL Storage          | Relative (seconds from now) | Absolute (UNIX timestamp)  |
| After Restart        | TTL resets 🔄               | TTL preserved ✅           |
| WAL Replay           | All entries loaded          | Expired entries skipped ✅ |
| Expiration Semantics | Relative TTL                | Absolute timestamp ✅      |
| Crash Safe           | ❌ No                       | ✅ Yes                     |

---

## Need Help?

- **Documentation**: See [examples/](./examples/) for updated usage patterns
- **Issues**: Report migration issues on [GitHub Issues](https://github.com/deventlab/d-engine/issues)
- **Discussion**: Ask questions in [GitHub Discussions](https://github.com/deventlab/d-engine/discussions)

---

## Timeline

| Version | WAL Format          | Wire Protocol       | Migration Required                           |
| ------- | ------------------- | ------------------- | -------------------------------------------- |
| v0.1.x  | Relative TTL        | Compatible          | -                                            |
| v0.2.0+ | Absolute expiration | Compatible (v0.2.x) | ✅ Yes (clear WAL from v0.1.x)               |
| v0.2.3  | Same as v0.2.0+     | **Incompatible**    | ✅ Yes (protobuf enum changes + API changes) |

---

## 🚨 For v0.2.2 Users: Protobuf Enum Breaking Changes in v0.2.3

### What Changed

v0.2.3 introduces **breaking wire protocol changes** due to protobuf enum value shifts to comply with buf lint standards.

### ⚠️ Critical Impact

**Wire Protocol Incompatibility:**

- v0.2.3 nodes **CANNOT communicate** with v0.2.2 or earlier nodes
- No rolling upgrade possible - all cluster nodes must upgrade simultaneously
- Client SDKs must be upgraded to v0.2.3 to connect to upgraded clusters

### Enum Value Changes

#### NodeRole Enum

| Role      | Old Value | New Value | New Constant            |
| --------- | --------- | --------- | ----------------------- |
| -         | -         | 0         | `NODE_ROLE_UNSPECIFIED` |
| Follower  | 0         | 1         | `NODE_ROLE_FOLLOWER`    |
| Candidate | 1         | 2         | `NODE_ROLE_CANDIDATE`   |
| Leader    | 2         | 3         | `NODE_ROLE_LEADER`      |
| Learner   | 3         | 4         | `NODE_ROLE_LEARNER`     |

#### NodeStatus Enum

| Status     | Old Value | New Value | New Constant              |
| ---------- | --------- | --------- | ------------------------- |
| -          | -         | 0         | `NODE_STATUS_UNSPECIFIED` |
| Promotable | 0         | 1         | `NODE_STATUS_PROMOTABLE`  |
| ReadOnly   | 1         | 2         | `NODE_STATUS_READ_ONLY`   |
| Active     | 2         | 3         | `NODE_STATUS_ACTIVE`      |

#### ErrorCode Enum

| Error                  | Old Value | New Value | New Constant             |
| ---------------------- | --------- | --------- | ------------------------ |
| -                      | -         | 0         | `ERROR_CODE_UNSPECIFIED` |
| NotLeader              | 1         | 1         | `ERROR_CODE_NOT_LEADER`  |
| ... (others unchanged) |           |           |                          |

### Additional Protobuf Changes

- **Enum Prefixes**: All enum values now have proper prefixes (`NODE_ROLE_*`, `NODE_STATUS_*`, `ERROR_CODE_*`)
- **Field Naming**: All fields now use snake_case naming (`leader_id`, `prev_log_index`, `last_log_index`, etc.)

---

## Migration Steps for v0.2.2 → v0.2.3 Protobuf Changes

### Step 1: Update Configuration Files

Update any TOML configuration files that reference enum values:

**Old (v0.2.2):**

```toml
[cluster]
node_id = 1
role = 0        # Follower
status = 0      # Promotable
```

**New (v0.2.3):**

```toml
[cluster]
node_id = 1
role = 1        # NODE_ROLE_FOLLOWER
status = 1      # NODE_STATUS_PROMOTABLE
```

### Step 2: Upgrade All Cluster Nodes Simultaneously

**⚠️ No Rolling Upgrade Possible**

Since the wire protocol is incompatible, you must upgrade all nodes at once:

1. **Schedule maintenance window** (cluster will be unavailable during upgrade)
2. **Stop all nodes** in the cluster
3. **Upgrade binaries** to v0.2.3 on all nodes
4. **Update configuration files** (see Step 1)
5. **Start all nodes** simultaneously
6. **Verify cluster health** (check logs, run health checks)

**For Production Clusters:**

If you require high availability during upgrade:

1. **Set up a parallel v0.2.3 cluster** (new hardware/instances)
2. **Migrate data** to the new cluster (application-level migration)
3. **Switch traffic** to new cluster
4. **Decommission old cluster**

### Step 3: Upgrade Client SDKs

All client applications must upgrade their d-engine SDK to v0.2.3:

**Cargo.toml:**

```toml
[dependencies]
d-engine = { version = "0.2.3", features = ["client"] }
```

**Rebuild and redeploy** all client applications before connecting to upgraded cluster.

### Step 4: Verification

After upgrade, verify:

```bash
# Check all nodes started successfully
journalctl -u d-engine -f

# Verify cluster health
curl http://localhost:8080/health

# Test basic operations
d-engine-cli put test-key test-value
d-engine-cli get test-key
```

---

## 🚨 For v0.2.2 Users: API Changes in v0.2.3

### What Changed

v0.2.3 introduces **breaking API changes** to unify client interfaces and improve developer experience.

### Breaking Changes

#### 1. Unified Client API Trait

**Old (v0.2.2):**

```rust
use d_engine::client::KvClient;
use d_engine::client::KvError;

async fn example(client: impl KvClient) -> Result<(), KvError> {
    // ...
}
```

**New (v0.2.3):**

```rust
use d_engine::client::ClientApi;
use d_engine::client::ClientApiError;

async fn example(client: impl ClientApi) -> Result<(), ClientApiError> {
    // ...
}
```

**Migration Steps:**

- Replace `KvClient` with `ClientApi` in trait bounds
- Replace `KvError` with `ClientApiError` in error handling
- Update imports: `use d_engine::client::{ClientApi, ClientApiError};`

---

#### 2. Default Persistence Strategy

**Old (v0.2.2):** Default = `MemFirst` (write to memory, async flush to disk)

**New (v0.2.3):** Default = `DiskFirst` (Raft protocol compliance)

**Migration:**

If you want to restore v0.2.2 behavior, add to config:

```toml
[raft.persistence]
persistence_strategy = "MemFirst"
```

⚠️ **Warning:** `MemFirst` trades durability for performance. Only use in scenarios where data loss is acceptable.

---

### Non-Breaking Changes

- **CompareAndSwap (CAS)**: New atomic operation added
- **Drain-based batching**: Performance improvements (no API changes)
- **Client::refresh()**: New method for leader rediscovery

---

**Last Updated:** February 2026
