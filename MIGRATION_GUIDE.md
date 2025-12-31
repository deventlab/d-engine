# Migration Guide for d-engine v0.2.0

## 🎯 For New Users

**Starting fresh with v0.2.0?** No migration needed - skip this guide and go to [Quick Start](./examples/quick-start/).

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

| Version | WAL Format          | Migration Required |
| ------- | ------------------- | ------------------ |
| v0.1.x  | Relative TTL        | -                  |
| v0.2.0+ | Absolute expiration | ✅ Yes (clear WAL) |

---

**Version:** d-engine v0.2.0  
**Last Updated:** December 2025
