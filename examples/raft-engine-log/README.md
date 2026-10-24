# raft-engine-log

A 3-node embedded d-engine cluster whose Raft log is stored in
[raft-engine](https://github.com/tikv/raft-engine) instead of the RocksDB WAL.
Raft consensus and the RocksDB state machine are unchanged; only the log store
and the hard-state store are replaced.

**Status: experimental.** The storage adapter is covered by unit tests and a
3-node smoke run, but it has not been benchmarked on the target disk yet. It is
not a production recommendation.

## Why

Since the log is fsynced before a write is acknowledged, the cost of one
`fdatasync` dominates commit latency on network disks. raft-engine writes into
preallocated segment files and overwrites them, so a sync does not have to
commit file-length metadata the way appending to a growing file does. This
example measures whether that helps.

## Run

```sh
make start-cluster          # builds, then starts 3 nodes
```

| Node | API port | Health port | Data dir |
| ---- | -------- | ----------- | -------- |
| 1    | 8081     | 10001       | `db/1`   |
| 2    | 8082     | 10002       | `db/2`   |
| 3    | 8083     | 10003       | `db/3`   |

Write through the leader (`GET /primary` on a health port returns 200 on the
leader), read from any node:

```sh
curl -X POST localhost:8083/kv -H 'content-type: application/json' \
     -d '{"key":"k1","value":"v1"}'
curl localhost:8081/kv/k1
```

## Configuration

Log-store settings go in an optional `[raft_engine]` table of the same node
config file you pass to d-engine (`--config-path`). d-engine ignores the table;
the adapter reads it. Every key is optional.

```toml
[raft_engine]
target_file_size_mib = 64    # size at which the active log file is rotated
purge_threshold_mib = 512    # reclaim log files once the retained log exceeds this
prefill_limit_mib = 256      # zero-filled recycle pool prepared at startup
purge_interval_secs = 10     # how often expired log files are reclaimed
```

- A misspelled key, a zero value, or `purge_threshold_mib` below
  `target_file_size_mib` makes startup fail with a message naming the problem.
- The recycle pool holds `prefill_limit_mib / target_file_size_mib` files. A
  write into a recycled file overwrites blocks that are already allocated,
  which is what avoids the extra metadata commit on sync. The first log file is
  created fresh and does not have that property, so a run shorter than one
  segment never reaches a recycled file; use a small `target_file_size_mib`
  when benchmarking.
- Embedded code reads the table with `RaftEngineOptions::from_toml_file(path)`
  and opens the store with `RaftEngineStorage::open_with(dir, &options)`.

## Test

```sh
cargo nextest run --all-features --nocapture
```

The tests include the shared `StorageEngine` contract suite from
`d_engine::storage_engine_test`, plus restart recovery, tail replacement and
truncation, `reset` keeping the metadata, purge-boundary and hard-state
persistence, and purge-thread shutdown.

## Behavior to know

- raft-engine has no suffix-truncate command, so `truncate` re-appends the entry
  just before the cut, which drops everything after it in one atomic write.
  `replace_range` overwrites the tail the same way.
- Log entries and the metadata (hard state, purge boundary) live in separate
  raft-engine regions, so `reset` clears the log without touching the metadata.
- `persist_entries` and `save_hard_state` write without syncing. `flush()` runs
  `fdatasync`, matching the RocksDB adapter's drain-then-fsync model, so
  `durable_index` advances only after the physical sync.
- Entries are stored as d-engine's own encoded `Entry` bytes (see
  `src/entry_record.rs`), with no wrapper message.
- A background thread reclaims log files every 10 seconds. raft-engine never
  deletes files on its own.
- Each node preallocates `prefill_limit_mib` of zero-filled log files at startup
  (256 MiB by default; about 380 MB on disk with the default segment size).

## Not covered yet

- Tail corruption after a crash.
- Single-node failure and leader change under write load.
- Latency measurements on the target disk.
