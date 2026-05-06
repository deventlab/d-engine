# Snapshot Guarantees

This guide covers what d-engine guarantees about snapshot behavior, known limitations,
and configuration recommendations for production deployments.

## Guarantees

**Write operations are never blocked by snapshot generation.**
Snapshot creation runs in a background `tokio::spawn` task, isolating compression I/O
from the Raft event loop. Writes continue uninterrupted while a snapshot is being
compressed and written to disk.

**Committed data is never lost during interrupted transfers.**
If a snapshot transfer is interrupted mid-way (network drop, leader crash, receiver restart),
the stale temporary file is truncated on the next attempt — not appended to — and the
transfer restarts cleanly. The leader retries automatically until the follower catches up.

**Snapshot files are written atomically.**
The receiver assembles chunks into a temporary file (`temp-snapshot.part.tar.gz`) and
performs an atomic rename to the final path only after all chunks pass checksum validation.
A reader never observes a partially written snapshot file.

## Limitations

**P99 write latency may increase during snapshot generation.**
Compressing the state machine to disk is CPU-bound. Under high write throughput,
expect a transient P99 spike of 5–20ms during the compression window.

**Interrupted transfers restart from the beginning.**
d-engine does not support resuming a partial snapshot transfer. If a transfer is
interrupted after transferring 90% of a large snapshot, the next attempt retransfers
from chunk 0. For snapshots exceeding ~500 MB, ensure stable network conditions.

**No cross-datacenter snapshot optimization.**
Differential or incremental snapshot transfer is out of scope. Each transfer is a
full snapshot.

## Operational Boundaries

### When snapshots trigger

A snapshot is triggered when the number of unapplied log entries exceeds
`max_log_entries_before_snapshot`. After the snapshot is created, log entries older
than `retained_log_entries` before the snapshot index are purged.

Any follower whose `next_index` falls below the purge boundary will receive a full
snapshot instead of log entries.

### Chunk timeout

`receive_chunk_timeout_in_sec` (default: 30s) applies per-chunk on the receiver side.
For slow networks or chunks larger than the default 1 MB, increase this value:

```toml
[raft.snapshot]
receive_chunk_timeout_in_sec = 60
```

If this timeout fires, the receiver aborts the current transfer and the leader retries.

### Snapshot retention

`cleanup_retain_count` (default: 2) controls how many past snapshot files are kept on
disk after a new one is created. Keep at least 2 for rollback and debugging headroom.

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| `max_log_entries_before_snapshot` | 10000 | Log entries before snapshot triggers |
| `retained_log_entries` | 1000 | Log entries to retain after snapshot |
| `chunk_size` | 1 MB | Size of each transfer chunk in bytes |
| `receive_chunk_timeout_in_sec` | 30 | Per-chunk receive timeout on follower |
| `transfer_timeout_in_sec` | 600 | Overall transfer timeout |
| `max_bandwidth_mbps` | 0 (unlimited) | Transfer bandwidth cap |
| `cleanup_retain_count` | 2 | Number of past snapshot files to keep |

## Further Reading

- Consistency model: [`consistency_tuning`](crate::docs::server_guide::consistency_tuning)
