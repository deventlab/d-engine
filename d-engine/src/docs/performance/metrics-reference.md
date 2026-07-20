# d-engine Metrics Reference

Complete reference for the Prometheus-compatible metrics d-engine emits via the
[`metrics`](https://docs.rs/metrics) crate. See
[throughput-optimization-guide.md](./throughput-optimization-guide.md) for how
to install a recorder and general tuning advice — this document is the metric
catalog.

All metrics are always-on: they are simple atomic `Histogram`/`Gauge`/`Counter`
observations (nanosecond-scale), with no runtime toggle. There is no
d-engine-side on/off switch — installing a recorder is what turns them on.

Names are namespaced by layer: `core.*` metrics come from `d-engine-core`
(pure Raft protocol logic, storage/transport-agnostic). `server.*` metrics
come from `d-engine-server`'s concrete adaptors (gRPC transport, storage
engines) — where a metric is specific to one swappable storage engine
implementation, the engine name appears in the path (e.g. `server.storage.
rocksdb.*` vs `server.storage.file.*`).

---

## Write Pipeline Core

| Metric | Type | Normal Range | Answers |
|---|---|---|---|
| `core.raft.buffer.length{buffer}` | Gauge | Near 0, draining between ticks | Is the propose/linearizable/lease/eventual buffer backlogged? |
| `core.raft.fsync.duration_ms` | Histogram | p99 low single-digit ms on SSD | How long does one physical fsync take? |
| `core.raft.fsync.batch_entries` | Histogram | Grows with write concurrency | Is FsyncCoordinator coalescing concurrent writes? |
| `core.raft.fsync.inflight` | Gauge (0/1) | — | Is a fsync task running right now? |
| `core.raft.fsync.busy_nanos_total` | Counter | `rate(...)/1e9` should stay < 0.7 | fsync thread utilization |
| `server.storage.rocksdb.wal_flush_ms` | Histogram | p99 low single-digit ms | State machine's own RocksDB WAL flush duration (a separate DB from the Raft log — do not conflate with `fsync.duration_ms`). Only emitted when the `rocksdb` storage adaptor is active. |
| `server.storage.file.flush_ms` | Histogram | p99 low single-digit ms | Durability sync duration (`flush()` + `sync_all()`) for the default `file` storage adaptor. Only emitted when the `file` adaptor is active — no WAL concept, so this is the direct equivalent of `wal_flush_ms`. |
| `core.state_machine.apply_chunk.duration_ms` | Histogram | p99 low single-digit ms | How long does one apply_chunk call take? |
| `core.state_machine.apply_chunk.batch_size` | Histogram | Grows with write concurrency | Entries applied per chunk |
| `core.state_machine.apply_chunk.count` | Counter | — | Total apply_chunk invocations |
| `core.state_machine.apply_chunk.success` | Counter | ≈ `.count` | Successful applies |
| `core.state_machine.apply_chunk.error{error_type}` | Counter | 0 | Failed applies, classified by error type |
| `core.state_machine.apply.busy_nanos_total` | Counter | `rate(...)/1e9` should stay < 0.7 | SM apply thread utilization |
| `core.raft.commit_index` | Gauge | Monotonically increasing | Highest log index this node has committed |
| `core.raft.apply_index` | Gauge | Tracks `commit_index` closely | Highest log index this node has applied |

## Write Latency Breakdown (leader-only)

| Metric | Type | Normal Range | Answers |
|---|---|---|---|
| `core.raft.write.propose_to_commit_ms` | Histogram | Low single-digit ms | Client write → Raft commit |
| `core.raft.write.commit_to_apply_ms` | Histogram | Should stay well below `propose_to_commit_ms` | Raft commit → state machine apply |
| `core.raft.write.propose_to_apply_ms` | Histogram | Sum of the two above | End-to-end write latency (what the client experiences) |

### How the segments add up

For a single write request:

```
propose_to_commit_ms + commit_to_apply_ms = propose_to_apply_ms
```

This is an exact per-request identity — the three timestamps are recorded
against the same log index. **Percentiles do not add**: `p99(propose_to_commit)
+ p99(commit_to_apply)` will not generally equal `p99(propose_to_apply)`,
because the slowest 1% of requests in each stage aren't necessarily the same
requests. If you need to verify the identity, compare it per-request or via
the mean, not by summing percentiles.

If `commit_to_apply_ms` approaches or exceeds `propose_to_commit_ms`, state
machine apply — not replication — is the bottleneck. Use `commit_index -
apply_index` (below) to confirm whether apply is merely slow per-call or
genuinely falling behind.

### Detecting sustained apply backlog

`commit_to_apply_ms` measures single-call latency; it does not show a backlog
that grows over time. For that, compare the two index gauges directly:

```promql
core_raft_commit_index - core_raft_apply_index
```

A gap that stays near 0 means apply is keeping up. A gap that grows without
bound means apply throughput cannot keep pace with commit throughput — this is
a distinct failure mode from "apply is slow" and needs to be diagnosed
separately (check `apply_chunk.duration_ms` and `apply.busy_nanos_total`
together).

## Replication

| Metric | Type | Normal Range | Answers |
|---|---|---|---|
| `server.raft.replicate.rtt_ms{peer}` | Histogram | Should track your network's baseline RTT | AppendEntries round-trip time to a specific peer |
| `core.raft.snapshot.push_consecutive_failures` | Counter | 0 | Consecutive snapshot push failures to a peer |

## Cluster Health & Guardrails

| Metric | Type | Normal Range | Answers |
|---|---|---|---|
| `core.raft.backpressure.rejections{node_id,type}` | Counter | 0 | Requests rejected due to backpressure (write/read) |
| `core.membership.stale_learner_removed` | Counter | 0 | Learners auto-removed for falling too far behind |
| `core.cluster.unsafe_join_attempts` | Counter | 0 | Join requests rejected because they would create an even-voter cluster |

---

## Metric Granularity

d-engine's metrics operate at two different granularities. Comparing across
them directly leads to wrong conclusions:

| Granularity | Metrics |
|---|---|
| Per fsync/apply batch (may cover many requests) | `fsync.duration_ms`, `fsync.batch_entries`, `apply_chunk.duration_ms`, `apply_chunk.batch_size` |
| Per single client request | `write.propose_to_commit_ms`, `write.commit_to_apply_ms`, `write.propose_to_apply_ms` |

Example: `fsync.duration_ms` p99 of 5ms does not mean "any given
`propose_to_commit_ms` sample near 5ms is explained by that fsync" — one fsync
batch commonly covers dozens of concurrent write requests, each contributing
one `propose_to_commit_ms` sample. Use the batch-level metrics to judge
whether the storage layer itself is efficient, and the per-request metrics to
judge what an individual client actually experiences.

## Companion Metrics

When one of these is abnormal, check the paired metric before concluding
where the bottleneck is:

- `fsync.duration_ms` high → check `fsync.batch_entries` (is coalescing
  working?) and `fsync.inflight` (is more than one fsync racing to run — see
  `throughput-optimization-guide.md` for why that regressed throughput before)
- `apply_chunk.duration_ms` high → check `commit_index - apply_index` (single
  slow call vs. genuine sustained backlog are different problems)
- `propose_to_apply_ms` high, but `fsync.busy_nanos_total` and
  `apply.busy_nanos_total` rates are both low → the bottleneck is outside the
  storage pipeline; check `replicate.rtt_ms` and client-side/network factors
