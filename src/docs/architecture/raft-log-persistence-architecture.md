# Raft Log Persistence Architecture

This document outlines the design and behavior of the Raft log storage engine. It explains how logs are persisted, how the system handles reads and writes under different strategies, and how consistency is guaranteed across different configurations.

---

## Overview

Raft logs record the sequence of operations that must be replicated across all nodes in a Raft cluster. Correct and reliable storage of these logs is essential to maintaining the **linearizability** and **safety** guarantees of the protocol.

Our log engine supports two persistence strategies:

- **DiskFirst**: Prioritizes durability.
- **MemFirst**: Prioritizes performance.

Both strategies support configurable **flush policies** to control how memory contents are persisted to disk.

---

## Persistence Strategies

### DiskFirst

- **Write Path**: On append, entries are first synchronously written to disk. Once confirmed, they are cached in memory.
- **Read Path**: Reads are served from memory. If the requested entry is missing, it is loaded from disk and cached.
- **Startup Behavior**: Does **not** preload all entries from disk into memory. Instead, entries are loaded lazily on access.
- **Durability**: Ensures strong durability. A log is never considered accepted until it is safely written to disk.
- **Memory Use**: Memory acts as a read-through cache for performance optimization.

### MemFirst

- **Write Path**: Entries are first written to memory and acknowledged immediately. Disk persistence is handled **asynchronously** in the background.
- **Read Path**: Reads are served from memory only. If an entry is not present in memory, it is considered nonexistent.
- **Startup Behavior**: Loads **all** log entries from disk into memory during startup.
- **Durability**: Durability is best-effort and depends on the flush policy. Recent entries may be lost if a crash occurs before flushing.
- **Memory Use**: Memory holds the complete working set of logs.

---

## Flush Policies

Flush policies control how and when in-memory data is persisted to disk. These are especially relevant in `MemFirst` mode, but are **also applied in `DiskFirst`** to control how memory state is flushed (e.g., snapshots, metadata, etc).

### Types

- **Immediate**
  - Flush to disk immediately after every log write.
  - Ensures maximum durability, but higher I/O latency.

- **Batch { threshold, interval }**
  - Flush to disk when:
    - The number of unflushed entries exceeds `threshold`, **or**
    - The elapsed time since last flush exceeds `interval` milliseconds.
  - Balances performance and durability.
  - May lose recent entries on crash.

---

## Read & Write Semantics

| Operation          | DiskFirst                       | MemFirst                        |
| ------------------ | ------------------------------- | ------------------------------- |
| Write              | Write to disk → cache in memory | Write to memory → async flush   |
| Read               | From memory; fallback to disk   | Memory only; missing = absent   |
| Startup            | Lazy-loading on access          | Preload all entries into memory |
| Flush              | Controlled via `flush_policy`   | Controlled via `flush_policy`   |
| Data loss on crash | No (after disk fsync)           | Possible if not flushed         |

---

## Consistency Guarantees

| Property                  | DiskFirst   | MemFirst                          |
| ------------------------- | ----------- | --------------------------------- |
| Linearizability           | ✅ (strict) | ✅ (with quorum + sync on commit) |
| Durability (Post-Commit)  | ✅ Always   | ❌ Depends on flush policy        |
| Availability (Under Load) | ❌ Lower    | ✅ Higher                         |
| Crash Recovery            | ✅ Strong   | ❌ Recent entries may be lost     |
| Startup Readiness         | ✅ Fast     | ❌ Slower (full load)             |

---

## Recommended Use Cases

| Strategy  | Best For                                                                                                                                               |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| DiskFirst | Systems that require **strong durability** and **consistent recovery** (e.g., databases, distributed ledgers)                                          |
| MemFirst  | Systems that favor **latency and availability**, and can tolerate recovery from snapshots or re-election (e.g., in-memory caches, ephemeral workloads) |

---

## Developer Notes

- **Log Truncation & Compaction**: Logs should be truncated after snapshotting, regardless of strategy.
- **Backpressure**: In `MemFirst`, developers should implement backpressure if memory usage exceeds thresholds.
- **Lazy Loading**: In `DiskFirst`, avoid head-of-line blocking by prefetching future entries when cache misses occur.
- **Flush Daemon**: Use a background task to monitor and enforce flush policy under `MemFirst`.

---

## Future Improvements

- Snapshot-aware recovery to reduce startup times for `MemFirst`.
- Tiered storage support (e.g., WAL on SSD, archival on HDD or cloud).
- Intelligent adaptive flush control based on workload.

---
