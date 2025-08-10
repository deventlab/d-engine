## **Implementing Custom Storage Engines**

d-engine supports **pluggable storage** through the StorageEngine trait.

This storage engine is responsible for **persisting Raft log entries and latest vote metadata to disk**.

### **Architecture Context**

- The **RaftLog** trait (e.g., implemented by BufferedRaftLog) focuses on **Raft protocol-level log operations** — handling log sequencing, conflict resolution, and in-memory buffering.
- The **StorageEngine** trait is a **disk persistence backend** — it handles the actual reading/writing of log entries and vote metadata data to permanent storage.
- BufferedRaftLog can store logs in memory first (for performance) and flush them asynchronously to a StorageEngine.
- By default you can use **Built-in engines** (e.g., sled engine shipped with d-engine).
- But d-engine also enables you to inject your **Custom implementations** for RocksDB, SQLite, in-memory snapshots, cloud storage, etc.

### 1. Implement the Trait

```rust,ignore
use d_engine::storage::StorageEngine;
use async_trait::async_trait;

struct MyCustomStore;

#[async_trait]
impl StorageEngine for MyCustomStore {
    async fn persist_entries(&self, entries: Vec<Entry>) -> Result<()> {
        // Your implementation
    }

    // Other required methods...
}
```

### **2. Key Implementation Notes**

- **Atomicity**: Ensure write operations are atomic
- **Durability**: Flush writes to persistent storage
- **Consistency**: Maintain exactly-once semantics for log entries
- **Performance**: Batch operations where possible

---

### **3. StorageEngine API Reference**

The following table describes each method in the StorageEngine trait:

| **Method**                           | **Purpose**                                                                            | **Typical Usage in** BufferedRaftLog                                       |
| ------------------------------------ | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| persist_entries(entries: Vec<Entry>) | Persist multiple Raft log entries to disk.                                             | Called when flushing new log entries from memory to disk.                  |
| insert<K, V>(key, value)             | Insert arbitrary key-value data (state machine storage). Returns old value if present. | Used by state machine layer to store application state.                    |
| get<K>(key)                          | Retrieve a value by key from state machine storage.                                    | Reads state for queries or snapshot restoration.                           |
| entry(index)                         | Fetch a single Raft log entry by its index.                                            | Used during Raft log replication and consistency checks.                   |
| get_entries_range(range)             | Fetch a batch of log entries for a given inclusive index range.                        | Used for leader-to-follower log catch-up and restart recovery.             |
| purge_logs(cutoff_index)             | Delete log entries up to and including the specified LogId.                            | Used during log compaction after entries are applied to the state machine. |
| flush()                              | Ensure all pending writes are persisted to disk.                                       | Triggered on commit points or before shutdown.                             |
| reset()                              | Clear all stored logs and state.                                                       | Used during snapshot installation or cluster reinitialization.             |
| last_index()                         | Return the highest persisted log index.                                                | Used during recovery to rebuild in-memory state.                           |
| truncate(from_index)                 | Remove log entries from from_index onward.                                             | Used when conflicts are detected in log replication.                       |
| load_hard_state()                    | Load persisted Raft hard state (term, vote, commit index).                             | Called on node startup.                                                    |
| save_hard_state(hard_state)          | Persist Raft hard state to disk.                                                       | Called when term, vote, or commit index changes.                           |

### **4. RocksDB Reference Implementation**

For a production-grade implementation, see **rocksdb_engine.rs**:

- Uses **write batches** for atomic operations.
- Implements **log compaction** to reclaim disk space.
- Stores **snapshots** efficiently.

---

### **5. Register with NodeBuilder**

```rust,ignore
NodeBuilder::new(config, shutdown_rx)
    .storage_engine(Arc::new(MyCustomStore::new()))
    .build();
```

**6. Relationship Between RaftLog and StorageEngine**

| **Component**     | **Responsibility**                                                                                                                 | **Example**                 |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| **RaftLog**       | Protocol-level log management — sequencing, conflict resolution, matching terms.                                                   | BufferedRaftLog             |
| **StorageEngine** | Physical persistence of logs and state machine data.                                                                               | sled engine, RocksDB engine |
| **Integration**   | BufferedRaftLog maintains in-memory log entries and flushes them to the StorageEngine according to the chosen PersistenceStrategy. | MemFirst or DiskFirst modes |
