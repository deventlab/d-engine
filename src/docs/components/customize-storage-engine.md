## Implementing Custom Storage Engines

d-engine supports pluggable storage through the `StorageEngine` trait.
Here's how to implement a custom storage backend:

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

### **3. RocksDB Reference Implementation**

See **`rocksdb_engine.rs`** for a production-ready example:

- Uses write batches for atomic operations
- Implements log compaction
- Handles snapshot storage

### **4. Register with NodeBuilder**

```rust,ignore
NodeBuilder::new(config, shutdown_rx)
    .storage_engine(Arc::new(MyCustomStore::new()))
    .build();
```
