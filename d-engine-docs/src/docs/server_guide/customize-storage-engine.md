# Implementing Custom Storage Engines

d-engine supports **pluggable storage** through the `StorageEngine` trait. This storage engine is responsible for **persisting Raft log entries and metadata to durable storage**.

## Architecture Context

- The **StorageEngine** trait provides a **generic interface** for storage backends
- **Physical separation** between log storage (`LogStore`) and metadata storage (`MetaStore`)
- **Built-in implementations** include:
  - **File-based**: Production-ready file system storage
  - **RocksDB**: High-performance embedded database (requires `features = ["rocksdb"]`)
  - **Sled**: Modern embedded database
  - **In-memory**: Volatile storage for testing
- Custom implementations enable support for cloud storage, SQL databases, etc.

## 1. Implement the Trait

```rust,ignore
use d_engine::storage::{StorageEngine, LogStore, MetaStore};
use async_trait::async_trait;

struct CustomLogStore;
struct CustomMetaStore;

#[async_trait]
impl LogStore for CustomLogStore {
    async fn persist_entries(&self, entries: Vec<Entry>) -> Result<(), Error> {
        // Your implementation - batch operations recommended
        Ok(())
    }

    async fn entry(&self, index: u64) -> Result<Option<Entry>, Error> {
        // Retrieve single entry
        Ok(None)
    }

    // Implement all required methods...
}

impl MetaStore for CustomMetaStore {
    fn save_hard_state(&self, state: &HardState) -> Result<(), Error> {
        // Persist hard state atomically
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        // Load persisted hard state
        Ok(None)
    }
}

// Combine log and metadata stores
struct CustomStorageEngine {
    log_store: Arc<CustomLogStore>,
    meta_store: Arc<CustomMetaStore>,
}

impl StorageEngine for CustomStorageEngine {
    type LogStore = CustomLogStore;
    type MetaStore = CustomMetaStore;

    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

```

## 2. Key Implementation Notes

- **Atomicity**: Ensure write operations are atomic—use batch operations where possible
- **Durability**: Flush writes to persistent storage—implement `flush()` properly
- **Consistency**: Maintain exactly-once semantics for log entries
- **Performance**: Target >100k ops/sec for log persistence
- **Resource Management**: Clean up resources in `Drop` implementation

## 3. StorageEngine API Reference

### LogStore Methods

| Method              | Purpose                     | Performance Target     |
| ------------------- | --------------------------- | ---------------------- |
| `persist_entries()` | Batch persist log entries   | >100k entries/sec      |
| `entry()`           | Get single entry by index   | <1ms latency           |
| `get_entries()`     | Get entries in range        | <1ms for 10k entries   |
| `purge()`           | Remove logs up to index     | <100ms for 10k entries |
| `truncate()`        | Remove entries from index   | <100ms                 |
| `flush()`           | Sync writes to disk         | Varies by backend      |
| `reset()`           | Clear all data              | <1s                    |
| `last_index()`      | Get highest persisted index | <100μs                 |

### MetaStore Methods

| Method              | Purpose                 | Criticality            |
| ------------------- | ----------------------- | ---------------------- |
| `save_hard_state()` | Persist term/vote state | High - atomic required |
| `load_hard_state()` | Load persisted state    | High                   |
| `flush()`           | Sync metadata writes    | Medium                 |

## 4. Testing Your Implementation

Use the standardized test suite to ensure compatibility:

```rust,ignore
use d_engine::storage_engine_test::{StorageEngineBuilder, StorageEngineTestSuite};
use tempfile::TempDir;

struct CustomStorageEngineBuilder {
    temp_dir: TempDir,
}

#[async_trait]
impl StorageEngineBuilder for CustomStorageEngineBuilder {
    type Engine = CustomStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        let path = self.temp_dir.path().join("storage");
        let engine = CustomStorageEngine::new(path).await?;
        Ok(Arc::new(engine))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        Ok(()) // TempDir auto-cleans on drop
    }
}

#[tokio::test]
async fn test_custom_storage_engine() -> Result<(), Error> {
    let builder = CustomStorageEngineBuilder::new();
    StorageEngineTestSuite::run_all_tests(builder).await
}

#[tokio::test]
async fn test_performance() -> Result<(), Error> {
    let builder = CustomStorageEngineBuilder::new();
    let engine = builder.build().await?;
    let log_store = engine.log_store();

    // Performance test: persist 10,000 entries
    let start = std::time::Instant::now();
    let entries = (1..=10000)
        .map(|i| create_test_entry(i, i))
        .collect();

    log_store.persist_entries(entries).await?;
    let duration = start.elapsed();

    assert!(duration.as_millis() < 1000, "Should persist 10k entries in <1s");
    Ok(())
}

```

## 5. Register with NodeBuilder

```rust,ignore
use d_engine::NodeBuilder;

let storage_engine = Arc::new(CustomStorageEngine::new().await?);

NodeBuilder::new(config, shutdown_rx)
    .storage_engine(storage_engine)  // Required component
    .start_server()
    .await?;

```

## 6. Production Examples

Reference implementations available in:

- `src/storage/adaptors/rocksdb/` - RocksDB storage engine
- `src/storage/adaptors/sled/` - Sled storage engine
- `src/storage/adaptors/file/` - File-based storage
- `src/storage/adaptors/mem/` - In-memory storage

Enable RocksDB feature in your `Cargo.toml`:

```toml
d-engine = { version = "0.1.4", features = ["rocksdb"] }

```

## 7. Performance Optimization

- **Batch Operations**: Use batch writes for log persistence
- **Caching**: Cache frequently accessed data (e.g., last index)
- **Concurrency**: Use appropriate locking strategies
- **Compression**: Consider compressing log entries for large deployments

See `src/storage/adaptors/rocksdb/rocksdb_engine.rs` for a production-grade implementation featuring:

- Write batches for atomic operations
- Log compaction to reclaim disk space
- Efficient snapshot storage
- Metrics integration
