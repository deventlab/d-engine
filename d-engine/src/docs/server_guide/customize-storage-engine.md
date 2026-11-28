# Implementing Custom Storage Engines

d-engine supports **pluggable storage** through the `StorageEngine` trait. This storage engine is responsible for **persisting Raft log entries and metadata to durable storage**.

## Architecture Context

- The **StorageEngine** trait provides a **generic interface** for storage backends
- One trait, `StorageEngine`, keeps both the Raft log and the hard state (current term and vote)
- **Built-in implementations** include:
  - **File-based**: Production-ready file system storage
  - **RocksDB**: High-performance embedded database (requires `features = ["rocksdb"]`)
- Custom implementations enable support for cloud storage, SQL databases, etc.

## 1. Implement the Trait

```rust,ignore
use d_engine::StorageEngine;
use async_trait::async_trait;

struct CustomStorageEngine;

#[async_trait]
impl StorageEngine for CustomStorageEngine {
    async fn persist_entries(&self, entries: Vec<Entry>) -> Result<(), Error> {
        // Your implementation - batch operations recommended
        Ok(())
    }

    async fn entry(&self, index: u64) -> Result<Option<Entry>, Error> {
        // Retrieve single entry
        Ok(None)
    }

    fn save_hard_state(&self, state: &HardState) -> Result<(), Error> {
        // Persist the hard state (term and vote); it is made durable by the next flush()
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        // Load the persisted hard state, or None on a node that never saved one
        Ok(None)
    }

    fn flush(&self) -> Result<(), Error> {
        // Make everything written so far durable: log entries and the hard state.
        // A store whose writes are already durable still implements it, as Ok(()).
        Ok(())
    }

    // Implement all required methods...
}
```

## 2. Key Implementation Notes

- **Atomicity**: Ensure write operations are atomic—use batch operations where possible
- **Durability**: The engine calls `flush()` after every batch and counts entries as durable
  only once it returns `Ok`. If `flush()` returns before the data is on stable storage, a power
  loss can discard writes the cluster already acknowledged. If your backend already syncs inside
  each write (for example a store that fsyncs on every commit), `flush()` returns `Ok(())`.
- **Hard state**: `save_hard_state()` is always followed by `flush()` before the node answers a
  vote or an AppendEntries that changed the term, so both must make the hard state durable
- **Consistency**: Maintain exactly-once semantics for log entries
- **Performance**: Target >100k ops/sec for log persistence. Do not call `fsync` inside
  `persist_entries()`—the framework batches entries and calls `flush()` once per batch,
  which amortises the `fsync` cost across many entries.
- **Resource Management**: Clean up resources in `Drop` implementation

## 3. StorageEngine API Reference

### StorageEngine Methods

| Method               | Purpose                                                                                                         | Performance Target     |
| -------------------- | --------------------------------------------------------------------------------------------------------------- | ---------------------- |
| `persist_entries()`  | Batch persist log entries                                                                                       | >100k entries/sec      |
| `entry()`            | Get single entry by index                                                                                       | <1ms latency           |
| `get_entries()`      | Get entries in range                                                                                            | <1ms for 10k entries   |
| `purge()`            | Remove logs up to index                                                                                         | <100ms for 10k entries |
| `truncate()`         | Remove entries from index                                                                                       | <100ms                 |
| `save_hard_state()`  | Persist term/vote state (durable after the next `flush()`)                                                      | High - atomic required |
| `load_hard_state()`  | Load persisted term/vote state                                                                                  | High                   |
| `flush()`            | Make log entries and the hard state durable; required, `Ok(())` if writes are already durable                   | Varies by backend      |
| `last_index()`       | Get highest persisted index                                                                                     | <100μs                 |

## 4. Testing Your Implementation

Use the standardized test suite to ensure compatibility:

```rust,ignore
use std::sync::atomic::{AtomicUsize, Ordering};
use d_engine::storage_engine_test::{StorageEngineBuilder, StorageEngineTestSuite};
use tempfile::TempDir;

struct CustomStorageEngineBuilder {
    temp_dir: TempDir,
    builds: AtomicUsize, // every build() gets its own directory
}

#[async_trait]
impl StorageEngineBuilder for CustomStorageEngineBuilder {
    type Engine = CustomStorageEngine;

    // A new, empty engine on every call: tests never have to wipe it first.
    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.reopen().await
    }

    // The storage the last `build()` used, opened again: a restart.
    async fn reopen(&self) -> Result<Arc<Self::Engine>, Error> {
        let n = self.builds.load(Ordering::SeqCst);
        let path = self.temp_dir.path().join(format!("storage_{n}"));
        Ok(Arc::new(CustomStorageEngine::new(path).await?))
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
    // Performance test: persist 10,000 entries
    let start = std::time::Instant::now();
    let entries = (1..=10000)
        .map(|i| create_test_entry(i, i))
        .collect();

    engine.persist_entries(entries).await?;
    let duration = start.elapsed();

    assert!(duration.as_millis() < 1000, "Should persist 10k entries in <1s");
    Ok(())
}

```

## 5. Use It

```rust,ignore
use d_engine::{EmbeddedEngine, FileStateMachine};

let storage_engine = Arc::new(CustomStorageEngine::new().await?);
let state_machine = FileStateMachine::new("./data/state_machine").await?;

let engine = EmbeddedEngine::start_custom(
    "./data",       // data_dir
    storage_engine,
    state_machine,
    None,           // optional config file
).await?;
```

Running a standalone gRPC server instead of embedding? Use `StandaloneEngine::run_custom` — same arguments, plus a shutdown signal.

## 6. Production Examples

Reference implementations available in:

- `d-engine-server/src/storage/adaptors/rocksdb/` - RocksDB storage engine
- `d-engine-server/src/storage/adaptors/file/` - File-based storage
- `examples/sled-cluster/` - Sled storage engine
- `examples/raft-engine-log/` - raft-engine storage engine

Enable RocksDB feature in your `Cargo.toml`:

```toml
d-engine = { version = "0.2", features = ["rocksdb"] }

```

## 7. Performance Optimization

- **Batch Operations**: Use batch writes for log persistence
- **Caching**: Cache frequently accessed data (e.g., last index)
- **Concurrency**: Use appropriate locking strategies
- **Compression**: Consider compressing log entries for large deployments

See `d-engine-server/src/storage/adaptors/rocksdb/rocksdb_storage_engine.rs` for a production-grade implementation featuring:

- Write batches for atomic operations
- Log compaction to reclaim disk space
- Efficient snapshot storage
- Metrics integration
