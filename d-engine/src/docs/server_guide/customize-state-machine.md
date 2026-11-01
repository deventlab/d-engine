# Implementing Custom State Machines

d-engine supports **pluggable state machines** through the `StateMachine` trait. This is where your application's core logic executes—processing committed log entries to update application state.

## Architecture Context

- The **StateMachine** is the **application logic layer** that processes committed Raft log entries
- The **StateMachineHandler** manages the lifecycle: applying entries, triggering snapshots, handling purges
- **Default implementations** include file-based and RocksDB storage (requires `features = ["rocksdb"]`)
- Custom implementations enable specialized behaviors for key-value stores, document databases, etc.

## 1. Implement the Trait

```rust,ignore
use d_engine::{ApplyEntry, ApplyResult, Command, Error, LogId, SnapshotMetadata, StateMachine};
use async_trait::async_trait;

struct CustomStateMachine {
    // Your storage backend
    backend: Arc<dyn ApplicationStorage>,
    last_applied: AtomicU64,
    snapshot_meta: Mutex<Option<SnapshotMetadata>>
}

#[async_trait]
impl StateMachine for CustomStateMachine {

    // close() has a default no-op implementation.
    // Override it if your backend must persist something at shutdown (e.g. data, metadata) or
    // holds an exclusive OS resource (a lock file, an open DB handle). d-engine calls it once,
    // as the worker's last step: after every committed entry has been applied when the worker
    // drains cleanly, but shutdown continues without that guarantee if
    // `shutdown_apply_timeout_ms` expires. It must be synchronous and idempotent, release its
    // resources even if persisting fails, and return the first error.
    // If you share your storage handle with the application (a cloneable state machine), the
    // state machine is not dropped when the engine stops, so do not rely on Drop alone.
    //
    // fn close(&self) -> Result<(), Error> {
    //     let flushed = self.backend.flush();
    //     self.backend.close(); // release lock file / DB handle
    //     flushed
    // }

    fn get(&self, key_buffer: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        // Retrieve value by key
        self.backend.get(key_buffer)
    }

    async fn apply_chunk(&self, chunk: &[ApplyEntry]) -> Result<Vec<ApplyResult>, Error> {
        // The framework pre-decodes proto bytes — match on Command directly, no prost needed.
        let mut results = Vec::with_capacity(chunk.len());
        for entry in chunk {
            let result = match &entry.command {
                Command::Insert { key, value } => {
                    self.backend.put(key, value)?;
                    ApplyResult::success(entry.index)
                }
                Command::Delete { key } => {
                    self.backend.delete(key)?;
                    ApplyResult::success(entry.index)
                }
                Command::CompareAndSwap { key, expected, value } => {
                    // CAS must return failure when the comparison does not match.
                    // Callers (e.g. client CAS requests) inspect ApplyResult::succeeded
                    // to determine whether the swap actually took effect.
                    let succeeded = self.backend.cas(key, expected.as_deref(), value)?;
                    if succeeded {
                        ApplyResult::success(entry.index)
                    } else {
                        ApplyResult::failure(entry.index)
                    }
                }
                Command::Batch { ops } => {
                    for op in ops {
                        match op {
                            d_engine::BatchOp::Insert { key, value } => {
                                self.backend.put(key, value)?;
                            }
                            d_engine::BatchOp::Delete { key } => {
                                self.backend.delete(key)?;
                            }
                        }
                    }
                    ApplyResult::success(entry.index)
                }
                Command::Noop => ApplyResult::success(entry.index),
            };
            results.push(result);
        }
        Ok(results)
    }


    // Implement all required methods...
}

```

## 2. Key Implementation Notes

- **Atomic Operations**: Ensure `apply_chunk()` either fully applies or fails the entire batch
- **Idempotency**: Handle duplicate entries safely—Raft may resend committed entries
- **Snapshot Isolation**: `apply_snapshot_from_file()` must atomically replace state
- **Snapshot Checksums**: The engine computes the checksum of the directory your `generate_snapshot_data()` writes, and verifies it before it calls `apply_snapshot_from_file()`. You neither compute nor check one
- **Snapshot Install**: The engine decides whether a snapshot is installed. `apply_snapshot_from_file()` is called only for a snapshot newer than `last_applied()`, and the engine checks afterwards that `last_applied()` equals the snapshot boundary
- **Restore On Open**: The engine never asks you to restore state. When your state machine is opened, `last_applied()` must return the last entry whose effects are durable (persist it together with the data it describes), and `snapshot_metadata()` must return the value most recently passed to `persist_last_snapshot_metadata()`
- **Concurrency Control**: Use appropriate locking for state mutations
- **Resource Cleanup**: Implement `Drop` to ensure proper flush on shutdown

## 3. StateMachine API Reference

| Method                             | Purpose                                                                                                                                                                                              | Sync/Async | Criticality |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ----------- |
| `close()`                          | Final step at shutdown: persist what must survive a restart and release exclusive OS resources (e.g. DB lock file), even if persisting fails; returns the first error. Idempotent. Default `Ok(())`. | Sync       | Medium      |
| `get()`                            | Read value by key                                                                                                                                                                                    | Sync       | High        |
| `apply_chunk()`                    | Apply committed entries                                                                                                                                                                              | Async      | Critical    |
| `last_applied()`                   | Get last applied index                                                                                                                                                                               | Sync       | High        |
| `snapshot_metadata()`              | Get current snapshot metadata                                                                                                                                                                        | Sync       | Medium      |
| `persist_last_snapshot_metadata()` | Persist snapshot metadata                                                                                                                                                                            | Sync       | Medium      |
| `apply_snapshot_from_file()`       | Replace state with snapshot                                                                                                                                                                          | Async      | Critical    |
| `generate_snapshot_data()`         | Export a snapshot into the given directory (the engine checksums it)                                                                                                                                                                                  | Async      | High        |
| `reset()`                          | Reset to initial state                                                                                                                                                                               | Async      | Medium      |

## 4. Testing Your Implementation

### 4.1 Functional Tests

Use the built-in test suite to validate correctness:

```rust,ignore
use d_engine::{StateMachine, Error};
use tempfile::TempDir;

struct TestStateMachineBuilder {
    temp_dir: TempDir,
}

impl TestStateMachineBuilder {
    async fn build(&self) -> Result<Arc<CustomStateMachine>, Error> {
        let path = self.temp_dir.path().join("sm_test");
        let sm = CustomStateMachine::new(path).await?;
        Ok(Arc::new(sm))
    }
}

#[tokio::test]
async fn test_custom_state_machine() -> Result<(), Error> {
    let builder = TestStateMachineBuilder::new();
    let sm = builder.build().await?;

    // Test apply_chunk with sample entries
    let entries = vec![create_test_entry(1, 1)];
    sm.apply_chunk(&entries).await?;
    assert_eq!(sm.last_applied().index, 1);

    // Test snapshot operations
    let temp_dir = TempDir::new()?;
    sm.generate_snapshot_data(temp_dir.path().to_path_buf(), LogId::new(1, 1)).await?;

    sm.close()?;
    Ok(())
}

```

### 4.2 Performance Tests

Use `run_performance_tests()` to detect performance regressions:

```rust,ignore
use d_engine::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};

#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored
async fn test_performance() -> Result<(), Error> {
    struct MyStateMachineBuilder { /* ... */ }

    #[async_trait]
    impl StateMachineBuilder for MyStateMachineBuilder {
        async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
            // Create your state machine
            Ok(Arc::new(MyStateMachine::new().await?))
        }

        async fn cleanup(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    let builder = MyStateMachineBuilder::new();

    // Runs smoke test (100 entries < 1s) and scalability test (O(N) complexity)
    StateMachineTestSuite::run_performance_tests(builder).await?;
    Ok(())
}

```

**Performance Thresholds:**

- **Smoke Test**: 100 entries in < 1 second
- **Scalability**: 1000 entries should be ~10x slower than 100 entries (not 100x)

Run performance tests locally: `cargo test -- --ignored`

## 5. Use It

```rust,ignore
use d_engine::{EmbeddedEngine, FileStorageEngine};

let storage_engine = Arc::new(FileStorageEngine::new("./data/storage").await?);
let state_machine = CustomStateMachine::new().await?;

let engine = EmbeddedEngine::start_custom(
    "./data",       // data_dir
    storage_engine,
    state_machine,
    None,           // optional config file
).await?;
```

Running a standalone gRPC server instead of embedding? Use `StandaloneEngine::run_custom` — same arguments, plus a shutdown signal.

## 6. Production Examples

See complete implementations in:

- `examples/three-nodes-standalone/src/main.rs` - Production cluster with RocksDB
- `examples/sled-cluster/src/main.rs` - Sled state machine
- `src/storage/adaptors/` - Built-in implementations

Enable RocksDB feature in your `Cargo.toml`:

```toml
d-engine = { version = "0.2", features = ["rocksdb"] }
```
