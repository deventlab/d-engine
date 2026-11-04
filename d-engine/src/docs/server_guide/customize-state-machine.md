# Implementing Custom State Machines

d-engine supports **pluggable state machines** through the `StateMachine` trait. This is where your application's core logic executes—processing committed log entries to update application state.

## Architecture Context

- The **StateMachine** is the **application logic layer** that processes committed Raft log entries
- The **StateMachineHandler** manages the lifecycle: applying entries, triggering snapshots, handling purges
- **Default implementations** include file-based and RocksDB storage (requires `features = ["rocksdb"]`)
- Custom implementations enable specialized behaviors for key-value stores, document databases, etc.

## 1. Implement the Trait

```rust,ignore
use std::io;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine::{ApplyEntry, ApplyResult, Command, LogId, StateMachine};

struct CustomStateMachine {
    // Your storage backend. Its calls return `io::Result`. If your storage library has its own
    // error type, convert it where you call it: `.map_err(io::Error::other)?`.
    backend: Arc<dyn ApplicationStorage>,
}

#[async_trait]
impl StateMachine for CustomStateMachine {

    // close() has a default no-op implementation.
    // Override it if your backend must persist something at shutdown (e.g. data, metadata) or
    // holds an exclusive OS resource (a lock file, an open DB handle). d-engine calls it once,
    // as the worker's last step, after it has stopped serving reads: nothing is called on the
    // state machine once close() has started. It must be synchronous, release its resources even
    // if persisting fails, and return the first error; a second call returns Ok(()).
    // Do not rely on it for crash safety: if `shutdown_apply_timeout_ms` expires it runs after the
    // node has returned, or not at all if the process exits first. A restart must be correct
    // without it, so persist last_applied together with the data it describes.
    // If you share your storage handle with the application (a cloneable state machine), the
    // state machine is not dropped when the engine stops, so do not rely on Drop alone.
    //
    // fn close(&self) -> io::Result<()> {
    //     let flushed = self.backend.flush();
    //     self.backend.close(); // release lock file / DB handle
    //     flushed
    // }

    fn get(&self, key_buffer: &[u8]) -> io::Result<Option<Bytes>> {
        // Retrieve value by key
        self.backend.get(key_buffer)
    }

    async fn apply_chunk(&self, chunk: &[ApplyEntry]) -> io::Result<Vec<ApplyResult>> {
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

    // The other required methods are `last_applied`, `apply_snapshot_from_file` and
    // `generate_snapshot_data`; see the table in section 3.
}

```

## 2. Key Implementation Notes

- **Atomic Operations**: Ensure `apply_chunk()` either fully applies or fails the entire batch
- **Idempotency**: Handle duplicate entries safely—Raft may resend committed entries
- **Snapshot Isolation**: `apply_snapshot_from_file()` must atomically replace state
- **Snapshot Checksums**: The engine computes the checksum of the directory your `generate_snapshot_data()` writes, and verifies it before it calls `apply_snapshot_from_file()`. You neither compute nor check one
- **Snapshot Position**: `generate_snapshot_data()` returns the position of the last applied entry the exported data contains. Read it from the same consistent view the data comes from (one read transaction, a checkpoint, or a read lock that `apply_chunk` also takes when it updates the position), never from a separate counter: `apply_chunk` keeps running during the export
- **Snapshot Install**: The engine decides whether a snapshot is installed. `apply_snapshot_from_file()` is called only for a snapshot newer than `last_applied()`, and the engine checks afterwards that `last_applied()` equals the snapshot boundary
- **Restore On Open**: The engine never asks you to restore state. When your state machine is opened, `last_applied()` must return the last entry whose effects are durable (persist it together with the data it describes). You store nothing about snapshots: the engine records the snapshots it holds itself
- **Concurrency Control**: Use appropriate locking for state mutations
- **Errors**: Every method returns `std::io::Result`. Report a failure with `io::Error::other("disk full")`, or convert your storage library's error with `.map_err(io::Error::other)?`; the original error stays attached for the log. The engine does not look at the error kind. A failed `apply_chunk()` or `apply_snapshot_from_file()` stops the node, which then recovers from its replicas, because a committed entry cannot be skipped. A failed `get()`, `get_multi()` or `scan_prefix()` fails only that read
- **Reads During A Snapshot Install**: A read can run while `apply_snapshot_from_file()` replaces the state. It must see the old state, the new state, or fail; never a mix of the two
- **Resource Cleanup**: Implement `close()` to flush and release locks and handles on shutdown (see above). Do not rely on `Drop` alone

## 3. StateMachine API Reference

| Method                       | Purpose                                                                                                                                                                                                                                                  | Sync/Async | Criticality |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ----------- |
| `close()`                    | Last step at shutdown: persist what must survive a restart and release exclusive OS resources (e.g. DB lock file), even if persisting fails; returns the first error. May be called more than once. Nothing is called after it starts. Default `Ok(())`. | Sync       | Medium      |
| `get()`                      | Read value by key                                                                                                                                                                                                                                        | Sync       | High        |
| `get_multi()`                | Read several keys from one consistent state. Default calls `get()` per key; override it if your storage can read a batch from one snapshot                                                                                                               | Sync       | Medium      |
| `scan_prefix()`              | Return every key with the given prefix. Default returns an error; override it to support prefix scans (e.g. watch)                                                                                                                                       | Sync       | Optional    |
| `apply_chunk()`              | Apply committed entries                                                                                                                                                                                                                                  | Async      | Critical    |
| `last_applied()`             | Get last applied index                                                                                                                                                                                                                                   | Sync       | High        |
| `apply_snapshot_from_file()` | Replace state with snapshot                                                                                                                                                                                                                              | Async      | Critical    |
| `generate_snapshot_data()`   | Export a snapshot into the given directory and return the position of the last entry it contains (the engine checksums it)                                                                                                                               | Async      | High        |

## 4. Testing Your Implementation

### 4.1 Functional Tests

Implement `StateMachineBuilder` once, then run the built-in suite against your state machine. It checks the contract described above, including that a restart finds the last applied entry together with its data:

```rust,ignore
use d_engine::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use d_engine::{Error, StateMachine};

struct MyStateMachineBuilder { /* e.g. a temporary directory */ }

#[async_trait]
impl StateMachineBuilder for MyStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        // A state machine on new, empty storage (a new directory on every call)
        Ok(Arc::new(CustomStateMachine::new().await?))
    }

    async fn reopen(&self) -> Result<Arc<dyn StateMachine>, Error> {
        // The storage the last build() used, with whatever it holds now (a restart)
        Ok(Arc::new(CustomStateMachine::open_existing().await?))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        Ok(())
    }

    // Optional: override `build_from_crash_image()` to also test recovery after a crash.
    // It returns `None` by default, and that test is skipped.
}

#[tokio::test]
async fn test_custom_state_machine() -> Result<(), Error> {
    StateMachineTestSuite::run_all_tests(MyStateMachineBuilder::new()).await
}
```

### 4.2 Performance Tests

Use `run_performance_tests()` with the same builder to detect performance regressions:

```rust,ignore
#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored
async fn test_performance() -> Result<(), Error> {
    // Runs smoke test (100 entries < 1s) and scalability test (O(N) complexity)
    StateMachineTestSuite::run_performance_tests(MyStateMachineBuilder::new()).await
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
- `examples/sled-cluster/src/sled_state_machine.rs` - Sled state machine
- `d-engine-server/src/storage/adaptors/` - Built-in file and RocksDB implementations

Enable RocksDB feature in your `Cargo.toml`:

```toml
d-engine = { version = "0.2", features = ["rocksdb"] }
```
