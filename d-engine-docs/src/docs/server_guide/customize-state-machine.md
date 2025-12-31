# Implementing Custom State Machines

d-engine supports **pluggable state machines** through the `StateMachine` trait. This is where your application's core logic executes—processing committed log entries to update application state.

## Architecture Context

- The **StateMachine** is the **application logic layer** that processes committed Raft log entries
- The **StateMachineHandler** manages the lifecycle: applying entries, triggering snapshots, handling purges
- **Default implementations** include file-based and RocksDB storage (requires `features = ["rocksdb"]`)
- Custom implementations enable specialized behaviors for key-value stores, document databases, etc.

## 1. Implement the Trait

```rust,ignore
use d_engine::{StateMachine, Result, Entry, LogId, SnapshotMetadata};
use async_trait::async_trait;

struct CustomStateMachine {
    // Your storage backend
    backend: Arc<dyn ApplicationStorage>,
    last_applied: AtomicU64,
    snapshot_meta: Mutex<Option<SnapshotMetadata>>
}

#[async_trait]
impl StateMachine for CustomStateMachine {
    fn start(&self) -> Result<(), Error> {
        // Initialize your state machine
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        // Cleanup resources
        Ok(())
    }

    fn is_running(&self) -> bool {
        // Return running status
        true
    }

    fn get(&self, key_buffer: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        // Retrieve value by key
        self.backend.get(key_buffer)
    }

    fn entry_term(&self, entry_id: u64) -> Option<u64> {
        // Return term for specific entry
        Some(1)
    }

    async fn apply_chunk(&self, chunk: Vec<Entry>) -> Result<()> {
        // Deserialize and process entries
        for entry in chunk {
            let cmd: AppCommand = bincode::deserialize(&entry.data)?;
            self.backend.execute(cmd)?;
        }
        Ok(())
    }

    fn len(&self) -> usize {
        // Return number of entries
        self.backend.count()
    }

    // Implement all required methods...
}

```

## 2. Key Implementation Notes

- **Atomic Operations**: Ensure `apply_chunk()` either fully applies or fails the entire batch
- **Idempotency**: Handle duplicate entries safely—Raft may resend committed entries
- **Snapshot Isolation**: `apply_snapshot_from_file()` must atomically replace state
- **Checksum Validation**: Mandatory for snapshot integrity—validate before application
- **Concurrency Control**: Use appropriate locking for state mutations
- **Resource Cleanup**: Implement `Drop` to ensure proper flush on shutdown

## 3. StateMachine API Reference

| Method                             | Purpose                            | Sync/Async | Criticality |
| ---------------------------------- | ---------------------------------- | ---------- | ----------- |
| `start()`                          | Initialize state machine service   | Sync       | High        |
| `stop()`                           | Graceful shutdown                  | Sync       | High        |
| `is_running()`                     | Check service status               | Sync       | Medium      |
| `get()`                            | Read value by key                  | Sync       | High        |
| `entry_term()`                     | Get term for log index             | Sync       | Medium      |
| `apply_chunk()`                    | Apply committed entries            | Async      | Critical    |
| `len()`                            | Get entry count                    | Sync       | Low         |
| `is_empty()`                       | Check if empty                     | Sync       | Low         |
| `update_last_applied()`            | Update applied index in memory     | Sync       | High        |
| `last_applied()`                   | Get last applied index             | Sync       | High        |
| `persist_last_applied()`           | Persist applied index              | Sync       | High        |
| `update_last_snapshot_metadata()`  | Update snapshot metadata in memory | Sync       | Medium      |
| `snapshot_metadata()`              | Get current snapshot metadata      | Sync       | Medium      |
| `persist_last_snapshot_metadata()` | Persist snapshot metadata          | Sync       | Medium      |
| `apply_snapshot_from_file()`       | Replace state with snapshot        | Async      | Critical    |
| `generate_snapshot_data()`         | Create new snapshot                | Async      | High        |
| `save_hard_state()`                | Persist term/vote state            | Sync       | High        |
| `flush()`                          | Sync writes to storage             | Sync       | High        |
| `flush_async()`                    | Async flush                        | Async      | High        |
| `reset()`                          | Reset to initial state             | Async      | Medium      |

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

    // Test basic functionality
    sm.start()?;
    assert!(sm.is_running());

    // Test apply_chunk with sample entries
    let entries = vec![create_test_entry(1, 1)];
    sm.apply_chunk(entries).await?;
    assert_eq!(sm.len(), 1);

    // Test snapshot operations
    let temp_dir = TempDir::new()?;
    let checksum = sm.generate_snapshot_data(temp_dir.path().to_path_buf(), LogId::new(1, 1)).await?;
    assert!(!checksum.is_zero());

    sm.stop()?;
    Ok(())
}

```

### 4.2 Performance Tests

Use `run_performance_tests()` to detect performance regressions:

```rust,ignore
use d_engine_core::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};

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

## 5. Register with NodeBuilder

```rust,ignore
use d_engine::NodeBuilder;

let custom_sm = Arc::new(CustomStateMachine::new().await?);

NodeBuilder::new(config, shutdown_rx)
    .state_machine(custom_sm)  // Required component
    .start()
    .await?;
```

## 6. Production Examples

See complete implementations in:

- `examples/rocksdb-cluster/src/main.rs` - RocksDB state machine
- `examples/sled-cluster/src/main.rs` - Sled state machine
- `src/storage/adaptors/` - Built-in implementations

Enable RocksDB feature in your `Cargo.toml`:

```toml
d-engine = { version = "0.1.4", features = ["rocksdb"] }
```
