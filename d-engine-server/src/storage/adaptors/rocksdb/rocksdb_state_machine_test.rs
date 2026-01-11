use crate::{Error, StateMachine};
use d_engine_core::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use std::sync::Arc;
use tempfile::TempDir;
use tonic::async_trait;

use super::RocksDBStateMachine;

struct RocksDBStateMachineBuilder {
    temp_dir: TempDir,
}

impl RocksDBStateMachineBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("Failed to create temp dir"),
        }
    }
}

#[async_trait]
impl StateMachineBuilder for RocksDBStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        let path = self.temp_dir.path().join("rocksdb_sm");
        let sm = RocksDBStateMachine::new(path)?;
        Ok(Arc::new(sm))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // TempDir automatically cleans up on drop
        Ok(())
    }
}

#[tokio::test]
async fn test_rocksdb_state_machine_suite() {
    let builder = RocksDBStateMachineBuilder::new();
    StateMachineTestSuite::run_all_tests(builder)
        .await
        .expect("RocksDBStateMachine should pass all tests");
}

#[tokio::test]
async fn test_rocksdb_state_machine_performance() {
    let builder = RocksDBStateMachineBuilder::new();
    StateMachineTestSuite::run_performance_tests(builder)
        .await
        .expect("RocksDBStateMachine should pass performance tests");
}
