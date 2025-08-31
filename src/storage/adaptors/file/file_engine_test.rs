use std::sync::Arc;

use tempfile::TempDir;
use tonic::async_trait;
use tracing_test::traced_test;

use super::FileStorageEngine;
use crate::storage::state_machine_test::StateMachineBuilder;
use crate::storage::state_machine_test::StateMachineTestSuite;
use crate::storage::storage_engine_test::StorageEngineBuilder;
use crate::storage::storage_engine_test::StorageEngineTestSuite;
use crate::Error;
use crate::FileStateMachine;
use crate::StateMachine;

struct FileStorageEngineBuilder {
    temp_dir: TempDir,
}

#[async_trait]
impl StorageEngineBuilder for FileStorageEngineBuilder {
    type Engine = FileStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        let storage_path = self.temp_dir.path().join("storage_engine");
        Ok(Arc::new(
            FileStorageEngine::new(storage_path).expect("Expect file init successfully"),
        ))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // TempDir automatically cleans up when dropped, so no need for explicit cleanup
        Ok(())
    }
}

struct FileStateMachineBuilder {
    temp_dir: TempDir, // Keep the temp dir alive for the duration of the test
}

#[async_trait]
impl StateMachineBuilder for FileStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        let state_machine_path = self.temp_dir.path().join("state_machine");
        Ok(Arc::new(
            FileStateMachine::new(state_machine_path, 1)
                .await
                .expect("Expect file init successfully"),
        ))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        // TempDir automatically cleans up when dropped, so no need for explicit cleanup
        Ok(())
    }
}

#[tokio::test]
#[traced_test]
async fn test_file_storage_engine() -> Result<(), Error> {
    let temp_dir = TempDir::new()?;
    StorageEngineTestSuite::run_all_tests(FileStorageEngineBuilder { temp_dir }).await
}

#[tokio::test]
#[traced_test]
async fn test_file_state_machine() -> Result<(), Error> {
    let temp_dir = TempDir::new()?;
    StateMachineTestSuite::run_all_tests(FileStateMachineBuilder { temp_dir }).await
}
