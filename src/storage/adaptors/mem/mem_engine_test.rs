use super::MemoryStorageEngine;
use crate::{
    storage::{
        state_machine_test::{StateMachineBuilder, StateMachineTestSuite},
        storage_engine_test::{StorageEngineBuilder, StorageEngineTestSuite},
    },
    Error, MemoryStateMachine, StateMachine,
};
use std::sync::Arc;
use tonic::async_trait;

struct MemStorageEngineBuilder;

#[async_trait]
impl StorageEngineBuilder for MemStorageEngineBuilder {
    type Engine = MemoryStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        Ok(Arc::new(MemoryStorageEngine::new()))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct MemStateMachineBuilder;

#[async_trait]
impl StateMachineBuilder for MemStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        Ok(Arc::new(MemoryStateMachine::new(1)))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::test]
#[traced_test]
async fn test_mem_storage_engine() -> Result<(), Error> {
    StorageEngineTestSuite::run_all_tests(MemStorageEngineBuilder).await
}

#[tokio::test]
#[traced_test]
async fn test_mem_state_machine() -> Result<(), Error> {
    StateMachineTestSuite::run_all_tests(MemStateMachineBuilder).await
}
