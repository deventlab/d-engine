use std::sync::Arc;

use async_trait::async_trait;
use d_engine_core::Error;
use d_engine_core::StateMachine;
use d_engine_core::state_machine_test::StateMachineBuilder;
use d_engine_core::state_machine_test::StateMachineTestSuite;
use d_engine_core::state_machine_test::copy_storage_dir;
use d_engine_core::storage_engine_test::StorageEngineBuilder;
use d_engine_core::storage_engine_test::StorageEngineTestSuite;
use tempfile::TempDir;
use tracing_test::traced_test;

use super::FileStorageEngine;
use crate::FileStateMachine;

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
    // How many times `build()` ran: every build gets its own directory.
    builds: std::sync::atomic::AtomicUsize,
}

impl FileStateMachineBuilder {
    /// The directory the last `build()` used.
    fn current_dir(&self) -> std::path::PathBuf {
        let n = self.builds.load(std::sync::atomic::Ordering::SeqCst);
        self.temp_dir.path().join(format!("state_machine_{n}"))
    }
}

#[async_trait]
impl StateMachineBuilder for FileStateMachineBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        self.builds.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Arc::new(
            FileStateMachine::new(self.current_dir())
                .await
                .expect("Expect file init successfully"),
        ))
    }

    async fn reopen(&self) -> Result<Arc<dyn StateMachine>, Error> {
        Ok(Arc::new(
            FileStateMachine::new(self.current_dir())
                .await
                .expect("Expect file init successfully"),
        ))
    }

    async fn build_from_crash_image(&self) -> Result<Option<Arc<dyn StateMachine>>, Error> {
        let image = self.temp_dir.path().join("state_machine_crash_image");
        let _ = std::fs::remove_dir_all(&image);
        copy_storage_dir(&self.current_dir(), &image)?;
        Ok(Some(Arc::new(FileStateMachine::new(image).await?)))
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
    StateMachineTestSuite::run_all_tests(FileStateMachineBuilder {
        temp_dir,
        builds: std::sync::atomic::AtomicUsize::new(0),
    })
    .await
}
