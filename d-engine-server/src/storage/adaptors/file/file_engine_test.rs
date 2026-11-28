use std::sync::Arc;

use async_trait::async_trait;
use d_engine_core::Error;
use d_engine_core::state_machine_test::copy_storage_dir;
use d_engine_core::storage_engine_test::StorageEngineBuilder;
use d_engine_core::storage_engine_test::StorageEngineTestSuite;
use tempfile::TempDir;
use tracing_test::traced_test;

use super::FileStorageEngine;

struct FileStorageEngineBuilder {
    temp_dir: TempDir,
    // How many times `build()` ran: every build gets its own directory.
    builds: std::sync::atomic::AtomicUsize,
}

impl FileStorageEngineBuilder {
    fn new(temp_dir: TempDir) -> Self {
        Self {
            temp_dir,
            builds: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// The directory the last `build()` used.
    fn current_dir(&self) -> std::path::PathBuf {
        let n = self.builds.load(std::sync::atomic::Ordering::SeqCst);
        self.temp_dir.path().join(format!("storage_engine_{n}"))
    }
}

#[async_trait]
impl StorageEngineBuilder for FileStorageEngineBuilder {
    type Engine = FileStorageEngine;

    async fn build(&self) -> Result<Arc<Self::Engine>, Error> {
        self.builds.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.reopen().await
    }

    async fn reopen(&self) -> Result<Arc<Self::Engine>, Error> {
        Ok(Arc::new(
            FileStorageEngine::new(self.current_dir()).expect("Expect file init successfully"),
        ))
    }

    async fn build_from_crash_image(&self) -> Result<Option<Arc<Self::Engine>>, Error> {
        let image = self.temp_dir.path().join("storage_engine_crash_image");
        let _ = std::fs::remove_dir_all(&image);
        copy_storage_dir(&self.current_dir(), &image)?;
        Ok(Some(Arc::new(FileStorageEngine::new(image)?)))
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
    StorageEngineTestSuite::run_all_tests(FileStorageEngineBuilder::new(temp_dir)).await
}
