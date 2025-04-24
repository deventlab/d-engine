use std::io::Read;

use crate::Result;

pub trait Snapshot: Send + Sync {
    fn read(&self) -> Result<Box<dyn Read>>;
    // fn metadata(&self) -> SnapshotMetadata;
}
