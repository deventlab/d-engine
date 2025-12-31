use std::error::Error;

use bytes::Bytes;

use crate::server::storage::SnapshotMetadata;

impl SnapshotMetadata {
    pub fn checksum_array(&self) -> Result<[u8; 32], Box<dyn Error>> {
        if self.checksum.len() == 32 {
            let mut array = [0u8; 32];
            array.copy_from_slice(&self.checksum);
            Ok(array)
        } else {
            Err(format!("Invalid checksum length: {}", self.checksum.len()).into())
        }
    }

    pub fn set_checksum_array(
        &mut self,
        array: [u8; 32],
    ) {
        self.checksum = Bytes::copy_from_slice(&array);
    }
}
