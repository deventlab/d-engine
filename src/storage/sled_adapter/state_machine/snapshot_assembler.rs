use crate::Result;

struct SnapshotAssembler {
    temp_dir: TempDir,
    received: BTreeMap<u32, Vec<u8>>,
    total_size: usize,
}

impl SnapshotAssembler {
    async fn write_chunk(
        &mut self,
        index: u32,
        data: Vec<u8>,
    ) -> Result<()> {
        // Check if the block index is continuous
        if index as usize != self.received.len() {
            return Err(anyhow!("Out-of-order chunk"));
        }

        // Memory control: flush disk every 10 blocks written
        self.received.insert(index, data);
        if self.received.len() % 10 == 0 {
            self.flush_to_disk().await?;
        }

        Ok(())
    }

    async fn flush_to_disk(&mut self) -> Result<()> {
        let file_path = self.temp_dir.path().join("snapshot.part");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        for (_, data) in self.received.drain() {
            file.write_all(&data).await?;
        }

        Ok(())
    }

    fn finalize(self) -> PathBuf {
        self.temp_dir.into_path()
    }
}
