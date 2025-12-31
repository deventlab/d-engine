use std::path::PathBuf;

use crate::{file_io::validate_checksum, proto::storage::snapshot_ack::ChunkStatus, Result, SnapshotError};
use tokio::{sync::mpsc, time::Instant};

use crate::{
    proto::storage::{SnapshotAck, SnapshotChunk, SnapshotMetadata},
    SnapshotAssembler,
};

struct SnapshotStreamProcessor {
    assembler: SnapshotAssembler,
    last_received: Instant,
    term_check: Option<(u64, u32)>,
    metadata: Option<SnapshotMetadata>,
    total_chunks: Option<u32>,
    count: u32,
}

impl SnapshotStreamProcessor {
    async fn process_chunk(
        &mut self,
        chunk: SnapshotChunk,
        ack_tx: Option<&mut mpsc::Sender<SnapshotAck>>,
    ) -> Result<()> {
        // 1. Validate leader consistency
        self.validate_leader(&chunk)?;

        // 2. Verify checksum
        if !validate_checksum(&chunk.data, &chunk.chunk_checksum) {
            return Err(SnapshotError::ChecksumMismatch.into());
        }

        // 3. Write to temporary file
        self.assembler.write_chunk(chunk.seq, &chunk.data).await?;

        // 4. Send ACK if in pull-mode
        if let Some(tx) = ack_tx {
            let ack = SnapshotAck {
                seq: chunk.seq,
                status: ChunkStatus::Accepted.into(),
                next_requested: chunk.seq + 1,
            };
            tx.send(ack).await?;
        }

        Ok(())
    }

    fn finalize(self) -> Result<(SnapshotMetadata, PathBuf)> {
        // Validate chunk count and finalize
    }
}
