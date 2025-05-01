mod sled_snapshot_store;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tonic::async_trait;

use crate::Result;
use crate::SnapshotMetadata;

pub(crate) struct SledSnapshotData {
    pub(crate) metadata: SnapshotMetadata,
    pub(crate) data: Bytes,
}

/// Streaming snapshot reader for large files
///
/// Implements zero-copy streaming using BufReader's internal buffer
struct StreamingSnapshot {
    path: PathBuf,
    metadata: SnapshotMetadata,
    reader: Option<BufReader<File>>,
}

#[async_trait]
impl crate::Snapshot for StreamingSnapshot {
    fn metadata(&self) -> SnapshotMetadata {
        self.metadata.clone()
    }

    async fn data_stream(&self) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        // Delay opening the file (when really needed)
        let file = tokio::fs::File::open(&self.path).await?;

        Ok(Box::new(SnapshotStream {
            reader: BufReader::with_capacity(2 * 1024 * 1024, file), // 2MB buffer
            chunk_size: 1024 * 1024,                                 // up to 1MB at a time
        }))
    }
}

// Core streaming implementation
struct SnapshotStream {
    reader: BufReader<File>,
    chunk_size: usize,
}

impl AsyncRead for SnapshotStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Control the maximum read size of 1MB each time
        let max_bytes = std::cmp::min(buf.remaining(), self.chunk_size);

        // Temporarily truncate ReadBuf to control how much to read at a time
        let orig_filled = buf.filled().len();

        // Read the original buf directly
        let poll = Pin::new(&mut self.reader).poll_read(cx, buf);

        if let Poll::Ready(Ok(())) = &poll {
            let new_filled = buf.filled().len();
            if new_filled - orig_filled > max_bytes {
                // Exceeded chunk_size, rollback
                let adjust_len = orig_filled + max_bytes;
                buf.set_filled(adjust_len);
            }
        }

        poll
    }
}
