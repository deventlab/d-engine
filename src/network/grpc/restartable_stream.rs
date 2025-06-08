use crate::proto::storage::SnapshotChunk;
use crate::Result;
use futures::Stream;
use futures::StreamExt;
use std::collections::VecDeque;

#[derive(Clone)]
pub(crate) struct RestartableStream<T> {
    inner: T,
    buffer: VecDeque<SnapshotChunk>,
    current_seq: u32,
    source_exhausted: bool,
}

impl<T> RestartableStream<T>
where
    T: Stream<Item = Result<SnapshotChunk>> + Unpin,
{
    pub(crate) fn new(stream: T) -> Self {
        Self {
            inner: stream,
            buffer: VecDeque::new(),
            current_seq: 0,
            source_exhausted: false,
        }
    }

    pub(crate) async fn seek(
        &mut self,
        seq: u32,
    ) -> Result<()> {
        // If the buffer has been buffered to the requested position
        if let Some(chunk) = self.buffer.front() {
            if chunk.seq == seq {
                return Ok(());
            }
        }

        // Clear the buffer and reset
        self.buffer.clear();
        self.current_seq = seq;
        self.source_exhausted = false;
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Option<Result<SnapshotChunk>> {
        // Read from the buffer first
        if let Some(chunk) = self.buffer.pop_front() {
            return Some(Ok(chunk));
        }

        // If the source is exhausted, return directly
        if self.source_exhausted {
            return None;
        }

        // Read new data from the source stream
        while let Some(item) = self.inner.next().await {
            match item {
                Ok(chunk) => {
                    // Skip the sent chunks
                    if chunk.seq < self.current_seq {
                        continue;
                    }

                    // Buffer subsequent chunks
                    if chunk.seq > self.current_seq {
                        self.buffer.push_back(chunk);
                        continue;
                    }

                    // Return current chunk
                    self.current_seq += 1;
                    return Some(Ok(chunk));
                }
                Err(e) => {
                    self.source_exhausted = true;
                    return Some(Err(e));
                }
            }
        }

        self.source_exhausted = true;
        None
    }
}
