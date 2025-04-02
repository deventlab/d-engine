use std::collections::VecDeque;
use std::time::Duration;

use log::trace;
use tokio::time::Instant;

pub struct BatchBuffer<E> {
    pub(super) max_batch_size: usize,
    pub(super) batch_timeout: Duration,
    pub(super) buffer: VecDeque<E>,
    pub(super) last_flush: Instant,
}

impl<E> BatchBuffer<E> {
    pub fn new(
        max_batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            max_batch_size,
            batch_timeout,
            buffer: VecDeque::with_capacity(max_batch_size),
            last_flush: Instant::now(),
        }
    }

    pub fn push(
        &mut self,
        request: E,
    ) -> Option<usize> {
        self.buffer.push_back(request);
        trace!(
            "BatchBuffer::push, self.max_batch_size={}, self.buffer.len()={}",
            self.max_batch_size,
            self.buffer.len()
        );
        if self.buffer.len() >= self.max_batch_size {
            Some(self.buffer.len())
        } else {
            None
        }
    }

    pub fn should_flush(&self) -> bool {
        !self.buffer.is_empty() && self.last_flush.elapsed() > self.batch_timeout
    }

    pub fn take(&mut self) -> VecDeque<E> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.buffer)
    }
}
