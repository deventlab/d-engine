//! A oneshot channel implementation that can optionally be cloned in test environments.
//!
//! Unlike `StreamResponseSender` which is specialized for gRPC streaming responses
//! (`Result<tonic::Streaming<SnapshotChunk>, Status>`), this provides a generic oneshot
//! channel for any `T: Send`:
//! - Production: Regular oneshot semantics (non-cloneable)
//! - Tests: Uses broadcast channel to allow cloning senders
//!
//! Key differences from `StreamResponseSender`:
//! 1. Generic vs specialized (gRPC streaming)
//! 2. Simpler error handling
//! 3. Same test-friendly cloning pattern

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[cfg(test)]
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tonic::Status;

use crate::proto::storage::SnapshotChunk;

pub trait RaftOneshot<T: Send> {
    type Sender: Send + Sync;
    type Receiver: Send + Sync;

    fn new() -> (Self::Sender, Self::Receiver);
}

pub(crate) struct MaybeCloneOneshot;

#[allow(dead_code)]
pub(crate) struct MaybeCloneOneshotSender<T: Send> {
    inner: oneshot::Sender<T>,

    #[cfg(test)]
    test_inner: Option<broadcast::Sender<T>>, // None for non-cloneable types
}

impl<T: Send> Debug for MaybeCloneOneshotSender<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("MaybeCloneOneshotSender").finish()
    }
}

#[allow(dead_code)]
pub(crate) struct MaybeCloneOneshotReceiver<T: Send> {
    inner: oneshot::Receiver<T>,

    #[cfg(test)]
    test_inner: Option<broadcast::Receiver<T>>, // None for non-cloneable types
}
#[cfg(test)]
impl<T: Send> MaybeCloneOneshotSender<T> {
    pub fn send(
        &self,
        value: T,
    ) -> Result<usize, broadcast::error::SendError<T>> {
        if let Some(tx) = &self.test_inner {
            tx.send(value)
        } else {
            // Fallback for non-cloneable types
            panic!("Cannot broadcast non-cloneable type in tests");
        }
    }
}

#[cfg(not(test))]
impl<T: Send> MaybeCloneOneshotSender<T> {
    pub fn send(
        self,
        value: T,
    ) -> Result<(), T> {
        self.inner.send(value)
    }
}

impl<T: Send + Clone> MaybeCloneOneshotReceiver<T> {
    #[cfg(test)]
    pub async fn recv(&mut self) -> Result<T, broadcast::error::RecvError> {
        if let Some(ref mut rx) = &mut self.test_inner {
            rx.recv().await
        } else {
            // Fallback for non-cloneable types
            panic!("Cannot broadcast non-cloneable type in tests");
        }
    }
}

#[cfg(not(test))]
impl<T: Send + Clone> Future for MaybeCloneOneshotReceiver<T> {
    type Output = Result<T, oneshot::error::RecvError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll(cx)
    }
}
#[cfg(test)]
impl<T: Send + Clone> Future for MaybeCloneOneshotReceiver<T> {
    type Output = Result<T, broadcast::error::RecvError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Using the recv method of tokio::sync::broadcast::Receiver
        if let Some(ref mut rx) = &mut this.test_inner {
            match rx.try_recv() {
                Ok(value) => Poll::Ready(Ok(value)),
                Err(broadcast::error::TryRecvError::Empty) => {
                    // Register a Waker to wake up the task when data arrives
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(broadcast::error::TryRecvError::Closed) => Poll::Ready(Err(broadcast::error::RecvError::Closed)),
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    Poll::Ready(Err(broadcast::error::RecvError::Lagged(n)))
                }
            }
        } else {
            // Fallback for non-cloneable types
            panic!("Cannot broadcast non-cloneable type in tests");
        }
    }
}

#[cfg(test)]
impl<T: Send + Clone> Clone for MaybeCloneOneshotSender<T> {
    fn clone(&self) -> Self {
        let (sender, _) = oneshot::channel();
        Self {
            inner: sender,
            test_inner: self.test_inner.clone(),
        }
    }
}
#[cfg(test)]
impl<T: Send + Clone> Clone for MaybeCloneOneshotReceiver<T> {
    fn clone(&self) -> Self {
        let (_, receiver) = oneshot::channel();

        Self {
            inner: receiver,
            test_inner: Some(self.test_inner.as_ref().unwrap().resubscribe()),
        }
    }
}
#[cfg(test)]
impl<T: Send + Clone> RaftOneshot<T> for MaybeCloneOneshot {
    type Sender = MaybeCloneOneshotSender<T>;
    type Receiver = MaybeCloneOneshotReceiver<T>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (tx, rx) = oneshot::channel();
        let (test_tx, test_rx) = broadcast::channel(1);
        (
            MaybeCloneOneshotSender {
                inner: tx,
                test_inner: Some(test_tx),
            },
            MaybeCloneOneshotReceiver {
                inner: rx,
                test_inner: Some(test_rx),
            },
        )
    }
}

#[cfg(not(test))]
impl<T: Send> RaftOneshot<T> for MaybeCloneOneshot {
    type Sender = MaybeCloneOneshotSender<T>;
    type Receiver = MaybeCloneOneshotReceiver<T>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (tx, rx) = oneshot::channel();
        (
            MaybeCloneOneshotSender {
                inner: tx,
                #[cfg(test)]
                test_inner: None,
            },
            MaybeCloneOneshotReceiver {
                inner: rx,
                #[cfg(test)]
                test_inner: None,
            },
        )
    }
}

#[derive(Debug)]
pub(crate) struct StreamResponseSender {
    inner: oneshot::Sender<std::result::Result<tonic::Streaming<SnapshotChunk>, Status>>,

    #[cfg(test)]
    test_inner: Option<broadcast::Sender<std::result::Result<tonic::Streaming<SnapshotChunk>, Status>>>,
}

impl StreamResponseSender {
    pub fn new() -> (
        Self,
        oneshot::Receiver<std::result::Result<tonic::Streaming<SnapshotChunk>, Status>>,
    ) {
        let (inner_tx, inner_rx) = oneshot::channel();
        (
            Self {
                inner: inner_tx,
                #[cfg(test)]
                test_inner: None,
            },
            inner_rx,
        )
    }

    pub fn send(
        self,
        value: std::result::Result<tonic::Streaming<SnapshotChunk>, Status>,
    ) -> Result<(), std::result::Result<tonic::Streaming<SnapshotChunk>, Status>> {
        #[cfg(not(test))]
        return self.inner.send(value);

        #[cfg(test)]
        if let Some(tx) = self.test_inner {
            tx.send(value).map(|_| ()).map_err(|e| e.0)
        } else {
            self.inner.send(value)
        }
    }
}
