use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{broadcast, oneshot};

pub trait RaftOneshot<T: Send> {
    type Sender: Send + Sync;
    type Receiver: Send + Sync;

    fn new() -> (Self::Sender, Self::Receiver);
}

#[derive(Clone)]
pub struct MaybeCloneOneshot;

#[derive(Debug)]
pub struct MaybeCloneOneshotSender<T: Send + Clone> {
    inner: oneshot::Sender<T>,

    #[cfg(test)]
    test_inner: broadcast::Sender<T>,
}
pub struct MaybeCloneOneshotReceiver<T: Send + Clone> {
    inner: oneshot::Receiver<T>,

    #[cfg(test)]
    test_inner: broadcast::Receiver<T>,
}

impl<T: Send + Clone> MaybeCloneOneshotSender<T> {
    #[cfg(not(test))]
    pub fn send(self, value: T) -> Result<(), T> {
        self.inner.send(value)
    }

    #[cfg(test)]
    pub fn send(&self, value: T) -> Result<usize, tokio::sync::broadcast::error::SendError<T>> {
        self.test_inner.send(value)
    }
}

impl<T: Send + Clone> MaybeCloneOneshotReceiver<T> {
    #[cfg(test)]
    pub async fn recv(&mut self) -> Result<T, broadcast::error::RecvError> {
        self.test_inner.recv().await
    }
}

#[cfg(not(test))]
impl<T: Send + Clone> Future for MaybeCloneOneshotReceiver<T> {
    type Output = Result<T, oneshot::error::RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll(cx)
    }
}
#[cfg(test)]
impl<T: Send + Clone> Future for MaybeCloneOneshotReceiver<T> {
    type Output = Result<T, broadcast::error::RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Using the recv method of tokio::sync::broadcast::Receiver
        match this.test_inner.try_recv() {
            Ok(value) => Poll::Ready(Ok(value)),
            Err(broadcast::error::TryRecvError::Empty) => {
                // Register a Waker to wake up the task when data arrives
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                Poll::Ready(Err(broadcast::error::RecvError::Closed))
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Poll::Ready(Err(broadcast::error::RecvError::Lagged(n)))
            }
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
            test_inner: self.test_inner.resubscribe(),
        }
    }
}

impl<T: Send + Clone> RaftOneshot<T> for MaybeCloneOneshot {
    type Sender = MaybeCloneOneshotSender<T>;
    type Receiver = MaybeCloneOneshotReceiver<T>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (tx, rx) = oneshot::channel();

        #[cfg(test)]
        let (test_tx, test_rx) = broadcast::channel::<T>(1);

        (
            MaybeCloneOneshotSender {
                inner: tx,
                #[cfg(test)]
                test_inner: test_tx,
            },
            MaybeCloneOneshotReceiver {
                inner: rx,
                #[cfg(test)]
                test_inner: test_rx,
            },
        )
    }
}
