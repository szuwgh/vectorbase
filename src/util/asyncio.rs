use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc as StdArc, Mutex as StdMutex};
use std::task::{Context, Poll, Waker};

unsafe impl<T> Send for UnbufferedSender<T> {}
unsafe impl<T> Sync for UnbufferedSender<T> {}

pub(crate) struct UnbufferedSender<T> {
    sender_semaphore: Arc<Semaphore>,
    sender: Sender<T>,
}

unsafe impl<T> Send for UnbufferedReceiver<T> {}
unsafe impl<T> Sync for UnbufferedReceiver<T> {}

pub(crate) struct UnbufferedReceiver<T> {
    sender_semaphore: Arc<Semaphore>,
    receiver: Mutex<Receiver<T>>,
}

pub(crate) fn un_buffered_channel<T>() -> (UnbufferedSender<T>, UnbufferedReceiver<T>) {
    let (sender, receiver) = mpsc::channel(1);
    let sender_semaphore = Arc::new(Semaphore::new(0));
    (
        UnbufferedSender {
            sender_semaphore: sender_semaphore.clone(),
            sender: sender,
        },
        UnbufferedReceiver {
            sender_semaphore: sender_semaphore.clone(),
            receiver: Mutex::new(receiver),
        },
    )
}

impl<T> UnbufferedSender<T> {
    pub(crate) async fn send(&self, value: T) {
        self.sender.send(value).await.unwrap();
        self.sender_semaphore.acquire().await.unwrap().forget();
    }

    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(value)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl<T> UnbufferedReceiver<T> {
    pub(crate) async fn recv(&self) -> Option<T> {
        self.sender_semaphore.add_permits(1);
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await
    }

    pub(crate) fn is_closed(&self) -> bool {
        let receiver = self.receiver.try_lock().unwrap();
        receiver.is_closed()
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = self.receiver.try_lock();
        match receiver {
            Ok(mut receiver) => {
                self.sender_semaphore.add_permits(1);
                receiver.try_recv()
            }
            Err(_) => return Err(TryRecvError::Empty),
        }
    }
}

pub struct WaitGroup {
    inner: Arc<Inner>,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::new()),
        }
    }
}

impl WaitGroup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker(&self) -> Worker {
        self.inner.count.fetch_add(1, Ordering::Relaxed);
        Worker {
            inner: self.inner.clone(),
        }
    }

    pub async fn wait(&self) {
        WaitGroupFuture::new(&self.inner).await
    }
}

struct WaitGroupFuture<'a> {
    inner: &'a StdArc<Inner>,
}

impl<'a> WaitGroupFuture<'a> {
    fn new(inner: &'a Arc<Inner>) -> Self {
        Self { inner }
    }
}

impl Future for WaitGroupFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.inner.count.load(Ordering::Relaxed) == 0 {
            return Poll::Ready(());
        }

        let waker = cx.waker().clone();
        *self.inner.waker.lock().unwrap() = Some(waker);

        match self.inner.count.load(Ordering::Relaxed) {
            0 => Poll::Ready(()),
            _ => Poll::Pending,
        }
    }
}

struct Inner {
    waker: StdMutex<Option<Waker>>,
    count: AtomicUsize,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            waker: StdMutex::new(None),
        }
    }
}

/// A worker registered in a `WaitGroup`.
///
/// Refer to the [crate level documentation](crate) for details.
pub struct Worker {
    inner: Arc<Inner>,
}

impl Worker {
    /// Notify the `WaitGroup` that this worker has finished execution.
    pub fn done(self) {
        drop(self)
    }
}

impl Clone for Worker {
    /// Cloning a worker increments the primary reference count and returns a new worker for use in
    /// another task.
    fn clone(&self) -> Self {
        self.inner.count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let count = self.inner.count.fetch_sub(1, Ordering::Relaxed);
        // We are the last worker
        if count == 1 {
            if let Some(waker) = self.inner.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }
}

impl fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count.load(Ordering::Relaxed);
        f.debug_struct("WaitGroup").field("count", &count).finish()
    }
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count.load(Ordering::Relaxed);
        f.debug_struct("Worker").field("count", &count).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use tokio::time;
    use tokio::time::Duration;
    #[test]
    fn test_unbuffer() {
        let rt = Runtime::new().unwrap();

        let (sender, receiver) = un_buffered_channel();

        rt.block_on(async {
            tokio::spawn(async move {
                for i in 0..5 {
                    time::sleep(Duration::from_secs(2)).await;
                    tokio::select! {
                       Some(k) = receiver.recv() => {
                         println!("接收{}", k);
                        }
                    }
                }
            });
            time::sleep(Duration::from_secs(2)).await;
            // 发送一个消息到 mpsc 通道
            for i in 0..5 {
                let _ = sender.send(i).await;
                println!("发送：{}", i);
            }

            time::sleep(Duration::from_secs(10)).await;
        });
    }
}
