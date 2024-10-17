use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, Semaphore};

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
        if receiver.is_closed() {
            println!("读通道已关闭");
        }
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
