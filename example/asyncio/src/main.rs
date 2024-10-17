use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, Semaphore};
use tokio::time;
use tokio::time::Duration;
pub(crate) struct UnbufferedSender<T> {
    sender_semaphore: Arc<Semaphore>,
    sender: Sender<T>,
}

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
}

impl<T> UnbufferedReceiver<T> {
    pub(crate) async fn recv(&self) -> Option<T> {
        self.sender_semaphore.add_permits(1);
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await
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
fn main() {
    let rt = Runtime::new().unwrap();

    let (sender, receiver) = un_buffered_channel();

    rt.block_on(async {
        tokio::spawn(async move {
            // 发送一个消息到 mpsc 通道
            for i in 0..5 {
                time::sleep(Duration::from_secs(2)).await;
                let _ = sender.send(i).await;
                println!("发送：{}", i);
            }
        });

        time::sleep(Duration::from_secs(2)).await;

        loop {
            tokio::select! {
               Some(k) = receiver.recv() => {
                 println!("接收{}", k);
                }
            }
        }
    });
}
