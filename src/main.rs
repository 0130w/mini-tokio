use std::{
    pin::Pin,
    sync::{Arc, Mutex, mpsc},
    task::Poll,
};

use futures::task::ArcWake;
use tokio::time::Instant;

struct Computation {
    when: Instant,
}

impl Future for Computation {
    type Output = u32;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let now = Instant::now();
        if now - self.when < tokio::time::Duration::from_micros(100) {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            println!("42");
            Poll::Ready(42)
        }
    }
}

struct TaskFuture {
    // In Rust, future is computation itself, poll is the way of progressing computation
    future: Pin<Box<dyn Future<Output = u32> + Send>>,
    // Used for handling spurious wakeup
    poll: Poll<u32>,
}

impl TaskFuture {
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = u32> + Send + 'static,
    {
        TaskFuture {
            future: Box::pin(future),
            poll: Poll::Pending,
        }
    }
}

struct Task {
    task_future: Mutex<TaskFuture>,    // impl Send
    executor: mpsc::Sender<Arc<Task>>, // associated task executor
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.scheduled();
    }
}

impl Task {
    fn scheduled(self: &Arc<Self>) {
        let _ = self.executor.send(self.clone());
    }

    fn poll(self: &Arc<Self>) {
        let waker = futures::task::waker_ref(self);
        let mut cx = std::task::Context::from_waker(&waker);
        if let Ok(mut holding_task_future) = self.task_future.try_lock() {
            // acquire lock
            if holding_task_future.poll.is_pending() {
                holding_task_future.poll = holding_task_future.future.as_mut().poll(&mut cx);
            }
        }
    }
}

struct MiniTokio {
    scheduled: mpsc::Receiver<Arc<Task>>,
    sender: mpsc::Sender<Arc<Task>>,
}

impl MiniTokio {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        MiniTokio {
            scheduled: receiver,
            sender,
        }
    }

    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = u32> + Send + 'static,
    {
        let task = Arc::new(Task {
            task_future: Mutex::new(TaskFuture::new(future)),
            executor: self.sender.clone(),
        });
        let _ = self.sender.send(task);
    }

    pub fn run(&mut self) {
        while let Ok(task) = self.scheduled.recv() {
            task.poll();
        }
    }
}

fn main() {
    let mut mini_tokio = MiniTokio::new();
    let computation = Computation {
        when: Instant::now(),
    };
    mini_tokio.spawn(computation);
    mini_tokio.run();
}
