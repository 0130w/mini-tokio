use std::{
    pin::Pin,
    sync::{Arc, Mutex, mpsc},
    task::Poll,
};

use futures::task::ArcWake;

struct Computation {}

impl Future for Computation {
    type Output = u32;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {}
}

struct TaskFuture<Out> {
    // In Rust, future is computation itself, poll is the way of progressing computation
    future: Pin<Box<dyn Future<Output = Out> + Send>>,
    // Used for handling spurious wakeup
    poll: Poll<()>,
}

impl<Out> TaskFuture<Out> {
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = Out> + Send + 'static,
    {
        TaskFuture {
            future: Box::pin(future),
            poll: Poll::Pending,
        }
    }
}

struct Task<Out> {
    task_future: Mutex<TaskFuture<Out>>,    // impl Send
    executor: mpsc::Sender<Arc<Task<Out>>>, // associated task executor
}

impl<Out> ArcWake for Task<Out> {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.scheduled();
    }
}

impl<Out> Task<Out> {
    fn scheduled(self: &Arc<Self>) {
        self.executor.send(self.clone());
    }

    fn poll(self: Arc<Self>) {}
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

    pub fn spawn<F, Out>(&mut self, future: F)
    where
        F: Future<Output = Out> + Send + 'static,
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
    let computation = Computation {};
    mini_tokio.spawn(computation);
    mini_tokio.run();
}
