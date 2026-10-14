use std::{
    pin::Pin,
    sync::{Arc, Mutex, mpsc},
    task::{Context, Poll},
};

use futures::task::ArcWake;

struct Computation {}

impl Future for Computation {
    type Output = u32;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        unimplemented!()
    }
}

struct TaskFuture<Out>
where
    Out: Send,
{
    // In Rust, future is computation itself, poll is the way of progressing computation
    future: Pin<Box<dyn Future<Output = Out> + Send>>,
    // Used for handling spurious wakeup
    poll: Poll<Out>,
}

impl<Out> TaskFuture<Out>
where
    Out: Send,
{
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

struct Task<Out>
where
    Out: Send,
{
    task_future: Mutex<TaskFuture<Out>>,    // impl Send
    executor: mpsc::Sender<Arc<Task<Out>>>, // associated task executor
}

impl<Out> ArcWake for Task<Out>
where
    Out: Send,
{
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.scheduled();
    }
}

impl<Out> Task<Out>
where
    Out: Send,
{
    fn scheduled(self: &Arc<Self>) {
        self.executor.send(self.clone());
    }

    fn poll(self: Arc<Self>, cx: &mut Context<'_>) {
        if let Ok(mut holding_task_future) = self.task_future.try_lock() {
            // acquire lock
            if holding_task_future.poll.is_pending() {
                holding_task_future.poll = holding_task_future.future.as_mut().poll(cx);
            }
        }
    }
}

struct MiniTokio<Out>
where
    Out: Send,
{
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
