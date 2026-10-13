use std::{
    pin::Pin,
    sync::{Arc, mpsc},
};

// future is computation itself, poll is the way of progressing computation

fn main() {
    let mut mini_tokio = MiniTokio::new();
    mini_tokio.run();
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

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
        F: Future<Output = ()> + Send + 'static,
    {
    }

    pub fn run(&mut self) {
        while let Some(mut task) = self.tasks.pop_front() {
            if task.as_mut().poll(cx).is_pending() {
                self.tasks.push_back(task);
            }
        }
    }
}
