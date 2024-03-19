use super::{operator::TaskMessage, worker_thread::WorkerThread};
use crate::system::{Component, ComponentContext, Handler, Receiver, System};
use async_trait::async_trait;
use std::fmt::Debug;

/// The dispatcher is responsible for distributing tasks to worker threads.
/// It is a component that receives tasks and distributes them to worker threads.
/**```
                            ┌─────────────────────────────────────────┐
                            │                                         │
                            │                                         │
                            │                                         │
    TaskMessage ───────────►├─────┐          Dispatcher               │
                            │     ▼                                   │
                            │    ┌┬───────────────────────────────┐   │
                            │    │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│   │
                            └────┴──────────────┴─────────────────┴───┘
                                                ▲
                                                │     │
                                                │     │
                           TaskRequestMessage   │     │  TaskMessage
                                                │     │
                                                │     │
                                                      ▼
                     ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     │     Worker     │   │     Worker     │   │     Worker     │
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     └────────────────┘   └────────────────┘   └────────────────┘
```
# Implementation notes
- The dispatcher has a queue of tasks that it distributes to worker threads
- A worker thread sends a TaskRequestMessage to the dispatcher when it is ready for a new task
- If not task is available for the worker thread, the dispatcher will place that worker's reciever
    in a queue and send a task to the worker when it recieves another one
*/
#[derive(Debug)]
struct Dispatcher {
    task_queue: Vec<TaskMessage>,
    waiters: Vec<TaskRequestMessage>,
    n_worker_threads: usize,
}

impl Dispatcher {
    /// Create a new dispatcher
    /// # Parameters
    /// - n_worker_threads: The number of worker threads to use
    pub fn new(n_worker_threads: usize) -> Self {
        Dispatcher {
            task_queue: Vec::new(),
            waiters: Vec::new(),
            n_worker_threads,
        }
    }

    fn spawn_workers(
        &self,
        system: &mut System,
        self_receiver: Box<dyn Receiver<TaskRequestMessage>>,
    ) {
        for _ in 0..self.n_worker_threads {
            let worker = WorkerThread::new(self_receiver.clone());
            system.start_component(worker);
        }
    }

    async fn enqueue_task(&mut self, task: TaskMessage) {
        // If a worker is waiting for a task, send it to the worker in FIFO order
        // Otherwise, add it to the task queue
        match self.waiters.pop() {
            Some(channel) => match channel.reply_to.send(task).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error sending task to worker: {:?}", e);
                }
            },
            None => {
                self.task_queue.push(task);
            }
        }
    }

    async fn handle_work_request(&mut self, worker: TaskRequestMessage) {
        match self.task_queue.pop() {
            Some(task) => match worker.reply_to.send(task).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error sending task to worker: {:?}", e);
                }
            },
            None => {
                self.waiters.push(worker);
            }
        }
    }
}

/// A message that a worker thread sends to the dispatcher to request a task
#[derive(Debug)]
pub(super) struct TaskRequestMessage {
    reply_to: Box<dyn Receiver<TaskMessage>>,
}

impl TaskRequestMessage {
    /// Create a new TaskRequestMessage
    /// # Parameters
    /// - reply_to: The receiver to send the task to, this is the worker thread
    /// that is requesting the task
    pub(super) fn new(reply_to: Box<dyn Receiver<TaskMessage>>) -> Self {
        TaskRequestMessage { reply_to }
    }
}

#[async_trait]
impl Component for Dispatcher {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        self.spawn_workers(&mut ctx.system.clone(), ctx.sender.as_receiver());
    }
}

#[async_trait]
impl Handler<TaskMessage> for Dispatcher {
    async fn handle(&mut self, task: TaskMessage, _ctx: &ComponentContext<Dispatcher>) {
        self.enqueue_task(task).await;
    }
}

// Worker sends a request for task
#[async_trait]
impl Handler<TaskRequestMessage> for Dispatcher {
    async fn handle(&mut self, message: TaskRequestMessage, _ctx: &ComponentContext<Dispatcher>) {
        self.handle_work_request(message).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::operator::{wrap, Operator},
        system::System,
    };

    #[derive(Debug)]
    struct MockOperator {}
    #[async_trait]
    impl Operator<f32, String> for MockOperator {
        async fn run(&self, input: &f32) -> String {
            println!(
                "Running MockOperator on thread {:?}",
                std::thread::current().id()
            );
            // sleep to simulate work
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            input.to_string()
        }
    }

    #[derive(Debug)]
    struct MockDispatchUser {
        pub dispatcher: Box<dyn Receiver<TaskMessage>>,
    }
    #[async_trait]
    impl Component for MockDispatchUser {
        fn queue_size(&self) -> usize {
            1000 // TODO: make configurable
        }

        async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
            let task = wrap(Box::new(MockOperator {}), 42.0, ctx.sender.as_receiver());
            let res = self.dispatcher.send(task).await;
            // TODO: handle error
            // dispatch a new task every 100ms, 10 times
            let duration = std::time::Duration::from_millis(100);
            ctx.scheduler
                .schedule_interval(ctx.sender.clone(), (), duration, Some(20), ctx);
        }
    }
    #[async_trait]
    impl Handler<String> for MockDispatchUser {
        async fn handle(&mut self, message: String, _ctx: &ComponentContext<MockDispatchUser>) {
            println!(
                "Received message: {} on thread {:?}",
                message,
                std::thread::current().id()
            );
        }
    }
    #[async_trait]
    impl Handler<()> for MockDispatchUser {
        async fn handle(&mut self, message: (), ctx: &ComponentContext<MockDispatchUser>) {
            let task = wrap(Box::new(MockOperator {}), 42.0, ctx.sender.as_receiver());
            let res = self.dispatcher.send(task).await;
        }
    }

    #[tokio::test]
    async fn test_dispatcher() {
        let mut system = System::new();
        let dispatcher = Dispatcher::new(24);
        let dispatcher_handle = system.start_component(dispatcher);
        let dispatch_user = MockDispatchUser {
            dispatcher: dispatcher_handle.receiver(),
        };
        let dispatch_user_handle = system.start_component(dispatch_user);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // sleep for a bit to allow the dispatcher to process the message, TODO: change to a join that waits for all messages to be consumed
        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
    }
}
