use super::{operator::TaskMessage, worker_thread::WorkerThread};
use crate::{
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
    system::{Component, ComponentContext, Handler, Receiver, System},
};
use async_trait::async_trait;
use std::fmt::Debug;

/// The dispatcher is responsible for distributing tasks to worker threads.
/// It is a component that receives tasks and distributes them to worker threads.
/**
```plaintext
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
## Implementation notes
- The dispatcher has a queue of tasks that it distributes to worker threads
- A worker thread sends a TaskRequestMessage to the dispatcher when it is ready for a new task
- If no task is available for the worker thread, the dispatcher will place that worker's reciever
    in a queue and send a task to the worker when it recieves another one
- The reason to introduce this abstraction is to allow us to control fairness and dynamically adjust
  system utilization. It also makes mechanisms like pausing/stopping work easier.
  It would have likely been more performant to use the Tokio MT runtime, but we chose to use
  this abstraction to grant us flexibility. We can always switch to Tokio MT later if we need to,
  or make this dispatcher much more performant through implementing memory-awareness, task-batches,
  coarser work-stealing, and other optimizations.
*/
#[derive(Debug)]
pub(crate) struct Dispatcher {
    task_queue: Vec<TaskMessage>,
    waiters: Vec<TaskRequestMessage>,
    n_worker_threads: usize,
    queue_size: usize,
    worker_queue_size: usize,
}

impl Dispatcher {
    /// Create a new dispatcher
    /// # Parameters
    /// - n_worker_threads: The number of worker threads to use
    /// - queue_size: The size of the components message queue
    /// - worker_queue_size: The size of the worker components queue
    pub fn new(n_worker_threads: usize, queue_size: usize, worker_queue_size: usize) -> Self {
        Dispatcher {
            task_queue: Vec::new(),
            waiters: Vec::new(),
            n_worker_threads,
            queue_size,
            worker_queue_size,
        }
    }

    /// Spawn worker threads
    /// # Parameters
    /// - system: The system to spawn the worker threads in
    /// - self_receiver: The receiver to send tasks to the worker threads, this is a address back to the dispatcher
    fn spawn_workers(
        &self,
        system: &mut System,
        self_receiver: Box<dyn Receiver<TaskRequestMessage>>,
    ) {
        for _ in 0..self.n_worker_threads {
            let worker = WorkerThread::new(self_receiver.clone(), self.worker_queue_size);
            system.start_component(worker);
        }
    }

    /// Enqueue a task to be processed
    /// # Parameters
    /// - task: The task to enqueue
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

    /// Handle a work request from a worker thread
    /// # Parameters
    /// - worker: The request for work
    /// If no work is available, the worker will be placed in a queue and a task will be sent to it
    /// when one is available
    async fn handle_work_request(&mut self, request: TaskRequestMessage) {
        match self.task_queue.pop() {
            Some(task) => match request.reply_to.send(task).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error sending task to worker: {:?}", e);
                }
            },
            None => {
                self.waiters.push(request);
            }
        }
    }
}

#[async_trait]
impl Configurable for Dispatcher {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Dispatcher::new(
            worker_config.dispatcher.num_worker_threads,
            worker_config.dispatcher.dispatcher_queue_size,
            worker_config.dispatcher.worker_queue_size,
        ))
    }
}

/// A message that a worker thread sends to the dispatcher to request a task
/// # Members
/// - reply_to: The receiver to send the task to, this is the worker thread
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

// ============= Component implementation =============

#[async_trait]
impl Component for Dispatcher {
    fn queue_size(&self) -> usize {
        self.queue_size
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    // Create a component that will schedule DISPATCH_COUNT invocations of the MockOperator
    // on an interval of DISPATCH_FREQUENCY_MS.
    // Each invocation will sleep for MOCK_OPERATOR_SLEEP_DURATION_MS to simulate work
    // Use THREAD_COUNT worker threads
    const MOCK_OPERATOR_SLEEP_DURATION_MS: u64 = 100;
    const DISPATCH_FREQUENCY_MS: u64 = 5;
    const DISPATCH_COUNT: usize = 50;
    const THREAD_COUNT: usize = 4;

    #[derive(Debug)]
    struct MockOperator {}
    #[async_trait]
    impl Operator<f32, String> for MockOperator {
        async fn run(&self, input: &f32) -> String {
            // sleep to simulate work
            tokio::time::sleep(tokio::time::Duration::from_millis(
                MOCK_OPERATOR_SLEEP_DURATION_MS,
            ))
            .await;
            input.to_string()
        }
    }

    #[derive(Debug)]
    struct MockDispatchUser {
        pub dispatcher: Box<dyn Receiver<TaskMessage>>,
        counter: Arc<AtomicUsize>, // We expect to recieve DISPATCH_COUNT messages
    }
    #[async_trait]
    impl Component for MockDispatchUser {
        fn queue_size(&self) -> usize {
            1000
        }

        async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
            // dispatch a new task every DISPATCH_FREQUENCY_MS for DISPATCH_COUNT times
            let duration = std::time::Duration::from_millis(DISPATCH_FREQUENCY_MS);
            ctx.scheduler.schedule_interval(
                ctx.sender.clone(),
                (),
                duration,
                Some(DISPATCH_COUNT),
                ctx,
            );
        }
    }
    #[async_trait]
    impl Handler<String> for MockDispatchUser {
        async fn handle(&mut self, message: String, ctx: &ComponentContext<MockDispatchUser>) {
            self.counter.fetch_add(1, Ordering::SeqCst);
            let curr_count = self.counter.load(Ordering::SeqCst);
            // Cancel self
            if curr_count == DISPATCH_COUNT {
                ctx.cancellation_token.cancel();
            }
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
        let dispatcher = Dispatcher::new(THREAD_COUNT, 1000, 1000);
        let dispatcher_handle = system.start_component(dispatcher);
        let counter = Arc::new(AtomicUsize::new(0));
        let dispatch_user = MockDispatchUser {
            dispatcher: dispatcher_handle.receiver(),
            counter: counter.clone(),
        };
        let mut dispatch_user_handle = system.start_component(dispatch_user);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // Join on the dispatch user, since it will kill itself after DISPATCH_COUNT messages
        dispatch_user_handle.join().await;
        // We should have received DISPATCH_COUNT messages
        assert_eq!(counter.load(Ordering::SeqCst), DISPATCH_COUNT);
    }
}
