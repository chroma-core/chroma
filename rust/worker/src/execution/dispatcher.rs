use super::{operator::TaskMessage, worker_thread::WorkerThread};
use crate::system::{Component, ComponentContext, Handler, Receiver};
use async_trait::async_trait;
use std::fmt::Debug;

#[derive(Debug)]
struct Dispatcher {
    task_queue: Vec<TaskMessage>,
    waiter_channels: Vec<TaskRequestMessage>,
    n_worker_threads: usize,
}

impl Dispatcher {
    pub fn new(n_worker_threads: usize) -> Self {
        Dispatcher {
            task_queue: Vec::new(),
            waiter_channels: Vec::new(),
            n_worker_threads,
        }
    }
}

#[async_trait]
impl Component for Dispatcher {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        for _ in 0..self.n_worker_threads {
            let worker = WorkerThread::new(ctx.sender.as_receiver());
            // TODO: it is a bit ugly that we have to clone system, but its cheap to clone
            // we can make this better later
            ctx.system.clone().start_component(worker);
        }
    }
}

#[derive(Debug)]
pub(super) struct TaskRequestMessage {
    reply_to: Box<dyn Receiver<TaskMessage>>,
}

impl TaskRequestMessage {
    pub(super) fn new(reply_to: Box<dyn Receiver<TaskMessage>>) -> Self {
        TaskRequestMessage { reply_to }
    }
}

// Orchestrator will send task here
#[async_trait]
impl Handler<TaskMessage> for Dispatcher {
    async fn handle(&mut self, task: TaskMessage, _ctx: &ComponentContext<Dispatcher>) {
        self.task_queue.push(task);
        if let Some(channel) = self.waiter_channels.pop() {
            match self.task_queue.pop() {
                Some(task) => {
                    channel.reply_to.send(task).await;
                }
                None => {}
            }
        }
    }
}

// Worker sends a request for task
#[async_trait]
impl Handler<TaskRequestMessage> for Dispatcher {
    async fn handle(&mut self, message: TaskRequestMessage, _ctx: &ComponentContext<Dispatcher>) {
        match self.task_queue.pop() {
            Some(task) => {
                message.reply_to.send(task).await;
            }
            None => {
                self.waiter_channels.push(message);
            }
        }
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
