use super::{dispatcher::TaskRequestMessage, operator::TaskMessage};
use crate::system::{Component, ComponentContext, ComponentRuntime, Handler, Receiver};
use async_trait::async_trait;
use std::fmt::{Debug, Formatter, Result};

pub(super) struct WorkerThread {
    dispatcher: Box<dyn Receiver<TaskRequestMessage>>,
}

impl WorkerThread {
    pub(super) fn new(dispatcher: Box<dyn Receiver<TaskRequestMessage>>) -> Self {
        WorkerThread { dispatcher }
    }
}

impl Debug for WorkerThread {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("WorkerThread").finish()
    }
}

#[async_trait]
impl Component for WorkerThread {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Dedicated
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) -> () {
        let req = TaskRequestMessage::new(ctx.sender.as_receiver());
        let res = self.dispatcher.send(req).await;
        // TODO: what to do with resp?
    }
}

#[async_trait]
impl Handler<TaskMessage> for WorkerThread {
    async fn handle(&mut self, task: TaskMessage, ctx: &ComponentContext<WorkerThread>) {
        task.run().await;
        let req: TaskRequestMessage = TaskRequestMessage::new(ctx.sender.as_receiver());
        let res = self.dispatcher.send(req).await;
        // TODO: probably schedule a retry if res is an error
    }
}
