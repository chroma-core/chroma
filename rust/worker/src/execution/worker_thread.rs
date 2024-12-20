use super::{dispatcher::TaskRequestMessage, operator::TaskMessage};
use crate::system::{Component, ComponentContext, ComponentRuntime, Handler, ReceiverForMessage};
use async_trait::async_trait;
use std::fmt::{Debug, Formatter, Result};
use tracing::{trace_span, Instrument, Span};

/// A worker thread is responsible for executing tasks
/// It sends requests to the dispatcher for new tasks.
/// # Implementation notes
/// - The actor loop will block until work is available
pub(super) struct WorkerThread {
    dispatcher: Box<dyn ReceiverForMessage<TaskRequestMessage>>,
    queue_size: usize,
}

impl WorkerThread {
    pub(super) fn new(
        dispatcher: Box<dyn ReceiverForMessage<TaskRequestMessage>>,
        queue_size: usize,
    ) -> WorkerThread {
        WorkerThread {
            dispatcher,
            queue_size,
        }
    }
}

impl Debug for WorkerThread {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("WorkerThread").finish()
    }
}

#[async_trait]
impl Component for WorkerThread {
    fn get_name() -> &'static str {
        "Worker thread"
    }

    fn queue_size(&self) -> usize {
        self.queue_size
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Dedicated
    }

    async fn start(&mut self, ctx: &ComponentContext<Self>) {
        let req = TaskRequestMessage::new(ctx.receiver());
        let _req = self.dispatcher.send(req, None).await;
        // TODO: what to do with resp?
    }
}

#[async_trait]
impl Handler<TaskMessage> for WorkerThread {
    type Result = ();

    async fn handle(&mut self, task: TaskMessage, ctx: &ComponentContext<WorkerThread>) {
        let child_span =
            trace_span!(parent: Span::current(), "Task execution", name = task.get_name());
        task.run().instrument(child_span).await;
        let req: TaskRequestMessage = TaskRequestMessage::new(ctx.receiver());
        let _res = self.dispatcher.send(req, None).await;
        // TODO: task run should be able to error and we should send it as part of the result
    }
}
