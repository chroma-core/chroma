use super::{dispatcher::TaskRequestMessage, operator::TaskMessage};
use crate::{Component, ComponentContext, ComponentRuntime, Handler, ReceiverForMessage};
use async_trait::async_trait;
use std::{
    fmt::{Debug, Formatter, Result},
    time::Duration,
};
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

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let req = TaskRequestMessage::new(ctx.receiver());
        let _req = self.dispatcher.send(req, None).await;
        // TODO: what to do with resp?
    }
}

#[async_trait]
impl Handler<TaskMessage> for WorkerThread {
    type Result = ();

    async fn handle(&mut self, mut task: TaskMessage, ctx: &ComponentContext<WorkerThread>) {
        tracing::info!("Worker thread: executing task {}", task.get_name());
        let child_span =
            trace_span!(parent: Span::current(), "Task execution", name = task.get_name());
        let task_timeout = Duration::from_secs(15);
        let (mark_done_tx, mut mark_done_rx) = tokio::sync::oneshot::channel();
        let task_name = task.get_name().to_string();
        tokio::spawn(async move {
            tokio::time::sleep(task_timeout).await;
            let attempted_recv = mark_done_rx.try_recv();
            match attempted_recv {
                Ok(_) => {
                    tracing::info!("Task {} completed", task_name)
                }
                Err(e) => match e {
                    tokio::sync::oneshot::error::TryRecvError::Empty => {
                        tracing::info!("Task {} timed out", task_name);
                    }
                    tokio::sync::oneshot::error::TryRecvError::Closed => {
                        tracing::error!("Never got confirmation for task {}", task_name);
                    }
                },
            };
        });
        task.run().instrument(child_span).await;
        mark_done_tx.send(()).unwrap_or_else(|_| {
            tracing::error!("Failed to send task completion signal");
        });
        let req: TaskRequestMessage = TaskRequestMessage::new(ctx.receiver());
        let res = self.dispatcher.send(req, None).await;
        if let Err(err) = res {
            tracing::error!("Error sending task request: {}", err);
        }
        // TODO: task run should be able to error and we should send it as part of the result
    }
}
