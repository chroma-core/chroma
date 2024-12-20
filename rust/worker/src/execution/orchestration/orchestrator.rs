use core::fmt::Debug;
use std::any::type_name;

use async_trait::async_trait;
use chroma_error::ChromaError;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tracing::Span;

use crate::{
    execution::{dispatcher::Dispatcher, operator::TaskMessage},
    system::{ChannelError, Component, ComponentContext, ComponentHandle, System},
};

#[async_trait]
pub trait Orchestrator: Debug + Send + Sized + 'static {
    type Output: Send;
    type Error: ChromaError + From<ChannelError> + From<RecvError>;

    /// Returns the handle of the dispatcher
    fn dispatcher(&self) -> ComponentHandle<Dispatcher>;

    /// Returns a vector of starting tasks that should be run in sequence
    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage>;

    fn name() -> &'static str {
        type_name::<Self>()
    }

    fn queue_size(&self) -> usize {
        1000
    }

    /// Runs the orchestrator in a system and returns the result
    async fn run(mut self, system: System) -> Result<Self::Output, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.set_result_channel(tx);
        let mut handle = system.start_component(self);
        let res = rx.await;
        handle.stop();
        res?
    }

    /// Sends a task to the dispatcher and return whether the task is successfully sent
    async fn send(&mut self, task: TaskMessage, ctx: &ComponentContext<Self>) -> bool {
        let res = self.dispatcher().send(task, Some(Span::current())).await;
        self.ok_or_terminate(res, ctx).is_some()
    }

    /// Sets the result channel of the orchestrator
    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>);

    /// Takes the result channel of the orchestrator. The channel should have been set when this is invoked
    fn take_result_channel(&mut self) -> Sender<Result<Self::Output, Self::Error>>;

    /// Terminate the orchestrator with a result
    fn terminate_with_result(
        &mut self,
        res: Result<Self::Output, Self::Error>,
        ctx: &ComponentContext<Self>,
    ) {
        let cancel = if let Err(err) = &res {
            tracing::error!("Error running {}: {}", Self::name(), err);
            true
        } else {
            false
        };

        let channel = self.take_result_channel();
        if channel.send(res).is_err() {
            tracing::error!("Error sending result for {}", Self::name());
        };

        if cancel {
            ctx.cancellation_token.cancel();
        }
    }

    /// Terminate the orchestrator if the result is an error. Returns the output if any.
    fn ok_or_terminate<O, E: Into<Self::Error>>(
        &mut self,
        res: Result<O, E>,
        ctx: &ComponentContext<Self>,
    ) -> Option<O> {
        match res {
            Ok(output) => Some(output),
            Err(error) => {
                self.terminate_with_result(Err(error.into()), ctx);
                None
            }
        }
    }
}

#[async_trait]
impl<O: Orchestrator> Component for O {
    fn get_name() -> &'static str {
        Self::name()
    }

    fn queue_size(&self) -> usize {
        self.queue_size()
    }

    async fn start(&mut self, ctx: &ComponentContext<Self>) {
        for task in self.initial_tasks(ctx) {
            if !self.send(task, ctx).await {
                break;
            }
        }
    }
}
