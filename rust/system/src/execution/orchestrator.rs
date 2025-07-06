use crate::{ChannelError, Component, ComponentContext, ComponentHandle, PanicError, System};
use async_trait::async_trait;
use chroma_error::ChromaError;
use core::fmt::Debug;
use std::any::type_name;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tracing::Span;

use crate::{Dispatcher, TaskMessage};

#[async_trait]
pub trait Orchestrator: Debug + Send + Sized + 'static {
    type Output: Send;
    type Error: ChromaError + From<PanicError> + From<ChannelError> + From<RecvError>;

    /// Returns the handle of the dispatcher
    fn dispatcher(&self) -> ComponentHandle<Dispatcher>;

    /// Returns a vector of starting tasks that should be run in sequence
    async fn initial_tasks(
        &mut self,
        _ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        vec![]
    }

    async fn on_start(&mut self, _ctx: &ComponentContext<Self>) {}

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
    async fn send(
        &mut self,
        task: TaskMessage,
        ctx: &ComponentContext<Self>,
        tracing_context: Option<Span>,
    ) -> bool {
        let res = self.dispatcher().send(task, tracing_context).await;
        self.ok_or_terminate(res, ctx).await.is_some()
    }

    /// Sets the result channel of the orchestrator
    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>);

    /// Takes the result channel of the orchestrator. The channel should have been set when this is invoked
    fn take_result_channel(&mut self) -> Sender<Result<Self::Output, Self::Error>>;

    async fn default_terminate_with_result(
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

    async fn cleanup(&mut self) {
        // Default cleanup does nothing
    }

    /// Terminate the orchestrator with a result
    /// Ideally no types that implement this trait should
    /// need to override this method.
    async fn terminate_with_result(
        &mut self,
        res: Result<Self::Output, Self::Error>,
        ctx: &ComponentContext<Self>,
    ) {
        self.cleanup().await;
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
    async fn ok_or_terminate<O: Send, E: Into<Self::Error> + Send>(
        &mut self,
        res: Result<O, E>,
        ctx: &ComponentContext<Self>,
    ) -> Option<O> {
        match res {
            Ok(output) => Some(output),
            Err(error) => {
                self.terminate_with_result(Err(error.into()), ctx).await;
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

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        for (task, tracing_context) in self.initial_tasks(ctx).await {
            if !self.send(task, ctx, tracing_context).await {
                break;
            }
        }

        self.on_start(ctx).await;
    }

    fn on_handler_panic(&mut self, panic_value: Box<dyn std::any::Any + Send>) {
        let channel = self.take_result_channel();
        let error = PanicError::new(panic_value);

        if channel.send(Err(O::Error::from(error))).is_err() {
            tracing::error!("Error reporting panic to {}", Self::name());
        };
    }
}
