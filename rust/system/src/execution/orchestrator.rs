use crate::{
    ChannelError, CleanupGuard, Component, ComponentContext, ComponentHandle, PanicError, System,
};
use async_trait::async_trait;
use chroma_error::ChromaError;
use core::fmt::Debug;
use std::any::type_name;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::{Dispatcher, TaskMessage};

#[derive(Debug)]
pub struct OrchestratorContext {
    pub dispatcher: ComponentHandle<Dispatcher>,

    // Used to cancel all spawned tasks.
    pub task_cancellation_token: CancellationToken,
}

impl OrchestratorContext {
    pub fn new(dispatcher: ComponentHandle<Dispatcher>) -> Self {
        Self {
            dispatcher,
            task_cancellation_token: CancellationToken::new(),
        }
    }
}

impl Drop for OrchestratorContext {
    fn drop(&mut self) {
        self.task_cancellation_token.cancel();
    }
}

#[async_trait]
pub trait Orchestrator: Debug + Send + Sized + 'static {
    type Output: Send;
    type Error: ChromaError + From<PanicError> + From<ChannelError> + From<RecvError>;

    /// Returns the handle of the dispatcher
    fn dispatcher(&self) -> ComponentHandle<Dispatcher>;

    fn context(&self) -> &OrchestratorContext;

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

        // We want to let the orchestrator clean up even if this future is dropped before
        // the orchestrator is finished.
        let _cleanup_guard = CleanupGuard::new(move || handle.stop());
        let res = rx.await;
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
    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>>;

    async fn default_terminate_with_result(
        &mut self,
        res: Result<Self::Output, Self::Error>,
        ctx: &ComponentContext<Self>,
    ) {
        let cancel = if let Err(err) = &res {
            if err.should_trace_error() {
                tracing::error!("Error running {}: {}", Self::name(), err);
            }
            true
        } else {
            false
        };

        let channel = self.take_result_channel();
        match channel {
            Some(channel) => {
                if channel.send(res).is_err() {
                    tracing::error!("Error sending result for {}", Self::name());
                }
            }
            None => {
                tracing::error!(
                    "No result channel set for {}. Cannot send result.",
                    Self::name()
                );
            }
        }

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
            if err.should_trace_error() {
                tracing::error!("Error running {}: {}", Self::name(), err);
            }
            true
        } else {
            false
        };

        let channel = self.take_result_channel();
        match channel {
            Some(channel) => {
                if channel.send(res).is_err() {
                    tracing::error!("Error sending result for {}", Self::name());
                }
            }
            None => {
                tracing::error!(
                    "No result channel set for {}. Cannot send result.",
                    Self::name()
                );
            }
        }

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

        match channel {
            Some(channel) => {
                if channel.send(Err(error.into())).is_err() {
                    tracing::error!("Error reporting panic to {}", Self::name());
                }
            }
            None => {
                tracing::error!(
                    "No result channel set for {}. Cannot report panic.",
                    Self::name()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::operator::{wrap, Operator, TaskResult},
        types::Handler,
        DispatcherConfig, ReceiverForMessage,
    };
    use async_trait::async_trait;
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Debug)]
    struct SleepingOperator {}

    #[async_trait]
    impl Operator<(), ()> for SleepingOperator {
        type Error = TestError;

        async fn run(&self, _: &()) -> Result<(), Self::Error> {
            // Sleep forever (or until cancelled)
            sleep(Duration::MAX).await;
            unreachable!("Should've been sleeping!")
        }
    }

    #[derive(Debug)]
    struct TestOrchestrator {
        context: OrchestratorContext,
        result_channel: Option<Sender<Result<(), TestError>>>,
        num_tasks: usize,
    }

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("Channel error: {0}")]
        Channel(#[from] ChannelError),
        #[error("Panic: {0}")]
        Panic(#[from] PanicError),
        #[error("Recv error: {0}")]
        Recv(#[from] RecvError),
        #[error("IO error: {0}")]
        Io(#[from] std::io::Error),
    }

    impl ChromaError for TestError {
        fn code(&self) -> chroma_error::ErrorCodes {
            chroma_error::ErrorCodes::Internal
        }
    }

    #[async_trait]
    impl Handler<TaskResult<(), TestError>> for TestOrchestrator {
        type Result = ();

        async fn handle(
            &mut self,
            message: TaskResult<(), TestError>,
            _ctx: &ComponentContext<Self>,
        ) -> Self::Result {
            message.result.unwrap();
        }
    }

    #[async_trait]
    impl Orchestrator for TestOrchestrator {
        type Output = ();
        type Error = TestError;

        fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
            self.context.dispatcher.clone()
        }

        fn context(&self) -> &OrchestratorContext {
            &self.context
        }

        async fn initial_tasks(
            &mut self,
            ctx: &ComponentContext<Self>,
        ) -> Vec<(TaskMessage, Option<Span>)> {
            let mut tasks = Vec::new();
            for _ in 0..self.num_tasks {
                let operator = SleepingOperator {};
                let task = wrap(
                    Box::new(operator),
                    (),
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                );
                tasks.push((task, None));
            }
            tasks
        }

        fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
            self.result_channel = Some(sender);
        }

        fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
            self.result_channel.take()
        }
    }

    #[derive(Debug)]
    struct SimpleOperator {}

    #[async_trait]
    impl Operator<(), ()> for SimpleOperator {
        type Error = TestError;

        async fn run(&self, _: &()) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TestReceiver<M> {
        sender: tokio::sync::mpsc::Sender<M>,
    }

    // Cannot automatically derive, see https://github.com/rust-lang/rust/issues/26925
    impl<M> Clone for TestReceiver<M> {
        fn clone(&self) -> Self {
            TestReceiver {
                sender: self.sender.clone(),
            }
        }
    }

    #[async_trait]
    impl<M: Debug + Send + 'static> ReceiverForMessage<M> for TestReceiver<M> {
        async fn send(
            &self,
            message: M,
            _: Option<tracing::Span>,
        ) -> Result<(), crate::ChannelError> {
            self.sender
                .send(message)
                .await
                .map_err(|error| crate::ChannelError::SendError(error.to_string()))
        }
    }

    #[tokio::test]
    async fn test_operator_cancellation() {
        let system = System::new();
        let num_workers = 2;

        // Create a dispatcher with a small number of worker threads
        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: num_workers,
            task_queue_limit: 1,
            dispatcher_queue_size: 1,
            worker_queue_size: 1,
            active_io_tasks: 10,
        });
        let dispatcher_handle = system.start_component(dispatcher);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<TaskResult<(), TestError>>(2);
        let test_receiver: Box<dyn ReceiverForMessage<TaskResult<(), TestError>>> =
            Box::new(TestReceiver { sender: tx });
        let task = wrap(
            Box::new(SimpleOperator {}),
            (),
            test_receiver,
            CancellationToken::new(),
        );

        // Check dispatcher state through a request
        println!("Checking dispatcher state");
        let _ = dispatcher_handle.request(task, None).await;
        match rx.recv().await.unwrap().into_inner() {
            Ok(_) => {}
            Err(err) => panic!(
                " Attached Function should have finished - workers should be cancelled {:?}",
                err
            ),
        }

        // Stop the system and verify cleanup
        system.stop().await;
        system.join().await;
    }
}
