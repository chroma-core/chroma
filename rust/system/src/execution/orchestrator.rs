use super::Operator;
use crate::{ChannelError, Component, ComponentContext, ComponentHandle, PanicError, System};
use crate::{Dispatcher, TaskMessage};
use async_trait::async_trait;
use chroma_error::ChromaError;
use core::fmt::Debug;
use parking_lot::Mutex;
use std::any::type_name;
use thiserror::Error;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tracing::Instrument;
use tracing::Span;

#[derive(Debug)]
struct WrappedOrchestratorOperator<O: Orchestrator>(Mutex<Option<O>>, System, Span);

#[derive(Debug, Error)]
#[error("Orchestrator was already consumed")]
pub struct WrappedOrchestratorOperatorError {}

#[async_trait]
impl<O: Orchestrator> Operator<(), O::Output> for WrappedOrchestratorOperator<O>
where
    O: Sync,
    O::Output: Sync,
    O::Error: From<WrappedOrchestratorOperatorError>,
{
    type Error = O::Error;

    fn get_type(&self) -> crate::OperatorType {
        crate::OperatorType::IO
    }

    fn get_name(&self) -> &'static str {
        O::name()
    }

    async fn run(&self, _: &()) -> Result<O::Output, Self::Error> {
        let orchestrator = {
            let mut orchestrator = self.0.lock();
            orchestrator
                .take()
                .ok_or(WrappedOrchestratorOperatorError {})?
        };

        orchestrator
            .run(self.1.clone())
            .instrument(self.2.clone())
            .await
    }
}

#[async_trait]
pub trait Orchestrator: Debug + Send + Sized + 'static {
    type Output: Send;
    type Error: ChromaError + From<PanicError> + From<ChannelError> + From<RecvError>;

    /// Returns the handle of the dispatcher
    fn dispatcher(&self) -> ComponentHandle<Dispatcher>;

    /// Returns a vector of starting tasks that should be run in sequence
    async fn initial_tasks(&mut self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage>;

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

    fn to_operator(self, system: System) -> Box<dyn Operator<(), Self::Output, Error = Self::Error>>
    where
        Self: Sync,
        <Self as Orchestrator>::Output: Sync,
        <Self as Orchestrator>::Error: From<WrappedOrchestratorOperatorError>,
    {
        Box::new(WrappedOrchestratorOperator(
            Mutex::new(Some(self)),
            system,
            Span::current(),
        ))
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
        for task in self.initial_tasks(ctx).await {
            if !self.send(task, ctx).await {
                break;
            }
        }
    }

    fn on_handler_panic(&mut self, panic_value: Box<dyn std::any::Any + Send>) {
        let channel = self.take_result_channel();
        let error = PanicError::new(panic_value);

        if channel.send(Err(O::Error::from(error))).is_err() {
            tracing::error!("Error reporting panic to {}", Self::name());
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wrap;
    use crate::ChannelError;
    use crate::ComponentContext;
    use crate::Handler;
    use crate::System;
    use crate::TaskResult;
    use async_trait::async_trait;
    use thiserror::Error;

    #[derive(Debug, Error)]
    enum TestOrchestratorError {
        #[error("Receive error: {0}")]
        Receive(#[from] RecvError),
        #[error("Channel error: {0}")]
        Channel(#[from] ChannelError),
        #[error("Panic error: {0}")]
        Panic(#[from] PanicError),
        #[error("Wrapped orchestrator error: {0}")]
        OrchestratorWrapper(#[from] WrappedOrchestratorOperatorError),
    }
    impl ChromaError for TestOrchestratorError {
        fn code(&self) -> chroma_error::ErrorCodes {
            chroma_error::ErrorCodes::Internal
        }
    }

    #[derive(Debug)]
    struct TestOrchestrator {
        result_channel: Option<Sender<Result<usize, TestOrchestratorError>>>,
        dispatcher: ComponentHandle<Dispatcher>,
        system: System,
        spawn_self: bool,
    }

    impl TestOrchestrator {
        fn new(dispatcher: ComponentHandle<Dispatcher>, system: System, spawn_self: bool) -> Self {
            Self {
                result_channel: None,
                dispatcher,
                system,
                spawn_self,
            }
        }
    }

    #[derive(Debug)]
    struct TestMessageRequest;

    #[async_trait]
    impl Orchestrator for TestOrchestrator {
        type Output = usize;
        type Error = TestOrchestratorError;

        fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
            self.dispatcher.clone()
        }
        async fn initial_tasks(&mut self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
            if self.spawn_self {
                let task = wrap(
                    TestOrchestrator::new(self.dispatcher.clone(), self.system.clone(), false)
                        .to_operator(self.system.clone()),
                    (),
                    ctx.receiver(),
                );

                return vec![task];
            }

            ctx.receiver()
                .send(TestMessageRequest {}, None)
                .await
                .unwrap();

            vec![]
        }
        fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
            self.result_channel = Some(sender);
        }
        fn take_result_channel(&mut self) -> Sender<Result<Self::Output, Self::Error>> {
            self.result_channel.take().unwrap()
        }
    }

    #[async_trait]
    impl Handler<TestMessageRequest> for TestOrchestrator {
        type Result = ();

        async fn handle(
            &mut self,
            _: TestMessageRequest,
            ctx: &ComponentContext<TestOrchestrator>,
        ) {
            self.default_terminate_with_result(Ok(42), ctx).await;
        }
    }

    #[async_trait]
    impl Handler<TaskResult<usize, TestOrchestratorError>> for TestOrchestrator {
        type Result = ();

        async fn handle(
            &mut self,
            result: TaskResult<usize, TestOrchestratorError>,
            ctx: &ComponentContext<TestOrchestrator>,
        ) {
            self.default_terminate_with_result(Ok(result.into_inner().unwrap()), ctx)
                .await;
        }
    }

    #[tokio::test]
    async fn test_wrapped_orchestrator() {
        let system = System::new();
        let dispatcher = Dispatcher::new(Default::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let orchestrator = TestOrchestrator::new(dispatcher_handle, system.clone(), true);

        let result = orchestrator.run(system).await.unwrap();
        assert_eq!(result, 42);
    }
}
