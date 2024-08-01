use crate::{
    errors::{ChromaError, ErrorCodes},
    system::ReceiverForMessage,
    utils::get_panic_message,
};
use async_trait::async_trait;
use futures::FutureExt;
use std::{fmt::Debug, panic::AssertUnwindSafe};
use thiserror::Error;
use uuid::Uuid;

pub(crate) enum OperatorType {
    IoOperator,
    Other,
}

/// An operator takes a generic input and returns a generic output.
/// It is a definition of a function.
#[async_trait]
pub(super) trait Operator<I, O>: Send + Sync + Debug
where
    I: Send + Sync,
    O: Send + Sync,
{
    type Error;
    // It would have been nice to do this with a default trait for result
    // but that's not stable in rust yet.
    async fn run(&self, input: &I) -> Result<O, Self::Error>;
    fn get_name(&self) -> &'static str;
    fn get_type(&self) -> OperatorType {
        OperatorType::Other
    }
}

#[derive(Debug, Error)]
pub(super) enum TaskError<Err> {
    #[error("Panic occurred while handling task: {0:?}")]
    Panic(Option<String>),
    #[error("Task failed with error: {0:?}")]
    TaskFailed(#[from] Err),
}

impl<Err> ChromaError for TaskError<Err>
where
    Err: Debug + ChromaError + 'static,
{
    fn code(&self) -> ErrorCodes {
        match self {
            TaskError::Panic(_) => ErrorCodes::Internal,
            TaskError::TaskFailed(e) => e.code(),
        }
    }
}

impl<Err> TaskError<Err>
where
    Err: Debug + ChromaError + 'static,
{
    pub(super) fn boxed(self) -> Box<dyn ChromaError> {
        Box::new(self)
    }
}

/// A task result is a wrapper around the result of a task.
/// It contains the task id for tracking purposes.
#[derive(Debug)]
pub(super) struct TaskResult<Output, Error> {
    result: Result<Output, TaskError<Error>>,
    task_id: Uuid,
}

impl<Output, Error> TaskResult<Output, Error> {
    pub(super) fn into_inner(self) -> Result<Output, TaskError<Error>> {
        self.result
    }

    pub(super) fn id(&self) -> Uuid {
        self.task_id
    }
}

/// A task is a wrapper around an operator and its input.
/// It is a description of a function to be run.
#[derive(Debug)]
struct Task<Input, Output, Error>
where
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
{
    operator: Box<dyn Operator<Input, Output, Error = Error>>,
    input: Input,
    reply_channel: Box<dyn ReceiverForMessage<TaskResult<Output, Error>>>,
    task_id: Uuid,
}

/// A message type used by the dispatcher to send tasks to worker threads.
pub(crate) type TaskMessage = Box<dyn TaskWrapper>;

/// A task wrapper is a trait that can be used to run a task. We use it to
/// erase the I, O types from the Task struct so that tasks.
#[async_trait]
pub(crate) trait TaskWrapper: Send + Debug {
    fn get_name(&self) -> &'static str;
    async fn run(&self);
    fn id(&self) -> Uuid;
    fn get_type(&self) -> OperatorType;
}

/// Implement the TaskWrapper trait for every Task. This allows us to
/// erase the I, O types from the Task struct so that tasks can be
/// stored in a homogenous queue regardless of their input and output types.
#[async_trait]
impl<Input, Output, Error> TaskWrapper for Task<Input, Output, Error>
where
    Error: Debug + Send,
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
{
    fn get_name(&self) -> &'static str {
        self.operator.get_name()
    }

    async fn run(&self) {
        let result = AssertUnwindSafe(self.operator.run(&self.input))
            .catch_unwind()
            .await;

        match result {
            Ok(result) => {
                // If this (or similarly, the .send() below) errors, it means the receiver was dropped.
                // There are valid reasons for this to happen (e.g. the component was stopped) so we ignore the error.
                // Another valid case is when the PrefetchIoOperator finishes
                // after the orchestrator component was stopped.
                match self
                    .reply_channel
                    .send(
                        TaskResult {
                            result: result.map_err(|e| TaskError::TaskFailed(e)),
                            task_id: self.task_id,
                        },
                        None,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!(
                            "Failed to send task result for task {} to reply channel: {}",
                            self.task_id,
                            err
                        );
                    }
                }
            }
            Err(panic_value) => {
                let panic_message = get_panic_message(panic_value);

                match self
                    .reply_channel
                    .send(
                        TaskResult {
                            result: Err(TaskError::Panic(panic_message.clone())),
                            task_id: self.task_id,
                        },
                        None,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!(
                            "Failed to send task result for task {} to reply channel: {}",
                            self.task_id,
                            err
                        );
                    }
                };

                // Re-panic so the message handler can catch it
                panic!(
                    "{}",
                    panic_message.unwrap_or("Unknown panic occurred in task".to_string())
                );
            }
        };
    }

    fn id(&self) -> Uuid {
        self.task_id
    }

    fn get_type(&self) -> OperatorType {
        self.operator.get_type()
    }
}

/// Wrap an operator and its input into a task message.
pub(super) fn wrap<Input, Output, Error>(
    operator: Box<dyn Operator<Input, Output, Error = Error>>,
    input: Input,
    reply_channel: Box<dyn ReceiverForMessage<TaskResult<Output, Error>>>,
) -> TaskMessage
where
    Error: Debug + Send + 'static,
    Input: Send + Sync + Debug + 'static,
    Output: Send + Sync + Debug + 'static,
{
    let id = Uuid::new_v4();
    Box::new(Task {
        operator,
        input,
        reply_channel,
        task_id: id,
    })
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use crate::{
        execution::dispatcher::Dispatcher,
        system::{Component, ComponentContext, ComponentHandle, Handler, System},
    };

    use super::*;

    #[derive(Debug)]
    struct MockOperator {}
    #[async_trait]
    impl Operator<(), ()> for MockOperator {
        type Error = ();

        fn get_name(&self) -> &'static str {
            "MockOperator"
        }

        async fn run(&self, _: &()) -> Result<(), Self::Error> {
            println!("MockOperator running");
            panic!("MockOperator panicking");
        }
    }

    #[derive(Debug)]
    struct MockComponent {
        pub received_results: Arc<Mutex<Vec<TaskResult<(), ()>>>>,
        pub dispatcher: ComponentHandle<Dispatcher>,
    }
    #[async_trait]
    impl Component for MockComponent {
        fn get_name() -> &'static str {
            "Mock component"
        }

        fn queue_size(&self) -> usize {
            1000
        }

        async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
            let task = wrap(Box::new(MockOperator {}), (), ctx.receiver());
            self.dispatcher.send(task, None).await.unwrap();
        }
    }
    #[async_trait]
    impl Handler<TaskResult<(), ()>> for MockComponent {
        type Result = ();

        async fn handle(
            &mut self,
            message: TaskResult<(), ()>,
            ctx: &ComponentContext<MockComponent>,
        ) {
            self.received_results.lock().push(message);
            ctx.cancellation_token.cancel();
        }
    }

    #[tokio::test]
    async fn task_catches_panic() {
        let system = System::new();
        let dispatcher = Dispatcher::new(1, 1000, 1000);
        let dispatcher_handle = system.start_component(dispatcher);

        let received_results = Arc::new(Mutex::new(Vec::new()));
        let component = MockComponent {
            received_results: received_results.clone(),
            dispatcher: dispatcher_handle.clone(),
        };

        let mut handle = system.start_component(component);
        // yield to allow the operator to run
        tokio::task::yield_now().await;
        // the component will stop itself after it receives the result
        handle.join().await.unwrap();

        let results_guard = received_results.lock();
        let result = &results_guard.first().unwrap().result;

        assert_eq!(result.is_err(), true);
        matches!(result, Err(TaskError::Panic(Some(msg))) if *msg == "MockOperator panicking".to_string());
    }
}
