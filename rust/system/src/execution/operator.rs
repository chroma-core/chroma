use crate::{utils::PanicError, ReceiverForMessage};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use futures::FutureExt;
use std::{any::type_name, fmt::Debug, panic::AssertUnwindSafe};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub enum OperatorType {
    IO,
    Other,
}

/// An operator takes a generic input and returns a generic output.
/// It is a definition of a function.
#[async_trait]
pub trait Operator<I, O>: Send + Sync + Debug
where
    I: Send + Sync,
    O: Send + Sync,
{
    type Error;
    // It would have been nice to do this with a default trait for result
    // but that's not stable in rust yet.
    async fn run(&self, input: &I) -> Result<O, Self::Error>;
    fn get_name(&self) -> &'static str {
        type_name::<Self>()
    }
    fn get_type(&self) -> OperatorType {
        OperatorType::Other
    }
    /// By default operators will log an error event if their sender is dropped when sending the result.
    /// This is not always desired, e.g. when creating a "fire-and-forget" operator (data prefetching); so this method can be overridden to return false.
    fn errors_when_sender_dropped(&self) -> bool {
        true
    }

    fn can_cancel(&self) -> bool {
        true
    }
}

#[derive(Debug, Error)]
pub enum TaskError<Err> {
    #[error("Panic occurred while handling task: {0:?}")]
    Panic(PanicError),
    #[error("Task failed with error: {0:?}")]
    TaskFailed(#[from] Err),
    #[error("Task was aborted")]
    Aborted,
}

impl<Err: ChromaError> ChromaError for TaskError<Err>
where
    Err: Debug + ChromaError + 'static,
{
    fn code(&self) -> ErrorCodes {
        match self {
            TaskError::Panic(_) => ErrorCodes::Internal,
            TaskError::TaskFailed(e) => e.code(),
            TaskError::Aborted => ErrorCodes::ResourceExhausted,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            TaskError::Panic(_) => true,
            TaskError::TaskFailed(e) => e.should_trace_error(),
            TaskError::Aborted => true,
        }
    }
}

/// A task result is a wrapper around the result of a task.
/// It contains the task id for tracking purposes.
#[derive(Debug)]
pub struct TaskResult<Output, Error> {
    pub(crate) result: Result<Output, TaskError<Error>>,
    pub(crate) task_id: Uuid,
}

impl<Output, Error> TaskResult<Output, Error> {
    pub fn into_inner(self) -> Result<Output, TaskError<Error>> {
        self.result
    }

    #[allow(dead_code)]
    pub(super) fn id(&self) -> Uuid {
        self.task_id
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TaskState {
    NotStarted,
    Running,
    Aborted,
    // There is no FinishedState to simplify the implementation.  Adding one requires covering all
    // cases and makes a diamond state machine.  Having just one valid transition (from NotStarted
    // to anything else) simplfies the implementation.
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
    task_state: TaskState,
    cancellation_token: Option<CancellationToken>,
}

impl<Input, Output, Error> Task<Input, Output, Error>
where
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
    Error: Debug + Send + ChromaError,
{
    async fn main_run(&mut self) {
        if self.task_state != TaskState::NotStarted {
            tracing::error!(
                "Task {} is already running or has already finished",
                self.task_id
            );
            return;
        }
        self.task_state = TaskState::Running;
        let result = AssertUnwindSafe(self.operator.run(&self.input))
            .catch_unwind()
            .await;

        match result {
            Ok(result) => {
                if let Err(err) = result.as_ref() {
                    if err.should_trace_error() {
                        tracing::error!("Task {} failed with error: {:?}", self.task_id, err);
                    }
                }

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
                        if self.operator.errors_when_sender_dropped() {
                            tracing::error!(
                                "Failed to send task result for task {} to reply channel: {}",
                                self.task_id,
                                err
                            );
                        } else {
                            tracing::debug!(
                                "Failed to send task result for task {} to reply channel: {}",
                                self.task_id,
                                err
                            );
                        }
                    }
                }
            }
            Err(panic_value) => {
                tracing::error!("Task {} panicked: {:?}", self.task_id, panic_value);

                match self
                    .reply_channel
                    .send(
                        TaskResult {
                            result: Err(TaskError::Panic(PanicError::new(panic_value))),
                            task_id: self.task_id,
                        },
                        None,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        if self.operator.errors_when_sender_dropped() {
                            tracing::error!(
                                "Failed to send task result for task {} to reply channel: {}",
                                self.task_id,
                                err
                            );
                        }
                    }
                };
            }
        };
    }
}

/// A message type used by the dispatcher to send tasks to worker threads.
pub type TaskMessage = Box<dyn TaskWrapper>;

/// A task wrapper is a trait that can be used to run a task. We use it to
/// erase the I, O types from the Task struct so that tasks.
#[async_trait]
pub trait TaskWrapper: Send + Debug {
    fn get_name(&self) -> &'static str;
    async fn run(&mut self);
    #[allow(dead_code)]
    fn id(&self) -> Uuid;
    fn get_type(&self) -> OperatorType;
    async fn abort(&mut self);
}

/// Implement the TaskWrapper trait for every Task. This allows us to
/// erase the I, O types from the Task struct so that tasks can be
/// stored in a homogenous queue regardless of their input and output types.
#[async_trait]
impl<Input, Output, Error> TaskWrapper for Task<Input, Output, Error>
where
    Error: Debug + Send + ChromaError,
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
{
    fn get_name(&self) -> &'static str {
        self.operator.get_name()
    }

    async fn run(&mut self) {
        let cancellation_token = self.cancellation_token.clone();
        match cancellation_token {
            Some(token) => {
                tokio::select! {
                    _ = token.cancelled() => { self.abort().await; }
                    _ = self.main_run() => {}
                }
            }
            None => self.main_run().await,
        }
    }

    fn id(&self) -> Uuid {
        self.task_id
    }

    fn get_type(&self) -> OperatorType {
        self.operator.get_type()
    }

    async fn abort(&mut self) {
        self.task_state = TaskState::Aborted;
        match self
            .reply_channel
            .send(
                TaskResult {
                    result: Err(TaskError::Aborted),
                    task_id: self.task_id,
                },
                None,
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if self.operator.errors_when_sender_dropped() {
                    tracing::error!(
                        "Failed to send task result for task {} to reply channel: {}",
                        self.task_id,
                        err
                    );
                } else {
                    tracing::debug!(
                        "Failed to send task result for task {} to reply channel: {}",
                        self.task_id,
                        err
                    );
                }
            }
        }
    }
}

/// Wrap an operator and its input into a task message.
pub fn wrap<Input, Output, Error>(
    operator: Box<dyn Operator<Input, Output, Error = Error>>,
    input: Input,
    reply_channel: Box<dyn ReceiverForMessage<TaskResult<Output, Error>>>,
    cancellation_token: CancellationToken,
) -> TaskMessage
where
    Error: ChromaError + Debug + Send + 'static,
    Input: Send + Sync + Debug + 'static,
    Output: Send + Sync + Debug + 'static,
{
    let id = Uuid::new_v4();
    let mut token = Some(cancellation_token);

    if !operator.can_cancel() {
        token = None;
    }
    Box::new(Task {
        operator,
        input,
        reply_channel,
        task_id: id,
        task_state: TaskState::NotStarted,
        cancellation_token: token,
    })
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use crate::{
        execution::dispatcher::Dispatcher,
        DispatcherConfig, {Component, ComponentContext, ComponentHandle, Handler, System},
    };

    use super::*;

    #[derive(Debug)]
    struct MockOperator {}
    #[async_trait]
    impl Operator<(), ()> for MockOperator {
        type Error = std::io::Error;

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
        pub received_results: Arc<Mutex<Vec<TaskResult<(), std::io::Error>>>>,
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
            let task = wrap(
                Box::new(MockOperator {}),
                (),
                ctx.receiver(),
                ctx.cancellation_token.clone(),
            );
            self.dispatcher.send(task, None).await.unwrap();
        }
    }
    #[async_trait]
    impl Handler<TaskResult<(), std::io::Error>> for MockComponent {
        type Result = ();

        async fn handle(
            &mut self,
            message: TaskResult<(), std::io::Error>,
            ctx: &ComponentContext<MockComponent>,
        ) {
            self.received_results.lock().push(message);
            ctx.cancellation_token.cancel();
        }
    }

    #[tokio::test]
    async fn task_catches_panic() {
        let system = System::new();
        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: 1,
            task_queue_limit: 1000,
            dispatcher_queue_size: 1000,
            worker_queue_size: 1000,
            active_io_tasks: 1000,
        });
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

        assert!(result.is_err());
        let err = result.as_ref().unwrap_err();
        assert!(err.to_string().contains("MockOperator panicking"));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("should trace: {0}")]
    struct MockError(bool);

    impl ChromaError for MockError {
        fn code(&self) -> chroma_error::ErrorCodes {
            chroma_error::ErrorCodes::InvalidArgument
        }

        fn should_trace_error(&self) -> bool {
            self.0
        }
    }

    #[test]
    fn should_trace_flush_compaction_error() {
        let fce = MockError(true);
        let te: TaskError<MockError> = fce.into();
        assert!(te.should_trace_error());
    }

    #[test]
    fn should_not_trace_flush_compaction_error() {
        let fce = MockError(false);
        let te: TaskError<MockError> = fce.into();
        assert!(!te.should_trace_error());
    }
}
