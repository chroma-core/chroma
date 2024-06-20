use crate::{errors, system::Receiver};
use async_trait::async_trait;
use futures::FutureExt;
use std::{fmt::Debug, panic::AssertUnwindSafe};
use thiserror::Error;
use uuid::Uuid;

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
}

#[derive(Debug, Error)]
pub(super) enum TaskError<Error> {
    #[error("Error in task: {0}")]
    Other(Error),
    #[error("Task panicked")]
    Panic(Option<String>),
}

impl<Error> errors::ChromaError for TaskError<Error>
where
    Error: std::error::Error + errors::ChromaError + Send,
{
    fn code(&self) -> errors::ErrorCodes {
        match self {
            TaskError::Other(error) => error.code(),
            TaskError::Panic(_) => errors::ErrorCodes::UNKNOWN,
        }
    }
}

impl errors::ChromaError for Box<dyn errors::ChromaError> {
    fn code(&self) -> errors::ErrorCodes {
        (**self).code()
    }
}

impl From<TaskError<Box<dyn errors::ChromaError>>> for Box<dyn errors::ChromaError> {
    fn from(error: TaskError<Box<dyn errors::ChromaError>>) -> Self {
        Box::new(error)
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
    reply_channel: Box<dyn Receiver<TaskResult<Output, Error>>>,
    task_id: Uuid,
}

/// A message type used by the dispatcher to send tasks to worker threads.
pub(crate) type TaskMessage = Box<dyn TaskWrapper>;

/// A task wrapper is a trait that can be used to run a task. We use it to
/// erase the I, O types from the Task struct so that tasks.
#[async_trait]
pub(crate) trait TaskWrapper: Send + Debug {
    async fn run(&self);
    fn id(&self) -> Uuid;
}

/// Implement the TaskWrapper trait for every Task. This allows us to
/// erase the I, O types from the Task struct so that tasks can be
/// stored in a homogenous queue regardless of their input and output types.
#[async_trait]
impl<Input, Output, Error> TaskWrapper for Task<Input, Output, Error>
where
    Error: Debug,
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
{
    async fn run(&self) {
        let task_result = match AssertUnwindSafe(self.operator.run(&self.input))
            .catch_unwind()
            .await
        {
            Ok(result) => TaskResult {
                result: result.map_err(TaskError::Other),
                task_id: self.task_id,
            },
            Err(panic_value) => {
                #[allow(clippy::manual_map)]
                let panic_value = if let Some(s) = panic_value.downcast_ref::<&str>() {
                    Some(&**s)
                } else if let Some(s) = panic_value.downcast_ref::<String>() {
                    Some(s.as_str())
                } else {
                    None
                };

                TaskResult {
                    result: Err(TaskError::Panic(panic_value.map(|s| s.to_string()))),
                    task_id: self.task_id,
                }
            }
        };
        let res = self.reply_channel.send(task_result, None).await;
        // TODO: if this errors, it means the caller was dropped
    }

    fn id(&self) -> Uuid {
        self.task_id
    }
}

/// Wrap an operator and its input into a task message.
pub(super) fn wrap<Input, Output, Error>(
    operator: Box<dyn Operator<Input, Output, Error = Error>>,
    input: Input,
    reply_channel: Box<dyn Receiver<TaskResult<Output, Error>>>,
) -> TaskMessage
where
    Error: Debug + 'static,
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
