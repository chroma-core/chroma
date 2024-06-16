use crate::system::Receiver;
use async_trait::async_trait;
use std::fmt::Debug;
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

/// A task result is a wrapper around the result of a task.
/// It contains the task id for tracking purposes.
#[derive(Debug)]
pub(super) struct TaskResult<Output, Error> {
    result: Result<Output, Error>,
    task_id: Uuid,
}

impl<Output, Error> TaskResult<Output, Error> {
    pub(super) fn into_inner(self) -> Result<Output, Error> {
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
        let result = self.operator.run(&self.input).await;
        let task_result = TaskResult {
            result,
            task_id: self.task_id,
        };
        let _res = self.reply_channel.send(task_result, None).await;
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
