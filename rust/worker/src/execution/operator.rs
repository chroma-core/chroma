use crate::system::Receiver;
use async_trait::async_trait;
use std::fmt::Debug;

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
    reply_channel: Box<dyn Receiver<Result<Output, Error>>>,
    tracing_context: Option<tracing::Id>,
}

/// A message type used by the dispatcher to send tasks to worker threads.
pub(crate) type TaskMessage = Box<dyn TaskWrapper>;

/// A task wrapper is a trait that can be used to run a task. We use it to
/// erase the I, O types from the Task struct so that tasks.
#[async_trait]
pub(crate) trait TaskWrapper: Send + Debug {
    async fn run(&self);
    fn getTracingContext(&self) -> Option<tracing::Id>;
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
        let output = self.operator.run(&self.input).await;
        let res = self.reply_channel.send(output).await;
        // TODO: if this errors, it means the caller was dropped
    }

    fn getTracingContext(&self) -> Option<tracing::Id> {
        self.tracing_context.clone()
    }
}

/// Wrap an operator and its input into a task message.
pub(super) fn wrap<Input, Output, Error>(
    operator: Box<dyn Operator<Input, Output, Error = Error>>,
    input: Input,
    reply_channel: Box<dyn Receiver<Result<Output, Error>>>,
    tracing_context: Option<tracing::Id>,
) -> TaskMessage
where
    Error: Debug + 'static,
    Input: Send + Sync + Debug + 'static,
    Output: Send + Sync + Debug + 'static,
{
    Box::new(Task {
        operator,
        input,
        reply_channel,
        tracing_context,
    })
}
