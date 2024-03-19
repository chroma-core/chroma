use crate::{log::log::Log, system::Receiver, types::EmbeddingRecord};
use async_trait::async_trait;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Debug)]
struct Task<Input, Output>
where
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
{
    operator: Box<dyn Operator<Input, Output>>,
    input: Input,
    reply_channel: Box<dyn Receiver<Output>>,
}

pub(super) type TaskMessage = Box<dyn TaskWrapper>;

#[async_trait]
pub(super) trait TaskWrapper: Send + Debug {
    async fn run(&self);
}

#[async_trait]
impl<Input, Output> TaskWrapper for Task<Input, Output>
where
    Input: Send + Sync + Debug,
    Output: Send + Sync + Debug,
{
    async fn run(&self) {
        let output = self.operator.run(&self.input).await;
        let res = self.reply_channel.send(output);
        // TODO: if this errors, it means the caller was dropped
    }
}

pub(super) fn wrap<Input, Output>(
    operator: Box<dyn Operator<Input, Output>>,
    input: Input,
    reply_channel: Box<dyn Receiver<Output>>,
) -> TaskMessage
where
    Input: Send + Sync + Debug + 'static,
    Output: Send + Sync + Debug + 'static,
{
    Box::new(Task {
        operator,
        input,
        reply_channel,
    })
}

#[async_trait]
pub(super) trait Operator<I, O>: Send + Sync + Debug
where
    I: Send + Sync,
    O: Send + Sync,
{
    async fn run(&self, input: &I) -> O;
}
