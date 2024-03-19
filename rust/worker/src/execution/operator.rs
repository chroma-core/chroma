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

#[derive(Debug)]
pub(super) struct PullLogsOperator {
    client: Box<dyn Log>,
}

impl PullLogsOperator {
    pub fn new(client: Box<dyn Log>) -> Self {
        PullLogsOperator { client }
    }
}

#[derive(Debug)]
pub(super) struct PullLogsInput {
    collection_id: Uuid,
    offset: i64,
    batch_size: i32,
}

impl PullLogsInput {
    pub(super) fn new(collection_id: Uuid, offset: i64, batch_size: i32) -> Self {
        PullLogsInput {
            collection_id,
            offset,
            batch_size,
        }
    }
}

#[derive(Debug)]
pub(super) struct PullLogsOutput {
    // TODO: Standardize on Vec<Box<EmbeddingRecord>> as our data chunk type and add a type alias
    logs: Vec<Box<EmbeddingRecord>>,
}

#[async_trait]
impl Operator<PullLogsInput, PullLogsOutput> for PullLogsOperator {
    async fn run(&self, input: &PullLogsInput) -> PullLogsOutput {
        let mut client_clone = self.client.clone();
        let logs = client_clone
            .read(
                input.collection_id.to_string(),
                input.offset,
                input.batch_size,
            )
            .await
            .unwrap();
        PullLogsOutput { logs }
    }
}
