use crate::{execution::operator::Operator, log::log::Log, types::EmbeddingRecord};
use async_trait::async_trait;
use uuid::Uuid;

#[derive(Debug)]
pub struct PullLogsOperator {
    client: Box<dyn Log>,
}

impl PullLogsOperator {
    pub fn new(client: Box<dyn Log>) -> Self {
        PullLogsOperator { client }
    }
}

#[derive(Debug)]
pub struct PullLogsInput {
    collection_id: Uuid,
    offset: i64,
    batch_size: i32,
}

impl PullLogsInput {
    pub fn new(collection_id: Uuid, offset: i64, batch_size: i32) -> Self {
        PullLogsInput {
            collection_id,
            offset,
            batch_size,
        }
    }
}

#[derive(Debug)]
pub struct PullLogsOutput {
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
