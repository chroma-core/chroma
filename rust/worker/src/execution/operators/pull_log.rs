use crate::{execution::operator::Operator, log::log::Log, types::EmbeddingRecord};
use async_trait::async_trait;
use uuid::Uuid;

/// The pull logs operator is responsible for reading logs from the log service.
#[derive(Debug)]
pub struct PullLogsOperator {
    client: Box<dyn Log>,
}

impl PullLogsOperator {
    /// Create a new pull logs operator.
    /// # Parameters
    /// * `client` - The log client to use for reading logs.
    pub fn new(client: Box<dyn Log>) -> Box<Self> {
        Box::new(PullLogsOperator { client })
    }
}

/// The input to the pull logs operator.
/// # Parameters
/// * `collection_id` - The collection id to read logs from.
/// * `offset` - The offset to start reading logs from.
/// * `batch_size` - The number of log entries to read.
#[derive(Debug)]
pub struct PullLogsInput {
    collection_id: Uuid,
    offset: i64,
    batch_size: i32,
}

impl PullLogsInput {
    /// Create a new pull logs input.
    /// # Parameters
    /// * `collection_id` - The collection id to read logs from.
    /// * `offset` - The offset to start reading logs from.
    /// * `batch_size` - The number of log entries to read.
    pub fn new(collection_id: Uuid, offset: i64, batch_size: i32) -> Self {
        PullLogsInput {
            collection_id,
            offset,
            batch_size,
        }
    }
}

/// The output of the pull logs operator.
#[derive(Debug)]
pub struct PullLogsOutput {
    logs: Vec<Box<EmbeddingRecord>>,
}

impl PullLogsOutput {
    /// Create a new pull logs output.
    /// # Parameters
    /// * `logs` - The logs that were read.
    pub fn new(logs: Vec<Box<EmbeddingRecord>>) -> Self {
        PullLogsOutput { logs }
    }

    /// Get the log entries that were read by an invocation of the pull logs operator.
    /// # Returns
    /// The log entries that were read.
    pub fn logs(&self) -> &Vec<Box<EmbeddingRecord>> {
        &self.logs
    }
}

#[async_trait]
impl Operator<PullLogsInput, PullLogsOutput> for PullLogsOperator {
    async fn run(&self, input: &PullLogsInput) -> PullLogsOutput {
        // We expect the log to be cheaply cloneable, we need to clone it since we need
        // a mutable reference to it. Not necessarily the best, but it works for our needs.
        let mut client_clone = self.client.clone();
        let logs = client_clone
            .read(
                input.collection_id.to_string(),
                input.offset,
                input.batch_size,
                None,
            )
            .await
            .unwrap();
        PullLogsOutput::new(logs)
    }
}
