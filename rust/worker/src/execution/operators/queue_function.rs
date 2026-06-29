use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_system::{Operator, OperatorType};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use thiserror::Error;

use crate::work_queue::work_queue_client::WorkQueueClient;

/// The queue function operator queues async attached functions for external processing.
#[derive(Debug)]
pub struct QueueFunctionOperator {
    pub work_queue_client: WorkQueueClient,
}

impl QueueFunctionOperator {
    pub fn new(work_queue_client: WorkQueueClient) -> Self {
        Self { work_queue_client }
    }
}

/// Input to the queue function operator.
#[derive(Debug, Clone)]
pub struct QueueFunctionInput {
    pub attached_function_id: AttachedFunctionUuid,
    pub input_collection_id: CollectionUuid,
    pub completion_offset: i64,
    pub compaction_offset: Option<i64>,
}

impl QueueFunctionInput {
    pub fn new(
        attached_function_id: AttachedFunctionUuid,
        input_collection_id: CollectionUuid,
        completion_offset: i64,
        compaction_offset: Option<i64>,
    ) -> Self {
        Self {
            attached_function_id,
            input_collection_id,
            completion_offset,
            compaction_offset,
        }
    }
}

/// Output from the queue function operator - empty as we only care about success/failure.
#[derive(Debug, Clone)]
pub struct QueueFunctionOutput;

#[derive(Debug, Error)]
pub enum QueueFunctionError {
    #[error("Failed to queue work: {0}")]
    QueueError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for QueueFunctionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            QueueFunctionError::QueueError(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<QueueFunctionInput, QueueFunctionOutput> for QueueFunctionOperator {
    type Error = QueueFunctionError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(&self, input: &QueueFunctionInput) -> Result<QueueFunctionOutput, Self::Error> {
        tracing::info!(
            "Queuing async attached function - function_id: {}, collection_id: {}, offset: {}, compaction_offset: {:?}",
            input.attached_function_id,
            input.input_collection_id,
            input.completion_offset,
            input.compaction_offset
        );

        let mut client = self.work_queue_client.clone();
        client
            .push_work(
                input.attached_function_id.to_string(),
                input.input_collection_id.to_string(),
                input.completion_offset,
                input.compaction_offset,
            )
            .await
            .map_err(QueueFunctionError::QueueError)?;

        Ok(QueueFunctionOutput)
    }
}
