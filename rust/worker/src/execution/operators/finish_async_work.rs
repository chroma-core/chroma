use crate::work_queue::work_queue_client::WorkQueueClient;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_system::Operator;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct FinishAsyncWorkInput {
    pub function_id: AttachedFunctionUuid,
    pub input_collection_id: CollectionUuid,
    pub completion_offset: i64,
    pub work_queue_client: WorkQueueClient,
}

impl FinishAsyncWorkInput {
    pub fn new(
        function_id: AttachedFunctionUuid,
        input_collection_id: CollectionUuid,
        completion_offset: i64,
        work_queue_client: WorkQueueClient,
    ) -> Self {
        Self {
            function_id,
            input_collection_id,
            completion_offset,
            work_queue_client,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FinishAsyncWorkOutput {}

#[derive(Error, Debug)]
pub enum FinishAsyncWorkError {
    #[error("Failed to finish work in work queue: {0}")]
    WorkQueueError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FinishAsyncWorkError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            FinishAsyncWorkError::WorkQueueError(e) => e.code(),
        }
    }
}

/// FinishAsyncWorkOperator is responsible for marking async work as complete in the work queue.
/// This is used for async consumer functions that process data asynchronously.
#[derive(Debug)]
pub struct FinishAsyncWorkOperator {}

impl FinishAsyncWorkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for FinishAsyncWorkOperator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Operator<FinishAsyncWorkInput, FinishAsyncWorkOutput> for FinishAsyncWorkOperator {
    type Error = FinishAsyncWorkError;

    fn get_name(&self) -> &'static str {
        "FinishAsyncWorkOperator"
    }

    async fn run(
        &self,
        input: &FinishAsyncWorkInput,
    ) -> Result<FinishAsyncWorkOutput, FinishAsyncWorkError> {
        let mut work_queue_client = input.work_queue_client.clone();

        // Call finish_work on the work queue client
        work_queue_client
            .finish_work(
                input.function_id.0.to_string(),
                input.input_collection_id.0.to_string(),
                input.completion_offset,
            )
            .await?;

        tracing::info!(
            "Successfully marked async work as complete - function: {}, collection: {}, offset: {}",
            input.function_id.0,
            input.input_collection_id.0,
            input.completion_offset
        );

        Ok(FinishAsyncWorkOutput {})
    }
}
