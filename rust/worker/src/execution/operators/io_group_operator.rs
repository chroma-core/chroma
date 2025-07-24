use chroma_error::ChromaError;
use chroma_system::{Operator, OperatorType, TaskWrapper};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;
use tonic::async_trait;

#[derive(Debug, Default)]
pub struct IoGroupOperator {}

impl IoGroupOperator {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct IoGroupOperatorInput {
    #[allow(clippy::type_complexity)]
    sub_tasks: Arc<Mutex<Option<Vec<Box<dyn TaskWrapper>>>>>,
}

impl IoGroupOperatorInput {
    #[allow(clippy::type_complexity)]
    pub fn new(sub_tasks: Arc<Mutex<Option<Vec<Box<dyn TaskWrapper>>>>>) -> Self {
        Self { sub_tasks }
    }
}

#[derive(Debug)]
pub struct IoGroupOperatorOutput {}

#[derive(Debug, Error)]
pub enum IoGroupOperatorError {}

impl ChromaError for IoGroupOperatorError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<IoGroupOperatorInput, IoGroupOperatorOutput> for IoGroupOperator {
    type Error = IoGroupOperatorError;

    async fn run(
        &self,
        input: &IoGroupOperatorInput,
    ) -> Result<IoGroupOperatorOutput, IoGroupOperatorError> {
        let mut subtasks = input.sub_tasks.lock().take().unwrap();
        let mut futures = FuturesUnordered::new();
        for task in subtasks.iter_mut() {
            let fut = task.run();
            futures.push(fut);
        }

        while let Some(result) = futures.next().await {
            // No-op since subtask results are sent to the caller
        }

        Ok(IoGroupOperatorOutput {})
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
