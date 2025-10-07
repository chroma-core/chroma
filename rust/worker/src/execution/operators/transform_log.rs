use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord};
use thiserror::Error;

#[derive(Debug)]
pub struct TransformOperator {}

#[derive(Debug)]
pub struct TransformInput {
    pub(crate) records: Chunk<LogRecord>,
}

impl TransformInput {
    pub fn new(records: Chunk<LogRecord>) -> Self {
        TransformInput { records }
    }
}

#[derive(Debug)]
pub struct TransformOutput {
    pub(crate) records: Chunk<LogRecord>,
}

#[derive(Debug, Error)]
#[error("Failed to transform records.")]
pub struct TransformError;

impl ChromaError for TransformError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl TransformOperator {
    pub fn new() -> Box<Self> {
        Box::new(TransformOperator {})
    }

    pub fn transform(&self, records: &Chunk<LogRecord>) -> Chunk<LogRecord> {
        records.clone()
    }
}

#[async_trait]
impl Operator<TransformInput, TransformOutput> for TransformOperator {
    type Error = TransformError;

    fn get_name(&self) -> &'static str {
        "TransformOperator"
    }

    async fn run(&self, input: &TransformInput) -> Result<TransformOutput, TransformError> {
        let transformed_records = self.transform(&input.records);
        Ok(TransformOutput {
            records: transformed_records,
        })
    }
}
