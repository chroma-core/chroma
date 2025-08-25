use async_trait::async_trait;

use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{Merge, RecordMeasure};
use thiserror::Error;

#[derive(Debug)]
pub struct SparseKnnMergeInput {
    pub batch_measures: Vec<Vec<RecordMeasure>>,
}

#[derive(Debug, Default)]
pub struct SparseKnnMergeOutput {
    pub measures: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
#[error("Knn merge error (unreachable)")]
pub struct SparseKnnMergeError;

impl ChromaError for SparseKnnMergeError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<SparseKnnMergeInput, SparseKnnMergeOutput> for Merge {
    type Error = SparseKnnMergeError;

    async fn run(
        &self,
        input: &SparseKnnMergeInput,
    ) -> Result<SparseKnnMergeOutput, SparseKnnMergeError> {
        Ok(SparseKnnMergeOutput {
            measures: self.merge(input.batch_measures.clone()),
        })
    }
}
