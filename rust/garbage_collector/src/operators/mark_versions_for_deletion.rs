use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_proto::version_file::VersionFile;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct MarkVersionsForDeletionOperator {}

pub struct MarkVersionsForDeletionInput {
    pub version_file: VersionFile,
    pub sysdb_client: Box<SysDb>,
}

#[derive(Debug)]
pub struct MarkVersionsForDeletionOutput {
    pub version_file: VersionFile,
}

#[derive(Error, Debug)]
pub enum MarkVersionsForDeletionError {
    #[error("Error marking versions in sysdb: {0}")]
    SysDBError(String),
}

impl ChromaError for MarkVersionsForDeletionError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<MarkVersionsForDeletionInput, MarkVersionsForDeletionOutput>
    for MarkVersionsForDeletionOperator
{
    type Error = MarkVersionsForDeletionError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &MarkVersionsForDeletionInput,
    ) -> Result<MarkVersionsForDeletionOutput, MarkVersionsForDeletionError> {
        // TODO: Implement logic to mark versions for deletion in sysdb
        Ok(MarkVersionsForDeletionOutput {
            version_file: input.version_file.clone(),
        })
    }
}
