use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{GetError, Storage};
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct FetchVersionFileOperator {}

pub struct FetchVersionFileInput {
    pub version_file_path: String,
    pub storage: Storage,
}

impl Debug for FetchVersionFileInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchVersionFileInput").finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct FetchVersionFileOutput {
    // TODO(Sanket):  Use a more appropriate type for the version file content.
    version_file_content: String,
}

#[derive(Error, Debug)]
pub enum FetchVersionFileError {
    #[error("Error fetching version file")]
    S3ReadError(#[from] GetError),
    #[error("Error parsing version file")]
    ParseError,
}

impl ChromaError for FetchVersionFileError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchVersionFileError::S3ReadError(e) => e.code(),
            FetchVersionFileError::ParseError => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<FetchVersionFileInput, FetchVersionFileOutput> for FetchVersionFileOperator {
    type Error = FetchVersionFileError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &FetchVersionFileInput,
    ) -> Result<FetchVersionFileOutput, FetchVersionFileError> {
        let file_contents = match input.storage.get_parallel(&input.version_file_path).await {
            Ok(contents) => contents,
            Err(e) => return Err(FetchVersionFileError::S3ReadError(e)),
        };
        // TODO(Sanket): Convert bytes to proto.
        let data = match String::from_utf8(file_contents.to_vec()) {
            Ok(data) => data,
            Err(_) => return Err(FetchVersionFileError::ParseError),
        };
        Ok(FetchVersionFileOutput {
            version_file_content: data,
        })
    }
}
