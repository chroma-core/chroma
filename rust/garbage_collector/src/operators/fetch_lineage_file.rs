use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_storage::{GetOptions, Storage};
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::CollectionLineageFile;
use prost::Message;
use thiserror::Error;

#[derive(Clone)]
pub struct FetchLineageFileInput {
    storage: Storage,
    lineage_file_path: String,
}

impl std::fmt::Debug for FetchLineageFileInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchLineageFileInput")
            .field("lineage_file_path", &self.lineage_file_path)
            .finish()
    }
}

impl FetchLineageFileInput {
    pub fn new(storage: Storage, lineage_file_path: String) -> Self {
        Self {
            storage,
            lineage_file_path,
        }
    }
}

#[derive(Debug)]
pub struct FetchLineageFileOutput(pub CollectionLineageFile);

#[derive(Debug, Error)]
pub enum FetchLineageFileError {
    #[error("Error fetching lineage file: {0}")]
    Storage(#[from] chroma_storage::StorageError),
    #[error("Error decoding lineage file: {0}")]
    Decode(#[from] prost::DecodeError),
}

impl ChromaError for FetchLineageFileError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            FetchLineageFileError::Storage(err) => err.code(),
            FetchLineageFileError::Decode(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FetchLineageFileOperator {}

impl FetchLineageFileOperator {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Operator<FetchLineageFileInput, FetchLineageFileOutput> for FetchLineageFileOperator {
    type Error = FetchLineageFileError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &FetchLineageFileInput,
    ) -> Result<FetchLineageFileOutput, Self::Error> {
        let lineage_file = input
            .storage
            .get(&input.lineage_file_path, GetOptions::default())
            .await?;
        let lineage = CollectionLineageFile::decode(lineage_file.as_slice())?;
        Ok(FetchLineageFileOutput(lineage))
    }
}
