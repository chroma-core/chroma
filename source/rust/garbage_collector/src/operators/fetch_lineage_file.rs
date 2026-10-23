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

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_storage::{test_storage, PutOptions};
    use chroma_system::Operator;
    use chroma_types::chroma_proto::CollectionVersionDependency;

    #[tokio::test]
    async fn test_fetch_lineage_file_success() {
        let (_storage_dir, storage) = test_storage();
        let lineage_file = CollectionLineageFile {
            dependencies: vec![CollectionVersionDependency {
                source_collection_id: "source".to_string(),
                source_collection_version: 7,
                target_collection_id: "target".to_string(),
            }],
        };
        let lineage_file_path = "lineage/test.bin".to_string();

        storage
            .put_bytes(
                &lineage_file_path,
                lineage_file.encode_to_vec(),
                PutOptions::default(),
            )
            .await
            .expect("lineage file should be written");

        let output = FetchLineageFileOperator::new()
            .run(&FetchLineageFileInput::new(
                storage,
                lineage_file_path.to_string(),
            ))
            .await
            .expect("lineage file should be fetched");

        assert_eq!(output.0, lineage_file);
    }

    #[tokio::test]
    async fn test_fetch_lineage_file_missing_object() {
        let (_storage_dir, storage) = test_storage();

        let err = FetchLineageFileOperator::new()
            .run(&FetchLineageFileInput::new(
                storage,
                "lineage/missing.bin".to_string(),
            ))
            .await
            .expect_err("missing lineage file should fail");

        assert!(matches!(err, FetchLineageFileError::Storage(_)));
    }

    #[tokio::test]
    async fn test_fetch_lineage_file_decode_error() {
        let (_storage_dir, storage) = test_storage();
        let lineage_file_path = "lineage/invalid.bin".to_string();

        storage
            .put_bytes(
                &lineage_file_path,
                b"not a protobuf".to_vec(),
                PutOptions::default(),
            )
            .await
            .expect("invalid lineage file should be written");

        let err = FetchLineageFileOperator::new()
            .run(&FetchLineageFileInput::new(storage, lineage_file_path))
            .await
            .expect_err("invalid lineage file should fail to decode");

        assert!(matches!(err, FetchLineageFileError::Decode(_)));
    }
}
