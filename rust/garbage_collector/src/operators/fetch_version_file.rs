//! Operator for GC. Fetches the collection version file from S3.
//!
//! Input:
//! - Version file path. Full file path without the bucket name.
//! - Storage
//!
//! Output:
//! - Version file content Vec<u8>

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{GetOptions, Storage, StorageError};
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::CollectionUuid;
use prost::Message;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct FetchVersionFileOperator {}

impl FetchVersionFileOperator {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct FetchVersionFileInput {
    version_file_path: String,
    storage: Storage,
}

impl FetchVersionFileInput {
    pub fn new(version_file_path: String, storage: Storage) -> Self {
        Self {
            version_file_path,
            storage,
        }
    }
}

impl Debug for FetchVersionFileInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchVersionFileInput")
            .field("version_file_path", &self.version_file_path)
            .finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct FetchVersionFileOutput {
    pub file: Arc<CollectionVersionFile>,
    pub collection_id: CollectionUuid,
}

#[derive(Error, Debug)]
pub enum FetchVersionFileError {
    #[error("Error fetching version file: {0}")]
    StorageError(#[from] StorageError),
    #[error("Error parsing version file")]
    ParseError(#[from] prost::DecodeError),
    #[error("Invalid storage configuration: {0}")]
    StorageConfigError(String),
    #[error("Missing collection ID in version file")]
    MissingCollectionId,
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

impl ChromaError for FetchVersionFileError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchVersionFileError::StorageError(e) => e.code(),
            FetchVersionFileError::ParseError(_) => ErrorCodes::Internal,
            FetchVersionFileError::StorageConfigError(_) => ErrorCodes::Internal,
            FetchVersionFileError::MissingCollectionId => ErrorCodes::Internal,
            FetchVersionFileError::InvalidUuid(_) => ErrorCodes::Internal,
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
        let content = input
            .storage
            .get(
                &input.version_file_path,
                GetOptions::new(StorageRequestPriority::P0),
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    path = %input.version_file_path,
                    "Failed to fetch version file"
                );
                FetchVersionFileError::StorageError(e)
            })?;

        tracing::info!(
            path = %input.version_file_path,
            size = content.len(),
            "Successfully fetched version file"
        );

        let version_file = CollectionVersionFile::decode(content.as_slice())?;
        let collection_id = CollectionUuid::from_str(
            &version_file
                .collection_info_immutable
                .as_ref()
                .ok_or(FetchVersionFileError::MissingCollectionId)?
                .collection_id,
        )
        .map_err(FetchVersionFileError::InvalidUuid)?;

        Ok(FetchVersionFileOutput {
            file: Arc::new(version_file),
            collection_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_storage::{test_storage, PutOptions};
    use chroma_types::chroma_proto::CollectionInfoImmutable;
    use chroma_types::chroma_proto::CollectionVersionHistory;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_fetch_version_file() {
        let (_storage_dir, storage) = test_storage();
        let collection_id = Uuid::new_v4();
        let test_file = CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: Uuid::new_v4().to_string(),
                database_id: Uuid::new_v4().to_string(),
                database_name: "test".to_string(),
                is_deleted: false,
                dimension: 3,
                collection_id: collection_id.to_string(),
                collection_name: "test".to_string(),
                collection_creation_secs: 0,
            }),
            version_history: Some(CollectionVersionHistory { versions: vec![] }),
        };
        let test_content = test_file.encode_to_vec();
        let test_file_path = "test_version_file.txt";

        // Add more detailed error handling for the put operation
        match storage
            .put_bytes(test_file_path, test_content.clone(), PutOptions::default())
            .await
        {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to write test file: {:?}", e);
            }
        }

        let operator = FetchVersionFileOperator {};
        let input = FetchVersionFileInput {
            version_file_path: test_file_path.to_string(),
            storage: storage.clone(),
        };

        let result = operator.run(&input).await.expect("Failed to run operator");

        assert_eq!(*result.file, test_file);
        assert_eq!(result.collection_id, CollectionUuid(collection_id));
    }

    #[tokio::test]
    async fn test_fetch_nonexistent_file() {
        let (_storage_dir, storage) = test_storage();
        let operator = FetchVersionFileOperator {};
        let input = FetchVersionFileInput {
            version_file_path: "nonexistent_file.txt".to_string(),
            storage,
        };

        let result = operator.run(&input).await;
        assert!(matches!(
            result,
            Err(FetchVersionFileError::StorageError(_))
        ));
    }

    #[tokio::test]
    async fn test_fetch_version_file_missing_collection_info() {
        let (_storage_dir, storage) = test_storage();
        let test_file = CollectionVersionFile {
            collection_info_immutable: None,
            version_history: Some(CollectionVersionHistory { versions: vec![] }),
        };
        let test_file_path = "missing_collection_info.bin";

        storage
            .put_bytes(
                test_file_path,
                test_file.encode_to_vec(),
                PutOptions::default(),
            )
            .await
            .expect("version file should be written");

        let err = FetchVersionFileOperator::new()
            .run(&FetchVersionFileInput::new(
                test_file_path.to_string(),
                storage,
            ))
            .await
            .expect_err("missing collection info should fail");

        assert!(matches!(err, FetchVersionFileError::MissingCollectionId));
    }

    #[tokio::test]
    async fn test_fetch_version_file_invalid_collection_uuid() {
        let (_storage_dir, storage) = test_storage();
        let test_file = CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: Uuid::new_v4().to_string(),
                database_id: Uuid::new_v4().to_string(),
                database_name: "test".to_string(),
                is_deleted: false,
                dimension: 3,
                collection_id: "not-a-uuid".to_string(),
                collection_name: "test".to_string(),
                collection_creation_secs: 0,
            }),
            version_history: Some(CollectionVersionHistory { versions: vec![] }),
        };
        let test_file_path = "invalid_collection_uuid.bin";

        storage
            .put_bytes(
                test_file_path,
                test_file.encode_to_vec(),
                PutOptions::default(),
            )
            .await
            .expect("version file should be written");

        let err = FetchVersionFileOperator::new()
            .run(&FetchVersionFileInput::new(
                test_file_path.to_string(),
                storage,
            ))
            .await
            .expect_err("invalid collection UUID should fail");

        assert!(matches!(err, FetchVersionFileError::InvalidUuid(_)));
    }
}
