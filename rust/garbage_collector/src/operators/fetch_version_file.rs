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
    use chroma_config::registry;
    use chroma_config::Configurable;
    use chroma_storage::s3_config_for_localhost_with_bucket_name;
    use chroma_storage::PutOptions;
    use chroma_types::chroma_proto::CollectionInfoImmutable;
    use chroma_types::chroma_proto::CollectionVersionHistory;
    use tracing_test::traced_test;
    use uuid::Uuid;

    async fn setup_test_storage() -> Storage {
        // Create storage config for Minio
        let storage_config = s3_config_for_localhost_with_bucket_name("chroma-storage").await;

        // Add more detailed logging
        tracing::info!("Setting up test storage with config: {:?}", storage_config);

        let registry = registry::Registry::new();
        Storage::try_from_config(&storage_config, &registry)
            .await
            .expect("Failed to create storage")
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_fetch_version_file() {
        let storage = setup_test_storage().await;
        let test_file = CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: Uuid::new_v4().to_string(),
                database_id: Uuid::new_v4().to_string(),
                database_name: "test".to_string(),
                is_deleted: false,
                dimension: 3,
                collection_id: Uuid::new_v4().to_string(),
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
            Ok(_) => tracing::info!("Successfully wrote test file"),
            Err(e) => {
                tracing::error!("Failed to write test file: {:?}", e);
                panic!("Failed to write test file: {:?}", e);
            }
        }

        // Create operator and input
        let operator = FetchVersionFileOperator {};
        let input = FetchVersionFileInput {
            version_file_path: test_file_path.to_string(),
            storage: storage.clone(),
        };

        // Run the operator
        let result = operator.run(&input).await.expect("Failed to run operator");

        // Verify the content
        assert_eq!(result.file, test_file.into());

        // Cleanup - Note: object_store doesn't have a delete method,
        // but the test bucket should be cleaned up between test runs
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_fetch_nonexistent_file() {
        let storage = setup_test_storage().await;
        let operator = FetchVersionFileOperator {};
        let input = FetchVersionFileInput {
            version_file_path: "nonexistent_file.txt".to_string(),
            storage,
        };

        // Run the operator and expect an error
        let result = operator.run(&input).await;
        assert!(matches!(
            result,
            Err(FetchVersionFileError::StorageError(_))
        ));
    }
}
