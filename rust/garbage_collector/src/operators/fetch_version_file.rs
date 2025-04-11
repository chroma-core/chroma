//! Operator for GC. Fetches the collection version file from S3.
//!
//! Input:
//! - Version file path. Full file path without the bucket name.
//! - Storage
//!
//! Output:
//! - Version file content Vec<u8>

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{GetOptions, Storage, StorageError};
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
        f.debug_struct("FetchVersionFileInput")
            .field("version_file_path", &self.version_file_path)
            .finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct FetchVersionFileOutput {
    version_file_content: Vec<u8>,
}

impl FetchVersionFileOutput {
    pub fn new(content: Arc<Vec<u8>>) -> Self {
        Self {
            version_file_content: (*content).clone(),
        }
    }

    pub fn version_file_content(&self) -> &[u8] {
        &self.version_file_content
    }
}

#[derive(Error, Debug)]
pub enum FetchVersionFileError {
    #[error("Error fetching version file: {0}")]
    StorageError(#[from] StorageError),
    #[error("Error parsing version file")]
    ParseError,
    #[error("Invalid storage configuration: {0}")]
    StorageConfigError(String),
}

impl ChromaError for FetchVersionFileError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchVersionFileError::StorageError(e) => e.code(),
            FetchVersionFileError::ParseError => ErrorCodes::Internal,
            FetchVersionFileError::StorageConfigError(_) => ErrorCodes::Internal,
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
        tracing::info!(
            path = %input.version_file_path,
            "Starting to fetch version file"
        );

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

        let output = FetchVersionFileOutput::new(content);
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::registry;
    use chroma_config::Configurable;
    use chroma_storage::config::{
        ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
    };
    use chroma_storage::PutOptions;
    use tracing_test::traced_test;

    async fn setup_test_storage() -> Storage {
        // Create storage config for Minio
        let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "chroma-storage".to_string(),
                r#type: ObjectStoreType::Minio,
            },
            upload_part_size_bytes: 1024 * 1024,
            download_part_size_bytes: 1024 * 1024,
            max_concurrent_requests: 10,
        });

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
        let test_content = vec![1, 2, 3, 4, 5];
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
        assert_eq!(result.version_file_content(), &test_content);

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
