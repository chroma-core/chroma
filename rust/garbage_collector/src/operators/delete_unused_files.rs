use crate::types::CleanupMode;
use crate::types::FilePathSet;
use crate::types::RENAMED_FILE_PREFIX;
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_storage::StorageError;
use chroma_system::{Operator, OperatorType};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct DeleteUnusedFilesOperator {
    storage: Storage,
    cleanup_mode: CleanupMode,
    tenant_id: String,
}

impl DeleteUnusedFilesOperator {
    pub fn new(storage: Storage, cleanup_mode: CleanupMode, tenant_id: String) -> Self {
        Self {
            storage,
            cleanup_mode,
            tenant_id,
        }
    }

    fn get_rename_path(&self, path: &str) -> String {
        format!("{}{}/{path}", RENAMED_FILE_PREFIX, self.tenant_id,)
    }
    async fn rename_with_path(
        &self,
        file_path: String,
        new_path: String,
    ) -> Result<(), StorageError> {
        self.storage.rename(&file_path, &new_path).await
    }
}

#[derive(Debug)]
pub struct DeleteUnusedFilesInput {
    pub unused_s3_files: FilePathSet,
}

#[derive(Debug)]
pub struct DeleteUnusedFilesOutput {
    pub num_files_deleted: usize,
}

#[derive(Error, Debug)]
pub enum DeleteUnusedFilesError {
    #[error("Error deleting file {path}: {message}")]
    DeleteError { path: String, message: String },
    #[error("Error renaming file {path}: {message}")]
    RenameError { path: String, message: String },
    #[error("Error writing deletion list {path}: {message}")]
    WriteListError { path: String, message: String },
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

impl ChromaError for DeleteUnusedFilesError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<DeleteUnusedFilesInput, DeleteUnusedFilesOutput> for DeleteUnusedFilesOperator {
    type Error = DeleteUnusedFilesError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &DeleteUnusedFilesInput,
    ) -> Result<DeleteUnusedFilesOutput, DeleteUnusedFilesError> {
        tracing::debug!(
            files_count = input.unused_s3_files.len(),
            cleanup_mode = ?self.cleanup_mode,
            "Starting deletion of unused files"
        );

        // NOTE(rohit):
        // We don't want to fail the entire operation if one file fails to rename or delete.
        // It's possible that the file was already renamed/deleted in the last run that
        // did not finish successfully (i.e. crashed before committing the work to SysDb).
        match self.cleanup_mode {
            CleanupMode::DryRunV2 => {}
            CleanupMode::Rename => {
                // Soft delete - rename the file
                if !input.unused_s3_files.is_empty() {
                    let mut rename_stream = futures::stream::iter(input.unused_s3_files.iter())
                        .map(move |file_path| {
                            let new_path = self.get_rename_path(&file_path);
                            self.rename_with_path(file_path, new_path)
                        })
                        .buffer_unordered(100);

                    // Process any errors that occurred
                    while let Some(result) = rename_stream.next().await {
                        if let Err(e) = result {
                            match e {
                                StorageError::NotFound { path, source } => {
                                    tracing::info!("Rename file {path} not found: {source}")
                                }
                                StorageError::AlreadyExists { path, source } => {
                                    tracing::info!("Rename file {path} already exists: {source}")
                                }
                                err => tracing::error!("Failed to rename: {err}"),
                            }
                        }
                    }
                }
            }
            CleanupMode::DeleteV2 => {
                // Hard delete - remove the file
                if !input.unused_s3_files.is_empty() {
                    let mut delete_stream = futures::stream::iter(input.unused_s3_files.iter())
                        // The S3 DeleteObjects API allows up to 1000 objects per request
                        .chunks(1000)
                        .then(|chunk| async move { self.storage.delete_many(chunk).await })
                        .boxed();

                    while let Some(delete_result) = delete_stream.try_next().await? {
                        if !delete_result.errors.is_empty() {
                            // Log the errors but don't fail the operation
                            for error in delete_result.errors {
                                match error {
                                    StorageError::NotFound { path, source } => {
                                        tracing::info!("Delete file {path} not found: {source}")
                                    }
                                    err => tracing::error!("Failed to delete: {err}"),
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(DeleteUnusedFilesOutput {
            num_files_deleted: input.unused_s3_files.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_index::hnsw_provider::FILES;
    use chroma_storage::local::LocalStorage;
    use chroma_storage::PutOptions;
    use std::path::Path;
    use tempfile::TempDir;

    async fn create_test_file(storage: &Storage, path: &str, content: &[u8]) {
        storage
            .put_bytes(path, content.to_vec(), PutOptions::default())
            .await
            .unwrap();
    }

    async fn setup_test_files(storage: &Storage) -> FilePathSet {
        let mut set = FilePathSet::new();

        // Create regular test files
        let test_files = vec!["file1.txt", "file2.txt"];
        for file in &test_files {
            create_test_file(storage, file, b"test content").await;
            set.insert_path(file.to_string());
        }

        // Create HNSW test files
        let hnsw_files = FILES
            .iter()
            .map(|file_name| format!("hnsw/prefix1/{}", file_name))
            .collect::<Vec<String>>();
        for file in &hnsw_files {
            create_test_file(storage, file, b"test content").await;
            set.insert_path(file.to_string());
        }

        set
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let unused_s3_files = setup_test_files(&storage).await;

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::DryRunV2,
            "test_tenant".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_s3_files.clone(),
        };

        operator.run(&input).await.unwrap();

        // Verify original files still exist
        for file in unused_s3_files.iter() {
            assert!(Path::new(&tmp_dir.path().join(file)).exists());
        }
    }

    #[tokio::test]
    async fn test_rename_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let unused_s3_files = setup_test_files(&storage).await;

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Rename,
            "test_tenant".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_s3_files.clone(),
        };

        operator.run(&input).await.unwrap();

        // Verify files were moved to deleted directory
        for file in unused_s3_files.iter() {
            let original_path = tmp_dir.path().join(&file);
            let new_path = tmp_dir
                .path()
                .join(format!("{}{}/{}", RENAMED_FILE_PREFIX, "test_tenant", file));
            assert!(!original_path.exists());
            assert!(new_path.exists());
        }
    }

    #[tokio::test]
    async fn test_delete_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let unused_s3_files = setup_test_files(&storage).await;

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::DeleteV2,
            "test_tenant".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_s3_files.clone(),
        };

        operator.run(&input).await.unwrap();

        // Verify files were deleted
        for file in unused_s3_files.iter() {
            assert!(!Path::new(&tmp_dir.path().join(file)).exists());
        }
    }

    #[tokio::test]
    async fn test_error_handling() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        let mut unused_files = FilePathSet::new();
        unused_files.insert_path("nonexistent.txt".to_string());

        // Test Delete mode - should succeed but record the error in deletion list
        let delete_operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::DeleteV2,
            "test_tenant".to_string(),
        );
        let result = delete_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files.clone(),
            })
            .await;
        assert!(result.is_ok());

        // Test Rename mode - should succeed but record the error in deletion list
        let rename_operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Rename,
            "test_tenant".to_string(),
        );
        let result = rename_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files.clone(),
            })
            .await;
        assert!(result.is_ok());

        // Test DryRun mode with nonexistent files (should succeed)
        let list_operator = DeleteUnusedFilesOperator::new(
            storage,
            CleanupMode::DryRunV2,
            "test_tenant".to_string(),
        );
        let result = list_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files,
            })
            .await;
        assert!(result.is_ok());
    }
}
