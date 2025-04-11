use crate::types::CleanupMode;
use crate::types::RENAMED_FILE_PREFIX;
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::HNSW_INDEX_S3_PREFIX;
use chroma_storage::Storage;
use chroma_storage::StorageError;
use chroma_system::{Operator, OperatorType};
use futures::stream::StreamExt;
use std::collections::HashSet;
use thiserror::Error;

#[derive(Clone)]
pub struct DeleteUnusedFilesOperator {
    storage: Storage,
    cleanup_mode: CleanupMode,
    collection_id: String,
}

impl std::fmt::Debug for DeleteUnusedFilesOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteUnusedFilesOperator")
            .field("cleanup_mode", &self.cleanup_mode)
            .field("collection_id", &self.collection_id)
            .finish_non_exhaustive()
    }
}

impl DeleteUnusedFilesOperator {
    pub fn new(storage: Storage, cleanup_mode: CleanupMode, collection_id: String) -> Self {
        tracing::debug!(
            cleanup_mode = ?cleanup_mode,
            collection_id = %collection_id,
            "Creating new DeleteUnusedFilesOperator"
        );
        Self {
            storage,
            cleanup_mode,
            collection_id,
        }
    }

    // TODO(rohit): Remove epoch, or may be use timestamp instead.
    fn get_rename_path(&self, path: &str, epoch: i64) -> String {
        format!(
            "{}{}/{epoch}/{path}",
            RENAMED_FILE_PREFIX, self.collection_id
        )
    }

    async fn delete_with_path(&self, file_path: String) -> Result<(), StorageError> {
        self.storage.delete(&file_path).await
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
    pub unused_s3_files: HashSet<String>,
    pub epoch_id: i64,
    pub hnsw_prefixes_for_deletion: Vec<String>,
}

#[derive(Debug)]
pub struct DeleteUnusedFilesOutput {
    pub deleted_files: HashSet<String>,
}

#[derive(Error, Debug)]
pub enum DeleteUnusedFilesError {
    #[error("Error deleting file {path}: {message}")]
    DeleteError { path: String, message: String },
    #[error("Error renaming file {path}: {message}")]
    RenameError { path: String, message: String },
    #[error("Error writing deletion list {path}: {message}")]
    WriteListError { path: String, message: String },
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
            hnsw_prefixes_count = input.hnsw_prefixes_for_deletion.len(),
            files = ?input.unused_s3_files,
            hnsw_prefixes = ?input.hnsw_prefixes_for_deletion,
            cleanup_mode = ?self.cleanup_mode,
            "Starting deletion of unused files"
        );

        // Generate list of HNSW files
        let hnsw_files: Vec<String> = input
            .hnsw_prefixes_for_deletion
            .iter()
            .flat_map(|prefix| {
                [
                    "header.bin",
                    "data_level0.bin",
                    "length.bin",
                    "link_lists.bin",
                ]
                .iter()
                .map(|file| format!("{}{}/{}", HNSW_INDEX_S3_PREFIX, prefix, file))
                .collect::<Vec<String>>()
            })
            .collect();

        // Create a list that contains all files that will be deleted.
        let mut all_files = input.unused_s3_files.iter().cloned().collect::<Vec<_>>();
        all_files.extend(hnsw_files);

        // NOTE(rohit):
        // We don't want to fail the entire operation if one file fails to rename or delete.
        // It's possible that the file was already renamed/deleted in the last run that
        // did not finish successfully (i.e. crashed before committing the work to SysDb).
        match self.cleanup_mode {
            CleanupMode::DryRun => {}
            CleanupMode::Rename => {
                // Soft delete - rename the file
                if !all_files.is_empty() {
                    let mut rename_stream = futures::stream::iter(all_files.clone())
                        .map(move |file_path| {
                            let new_path = self.get_rename_path(&file_path, input.epoch_id);
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
            CleanupMode::Delete => {
                // Hard delete - remove the file
                if !all_files.is_empty() {
                    let mut delete_stream = futures::stream::iter(all_files.clone())
                        .map(move |file_path| self.delete_with_path(file_path))
                        .buffer_unordered(100);

                    // Process any errors that occurred
                    while let Some(result) = delete_stream.next().await {
                        if let Err(e) = result {
                            match e {
                                StorageError::NotFound { path, source } => {
                                    tracing::info!("Rename file {path} not found: {source}")
                                }
                                err => tracing::error!("Failed to rename: {err}"),
                            }
                        }
                    }
                }
            }
        }

        Ok(DeleteUnusedFilesOutput {
            deleted_files: all_files.into_iter().collect(),
        })
    }
}

impl DeleteUnusedFilesOperator {}

#[cfg(test)]
mod tests {
    use super::*;
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

    async fn setup_test_files(storage: &Storage) -> (Vec<String>, Vec<String>) {
        // Create regular test files
        let test_files = vec!["file1.txt", "file2.txt"];
        for file in &test_files {
            create_test_file(storage, file, b"test content").await;
        }

        // Create HNSW test files
        let hnsw_files = vec![
            format!("{}{}/header.bin", HNSW_INDEX_S3_PREFIX, "prefix1"),
            format!("{}{}/data_level0.bin", HNSW_INDEX_S3_PREFIX, "prefix1"),
            format!("{}{}/length.bin", HNSW_INDEX_S3_PREFIX, "prefix1"),
            format!("{}{}/link_lists.bin", HNSW_INDEX_S3_PREFIX, "prefix1"),
        ];
        for file in &hnsw_files {
            create_test_file(storage, file, b"test content").await;
        }

        (
            test_files.iter().map(|s| s.to_string()).collect(),
            hnsw_files.iter().map(|s| s.to_string()).collect(),
        )
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let (test_files, _) = setup_test_files(&storage).await;

        let mut unused_files = HashSet::new();
        unused_files.extend(test_files.clone());

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::DryRun,
            "test_collection".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files.clone(),
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec!["prefix1".to_string()],
        };

        let result = operator.run(&input).await.unwrap();

        // Verify original files still exist
        for file in &test_files {
            assert!(result.deleted_files.contains(file));
            assert!(Path::new(&tmp_dir.path().join(file)).exists());
        }
    }

    #[tokio::test]
    async fn test_rename_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let (test_files, hnsw_files) = setup_test_files(&storage).await;

        let mut unused_files = HashSet::new();
        unused_files.extend(test_files.clone());

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Rename,
            "test_collection".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files.clone(),
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec!["prefix1".to_string()],
        };

        let result = operator.run(&input).await.unwrap();

        // Verify regular files were moved to deleted directory
        for file in &test_files {
            let original_path = tmp_dir.path().join(file);
            let new_path = tmp_dir.path().join(format!(
                "{}{}/123/{}",
                RENAMED_FILE_PREFIX, "test_collection", file
            ));
            assert!(!original_path.exists());
            assert!(new_path.exists());
            assert!(result.deleted_files.contains(file));
        }

        // Verify HNSW files were moved to deleted directory
        for file in &hnsw_files {
            let original_path = tmp_dir.path().join(file);
            let new_path = tmp_dir.path().join(format!(
                "{}{}/123/{}",
                RENAMED_FILE_PREFIX, "test_collection", file
            ));
            assert!(!original_path.exists());
            assert!(new_path.exists());
            assert!(result.deleted_files.contains(file));
        }
    }

    #[tokio::test]
    async fn test_delete_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let (test_files, hnsw_files) = setup_test_files(&storage).await;

        let mut unused_files = HashSet::new();
        unused_files.extend(test_files.clone());

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Delete,
            "test_collection".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files.clone(),
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec!["prefix1".to_string()],
        };

        let result = operator.run(&input).await.unwrap();

        // Verify regular files were deleted
        for file in &test_files {
            assert!(!Path::new(&tmp_dir.path().join(file)).exists());
            assert!(result.deleted_files.contains(file));
        }

        // Verify HNSW files were deleted
        for file in &hnsw_files {
            assert!(!Path::new(&tmp_dir.path().join(file)).exists());
            assert!(result.deleted_files.contains(file));
        }
    }

    #[tokio::test]
    async fn test_error_handling() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        let mut unused_files = HashSet::new();
        unused_files.insert("nonexistent.txt".to_string());

        // Test Delete mode - should succeed but record the error in deletion list
        let delete_operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Delete,
            "test_collection".to_string(),
        );
        let result = delete_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files.clone(),
                epoch_id: 123,
                hnsw_prefixes_for_deletion: vec![],
            })
            .await;
        assert!(result.is_ok());

        // Test Rename mode - should succeed but record the error in deletion list
        let rename_operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Rename,
            "test_collection".to_string(),
        );
        let result = rename_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files.clone(),
                epoch_id: 124,
                hnsw_prefixes_for_deletion: vec![],
            })
            .await;
        assert!(result.is_ok());

        // Test DryRun mode with nonexistent files (should succeed)
        let list_operator = DeleteUnusedFilesOperator::new(
            storage,
            CleanupMode::DryRun,
            "test_collection".to_string(),
        );
        let result = list_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files,
                epoch_id: 125,
                hnsw_prefixes_for_deletion: vec![],
            })
            .await;
        assert!(result.is_ok());
    }
}
