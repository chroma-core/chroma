use crate::types::CleanupMode;
use crate::types::{DELETE_LIST_FILE_PREFIX, RENAMED_FILE_PREFIX};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::HNSW_INDEX_S3_PREFIX;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use futures::stream::StreamExt;
use std::collections::HashSet;
use thiserror::Error;

struct DeletionListBuilder {
    files: Vec<String>,
    failed_files: Vec<String>,
}

impl DeletionListBuilder {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            failed_files: Vec::new(),
        }
    }

    fn add_files(mut self, files: &[String]) -> Self {
        self.files.extend(files.iter().cloned());
        self
    }

    fn add_failed_files(mut self, failed_files: &[String]) -> Self {
        self.failed_files.extend(failed_files.iter().cloned());
        self
    }

    fn build(mut self) -> String {
        let mut content = String::from("Deleted Files:\n");
        content.push_str(&self.files.join("\n"));

        if !self.failed_files.is_empty() {
            self.failed_files.sort();
            content.push_str("\n\nFailed files:\n");
            content.push_str(&self.failed_files.join("\n"));
        }

        content
    }
}

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

    fn get_deletion_list_path(&self, timestamp: i64) -> String {
        format!(
            "{}{}/{}.txt",
            DELETE_LIST_FILE_PREFIX, self.collection_id, timestamp
        )
    }

    async fn write_deletion_list(
        &self,
        files: &[String],
        timestamp: i64,
        failed_files: &[String],
    ) -> Result<(), DeleteUnusedFilesError> {
        let final_content = DeletionListBuilder::new()
            .add_files(files)
            .add_failed_files(failed_files)
            .build();

        let path = self.get_deletion_list_path(timestamp);

        tracing::info!(
            path = %path,
            file_count = files.len(),
            failed_count = failed_files.len(),
            "Writing deletion list to S3"
        );

        self.storage
            .put_bytes(&path, final_content.into_bytes(), Default::default())
            .await
            .map_err(|e| DeleteUnusedFilesError::WriteListError {
                path: path.clone(),
                message: e.to_string(),
            })?;

        Ok(())
    }

    async fn delete_with_path(&self, file_path: String) -> Result<(), FileOperationError> {
        self.storage
            .delete(&file_path)
            .await
            .map_err(|e| FileOperationError {
                path: file_path,
                error: e.to_string(),
            })
    }

    async fn rename_with_path(
        &self,
        file_path: String,
        new_path: String,
    ) -> Result<(), FileOperationError> {
        self.storage
            .rename(&file_path, &new_path)
            .await
            .map_err(|e| FileOperationError {
                path: file_path,
                error: e.to_string(),
            })
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

#[derive(Debug)]
struct FileOperationError {
    path: String,
    error: String,
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
        let mut file_operation_errors = Vec::new();
        match self.cleanup_mode {
            CleanupMode::ListOnly => {
                // Do nothing here. List is written to S3 for all modes later in this function.
            }
            CleanupMode::Rename => {
                // Soft delete - rename the file
                let mut futures = Vec::new();
                for file_path in &all_files {
                    let new_path = self.get_rename_path(file_path, input.epoch_id);
                    futures.push(self.rename_with_path(file_path.clone(), new_path));
                }

                let num_futures = futures.len();
                if num_futures > 0 {
                    let results: Vec<Result<(), FileOperationError>> =
                        futures::stream::iter(futures)
                            .buffer_unordered(num_futures)
                            .collect()
                            .await;

                    // Process any errors that occurred
                    for result in results {
                        if let Err(e) = result {
                            file_operation_errors.push(format!("{}: {}", e.path, e.error));
                        }
                    }
                }
            }
            CleanupMode::Delete => {
                // Hard delete - remove the file
                let mut futures = Vec::new();
                for file_path in &all_files {
                    futures.push(self.delete_with_path(file_path.clone()));
                }

                let num_futures = futures.len();
                if num_futures > 0 {
                    let results: Vec<Result<(), FileOperationError>> =
                        futures::stream::iter(futures)
                            .buffer_unordered(num_futures)
                            .collect()
                            .await;

                    // Process any errors that occurred
                    for result in results {
                        if let Err(e) = result {
                            file_operation_errors.push(format!("{}: {}", e.path, e.error));
                        }
                    }
                }
            }
        }

        // Write the deletion list with any potential failed files
        self.write_deletion_list(&all_files, input.epoch_id, &file_operation_errors)
            .await?;

        tracing::debug!(
            "File deletion operation completed with {} file operation errors",
            file_operation_errors.len()
        );
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
    async fn test_list_only_mode() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let (test_files, hnsw_files) = setup_test_files(&storage).await;

        let mut unused_files = HashSet::new();
        unused_files.extend(test_files.clone());

        let operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::ListOnly,
            "test_collection".to_string(),
        );
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files.clone(),
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec!["prefix1".to_string()],
        };

        let result = operator.run(&input).await.unwrap();

        // Verify deletion list file was created
        let deletion_list_path = tmp_dir.path().join(format!(
            "{}test_collection/123.txt",
            DELETE_LIST_FILE_PREFIX
        ));
        assert!(deletion_list_path.exists());

        // Verify original files still exist
        for file in &test_files {
            assert!(result.deleted_files.contains(file));
            assert!(Path::new(&tmp_dir.path().join(file)).exists());
        }

        // Read and verify deletion list content
        let content = std::fs::read_to_string(deletion_list_path).unwrap();
        let listed_files: HashSet<_> = content.lines().collect();
        for file in &test_files {
            assert!(listed_files.contains(file.as_str()));
        }
        for file in &hnsw_files {
            assert!(listed_files.contains(file.as_str()));
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

        // Verify deletion list was created
        let deletion_list_path = tmp_dir.path().join(format!(
            "{}test_collection/123.txt",
            DELETE_LIST_FILE_PREFIX
        ));
        assert!(deletion_list_path.exists());

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

        // Verify deletion list contents
        let content = std::fs::read_to_string(deletion_list_path).unwrap();
        let listed_files: HashSet<_> = content.lines().collect();
        for file in &test_files {
            assert!(listed_files.contains(file.as_str()));
        }
        for file in &hnsw_files {
            assert!(listed_files.contains(file.as_str()));
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

        // Verify deletion list was created
        let deletion_list_path = tmp_dir.path().join(format!(
            "{}test_collection/123.txt",
            DELETE_LIST_FILE_PREFIX
        ));
        assert!(deletion_list_path.exists());

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

        // Verify deletion list contents
        let content = std::fs::read_to_string(deletion_list_path).unwrap();
        let listed_files: HashSet<_> = content.lines().collect();
        for file in &test_files {
            assert!(listed_files.contains(file.as_str()));
        }
        for file in &hnsw_files {
            assert!(listed_files.contains(file.as_str()));
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

        // Verify deletion list contains the error
        let deletion_list_path = tmp_dir.path().join(format!(
            "{}test_collection/123.txt",
            DELETE_LIST_FILE_PREFIX
        ));
        let content = std::fs::read_to_string(deletion_list_path).unwrap();
        assert!(content.contains("Failed files:"));
        assert!(content.contains("nonexistent.txt"));

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

        // Verify deletion list contains the error
        let deletion_list_path = tmp_dir.path().join(format!(
            "{}test_collection/124.txt",
            DELETE_LIST_FILE_PREFIX
        ));
        let content = std::fs::read_to_string(deletion_list_path).unwrap();
        assert!(content.contains("Failed files:"));
        assert!(content.contains("nonexistent.txt"));

        // Test ListOnly mode with nonexistent files (should succeed)
        let list_operator = DeleteUnusedFilesOperator::new(
            storage,
            CleanupMode::ListOnly,
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

        // Verify deletion list was created even for nonexistent files
        let deletion_list_path = tmp_dir.path().join(format!(
            "{}test_collection/125.txt",
            DELETE_LIST_FILE_PREFIX
        ));
        assert!(deletion_list_path.exists());
    }
}
