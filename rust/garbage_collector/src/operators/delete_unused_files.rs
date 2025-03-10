use crate::types::CleanupMode;
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
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
        format!("gc/deleted/{epoch}/{path}")
    }

    fn get_deletion_list_path(&self, timestamp: i64) -> String {
        format!("gc/deletion-list/{}/{}.txt", self.collection_id, timestamp)
    }

    async fn write_deletion_list(
        &self,
        files: &[String],
        timestamp: i64,
        failed_files: &[String],
    ) -> Result<(), DeleteUnusedFilesError> {
        let all_files: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

        let content = all_files.join("\n");

        let mut final_content = content;
        if !failed_files.is_empty() {
            let mut sorted_failed = failed_files.to_vec();
            sorted_failed.sort();
            final_content.push_str("\n\nFailed files:\n");
            final_content.push_str(&sorted_failed.join("\n"));
        }

        let path = self.get_deletion_list_path(timestamp);

        tracing::info!(
            path = %path,
            file_count = all_files.len(),
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
                .map(|file| format!("hnsw/{}/{}", prefix, file))
                .collect::<Vec<String>>()
            })
            .collect();

        // Create a list that contains all files that will be deleted.
        let mut all_files = input.unused_s3_files.iter().cloned().collect::<Vec<_>>();
        all_files.extend(hnsw_files);

        // If we're in list-only mode, write the list and return
        if matches!(self.cleanup_mode, CleanupMode::ListOnly) {
            self.write_deletion_list(&all_files, input.epoch_id, &[])
                .await?;
            return Ok(DeleteUnusedFilesOutput {
                deleted_files: all_files.into_iter().collect(),
            });
        }

        let mut failed_files = Vec::new();
        for file_path in &all_files {
            if !self.delete_file(file_path, input.epoch_id).await? {
                failed_files.push(file_path.clone());
            }
        }

        // Write the deletion list with failed files
        self.write_deletion_list(&all_files, input.epoch_id, &failed_files)
            .await?;

        tracing::debug!("File deletion operation completed");

        Ok(DeleteUnusedFilesOutput {
            deleted_files: all_files.into_iter().collect(),
        })
    }
}

impl DeleteUnusedFilesOperator {
    async fn delete_file(
        &self,
        file_path: &str,
        epoch_id: i64,
    ) -> Result<bool, DeleteUnusedFilesError> {
        match self.cleanup_mode {
            CleanupMode::ListOnly => {
                tracing::info!(path = %file_path, "Would process file (list only mode)");
                Ok(true)
            }
            CleanupMode::Rename => {
                // Soft delete - rename the file
                let new_path = self.get_rename_path(file_path, epoch_id);
                tracing::info!(
                    old_path = %file_path,
                    new_path = %new_path,
                    "Renaming file for soft delete"
                );

                match self.storage.rename(file_path, &new_path).await {
                    Ok(_) => {
                        tracing::info!(
                            old_path = %file_path,
                            new_path = %new_path,
                            "Successfully renamed file"
                        );
                        Ok(true)
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            path = %file_path,
                            "Failed to rename file"
                        );
                        Err(DeleteUnusedFilesError::RenameError {
                            path: file_path.to_string(),
                            message: e.to_string(),
                        })
                    }
                }
            }
            CleanupMode::Delete => {
                // Hard delete - remove the file
                tracing::info!(path = %file_path, "Deleting file");

                match self.storage.delete(file_path).await {
                    Ok(_) => {
                        tracing::info!(path = %file_path, "Successfully deleted file");
                        Ok(true)
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            path = %file_path,
                            "Failed to delete file"
                        );
                        Err(DeleteUnusedFilesError::DeleteError {
                            path: file_path.to_string(),
                            message: e.to_string(),
                        })
                    }
                }
            }
        }
    }
}

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
            "hnsw/prefix1/header.bin",
            "hnsw/prefix1/data_level0.bin",
            "hnsw/prefix1/length.bin",
            "hnsw/prefix1/link_lists.bin",
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
        let deletion_list_path = tmp_dir
            .path()
            .join("gc/deletion-list/test_collection/123.txt");
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
        let deletion_list_path = tmp_dir
            .path()
            .join("gc/deletion-list/test_collection/123.txt");
        assert!(deletion_list_path.exists());

        // Verify regular files were moved to deleted directory
        for file in &test_files {
            let original_path = tmp_dir.path().join(file);
            let new_path = tmp_dir.path().join(format!("gc/deleted/123/{}", file));
            assert!(!original_path.exists());
            assert!(new_path.exists());
            assert!(result.deleted_files.contains(file));
        }

        // Verify HNSW files were moved to deleted directory
        for file in &hnsw_files {
            let original_path = tmp_dir.path().join(file);
            let new_path = tmp_dir.path().join(format!("gc/deleted/123/{}", file));
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
        let deletion_list_path = tmp_dir
            .path()
            .join("gc/deletion-list/test_collection/123.txt");
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

        // Test error handling for Delete mode
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
        assert!(matches!(
            result,
            Err(DeleteUnusedFilesError::DeleteError { .. })
        ));

        // Test error handling for Rename mode
        let rename_operator = DeleteUnusedFilesOperator::new(
            storage.clone(),
            CleanupMode::Rename,
            "test_collection".to_string(),
        );
        let result = rename_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files.clone(),
                epoch_id: 123,
                hnsw_prefixes_for_deletion: vec![],
            })
            .await;
        assert!(matches!(
            result,
            Err(DeleteUnusedFilesError::RenameError { .. })
        ));

        // Test ListOnly mode with nonexistent files (should succeed)
        let list_operator = DeleteUnusedFilesOperator::new(
            storage,
            CleanupMode::ListOnly,
            "test_collection".to_string(),
        );
        let result = list_operator
            .run(&DeleteUnusedFilesInput {
                unused_s3_files: unused_files,
                epoch_id: 123,
                hnsw_prefixes_for_deletion: vec![],
            })
            .await;
        assert!(result.is_ok());

        // Verify deletion list was created even for nonexistent files
        let deletion_list_path = tmp_dir
            .path()
            .join("gc/deletion-list/test_collection/123.txt");
        assert!(deletion_list_path.exists());
    }
}
