use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use std::collections::HashSet;
use thiserror::Error;

#[derive(Clone)]
pub struct DeleteUnusedFilesOperator {
    storage: Storage,
    soft_delete: bool,
}

impl std::fmt::Debug for DeleteUnusedFilesOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteUnusedFilesOperator")
            .field("soft_delete", &self.soft_delete)
            .finish_non_exhaustive()
    }
}

impl DeleteUnusedFilesOperator {
    pub fn new(storage: Storage, soft_delete: bool) -> Self {
        tracing::debug!(soft_delete, "Creating new DeleteUnusedFilesOperator");
        Self {
            storage,
            soft_delete,
        }
    }

    fn get_soft_delete_path(&self, path: &str, epoch: i64) -> String {
        format!("deleted/{epoch}/{path}")
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
        tracing::info!(
            files_count = input.unused_s3_files.len(),
            hnsw_prefixes_count = input.hnsw_prefixes_for_deletion.len(),
            files = ?input.unused_s3_files,
            hnsw_prefixes = ?input.hnsw_prefixes_for_deletion,
            soft_delete = self.soft_delete,
            "Starting deletion of unused files"
        );

        let mut deleted_files = HashSet::new();

        // Log and delete regular unused files
        println!("Deleting unused block files: {:?}", input.unused_s3_files);
        for file_path in &input.unused_s3_files {
            if !self
                .delete_file(file_path, input.epoch_id, &mut deleted_files)
                .await?
            {
                continue;
            }
        }

        // Log and delete HNSW files for each prefix
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
        println!("Deleting HNSW files: {:?}", hnsw_files);

        for prefix in &input.hnsw_prefixes_for_deletion {
            for file in [
                "header.bin",
                "data_level0.bin",
                "length.bin",
                "link_lists.bin",
            ]
            .iter()
            {
                let file_path = format!("hnsw/{}/{}", prefix, file);
                if !self
                    .delete_file(&file_path, input.epoch_id, &mut deleted_files)
                    .await?
                {
                    continue;
                }
            }
        }

        tracing::info!(
            deleted_count = deleted_files.len(),
            deleted_files = ?deleted_files,
            "File deletion operation completed successfully"
        );

        Ok(DeleteUnusedFilesOutput { deleted_files })
    }
}

impl DeleteUnusedFilesOperator {
    async fn delete_file(
        &self,
        file_path: &str,
        epoch_id: i64,
        deleted_files: &mut HashSet<String>,
    ) -> Result<bool, DeleteUnusedFilesError> {
        if self.soft_delete {
            // Soft delete - rename the file
            let new_path = self.get_soft_delete_path(file_path, epoch_id);
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
                    deleted_files.insert(file_path.to_string());
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
        } else {
            // Hard delete - remove the file
            tracing::info!(path = %file_path, "Deleting file");

            match self.storage.delete(file_path).await {
                Ok(_) => {
                    tracing::info!(path = %file_path, "Successfully deleted file");
                    deleted_files.insert(file_path.to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_storage::local::LocalStorage;
    use std::path::Path;
    use tempfile::TempDir;

    async fn create_test_file(storage: &Storage, path: &str, content: &[u8]) {
        storage.put_bytes(path, content.to_vec()).await.unwrap();
    }

    #[tokio::test]
    async fn test_hard_delete_success() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        // Create test files
        let test_files = vec!["file1.txt", "file2.txt"];
        for file in &test_files {
            create_test_file(&storage, file, b"test content").await;
        }

        // Create HNSW test files with correct filenames
        let hnsw_files = [
            "hnsw/prefix1/header.bin",
            "hnsw/prefix1/data_level0.bin",
            "hnsw/prefix1/length.bin",
            "hnsw/prefix1/link_lists.bin",
        ];
        for file in &hnsw_files {
            create_test_file(&storage, file, b"test content").await;
        }

        let mut unused_files = HashSet::new();
        unused_files.extend(test_files.iter().map(|s| s.to_string()));

        let operator = DeleteUnusedFilesOperator::new(storage.clone(), false);
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files.clone(),
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec!["prefix1".to_string()],
        };

        let _result = operator.run(&input).await.unwrap();

        // Verify regular files were deleted
        for file in test_files {
            assert!(!Path::new(&tmp_dir.path().join(file)).exists());
        }

        // Verify HNSW files were deleted
        for file in hnsw_files {
            assert!(!Path::new(&tmp_dir.path().join(file)).exists());
        }
    }

    #[tokio::test]
    async fn test_soft_delete_success() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        // Create test files
        let test_files = vec!["file1.txt", "file2.txt"];
        for file in &test_files {
            create_test_file(&storage, file, b"test content").await;
        }

        // Create HNSW test files with correct filenames
        let hnsw_files = [
            "hnsw/prefix1/header.bin",
            "hnsw/prefix1/data_level0.bin",
            "hnsw/prefix1/length.bin",
            "hnsw/prefix1/link_lists.bin",
        ];
        for file in &hnsw_files {
            create_test_file(&storage, file, b"test content").await;
        }

        let mut unused_files = HashSet::new();
        unused_files.extend(test_files.iter().map(|s| s.to_string()));

        let operator = DeleteUnusedFilesOperator::new(storage.clone(), true);
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files.clone(),
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec!["prefix1".to_string()],
        };

        let _result = operator.run(&input).await.unwrap();

        // Verify regular files were moved to deleted directory
        for file in test_files {
            let original_path = tmp_dir.path().join(file);
            let new_path = tmp_dir.path().join(format!("deleted/123/{}", file));
            assert!(!original_path.exists());
            assert!(new_path.exists());
        }

        // Verify HNSW files were moved to deleted directory
        for file in hnsw_files {
            let original_path = tmp_dir.path().join(file);
            let new_path = tmp_dir.path().join(format!("deleted/123/{}", file));
            assert!(!original_path.exists());
            assert!(new_path.exists());
        }
    }

    #[tokio::test]
    async fn test_delete_nonexistent_file() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        let mut unused_files = HashSet::new();
        unused_files.insert("nonexistent.txt".to_string());

        let operator = DeleteUnusedFilesOperator::new(storage, false);
        let input = DeleteUnusedFilesInput {
            unused_s3_files: unused_files,
            epoch_id: 123,
            hnsw_prefixes_for_deletion: vec![],
        };

        let result = operator.run(&input).await;
        assert!(matches!(
            result,
            Err(DeleteUnusedFilesError::DeleteError { .. })
        ));
    }
}
