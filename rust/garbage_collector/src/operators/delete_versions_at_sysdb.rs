use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{DeleteOptions, Storage};
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use futures::stream::StreamExt;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
pub struct DeleteVersionsAtSysDbOperator {
    pub storage: Storage,
}

impl std::fmt::Debug for DeleteVersionsAtSysDbOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteVersionsAtSysDbOperator")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct DeleteVersionsAtSysDbInput {
    pub version_file: Arc<CollectionVersionFile>,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
}

#[derive(Debug)]
pub struct DeleteVersionsAtSysDbOutput {
    pub version_file: Arc<CollectionVersionFile>,
    pub versions_to_delete: VersionListForCollection,
}

#[derive(Error, Debug)]
pub enum DeleteVersionsAtSysDbError {
    #[error("Unknown error occurred when deleting versions at sysdb")]
    UnknownError,
    #[error("Error deleting versions in sysdb: {0}")]
    SysDBError(String),
    #[error("Error deleting version file {path}: {message}")]
    DeleteFileError { path: String, message: String },
}

impl ChromaError for DeleteVersionsAtSysDbError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl DeleteVersionsAtSysDbOperator {
    async fn delete_version_files(
        &self,
        version_file: &CollectionVersionFile,
        versions_to_delete: &[i64],
    ) {
        // Handle case where version_history is None
        let version_history = match &version_file.version_history {
            Some(history) => history,
            None => return, // Nothing to delete if there's no version history
        };

        let version_files_to_delete: Vec<String> = version_history
            .versions
            .iter()
            .filter(|v| versions_to_delete.contains(&v.version))
            .filter_map(|v| {
                if !v.version_file_name.is_empty() && v.marked_for_deletion {
                    Some(v.version_file_name.clone())
                } else {
                    None
                }
            })
            .collect();

        if version_files_to_delete.is_empty() {
            return;
        }

        tracing::info!("Deleting version files");

        let mut futures = Vec::new();
        for file_path in &version_files_to_delete {
            let storage = self.storage.clone();
            let path = file_path.clone();
            futures.push(async move {
                storage
                    .delete(&path, DeleteOptions::default())
                    .await
                    .map_err(|e| (path, e.to_string()))
            });
        }

        let num_futures = futures.len();
        let results = futures::stream::iter(futures)
            .buffer_unordered(num_futures)
            .collect::<Vec<_>>()
            .await;

        // Process any errors that occurred during file deletion
        for result in results {
            if let Err((path, error)) = result {
                tracing::warn!(
                    error = %error,
                    path = %path,
                    "Failed to delete version file {}, continuing since it could have been deleted already",
                    path
                );
            }
        }
    }
}

#[async_trait]
impl Operator<DeleteVersionsAtSysDbInput, DeleteVersionsAtSysDbOutput>
    for DeleteVersionsAtSysDbOperator
{
    type Error = DeleteVersionsAtSysDbError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &DeleteVersionsAtSysDbInput,
    ) -> Result<DeleteVersionsAtSysDbOutput, DeleteVersionsAtSysDbError> {
        tracing::info!(
            collection_id = %input.versions_to_delete.collection_id,
            database_id = %input.versions_to_delete.database_id,
            tenant_id = %input.versions_to_delete.tenant_id,
            versions = ?input.versions_to_delete.versions,
            epoch_id = input.epoch_id,
            "Starting deletion of versions from SysDB"
        );

        let mut sysdb = input.sysdb_client.clone();

        if !input.versions_to_delete.versions.is_empty() {
            // First, delete the version files from the storage.
            self.delete_version_files(&input.version_file, &input.versions_to_delete.versions)
                .await;

            tracing::info!(
                versions = ?input.versions_to_delete.versions,
                "Deleting versions from SysDB"
            );

            match sysdb
                .delete_collection_version(vec![input.versions_to_delete.clone()])
                .await
            {
                Ok(results) => {
                    for (_, was_successful) in results {
                        if !was_successful {
                            return Err(DeleteVersionsAtSysDbError::UnknownError);
                        }
                    }

                    tracing::info!(
                        versions = ?input.versions_to_delete.versions,
                        "Successfully deleted versions from SysDB"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        versions = ?input.versions_to_delete.versions,
                        "Failed to delete versions from SysDB"
                    );
                    return Err(DeleteVersionsAtSysDbError::SysDBError(e.to_string()));
                }
            }
        } else {
            tracing::info!("No versions to delete from SysDB");
        }

        tracing::info!("Version deletion operation completed successfully");

        Ok(DeleteVersionsAtSysDbOutput {
            version_file: input.version_file.clone(),
            versions_to_delete: input.versions_to_delete.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_storage::local::LocalStorage;
    use chroma_sysdb::TestSysDb;
    use chroma_types::chroma_proto;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_delete_versions_success() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let sysdb = SysDb::Test(TestSysDb::new());

        // Create a version file with actual version history
        let version_file = Arc::new(CollectionVersionFile {
            version_history: Some(chroma_proto::CollectionVersionHistory { versions: vec![] }),
            ..Default::default()
        });

        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![2, 3, 4],
        };

        let input = DeleteVersionsAtSysDbInput {
            version_file: version_file.clone(),
            versions_to_delete: versions_to_delete.clone(),
            sysdb_client: sysdb,
            epoch_id: 123,
        };

        let operator = DeleteVersionsAtSysDbOperator {
            storage: storage.clone(),
        };
        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
        assert_eq!(output.versions_to_delete, versions_to_delete);
    }

    #[tokio::test]
    async fn test_delete_versions_empty_list() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let sysdb = SysDb::Test(TestSysDb::new());
        let version_file = Arc::new(CollectionVersionFile::default());
        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![],
        };

        let input = DeleteVersionsAtSysDbInput {
            version_file: version_file.clone(),
            versions_to_delete: versions_to_delete.clone(),
            sysdb_client: sysdb,
            epoch_id: 123,
        };

        let operator = DeleteVersionsAtSysDbOperator {
            storage: storage.clone(),
        };
        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
        assert_eq!(output.versions_to_delete, versions_to_delete);
    }

    #[tokio::test]
    async fn test_delete_version_files() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        // Create test files in the temporary directory
        let test_files = vec!["version_1", "version_2"];
        for file in &test_files {
            std::fs::write(tmp_dir.path().join(file), "test content").unwrap();
        }

        // Create version file with history
        let version_file = CollectionVersionFile {
            version_history: Some(chroma_proto::CollectionVersionHistory {
                versions: vec![
                    chroma_proto::CollectionVersionInfo {
                        version: 1,
                        version_file_name: "version_1".to_string(),
                        marked_for_deletion: true,
                        ..Default::default()
                    },
                    chroma_proto::CollectionVersionInfo {
                        version: 2,
                        version_file_name: "version_2".to_string(),
                        marked_for_deletion: true,
                        ..Default::default()
                    },
                    chroma_proto::CollectionVersionInfo {
                        version: 3,
                        version_file_name: "".to_string(), // Empty file name to test filtering
                        marked_for_deletion: true,
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        };

        let operator = DeleteVersionsAtSysDbOperator {
            storage: storage.clone(),
        };

        // Test deleting specific versions
        operator
            .delete_version_files(&version_file, &[1, 2, 3])
            .await;

        // Verify files were deleted
        for file in &test_files {
            assert!(
                !tmp_dir.path().join(file).exists(),
                "File {} should be deleted",
                file
            );
        }

        // Test with non-existent files (should not panic)
        operator
            .delete_version_files(&version_file, &[1, 2, 3])
            .await;
    }

    #[tokio::test]
    async fn test_delete_version_files_no_history() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        // Create version file without history
        let version_file = CollectionVersionFile {
            version_history: None,
            ..Default::default()
        };

        let operator = DeleteVersionsAtSysDbOperator {
            storage: storage.clone(),
        };

        // Should return early without error
        operator
            .delete_version_files(&version_file, &[1, 2, 3])
            .await;
    }

    #[tokio::test]
    async fn test_operator_deletes_version_files() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let sysdb = SysDb::Test(TestSysDb::new());

        // Create test files in the temporary directory
        let test_files = vec!["version_1", "version_2"];
        for file in &test_files {
            std::fs::write(tmp_dir.path().join(file), "test content").unwrap();
        }

        // Create version file with history
        let version_file = Arc::new(CollectionVersionFile {
            version_history: Some(chroma_proto::CollectionVersionHistory {
                versions: vec![
                    chroma_proto::CollectionVersionInfo {
                        version: 1,
                        version_file_name: "version_1".to_string(),
                        marked_for_deletion: true,
                        ..Default::default()
                    },
                    chroma_proto::CollectionVersionInfo {
                        version: 2,
                        version_file_name: "version_2".to_string(),
                        marked_for_deletion: true,
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        });

        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![1, 2],
        };

        let input = DeleteVersionsAtSysDbInput {
            version_file: version_file.clone(),
            versions_to_delete: versions_to_delete.clone(),
            sysdb_client: sysdb,
            epoch_id: 123,
        };

        let operator = DeleteVersionsAtSysDbOperator {
            storage: storage.clone(),
        };

        // Run the operator
        let result = operator.run(&input).await;
        assert!(result.is_ok());

        // Verify files were deleted
        for file in &test_files {
            assert!(
                !tmp_dir.path().join(file).exists(),
                "File {} should have been deleted by the operator",
                file
            );
        }

        // Verify the output matches our expectations
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
        assert_eq!(output.versions_to_delete, versions_to_delete);
    }

    #[tokio::test]
    async fn test_only_deletes_files_marked_for_deletion() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let sysdb = SysDb::Test(TestSysDb::new());

        // Create test files in the temporary directory
        let test_files = vec!["version_1", "version_2"];
        for file in &test_files {
            std::fs::write(tmp_dir.path().join(file), "test content").unwrap();
        }

        // Create version file with history
        let version_file = Arc::new(CollectionVersionFile {
            version_history: Some(chroma_proto::CollectionVersionHistory {
                versions: vec![
                    chroma_proto::CollectionVersionInfo {
                        version: 1,
                        version_file_name: "version_1".to_string(),
                        marked_for_deletion: true,
                        ..Default::default()
                    },
                    chroma_proto::CollectionVersionInfo {
                        version: 2,
                        version_file_name: "version_2".to_string(),
                        marked_for_deletion: false,
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        });

        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![1, 2],
        };

        let input = DeleteVersionsAtSysDbInput {
            version_file: version_file.clone(),
            versions_to_delete: versions_to_delete.clone(),
            sysdb_client: sysdb,
            epoch_id: 123,
        };

        let operator = DeleteVersionsAtSysDbOperator {
            storage: storage.clone(),
        };

        // Run the operator
        let result = operator.run(&input).await;
        assert!(result.is_ok());

        // Verify only the file marked for deletion was deleted
        // version_1 should be deleted
        assert!(
            !tmp_dir.path().join("version_1").exists(),
            "File version_1 should have been deleted"
        );
        // version_2 should still exist since it was not marked for deletion
        assert!(
            tmp_dir.path().join("version_2").exists(),
            "File version_2 should not have been deleted"
        );
    }
}
