use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use std::collections::HashSet;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct DeleteVersionsAtSysDbOperator {}

#[derive(Debug)]
pub struct DeleteVersionsAtSysDbInput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
    pub unused_s3_files: HashSet<String>,
}

#[derive(Debug)]
pub struct DeleteVersionsAtSysDbOutput {
    pub version_file: CollectionVersionFile,
    pub versions_to_delete: VersionListForCollection,
    pub unused_s3_files: HashSet<String>,
}

#[derive(Error, Debug)]
pub enum DeleteVersionsAtSysDbError {
    #[error("Error deleting versions in sysdb: {0}")]
    SysDBError(String),
}

impl ChromaError for DeleteVersionsAtSysDbError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
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

        tracing::info!(
            unused_files_count = input.unused_s3_files.len(),
            unused_files = ?input.unused_s3_files,
            "Unused S3 files that will be cleaned up after version deletion"
        );

        let mut sysdb = input.sysdb_client.clone();

        if !input.versions_to_delete.versions.is_empty() {
            tracing::info!(
                versions = ?input.versions_to_delete.versions,
                "Deleting versions from SysDB"
            );

            match sysdb
                .delete_collection_version(vec![input.versions_to_delete.clone()])
                .await
            {
                Ok(_) => {
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
            unused_s3_files: input.unused_s3_files.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sysdb::TestSysDb;

    #[tokio::test]
    async fn test_delete_versions_success() {
        let sysdb = SysDb::Test(TestSysDb::new());
        let version_file = CollectionVersionFile::default();
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
            unused_s3_files: HashSet::new(),
        };

        let operator = DeleteVersionsAtSysDbOperator {};
        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
        assert_eq!(output.versions_to_delete, versions_to_delete);
    }

    #[tokio::test]
    async fn test_delete_versions_empty_list() {
        let sysdb = SysDb::Test(TestSysDb::new());
        let version_file = CollectionVersionFile::default();
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
            unused_s3_files: HashSet::new(),
        };

        let operator = DeleteVersionsAtSysDbOperator {};
        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
        assert_eq!(output.versions_to_delete, versions_to_delete);
    }
}
