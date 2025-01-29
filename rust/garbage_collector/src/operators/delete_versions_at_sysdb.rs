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
    pub sysdb_client: Box<SysDb>,
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
        let mut sysdb = input.sysdb_client.clone();

        if !input.versions_to_delete.versions.is_empty() {
            let result = sysdb
                .delete_collection_version(vec![input.versions_to_delete.clone()])
                .await;
            if let Err(e) = result {
                return Err(DeleteVersionsAtSysDbError::SysDBError(e.to_string()));
            }
        }

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
        let sysdb = Box::new(SysDb::Test(TestSysDb::new()));
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
        let sysdb = Box::new(SysDb::Test(TestSysDb::new()));
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
