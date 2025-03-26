use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct MarkVersionsAtSysDbOperator {}

#[derive(Debug)]
pub struct MarkVersionsAtSysDbInput {
    pub version_file: CollectionVersionFile,
    pub versions_to_delete: VersionListForCollection,
    pub sysdb_client: SysDb,
    pub epoch_id: i64,
    pub oldest_version_to_keep: i64,
}

#[derive(Debug)]
pub struct MarkVersionsAtSysDbOutput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
    pub oldest_version_to_keep: i64,
}

#[derive(Error, Debug)]
pub enum MarkVersionsAtSysDbError {
    #[error("Error marking versions in sysdb: {0}")]
    SysDBError(String),
}

impl ChromaError for MarkVersionsAtSysDbError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<MarkVersionsAtSysDbInput, MarkVersionsAtSysDbOutput> for MarkVersionsAtSysDbOperator {
    type Error = MarkVersionsAtSysDbError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &MarkVersionsAtSysDbInput,
    ) -> Result<MarkVersionsAtSysDbOutput, MarkVersionsAtSysDbError> {
        let mut sysdb = input.sysdb_client.clone();
        if !input.versions_to_delete.versions.is_empty() {
            let result = sysdb
                .mark_version_for_deletion(input.epoch_id, vec![input.versions_to_delete.clone()])
                .await;
            if let Err(e) = result {
                return Err(MarkVersionsAtSysDbError::SysDBError(e.to_string()));
            }
        }

        Ok(MarkVersionsAtSysDbOutput {
            version_file: input.version_file.clone(),
            epoch_id: input.epoch_id,
            sysdb_client: input.sysdb_client.clone(),
            versions_to_delete: input.versions_to_delete.clone(),
            oldest_version_to_keep: input.oldest_version_to_keep,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sysdb::TestSysDb;

    #[tokio::test]
    async fn test_mark_versions_success() {
        let sysdb = SysDb::Test(TestSysDb::new());
        let version_file = CollectionVersionFile::default();
        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![2, 3, 4],
        };

        let input = MarkVersionsAtSysDbInput {
            version_file: version_file.clone(),
            versions_to_delete,
            sysdb_client: sysdb,
            epoch_id: 123,
            oldest_version_to_keep: 1,
        };

        let operator = MarkVersionsAtSysDbOperator {};
        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
    }

    #[tokio::test]
    async fn test_mark_versions_empty_list() {
        let sysdb = SysDb::Test(TestSysDb::new());
        let version_file = CollectionVersionFile::default();
        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![],
        };

        let input = MarkVersionsAtSysDbInput {
            version_file: version_file.clone(),
            versions_to_delete,
            sysdb_client: sysdb,
            epoch_id: 123,
            oldest_version_to_keep: 1,
        };

        let operator = MarkVersionsAtSysDbOperator {};
        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.version_file, version_file);
    }

    #[tokio::test]
    async fn test_mark_versions_error() {
        let sysdb = SysDb::Test(TestSysDb::new());
        let version_file = CollectionVersionFile::default();
        let versions_to_delete = VersionListForCollection {
            collection_id: "test_collection".to_string(),
            database_id: "default".to_string(),
            tenant_id: "default".to_string(),
            versions: vec![0],
        };

        let input = MarkVersionsAtSysDbInput {
            version_file,
            versions_to_delete,
            sysdb_client: sysdb,
            epoch_id: 123,
            oldest_version_to_keep: 1,
        };

        let operator = MarkVersionsAtSysDbOperator {};
        let result = operator.run(&input).await;

        assert!(result.is_err());
        match result {
            Err(MarkVersionsAtSysDbError::SysDBError(err)) => {
                assert_eq!(err, "Failed to mark version for deletion");
            }
            _ => panic!("Expected SysDBError"),
        }
    }
}
