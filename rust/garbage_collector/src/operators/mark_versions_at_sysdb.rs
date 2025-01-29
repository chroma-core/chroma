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
    pub sysdb_client: Box<SysDb>,
    pub epoch_id: i64,
}

#[derive(Debug)]
pub struct MarkVersionsAtSysDbOutput {
    pub version_file: CollectionVersionFile,
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
        })
    }
}
