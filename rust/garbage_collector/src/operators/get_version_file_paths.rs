use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{BatchGetCollectionVersionFilePathsError, CollectionUuid};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct GetVersionFilePathsInput {
    collection_ids: Vec<CollectionUuid>,
    sysdb: SysDb,
}

impl GetVersionFilePathsInput {
    pub fn new(collection_ids: Vec<CollectionUuid>, sysdb: SysDb) -> Self {
        Self {
            collection_ids,
            sysdb,
        }
    }
}

#[derive(Debug)]
pub struct GetVersionFilePathsOutput(pub HashMap<CollectionUuid, String>);

#[derive(Debug, Error)]
pub enum GetVersionFilePathsError {
    #[error("Error fetching version file paths: {0}")]
    SysDb(#[from] BatchGetCollectionVersionFilePathsError),
}

impl ChromaError for GetVersionFilePathsError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            GetVersionFilePathsError::SysDb(err) => err.code(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetVersionFilePathsOperator {}

impl GetVersionFilePathsOperator {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Operator<GetVersionFilePathsInput, GetVersionFilePathsOutput> for GetVersionFilePathsOperator {
    type Error = GetVersionFilePathsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &GetVersionFilePathsInput,
    ) -> Result<GetVersionFilePathsOutput, Self::Error> {
        let paths = input
            .sysdb
            .clone()
            .batch_get_collection_version_file_paths(input.collection_ids.clone())
            .await?;

        Ok(GetVersionFilePathsOutput(paths))
    }
}
