use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{BatchGetCollectionVersionFilePathsError, CollectionUuid, DatabaseName};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct GetVersionFilePathsInput {
    collection_ids: Vec<CollectionUuid>,
    sysdb: SysDb,
    database_name: DatabaseName,
}

impl GetVersionFilePathsInput {
    pub fn new(
        collection_ids: Vec<CollectionUuid>,
        sysdb: SysDb,
        database_name: DatabaseName,
    ) -> Self {
        Self {
            collection_ids,
            sysdb,
            database_name,
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
            .batch_get_collection_version_file_paths(
                input.collection_ids.clone(),
                Some(input.database_name.clone()),
            )
            .await?;

        Ok(GetVersionFilePathsOutput(paths))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sysdb::TestSysDb;
    use chroma_system::Operator;

    #[tokio::test]
    async fn test_get_version_file_paths_success() {
        let mut sysdb = SysDb::Test(TestSysDb::new());
        let database_name = DatabaseName::new("test_db").expect("valid database name");
        let collection_id = CollectionUuid::new();

        sysdb
            .create_collection(
                "test-tenant".to_string(),
                database_name.clone(),
                collection_id,
                "test-collection".to_string(),
                vec![],
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .expect("collection should be created");

        match &mut sysdb {
            SysDb::Test(test_sysdb) => {
                test_sysdb.set_collection_version_file_path(
                    collection_id,
                    "version-files/test.bin".to_string(),
                );
            }
            _ => panic!("expected test sysdb"),
        }

        let output = GetVersionFilePathsOperator::new()
            .run(&GetVersionFilePathsInput::new(
                vec![collection_id],
                sysdb,
                database_name,
            ))
            .await
            .expect("version file path lookup should succeed");

        assert_eq!(
            output.0,
            HashMap::from([(collection_id, "version-files/test.bin".to_string())])
        );
    }

    #[tokio::test]
    async fn test_get_version_file_paths_missing_path() {
        let mut sysdb = SysDb::Test(TestSysDb::new());
        let database_name = DatabaseName::new("test_db").expect("valid database name");
        let collection_id = CollectionUuid::new();

        sysdb
            .create_collection(
                "test-tenant".to_string(),
                database_name.clone(),
                collection_id,
                "test-collection".to_string(),
                vec![],
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .expect("collection should be created");

        let err = GetVersionFilePathsOperator::new()
            .run(&GetVersionFilePathsInput::new(
                vec![collection_id],
                sysdb,
                database_name,
            ))
            .await
            .expect_err("lookup should fail when the version file path is missing");

        assert!(matches!(err, GetVersionFilePathsError::SysDb(_)));
    }
}
