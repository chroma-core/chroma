use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{CollectionUuid, ListAttachedFunctionsError};
use std::fmt::{Debug, Formatter};
use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct FetchMinAttachedFunctionCompletionOffsetOperator {}

pub struct FetchMinAttachedFunctionCompletionOffsetInput {
    pub sysdb_client: SysDb,
    pub collection_ids: Vec<CollectionUuid>,
}

impl Debug for FetchMinAttachedFunctionCompletionOffsetInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchMinAttachedFunctionCompletionOffsetInput")
            .field("collection_ids", &self.collection_ids)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct FetchMinAttachedFunctionCompletionOffsetOutput {
    pub min_completion_offsets: std::collections::HashMap<CollectionUuid, Option<u64>>,
}

#[derive(Error, Debug)]
pub enum FetchMinAttachedFunctionCompletionOffsetError {
    #[error(transparent)]
    ListAttachedFunctions(#[from] ListAttachedFunctionsError),
}

impl ChromaError for FetchMinAttachedFunctionCompletionOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchMinAttachedFunctionCompletionOffsetError::ListAttachedFunctions(err) => err.code(),
        }
    }
}

#[async_trait]
impl
    Operator<
        FetchMinAttachedFunctionCompletionOffsetInput,
        FetchMinAttachedFunctionCompletionOffsetOutput,
    > for FetchMinAttachedFunctionCompletionOffsetOperator
{
    type Error = FetchMinAttachedFunctionCompletionOffsetError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &FetchMinAttachedFunctionCompletionOffsetInput,
    ) -> Result<
        FetchMinAttachedFunctionCompletionOffsetOutput,
        FetchMinAttachedFunctionCompletionOffsetError,
    > {
        let mut min_completion_offsets = std::collections::HashMap::new();
        for collection_id in &input.collection_ids {
            // TODO(tanujnay112): replace with a server-side MIN(completion_offset)
            // aggregate so we don't pay to serialize every AttachedFunction row.
            let attached_functions = input
                .sysdb_client
                .clone()
                .list_attached_functions(*collection_id)
                .await?;

            let min_completion_offset = attached_functions
                .iter()
                .map(|af| af.completion_offset)
                .min();
            min_completion_offsets.insert(*collection_id, min_completion_offset);
        }

        Ok(FetchMinAttachedFunctionCompletionOffsetOutput {
            min_completion_offsets,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_types::{AttachedFunction, AttachedFunctionUuid};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use uuid::Uuid;

    fn create_test_attached_function(
        id: AttachedFunctionUuid,
        name: &str,
        input_collection_id: CollectionUuid,
        output_collection_id: Option<CollectionUuid>,
        completion_offset: u64,
    ) -> AttachedFunction {
        AttachedFunction {
            id,
            name: name.to_string(),
            function_id: Uuid::new_v4(),
            input_collection_id,
            output_collection_name: format!("{}_output", name),
            output_collection_id,
            params: None,
            tenant_id: "test_tenant".to_string(),
            database_id: "test_db".to_string(),
            last_run: None,
            completion_offset,
            min_records_for_invocation: 10,
            is_deleted: false,
            is_async: true,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn test_fetch_min_offset_no_attached_functions() {
        let mut test_sysdb = TestSysDb::new();
        test_sysdb.set_attached_functions(HashMap::new());
        let sysdb = SysDb::Test(test_sysdb);

        let collection_id = CollectionUuid::new();
        let operator = FetchMinAttachedFunctionCompletionOffsetOperator::default();

        let input = FetchMinAttachedFunctionCompletionOffsetInput {
            sysdb_client: sysdb,
            collection_ids: vec![collection_id],
        };

        let output = operator.run(&input).await.unwrap();
        assert_eq!(
            output.min_completion_offsets.get(&collection_id),
            Some(&None)
        );
    }

    #[tokio::test]
    async fn test_fetch_min_offset_multiple_attached_functions() {
        let collection_id = CollectionUuid::new();

        let attached_fns = vec![
            create_test_attached_function(
                AttachedFunctionUuid::new(),
                "fn1",
                collection_id,
                Some(CollectionUuid::new()),
                150,
            ),
            create_test_attached_function(
                AttachedFunctionUuid::new(),
                "fn2",
                collection_id,
                Some(CollectionUuid::new()),
                80, // This is the minimum
            ),
            create_test_attached_function(
                AttachedFunctionUuid::new(),
                "fn3",
                collection_id,
                Some(CollectionUuid::new()),
                200,
            ),
        ];

        let mut test_sysdb = TestSysDb::new();
        let mut attached_functions = HashMap::new();
        attached_functions.insert(collection_id, attached_fns);
        test_sysdb.set_attached_functions(attached_functions);
        let sysdb = SysDb::Test(test_sysdb);

        let operator = FetchMinAttachedFunctionCompletionOffsetOperator::default();
        let input = FetchMinAttachedFunctionCompletionOffsetInput {
            sysdb_client: sysdb,
            collection_ids: vec![collection_id],
        };

        let output = operator.run(&input).await.unwrap();
        assert_eq!(
            output.min_completion_offsets.get(&collection_id),
            Some(&Some(80))
        );
    }

    #[tokio::test]
    async fn test_fetch_min_offset_different_collection() {
        let target_collection = CollectionUuid::new();
        let other_collection = CollectionUuid::new();

        // Create attached function on different collection
        let attached_fn = create_test_attached_function(
            AttachedFunctionUuid::new(),
            "test_fn",
            other_collection,
            Some(CollectionUuid::new()),
            100,
        );

        let mut test_sysdb = TestSysDb::new();
        let mut attached_functions = HashMap::new();
        attached_functions.insert(other_collection, vec![attached_fn]);
        test_sysdb.set_attached_functions(attached_functions);
        let sysdb = SysDb::Test(test_sysdb);

        let operator = FetchMinAttachedFunctionCompletionOffsetOperator::default();
        let input = FetchMinAttachedFunctionCompletionOffsetInput {
            sysdb_client: sysdb,
            collection_ids: vec![target_collection], // Different from where function is attached
        };

        let output = operator.run(&input).await.unwrap();
        assert_eq!(
            output.min_completion_offsets.get(&target_collection),
            Some(&None)
        );
    }

    #[tokio::test]
    async fn test_fetch_min_offset_multiple_collections() {
        let collection_a = CollectionUuid::new();
        let collection_b = CollectionUuid::new();

        let mut test_sysdb = TestSysDb::new();
        let mut attached_functions = HashMap::new();
        attached_functions.insert(
            collection_a,
            vec![create_test_attached_function(
                AttachedFunctionUuid::new(),
                "fn_a",
                collection_a,
                Some(CollectionUuid::new()),
                42,
            )],
        );
        attached_functions.insert(
            collection_b,
            vec![
                create_test_attached_function(
                    AttachedFunctionUuid::new(),
                    "fn_b1",
                    collection_b,
                    Some(CollectionUuid::new()),
                    100,
                ),
                create_test_attached_function(
                    AttachedFunctionUuid::new(),
                    "fn_b2",
                    collection_b,
                    Some(CollectionUuid::new()),
                    75,
                ),
            ],
        );
        test_sysdb.set_attached_functions(attached_functions);
        let sysdb = SysDb::Test(test_sysdb);

        let operator = FetchMinAttachedFunctionCompletionOffsetOperator::default();
        let input = FetchMinAttachedFunctionCompletionOffsetInput {
            sysdb_client: sysdb,
            collection_ids: vec![collection_a, collection_b],
        };

        let output = operator.run(&input).await.unwrap();
        assert_eq!(
            output.min_completion_offsets.get(&collection_a),
            Some(&Some(42))
        );
        assert_eq!(
            output.min_completion_offsets.get(&collection_b),
            Some(&Some(75))
        );
    }
}
