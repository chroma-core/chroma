use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::version_file::{VersionFileError, VersionFileManager};
use chroma_system::{Operator, OperatorType};
use chroma_types::{Collection, Segment};
use thiserror::Error;

use crate::execution::orchestration::async_function_boundary::{
    resolve_boundary_plan_from_version_file, AsyncFnBoundaryPlan,
};

#[derive(Clone, Debug)]
pub(crate) struct GetAsyncFnFetchBoundariesOperator;

impl GetAsyncFnFetchBoundariesOperator {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GetAsyncFnFetchBoundariesInput {
    pub collection: Collection,
    pub record_segment: Segment,
    pub completion_offset: i64,
    pub max_compaction_size: usize,
    pub blockfile_provider: BlockfileProvider,
}

pub(crate) type GetAsyncFnFetchBoundariesOutput = AsyncFnBoundaryPlan;

#[derive(Debug, Error)]
pub enum GetAsyncFnFetchBoundariesError {
    #[error("Blockfile provider storage is required for async function version lookup")]
    MissingBlockfileStorage,
    #[error("Error fetching version file: {0}")]
    VersionFile(#[from] VersionFileError),
    #[error("Async function boundary resolution failed: {0}")]
    Boundary(String),
}

impl ChromaError for GetAsyncFnFetchBoundariesError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<GetAsyncFnFetchBoundariesInput, GetAsyncFnFetchBoundariesOutput>
    for GetAsyncFnFetchBoundariesOperator
{
    type Error = GetAsyncFnFetchBoundariesError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &GetAsyncFnFetchBoundariesInput,
    ) -> Result<GetAsyncFnFetchBoundariesOutput, Self::Error> {
        let version_file = if input
            .collection
            .version_file_path
            .as_ref()
            .is_some_and(|path| !path.is_empty())
        {
            let storage = input
                .blockfile_provider
                .storage()
                .ok_or(GetAsyncFnFetchBoundariesError::MissingBlockfileStorage)?;
            let version_file_manager = VersionFileManager::new(storage.as_ref().clone());
            Some(version_file_manager.fetch(&input.collection).await?)
        } else {
            None
        };

        resolve_boundary_plan_from_version_file(
            version_file.as_ref(),
            input.completion_offset,
            input.max_compaction_size,
            &input.record_segment,
        )
        .map_err(GetAsyncFnFetchBoundariesError::Boundary)
    }
}
