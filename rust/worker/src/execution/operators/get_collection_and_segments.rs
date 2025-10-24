use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{CollectionAndSegments, CollectionUuid, GetCollectionWithSegmentsError};
use thiserror::Error;

/// The `GetCollectionAndSegmentsOperator` fetches a consistent snapshot of collection and segment information
/// for both input and output collections (which may be the same for regular compaction).
///
/// # Parameters
/// - `sysdb`: The sysdb client
/// - `input_collection_id`: The id for the input collection to be fetched
/// - `output_collection_id`: The id for the output collection to be fetched
///
/// # Inputs
/// - No input is required
///
/// # Outputs
/// - The input and output collection and segments information. If not found, an error will be thrown
#[derive(Clone, Debug)]
pub struct GetCollectionAndSegmentsOperator {
    pub sysdb: SysDb,
    pub input_collection_id: CollectionUuid,
    pub output_collection_id: CollectionUuid,
}

type GetCollectionAndSegmentsInput = ();

#[derive(Clone, Debug)]
pub struct GetCollectionAndSegmentsOutput {
    pub input: CollectionAndSegments,
    pub output: CollectionAndSegments,
}

#[derive(Debug, Error)]
pub enum GetCollectionAndSegmentsError {
    #[error(transparent)]
    SysDB(#[from] GetCollectionWithSegmentsError),
}

impl ChromaError for GetCollectionAndSegmentsError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            GetCollectionAndSegmentsError::SysDB(chroma_error) => chroma_error.code(),
        }
    }

    fn should_trace_error(&self) -> bool {
        let Self::SysDB(gcwse) = self;
        gcwse.should_trace_error()
    }
}

#[async_trait]
impl Operator<GetCollectionAndSegmentsInput, GetCollectionAndSegmentsOutput>
    for GetCollectionAndSegmentsOperator
{
    type Error = GetCollectionAndSegmentsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        _: &GetCollectionAndSegmentsInput,
    ) -> Result<GetCollectionAndSegmentsOutput, GetCollectionAndSegmentsError> {
        tracing::trace!(
            "[{}]: Fetching input collection {} and output collection {}",
            self.get_name(),
            self.input_collection_id.0,
            self.output_collection_id.0
        );

        let mut sysdb = self.sysdb.clone();

        // Fetch input collection and segments
        let input = sysdb
            .get_collection_with_segments(self.input_collection_id)
            .await?;

        // Fetch output collection and segments
        // If input and output are the same collection, clone instead of fetching twice
        let output = if self.input_collection_id == self.output_collection_id {
            input.clone()
        } else {
            sysdb
                .get_collection_with_segments(self.output_collection_id)
                .await?
        };

        Ok(GetCollectionAndSegmentsOutput { input, output })
    }
}
