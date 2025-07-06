use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{CollectionAndSegments, CollectionUuid, GetCollectionWithSegmentsError};
use thiserror::Error;

/// The `GetCollectionAndSegmentsOperator` fetches a consistent snapshot of collection and segment information
///
/// # Parameters
/// - `sysdb`: The sysdb client
/// - `collection_id`: The id for the collection to be fetched
///
/// # Inputs
/// - No input is required
///
/// # Outputs
/// - The collection and segments information. If not found, an error will be thrown
#[derive(Clone, Debug)]
pub struct GetCollectionAndSegmentsOperator {
    pub sysdb: SysDb,
    pub collection_id: CollectionUuid,
}

type GetCollectionAndSegmentsInput = ();

pub type GetCollectionAndSegmentsOutput = CollectionAndSegments;

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
            "[{}]: Collection ID {}",
            self.get_name(),
            self.collection_id.0
        );
        Ok(self
            .sysdb
            .clone()
            .get_collection_with_segments(self.collection_id)
            .await?)
    }
}
