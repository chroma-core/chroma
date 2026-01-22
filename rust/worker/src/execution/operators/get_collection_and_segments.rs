use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    CollectionAndSegments, CollectionUuid, DatabaseName, GetCollectionWithSegmentsError,
};
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
    pub database_name: DatabaseName,
}

impl GetCollectionAndSegmentsOperator {
    pub fn new(sysdb: SysDb, collection_id: CollectionUuid, database_name: DatabaseName) -> Self {
        Self {
            sysdb,
            collection_id,
            database_name,
        }
    }
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
            "[{}]: Collection ID {}",
            self.get_name(),
            self.collection_id.0
        );
        Ok(self
            .sysdb
            .clone()
            .get_collection_with_segments(Some(self.database_name.clone()), self.collection_id)
            .await?)
    }
}
