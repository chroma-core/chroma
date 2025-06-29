use std::time::Duration;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_system::{Operator, OperatorType};
use chroma_types::CollectionUuid;
use thiserror::Error;
use tokio::time::{error::Elapsed, timeout};

/// The `PurgeDirtyLog` operator add `Purge` entries in the dirty log for the specified collections
///
/// # Parameters
/// - `log_client`: The log service client
///
/// # Input
/// - `collection_uuids`: The uuids of the collections to purge
///
/// # Output
/// None
///
/// # Usage
/// It should be run periodically in the compaction manager to clear the entries for deleted collections
#[derive(Clone, Debug)]
pub struct PurgeDirtyLog {
    pub log_client: Log,
    pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct PurgeDirtyLogInput {
    pub collection_uuids: Vec<CollectionUuid>,
}

pub type PurgeDirtyLogOutput = ();

#[derive(Debug, Error)]
pub enum PurgeDirtyLogError {
    #[error(transparent)]
    LogService(#[from] Box<dyn ChromaError>),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
}

impl ChromaError for PurgeDirtyLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            PurgeDirtyLogError::LogService(chroma_error) => chroma_error.code(),
            PurgeDirtyLogError::Timeout(_) => ErrorCodes::DeadlineExceeded,
        }
    }
}

#[async_trait]
impl Operator<PurgeDirtyLogInput, PurgeDirtyLogOutput> for PurgeDirtyLog {
    type Error = PurgeDirtyLogError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &PurgeDirtyLogInput,
    ) -> Result<PurgeDirtyLogOutput, PurgeDirtyLogError> {
        timeout(
            self.timeout,
            self.log_client
                .clone()
                .purge_dirty_for_collection(input.collection_uuids.clone()),
        )
        .await??;
        Ok(())
    }
}
