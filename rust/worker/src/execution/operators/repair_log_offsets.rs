use std::time::Duration;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_system::{Operator, OperatorType};
use chroma_types::CollectionUuid;
use thiserror::Error;
use tokio::time::{error::Elapsed, timeout};
use tracing::Level;

/// The `RepairLogOffsets` operator to call update_collection_log_offset on every RLS node when
/// there's a condition where it reads zero records.
///
/// # Parameters
/// - `log_client`: The log service client
///
/// # Input
/// - `log_offsets_to_repair`: The collection, log offset pairs to update.
///
/// # Output
/// None
///
/// # Usage
/// It should be run periodically in the compaction manager when there are offsets to repair.
#[derive(Clone, Debug)]
pub struct RepairLogOffsets {
    pub log_client: Log,
    pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct RepairLogOffsetsInput {
    pub log_offsets_to_repair: Vec<(CollectionUuid, i64)>,
}

pub type RepairLogOffsetsOutput = ();

#[derive(Debug, Error)]
pub enum RepairLogOffsetsError {
    #[error(transparent)]
    LogService(#[from] Box<dyn ChromaError>),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
}

impl ChromaError for RepairLogOffsetsError {
    fn code(&self) -> ErrorCodes {
        match self {
            RepairLogOffsetsError::LogService(chroma_error) => chroma_error.code(),
            RepairLogOffsetsError::Timeout(_) => ErrorCodes::DeadlineExceeded,
        }
    }
}

#[async_trait]
impl Operator<RepairLogOffsetsInput, RepairLogOffsetsOutput> for RepairLogOffsets {
    type Error = RepairLogOffsetsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &RepairLogOffsetsInput,
    ) -> Result<RepairLogOffsetsOutput, RepairLogOffsetsError> {
        for (collection_id, offset) in input.log_offsets_to_repair.iter().cloned() {
            tracing::event!(Level::INFO, name = "repairing log offset", collection_id =? collection_id, offset =? offset);
            timeout(
                self.timeout,
                self.log_client
                    .clone()
                    .update_collection_log_offset_on_every_node(collection_id, offset),
            )
            .await??;
        }
        Ok(())
    }
}
