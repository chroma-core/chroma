use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_sysdb::SysDb;
use chroma_system::Operator;
use chroma_types::{AttachedFunctionUpdateInfo, AttachedFunctionUuid, CollectionFlushInfo};
use thiserror::Error;
use tonic;

/// The finish attached function operator is responsible for:
/// 1. Registering collection compaction results for all collections
/// 2. Updating attached function completion offset in the same transaction
#[derive(Debug)]
pub struct FinishAttachedFunctionOperator {}

impl FinishAttachedFunctionOperator {
    /// Create a new finish attached function operator.
    pub fn new() -> Box<Self> {
        Box::new(FinishAttachedFunctionOperator {})
    }
}

#[derive(Debug)]
/// The input for the finish attached function operator.
/// This input is used to complete the attached function workflow by:
/// - Flushing collection compaction data to sysdb for all collections
/// - Updating attached function completion offset in the same transaction
pub struct FinishAttachedFunctionInput {
    pub collections: Vec<CollectionFlushInfo>,
    pub attached_function_id: AttachedFunctionUuid,
    pub completion_offset: u64,

    pub sysdb: SysDb,
    pub log: Log,
}

impl FinishAttachedFunctionInput {
    /// Create a new finish attached function input.
    pub fn new(
        collections: Vec<CollectionFlushInfo>,
        attached_function_id: AttachedFunctionUuid,
        completion_offset: u64,

        sysdb: SysDb,
        log: Log,
    ) -> Self {
        FinishAttachedFunctionInput {
            collections,
            attached_function_id,
            completion_offset,
            sysdb,
            log,
        }
    }
}

#[derive(Debug)]
pub struct FinishAttachedFunctionOutput {
    pub collection_flush_results: Vec<chroma_types::FlushCompactionResponse>,
    pub completion_offset: u64,
}

#[derive(Error, Debug)]
pub enum FinishAttachedFunctionError {
    #[error("Failed to flush collection compaction: {0}")]
    FlushFailed(#[from] chroma_sysdb::FlushCompactionError),
    #[error("Invalid attached function ID: {0}")]
    InvalidFunctionId(String),
}

impl ChromaError for FinishAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishAttachedFunctionError::FlushFailed(e) => e.code(),
            FinishAttachedFunctionError::InvalidFunctionId(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[async_trait]
impl Operator<FinishAttachedFunctionInput, FinishAttachedFunctionOutput>
    for FinishAttachedFunctionOperator
{
    type Error = FinishAttachedFunctionError;

    fn get_name(&self) -> &'static str {
        "FinishAttachedFunctionOperator"
    }

    async fn run(
        &self,
        input: &FinishAttachedFunctionInput,
    ) -> Result<FinishAttachedFunctionOutput, FinishAttachedFunctionError> {
        let mut sysdb = input.sysdb.clone();

        // Create the attached function update info
        let attached_function_update = AttachedFunctionUpdateInfo {
            attached_function_id: input.attached_function_id,
            completion_offset: input.completion_offset,
        };

        // Flush all collection compaction results and update attached function in one RPC
        let flush_result = sysdb
            .flush_compaction_and_attached_function(
                input.collections.clone(),
                attached_function_update,
            )
            .await
            .map_err(FinishAttachedFunctionError::FlushFailed)?;

        // Convert the collection results to FlushCompactionResponse
        let collection_flush_results: Vec<chroma_types::FlushCompactionResponse> = flush_result
            .collections
            .into_iter()
            .map(|collection| chroma_types::FlushCompactionResponse {
                collection_id: collection.collection_id,
                collection_version: collection.collection_version,
                last_compaction_time: collection.last_compaction_time,
            })
            .collect();

        // TODO(tanujnay112): Can optimize the below to not happen on the output collection.

        // Update log offsets for all collections to ensure consistency
        // This must be done after the flush to ensure the log position in sysdb is always >= log service
        let mut log = input.log.clone();
        for collection in &input.collections {
            log.update_collection_log_offset(
                &collection.tenant_id,
                collection.collection_id,
                collection.log_position,
            )
            .await
            .map_err(|e| {
                FinishAttachedFunctionError::FlushFailed(
                    chroma_sysdb::FlushCompactionError::FailedToFlushCompaction(
                        tonic::Status::internal(format!("Failed to update log offset: {}", e)),
                    ),
                )
            })?;
        }

        Ok(FinishAttachedFunctionOutput {
            collection_flush_results,
            completion_offset: flush_result.completion_offset,
        })
    }
}
