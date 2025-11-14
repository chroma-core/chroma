use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_sysdb::SysDb;
use chroma_system::Operator;
use chroma_types::{
    AttachedFunctionUpdateInfo, AttachedFunctionUuid, CollectionFlushInfo, CollectionUuid,
    NonceUuid, Schema, SegmentFlushInfo,
};
use std::sync::Arc;
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
    pub attached_function_run_nonce: NonceUuid,
    pub completion_offset: u64,

    pub sysdb: SysDb,
    pub log: Log,
}

impl FinishAttachedFunctionInput {
    /// Create a new finish attached function input.
    pub fn new(
        collections: Vec<CollectionFlushInfo>,
        attached_function_id: AttachedFunctionUuid,
        attached_function_run_nonce: NonceUuid,
        completion_offset: u64,

        sysdb: SysDb,
        log: Log,
    ) -> Self {
        FinishAttachedFunctionInput {
            collections,
            attached_function_id,
            attached_function_run_nonce,
            completion_offset,
            sysdb,
            log,
        }
    }
}

#[derive(Debug)]
pub struct FinishAttachedFunctionOutput {
    pub flush_results: Vec<chroma_types::FlushCompactionAndAttachedFunctionResponse>,
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
            attached_function_run_nonce: input.attached_function_run_nonce.0,
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

        // Build individual flush results from the response
        let mut flush_results = Vec::with_capacity(flush_result.collections.len());
        for collection_result in &flush_result.collections {
            flush_results.push(chroma_types::FlushCompactionAndAttachedFunctionResponse {
                collections: vec![chroma_types::CollectionCompactionInfo {
                    collection_id: collection_result.collection_id,
                    collection_version: collection_result.collection_version,
                    last_compaction_time: collection_result.last_compaction_time,
                }],
                completion_offset: flush_result.completion_offset,
            });
        }

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

        Ok(FinishAttachedFunctionOutput { flush_results })
    }
}
