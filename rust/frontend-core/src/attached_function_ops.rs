//! Shared logic for attaching a server-side function to a collection.
//!
//! Both `chroma-frontend` (the public attach endpoint) and `foundation-api`
//! (`/init`) need to create an attached function, optionally backfill, and
//! then mark it ready via `finish_create_attached_function`. This module
//! owns that two-or-three-step flow so callers don't re-implement it.

use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::{Log, PushLogsError};
use chroma_sysdb::SysDb;
use chroma_types::{
    AttachedFunctionUuid, Cmek, Collection, CollectionUuid, DatabaseName,
    FinishCreateAttachedFunctionError, Operation, OperationRecord, Schema,
};

/// Errors that can occur when creating (and optionally backfilling) an
/// attached function.
#[derive(Debug, thiserror::Error)]
pub enum CreateAttachedFunctionError {
    #[error(transparent)]
    Attach(#[from] chroma_sysdb::AttachFunctionError),
    #[error(transparent)]
    FinishCreate(#[from] FinishCreateAttachedFunctionError),
    #[error("Failed to serialize output collection schema: {0}")]
    SchemaSerialize(#[from] serde_json::Error),
    #[error(transparent)]
    PushLogs(#[from] PushLogsError),
}

impl ChromaError for CreateAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Attach(e) => e.code(),
            Self::FinishCreate(e) => e.code(),
            Self::SchemaSerialize(_) => ErrorCodes::Internal,
            Self::PushLogs(e) => e.code(),
        }
    }
}

/// Create an attached function and mark it ready in one shot.
///
/// Idempotent: if the function already exists (`created = false`), the
/// finish step is skipped and the existing ID is returned.
#[allow(clippy::too_many_arguments)]
pub async fn create_attached_function(
    sysdb: &mut SysDb,
    name: String,
    operator_name: String,
    input_collection_id: CollectionUuid,
    output_collection_name: String,
    params: serde_json::Value,
    tenant: String,
    database_name: String,
    min_records_for_invocation: u64,
    output_schema: Schema,
) -> Result<(AttachedFunctionUuid, bool), CreateAttachedFunctionError> {
    let (id, created) = sysdb
        .create_attached_function(
            name,
            operator_name,
            input_collection_id,
            output_collection_name,
            params,
            tenant,
            database_name,
            min_records_for_invocation,
        )
        .await?;

    if !created {
        return Ok((id, false));
    }

    let schema_str = serde_json::to_string(&output_schema)?;
    sysdb
        .finish_create_attached_function(id, schema_str)
        .await?;

    Ok((id, true))
}

/// Create an attached function with a backfill step between create and
/// finish.
///
/// Backfill pushes `num_backfill_records` dummy `BackfillFn` records to
/// the input collection's log, triggering an initial compaction cycle so
/// the function can process existing data.
///
/// Idempotent: if the function already exists (`created = false`), both
/// the backfill and finish steps are skipped.
#[allow(clippy::too_many_arguments)]
pub async fn create_attached_function_with_backfill(
    sysdb: &mut SysDb,
    log: &mut Log,
    name: String,
    operator_name: String,
    input_collection_id: CollectionUuid,
    output_collection_name: String,
    params: serde_json::Value,
    tenant: String,
    database_name: DatabaseName,
    min_records_for_invocation: u64,
    output_schema: Schema,
    input_collection: &Collection,
    num_backfill_records: usize,
) -> Result<(AttachedFunctionUuid, bool), CreateAttachedFunctionError> {
    let (id, created) = sysdb
        .create_attached_function(
            name,
            operator_name,
            input_collection_id,
            output_collection_name,
            params,
            tenant.clone(),
            database_name.clone().into_string(),
            min_records_for_invocation,
        )
        .await?;

    if !created {
        return Ok((id, false));
    }

    // Backfill: push dummy records to force a compaction cycle.
    let dim = input_collection.dimension.unwrap_or(1) as usize;
    let fake_embedding = vec![0.0; dim];
    let cmek: Option<Cmek> = input_collection
        .schema
        .as_ref()
        .and_then(|s| s.cmek.clone());
    let records = vec![
        OperationRecord {
            id: "backfill_id".to_string(),
            embedding: Some(fake_embedding),
            encoding: None,
            metadata: None,
            document: None,
            operation: Operation::BackfillFn,
        };
        num_backfill_records
    ];
    log.push_logs(&tenant, database_name, input_collection_id, records, cmek)
        .await?;

    let schema_str = serde_json::to_string(&output_schema)?;
    sysdb
        .finish_create_attached_function(id, schema_str)
        .await?;

    Ok((id, true))
}
