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
    AttachFunctionError, AttachedFunction, AttachedFunctionUuid, Cmek, Collection, CollectionUuid,
    DatabaseName, FinishCreateAttachedFunctionError, Operation, OperationRecord, Schema,
};

/// Errors that can occur when creating (and optionally backfilling) an
/// attached function.
#[derive(Debug, thiserror::Error)]
pub enum CreateAttachedFunctionError {
    #[error(transparent)]
    Attach(#[from] chroma_sysdb::AttachFunctionError),
    #[error(transparent)]
    AddInput(#[from] AttachFunctionError),
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
            Self::AddInput(e) => e.code(),
            Self::FinishCreate(e) => e.code(),
            Self::SchemaSerialize(_) => ErrorCodes::Internal,
            Self::PushLogs(e) => e.code(),
        }
    }
}

#[derive(Debug)]
pub struct AddAttachedFunctionInputResult {
    pub attached_function: AttachedFunction,
    pub attached_function_id: AttachedFunctionUuid,
    pub created: bool,
    pub output_schema_str: String,
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

/// Add an input collection to an existing async attached function and mark
/// the new input ready when it is newly created.
pub async fn add_attached_function_input(
    sysdb: &mut SysDb,
    name: String,
    existing_input_collection_id: CollectionUuid,
    new_input_collection_id: CollectionUuid,
    database_name: DatabaseName,
) -> Result<(AttachedFunctionUuid, bool), CreateAttachedFunctionError> {
    let add_input_result = prepare_add_attached_function_input(
        sysdb,
        name,
        existing_input_collection_id,
        new_input_collection_id,
        database_name,
    )
    .await?;

    if !add_input_result.created {
        return Ok((add_input_result.attached_function_id, false));
    }

    sysdb
        .finish_create_attached_function(
            add_input_result.attached_function_id,
            add_input_result.output_schema_str,
        )
        .await?;

    Ok((add_input_result.attached_function_id, true))
}

pub async fn prepare_add_attached_function_input(
    sysdb_client: &mut SysDb,
    function_name: String,
    collection_uuid: CollectionUuid,
    input_collection_id: CollectionUuid,
    database_name: DatabaseName,
) -> Result<AddAttachedFunctionInputResult, AttachFunctionError> {
    let attached_function = sysdb_client
        .get_attached_functions(
            Some(function_name.clone()),
            Some(collection_uuid),
            vec![],
            true,
        )
        .await
        .map_err(|e| AttachFunctionError::Internal(Box::new(e)))?
        .into_iter()
        .next()
        .ok_or_else(|| AttachFunctionError::FunctionNotFound(function_name))?;

    if !attached_function.is_async {
        return Err(AttachFunctionError::InvalidArgument(
            "multiple input collections are only supported for async attached functions"
                .to_string(),
        ));
    }

    let output_collection_id = attached_function.output_collection_id.ok_or_else(|| {
        AttachFunctionError::Internal(Box::new(chroma_error::TonicError(tonic::Status::internal(
            "Attached function output collection is not ready",
        ))))
    })?;

    let output_collection = sysdb_client
        .get_collection_with_segments(Some(database_name), output_collection_id)
        .await
        .map_err(|e| AttachFunctionError::Internal(Box::new(e)))?;

    let output_schema = output_collection.collection.schema.ok_or_else(|| {
        AttachFunctionError::Internal(Box::new(chroma_error::TonicError(tonic::Status::internal(
            "Attached function output collection is missing schema",
        ))))
    })?;

    let output_schema_str = serde_json::to_string(&output_schema).map_err(|e| {
        AttachFunctionError::Internal(Box::new(chroma_error::TonicError(tonic::Status::internal(
            format!("Failed to serialize output collection schema: {}", e),
        ))))
    })?;

    let (attached_function_id, created) = sysdb_client
        .add_attached_function_input(attached_function.id, input_collection_id)
        .await
        .map_err(chroma_types::AttachFunctionError::from)?;

    Ok(AddAttachedFunctionInputResult {
        attached_function,
        attached_function_id,
        created,
        output_schema_str,
    })
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
    log.push_logs(
        &tenant,
        database_name,
        input_collection_id,
        records,
        cmek,
        None,
    )
    .await?;

    let schema_str = serde_json::to_string(&output_schema)?;
    sysdb
        .finish_create_attached_function(id, schema_str)
        .await?;

    Ok((id, true))
}
