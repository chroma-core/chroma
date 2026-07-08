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
    UpdateMetadataValue, CHROMA_BACKFILL_ATTACHED_FUNCTION_ID_KEY,
};

use crate::retry::retry_transient;

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

pub async fn push_backfill_records(
    log: &mut Log,
    tenant: &str,
    database_name: DatabaseName,
    input_collection_id: CollectionUuid,
    input_collection: &Collection,
    attached_function_id: AttachedFunctionUuid,
    num_backfill_records: usize,
) -> Result<(), PushLogsError> {
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
            metadata: Some(std::collections::HashMap::from([(
                CHROMA_BACKFILL_ATTACHED_FUNCTION_ID_KEY.to_string(),
                UpdateMetadataValue::Str(attached_function_id.to_string()),
            )])),
            document: None,
            operation: Operation::BackfillFn,
        };
        num_backfill_records
    ];
    log.push_logs(tenant, database_name, input_collection_id, records, cmek)
        .await
}

/// Create an attached function and mark it ready in one shot.
///
/// Idempotent: if the function already exists (`created = false`), the
/// finish step is skipped and the existing ID is returned.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        attached_function = %name,
        operator = %operator_name,
        input_collection_id = %input_collection_id,
        tenant = %tenant,
        database_name = %database_name,
    )
)]
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
    // The create RPC is idempotent (returns `created = false` when the function
    // already exists), so retrying a transient sysdb failure is safe.
    let (id, created) = retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let name = name.clone();
        let operator_name = operator_name.clone();
        let output_collection_name = output_collection_name.clone();
        let params = params.clone();
        let tenant = tenant.clone();
        let database_name = database_name.clone();
        async move {
            sysdb
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
                .await
        }
    })
    .await?;

    if !created {
        tracing::info!(attached_function = %name, "attached function already exists");
        return Ok((id, false));
    }

    // Retry `finish` on its own. It must NOT be folded into a whole-function
    // retry: on a retry the create RPC would return `created = false` and
    // short-circuit, silently skipping `finish` and leaving the function
    // half-attached. `finish_create_attached_function` is keyed on `id`, so
    // retrying it directly is idempotent.
    let schema_str = serde_json::to_string(&output_schema)?;
    retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let schema_str = schema_str.clone();
        async move { sysdb.finish_create_attached_function(id, schema_str).await }
    })
    .await?;

    tracing::info!(attached_function = %name, "created attached function");
    Ok((id, true))
}

/// Add an input collection to an existing async attached function and mark
/// the new input ready when it is newly created.
#[tracing::instrument(
    skip_all,
    fields(
        attached_function = %name,
        new_input_collection_id = %new_input_collection_id,
        database_name = %database_name.as_ref(),
    )
)]
pub async fn add_attached_function_input(
    sysdb: &mut SysDb,
    name: String,
    existing_input_collection_id: CollectionUuid,
    new_input_collection_id: CollectionUuid,
    database_name: DatabaseName,
) -> Result<(AttachedFunctionUuid, bool), CreateAttachedFunctionError> {
    // `prepare` only reads sysdb and issues the idempotent add-input RPC (it
    // performs no `finish`), so retrying the whole unit on a transient failure
    // is safe — unlike folding `finish` (below) into the same retry.
    let add_input_result = retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let name = name.clone();
        let database_name = database_name.clone();
        async move {
            prepare_add_attached_function_input(
                &mut sysdb,
                name,
                existing_input_collection_id,
                new_input_collection_id,
                database_name,
            )
            .await
        }
    })
    .await?;

    if !add_input_result.created {
        return Ok((add_input_result.attached_function_id, false));
    }

    let attached_function_id = add_input_result.attached_function_id;
    retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let output_schema_str = add_input_result.output_schema_str.clone();
        async move {
            sysdb
                .finish_create_attached_function(attached_function_id, output_schema_str)
                .await
        }
    })
    .await?;

    Ok((attached_function_id, true))
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
    push_backfill_records(
        log,
        &tenant,
        database_name,
        input_collection_id,
        input_collection,
        id,
        num_backfill_records,
    )
    .await?;

    let schema_str = serde_json::to_string(&output_schema)?;
    sysdb
        .finish_create_attached_function(id, schema_str)
        .await?;

    Ok((id, true))
}
