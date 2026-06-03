//! Shared attached-function helpers for HTTP frontends.

use chroma_sysdb::SysDb;
use chroma_types::{
    AttachFunctionError, AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName,
};

#[derive(Debug)]
pub struct AddAttachedFunctionInputResult {
    pub attached_function: AttachedFunction,
    pub attached_function_id: AttachedFunctionUuid,
    pub created: bool,
    pub output_schema_str: String,
}

pub async fn add_attached_function_input(
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
