use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::CollectionUuid;

define_uuid_newtype!(
    /// JobId is a wrapper around Uuid to provide a unified type for job identifiers.
    /// Jobs can be either collection compaction jobs or task execution jobs.
    JobId,
    new_v4
);

// Custom From implementations for JobId
impl From<CollectionUuid> for JobId {
    fn from(collection_uuid: CollectionUuid) -> Self {
        JobId(collection_uuid.0)
    }
}

impl From<AttachedFunctionUuid> for JobId {
    fn from(attached_function_uuid: AttachedFunctionUuid) -> Self {
        JobId(attached_function_uuid.0)
    }
}

define_uuid_newtype!(
    /// AttachedFunctionUuid is a wrapper around Uuid to provide a type for attached function identifiers.
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    AttachedFunctionUuid,
    new_v4
);

/// AttachedFunction represents an asynchronous function that is triggered by collection writes
/// to map records from a source collection to a target collection.
fn default_systemtime() -> SystemTime {
    SystemTime::UNIX_EPOCH
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AttachedFunction {
    /// Unique identifier for the attached function
    pub id: AttachedFunctionUuid,
    /// Human-readable name for the attached function instance
    pub name: String,
    /// UUID of the function/built-in definition this attached function uses
    pub function_id: uuid::Uuid,
    /// Source collection that triggers the attached function
    pub input_collection_id: CollectionUuid,
    /// Name of target collection where attached function output is stored
    pub output_collection_name: String,
    /// ID of the output collection (lazily filled in after creation)
    pub output_collection_id: Option<CollectionUuid>,
    /// Optional JSON parameters for the function
    pub params: Option<String>,
    /// Tenant name this attached function belongs to (despite field name, this is a name not a UUID)
    pub tenant_id: String,
    /// Database name this attached function belongs to (despite field name, this is a name not a UUID)
    pub database_id: String,
    /// Timestamp of the last successful function run
    #[serde(skip, default)]
    pub last_run: Option<SystemTime>,
    /// Completion offset: the WAL position up to which the attached function has processed records
    pub completion_offset: u64,
    /// Minimum number of new records required before the attached function runs again
    pub min_records_for_invocation: u64,
    /// Whether the attached function has been soft-deleted
    #[serde(skip, default)]
    pub is_deleted: bool,
    /// Timestamp when the attached function was created
    #[serde(default = "default_systemtime")]
    pub created_at: SystemTime,
    /// Timestamp when the attached function was last updated
    #[serde(default = "default_systemtime")]
    pub updated_at: SystemTime,
    // is_ready is a column in the database, but not in the struct because
    // it is not meant to be used in rust code. If it is false, rust code
    // should never even see it.
}

#[derive(Debug, thiserror::Error)]
pub enum AttachedFunctionConversionError {
    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),
    #[error("Attached function params aren't supported yet")]
    ParamsNotSupported,
}

impl TryFrom<crate::chroma_proto::AttachedFunction> for AttachedFunction {
    type Error = AttachedFunctionConversionError;

    fn try_from(
        attached_function: crate::chroma_proto::AttachedFunction,
    ) -> Result<Self, Self::Error> {
        // Parse attached_function_id
        let attached_function_id = attached_function
            .id
            .parse::<AttachedFunctionUuid>()
            .map_err(|_| {
                AttachedFunctionConversionError::InvalidUuid("attached_function_id".to_string())
            })?;

        // Parse function_id
        let function_id = attached_function
            .function_id
            .parse::<uuid::Uuid>()
            .map_err(|_| AttachedFunctionConversionError::InvalidUuid("function_id".to_string()))?;

        // Parse input_collection_id
        let input_collection_id = attached_function
            .input_collection_id
            .parse::<CollectionUuid>()
            .map_err(|_| {
                AttachedFunctionConversionError::InvalidUuid("input_collection_id".to_string())
            })?;

        // Parse output_collection_id if available
        let output_collection_id = attached_function
            .output_collection_id
            .map(|id| id.parse::<CollectionUuid>())
            .transpose()
            .map_err(|_| {
                AttachedFunctionConversionError::InvalidUuid("output_collection_id".to_string())
            })?;

        // Parse params if available - only allow empty JSON "{}" or empty struct for now.
        // TODO(tanujnay112): Process params when we allow them
        let params = if let Some(params_struct) = &attached_function.params {
            if !params_struct.fields.is_empty() {
                return Err(AttachedFunctionConversionError::ParamsNotSupported);
            }
            Some("{}".to_string())
        } else {
            None
        };

        // Parse timestamps
        let created_at = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_micros(attached_function.created_at);
        let updated_at = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_micros(attached_function.updated_at);

        Ok(AttachedFunction {
            id: attached_function_id,
            name: attached_function.name,
            function_id,
            input_collection_id,
            output_collection_name: attached_function.output_collection_name,
            output_collection_id,
            params,
            tenant_id: attached_function.tenant_id,
            database_id: attached_function.database_id,
            last_run: None, // Not available in proto
            completion_offset: attached_function.completion_offset,
            min_records_for_invocation: attached_function.min_records_for_invocation,
            is_deleted: false, // Not available in proto, would need to be fetched separately
            created_at,
            updated_at,
        })
    }
}
