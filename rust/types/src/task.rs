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
    /// Timestamp when the attached function should next run
    #[serde(skip, default = "default_systemtime")]
    pub next_run: SystemTime,
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
}
