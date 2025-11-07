use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::SystemTime;
use uuid::Uuid;

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

define_uuid_newtype!(
    /// NonceUuid is a wrapper around Uuid to provide a type for attached function execution nonces.
    NonceUuid,
    now_v7
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
    /// Next nonce (UUIDv7) for execution tracking
    pub next_nonce: NonceUuid,
    /// Lowest live nonce (UUIDv7) - marks the earliest epoch that still needs verification
    /// When lowest_live_nonce is Some and < next_nonce, it indicates finish failed and we should
    /// skip execution and only run the scout_logs recheck phase
    /// None indicates the attached function has never been scheduled (brand new)
    pub lowest_live_nonce: Option<NonceUuid>,
}

/// ScheduleEntry represents a scheduled attached function run for a collection.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScheduleEntry {
    pub collection_id: CollectionUuid,
    pub attached_function_id: Uuid,
    pub attached_function_run_nonce: NonceUuid,
    pub when_to_run: Option<DateTime<Utc>>,
    /// Lowest live nonce - marks the earliest nonce that still needs verification.
    /// Nonces less than this value are considered complete.
    pub lowest_live_nonce: Option<Uuid>,
}

impl TryFrom<crate::chroma_proto::ScheduleEntry> for ScheduleEntry {
    type Error = ScheduleEntryConversionError;

    fn try_from(proto: crate::chroma_proto::ScheduleEntry) -> Result<Self, Self::Error> {
        let collection_id = proto
            .collection_id
            .ok_or(ScheduleEntryConversionError::MissingField(
                "collection_id".to_string(),
            ))
            .and_then(|id| {
                CollectionUuid::from_str(&id).map_err(|_| {
                    ScheduleEntryConversionError::InvalidUuid("collection_id".to_string())
                })
            })?;

        let attached_function_id = proto
            .attached_function_id
            .ok_or(ScheduleEntryConversionError::MissingField(
                "attached_function_id".to_string(),
            ))
            .and_then(|id| {
                Uuid::parse_str(&id).map_err(|_| {
                    ScheduleEntryConversionError::InvalidUuid("attached_function_id".to_string())
                })
            })?;

        let attached_function_run_nonce = proto
            .run_nonce
            .ok_or(ScheduleEntryConversionError::MissingField(
                "run_nonce".to_string(),
            ))
            .and_then(|nonce| {
                Uuid::parse_str(&nonce)
                    .map(NonceUuid)
                    .map_err(|_| ScheduleEntryConversionError::InvalidUuid("run_nonce".to_string()))
            })?;

        let when_to_run = proto
            .when_to_run
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

        let lowest_live_nonce = proto
            .lowest_live_nonce
            .as_ref()
            .and_then(|nonce_str| Uuid::parse_str(nonce_str).ok());

        Ok(ScheduleEntry {
            collection_id,
            attached_function_id,
            attached_function_run_nonce,
            when_to_run,
            lowest_live_nonce,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScheduleEntryConversionError {
    #[error("Missing required field: {0}")]
    MissingField(String),
    #[error("Invalid UUID for field: {0}")]
    InvalidUuid(String),
}
