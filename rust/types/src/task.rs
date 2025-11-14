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

#[derive(Debug, thiserror::Error)]
pub enum AttachedFunctionConversionError {
    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),
}

fn prost_struct_to_json_string(
    prost_struct: &prost_types::Struct,
) -> Result<String, serde_json::Error> {
    use prost_types::value::Kind;

    let mut map = serde_json::Map::new();
    for (key, value) in &prost_struct.fields {
        if let Some(kind) = &value.kind {
            let json_value = match kind {
                Kind::NullValue(_) => serde_json::Value::Null,
                Kind::NumberValue(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                Kind::StringValue(s) => serde_json::Value::String(s.clone()),
                Kind::BoolValue(b) => serde_json::Value::Bool(*b),
                Kind::StructValue(s) => serde_json::Value::Object(
                    prost_struct_to_json_string(s)?
                        .parse::<serde_json::Value>()?
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
                Kind::ListValue(list) => serde_json::Value::Array(
                    list.values
                        .iter()
                        .map(|v| {
                            if let Some(kind) = &v.kind {
                                match kind {
                                    Kind::NullValue(_) => serde_json::Value::Null,
                                    Kind::NumberValue(n) => serde_json::Value::Number(
                                        serde_json::Number::from_f64(*n)
                                            .unwrap_or_else(|| serde_json::Number::from(0)),
                                    ),
                                    Kind::StringValue(s) => serde_json::Value::String(s.clone()),
                                    Kind::BoolValue(b) => serde_json::Value::Bool(*b),
                                    _ => serde_json::Value::Null, // Simplified for now
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        })
                        .collect(),
                ),
            };
            map.insert(key.clone(), json_value);
        }
    }

    serde_json::to_string(&serde_json::Value::Object(map))
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

        // Parse params if available
        let params = attached_function
            .params
            .map(|p| prost_struct_to_json_string(&p))
            .transpose()
            .map_err(|_| AttachedFunctionConversionError::InvalidUuid("params".to_string()))?;

        // Parse timestamps
        let created_at = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_micros(attached_function.created_at);
        let updated_at = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_micros(attached_function.updated_at);
        let next_run = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_micros(attached_function.next_run_at);

        // Parse nonces
        let next_nonce = attached_function
            .next_nonce
            .parse::<NonceUuid>()
            .map_err(|_| AttachedFunctionConversionError::InvalidUuid("next_nonce".to_string()))?;
        let lowest_live_nonce = attached_function
            .lowest_live_nonce
            .map(|nonce| nonce.parse::<NonceUuid>())
            .transpose()
            .map_err(|_| {
                AttachedFunctionConversionError::InvalidUuid("lowest_live_nonce".to_string())
            })?;

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
            next_run,
            completion_offset: attached_function.completion_offset,
            min_records_for_invocation: attached_function.min_records_for_invocation,
            is_deleted: false, // Not available in proto, would need to be fetched separately
            created_at,
            updated_at,
            next_nonce,
            lowest_live_nonce,
        })
    }
}
