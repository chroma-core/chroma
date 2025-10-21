use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::SystemTime;
use uuid::Uuid;

use crate::CollectionUuid;

/// TaskUuid is a wrapper around Uuid to provide a type for task identifiers.
#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TaskUuid(pub Uuid);

impl TaskUuid {
    pub fn new() -> Self {
        TaskUuid(Uuid::new_v4())
    }
}

impl std::str::FromStr for TaskUuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(TaskUuid(uuid)),
            Err(err) => Err(err),
        }
    }
}

impl std::fmt::Display for TaskUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Task represents an asynchronous task that is triggered by collection writes
/// to map records from a source collection to a target collection.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Task {
    /// Unique identifier for the task
    pub id: TaskUuid,
    /// Human-readable name for the task instance
    pub name: String,
    /// Name of the operator/built-in definition this task uses (despite field name, this is a name not a UUID)
    pub operator_id: String,
    /// Source collection that triggers the task
    pub input_collection_id: CollectionUuid,
    /// Name of target collection where task output is stored
    pub output_collection_name: String,
    /// ID of the output collection (lazily filled in after creation)
    pub output_collection_id: Option<String>,
    /// Optional JSON parameters for the operator
    pub params: Option<String>,
    /// Tenant name this task belongs to (despite field name, this is a name not a UUID)
    pub tenant_id: String,
    /// Database name this task belongs to (despite field name, this is a name not a UUID)
    pub database_id: String,
    /// Timestamp of the last successful task run
    #[serde(skip, default)]
    pub last_run: Option<SystemTime>,
    /// Timestamp when the task should next run (None if not yet scheduled)
    #[serde(skip, default)]
    pub next_run: Option<SystemTime>,
    /// Completion offset: the WAL position up to which the task has processed records
    pub completion_offset: u64,
    /// Minimum number of new records required before the task runs again
    pub min_records_for_task: u64,
    /// Whether the task has been soft-deleted
    #[serde(skip, default)]
    pub is_deleted: bool,
    /// Timestamp when the task was created
    pub created_at: SystemTime,
    /// Timestamp when the task was last updated
    pub updated_at: SystemTime,
}

/// ScheduleEntry represents a scheduled task run for a collection.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScheduleEntry {
    pub collection_id: CollectionUuid,
    pub task_id: Uuid,
    pub task_run_nonce: Uuid,
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

        let task_id = proto
            .task_id
            .ok_or(ScheduleEntryConversionError::MissingField(
                "task_id".to_string(),
            ))
            .and_then(|id| {
                Uuid::parse_str(&id)
                    .map_err(|_| ScheduleEntryConversionError::InvalidUuid("task_id".to_string()))
            })?;

        let task_run_nonce = proto
            .task_run_nonce
            .ok_or(ScheduleEntryConversionError::MissingField(
                "task_run_nonce".to_string(),
            ))
            .and_then(|nonce| {
                Uuid::parse_str(&nonce).map_err(|_| {
                    ScheduleEntryConversionError::InvalidUuid("task_run_nonce".to_string())
                })
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
            task_id,
            task_run_nonce,
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
