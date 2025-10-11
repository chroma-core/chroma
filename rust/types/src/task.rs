use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::CollectionUuid;

/// TaskUuid is a wrapper around Uuid to provide a type for task identifiers.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    ToSchema,
)]
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
