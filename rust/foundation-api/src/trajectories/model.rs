use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Names the origin of an action parameter, observation, or citation-bearing fact.
///
/// `Source` is transparent in JSON: it serializes as the same string it wraps.
///
/// # Examples
///
/// ```rust
/// let source = foundation_api::trajectories::Source::new("wiki");
/// assert_eq!(source.as_str(), "wiki");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Source(String);

impl Source {
    /// Create a source name from owned or borrowed string data.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying source name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Recover the owned source name.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for Source {
    /// Wrap owned string data as a source name without changing its content.
    fn from(value: String) -> Self {
        Source(value)
    }
}

impl From<&str> for Source {
    /// Copy borrowed string data into a source name.
    fn from(value: &str) -> Self {
        Source(value.to_string())
    }
}

impl AsRef<str> for Source {
    /// Borrow the source name through the standard string reference trait.
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Source {
    /// Format the source as its wrapped string name.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Represents one generated trajectory file in its canonical JSON shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GenerateTrajectoryFile {
    /// Batch number assigned by the producer, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<i64>,
    /// Offset of this trajectory within its producer batch, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_offset: Option<i64>,
    /// Producer worker identity, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// User-visible span or span object associated with the trajectory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    /// Attempt number assigned by retry orchestration, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<i64>,
    /// Number of deadlock retries observed before this attempt, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadlock_retries: Option<i64>,
    /// Producer-specific attempt path data preserved as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_paths: Option<Vec<Value>>,
    /// Start timestamp or numeric time value emitted by the producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<StringOrNumber>,
    /// Wall-clock duration of the attempt in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<f64>,
    /// Terminal status reported by the producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Producer-specific error payload preserved as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    /// Token, cost, and model usage metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    /// Citation metadata accumulated during the trajectory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<Citations>,
    /// Final to-do payloads emitted by the producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_todos: Option<Vec<Value>>,
    /// Ordered action and observation payloads under a stable trajectory UUID.
    pub trajectory: Trajectory,
    /// Unknown top-level fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Holds the durable identity and ordered entries of a generated trajectory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trajectory {
    /// Ordered alternation of actions and observations emitted by the producer.
    pub actions_and_observations: Vec<TrajectoryEntry>,
    /// Stable UUID assigned to this trajectory.
    pub id: Uuid,
}

/// Distinguishes the two entry forms present in a trajectory stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrajectoryEntry {
    /// A tool-selection step together with parameters and source attribution.
    Action(Action),
    /// Tool outputs and metadata observed after an action step.
    Observation(Observation),
}

/// Records one action step in a trajectory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Action {
    /// Tools selected by the model for this action step.
    pub tools: Vec<Tool>,
    /// JSON parameters passed to the corresponding tools.
    pub params: Vec<Value>,
    /// Sources associated with the corresponding tool calls.
    pub sources: Vec<Source>,
    /// Optional reasoning text emitted by the producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// Optional producer signature for the reasoning text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_signature: Option<String>,
}

/// Records one observation step in a trajectory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    /// Textual observations returned by the corresponding tool calls.
    pub observations: Vec<String>,
    /// Sources associated with the corresponding observations.
    pub sources: Vec<Source>,
    /// Optional metadata for the corresponding tool calls.
    pub tool_metadata: Vec<Option<ToolCallMetadata>>,
}

/// Captures a tool value together with schema and unknown extension fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tool {
    /// Schema describing the callable tool.
    pub tool_schema: ToolSchema,
    /// Unknown tool fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Describes the public schema of a callable tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSchema {
    /// Tool name.
    pub name: String,
    /// Human-readable tool description.
    #[serde(default)]
    pub description: String,
    /// JSON Schema-style parameter description.
    #[serde(default)]
    pub parameters: Value,
    /// Names of required parameter fields.
    #[serde(default)]
    pub required: Vec<String>,
    /// Unknown schema fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Records auxiliary metadata emitted for one tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallMetadata {
    /// Lock handoff payload emitted by orchestration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_handoff: Option<Value>,
    /// Lock waits observed by this tool call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_waits: Option<Vec<LockWait>>,
    /// Whether the call was skipped because another owner received a handoff.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_due_to_handoff: Option<bool>,
    /// Page identifiers surfaced to the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surfaced_page_ids: Option<Vec<String>>,
    /// Page identifier read by the call, when singular.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_page_id: Option<String>,
    /// Page identifier written or referenced by the call, when singular.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_id: Option<String>,
    /// Record identifiers produced or referenced by the call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_ids: Option<Vec<String>>,
    /// To-do payloads emitted by the call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub todos: Option<Vec<Value>>,
    /// Page write operation name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
    /// Page slug associated with the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    /// Source identifiers attributed to the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ids: Option<Vec<String>>,
    /// Categories assigned by the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,
    /// Latest raw source date observed by the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_raw_source_date: Option<StringOrNumber>,
    /// Unknown metadata fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Describes one interval in which a producer waited on a lock.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LockWait {
    /// Sequence number assigned to the wait event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<i64>,
    /// Page slug involved in the wait.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    /// Owner identity of the waiting producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiter_owner_id: Option<String>,
    /// Attempt identifier of the waiting producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiter_attempt_id: Option<i64>,
    /// Epoch of the waiting producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiter_epoch: Option<i64>,
    /// Lock or owner that blocked the waiting producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_on: Option<String>,
    /// Owner identity that blocked the wait.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_by_owner_id: Option<String>,
    /// Attempt identifier that blocked the wait.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_by_attempt_id: Option<i64>,
    /// Epoch that blocked the wait.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_by_epoch: Option<i64>,
    /// Owner identity at the queue head while waiting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_head_owner_id: Option<String>,
    /// Attempt identifier at the queue head while waiting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_head_attempt_id: Option<i64>,
    /// Epoch at the queue head while waiting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_head_epoch: Option<i64>,
    /// Number of queued waiters observed by the producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<i64>,
    /// Wait start timestamp or numeric time value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<StringOrNumber>,
    /// Wait end timestamp or numeric time value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<StringOrNumber>,
    /// Wait duration in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_s: Option<f64>,
    /// Unknown wait fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Captures token, cost, and model accounting for a generated trajectory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    /// Number of model calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n_calls: Option<u64>,
    /// Number of input tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    /// Number of output tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    /// Number of tokens read from cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    /// Number of tokens written to cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    /// Billed cost in US dollars.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    /// Estimated cost without cache savings in US dollars.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_without_cache_usd: Option<f64>,
    /// Number of calls whose model could not be identified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unknown_model_calls: Option<u64>,
    /// Model names observed during the trajectory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models_seen: Option<Vec<String>>,
    /// Unknown usage fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Captures citation and page-write facts accumulated by a trajectory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Citations {
    /// Input source identifiers available to the trajectory.
    #[serde(default)]
    pub input_ids: Vec<String>,
    /// Page identifiers surfaced to the model.
    #[serde(default)]
    pub surfaced_page_ids: Vec<String>,
    /// Page identifiers read by the trajectory.
    #[serde(default)]
    pub read_page_ids: Vec<String>,
    /// Final source identifiers grouped by page slug.
    #[serde(default)]
    pub final_citations: BTreeMap<String, Value>,
    /// Page slugs created by the trajectory.
    #[serde(default)]
    pub new_page_slugs: Vec<String>,
    /// Page slugs updated by the trajectory.
    #[serde(default)]
    pub updated_page_slugs: Vec<String>,
    /// Categories assigned by page slug.
    #[serde(default)]
    pub categories_assigned: BTreeMap<String, Value>,
    /// Unknown citation fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Represents the producer span in any shape accepted by historical JSON files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Span {
    /// String span representation.
    Text(String),
    /// Object span representation.
    Object(SpanObject),
    /// Any other JSON span representation preserved as-is.
    Other(Value),
}

/// Describes the object form of a producer span.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanObject {
    /// Human-readable span description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Producer-specific range expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// Unknown span fields preserved for forward compatibility.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Preserves fields whose historical JSON representation can be a string or number.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrNumber {
    /// String representation.
    String(String),
    /// Numeric representation.
    Number(f64),
    /// Any other JSON representation preserved as-is.
    Other(Value),
}

/// Marks whether a trajectory is still appendable or has been finalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WriteState {
    /// The trajectory can accept additional entries.
    Open,
    /// The trajectory should be treated as immutable complete data.
    Finalized,
}

/// Decode a generated trajectory file from JSON bytes.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the bytes are not valid JSON for a
/// [`GenerateTrajectoryFile`].
pub fn parse_generate_trajectory_bytes(
    bytes: &[u8],
) -> Result<GenerateTrajectoryFile, serde_json::Error> {
    serde_json::from_slice(bytes)
}
