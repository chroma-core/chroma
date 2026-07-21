use std::collections::BTreeMap;

use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

const WIKI_WRITE_TOOLS: [&str; 2] = ["wiki_apply_patch", "wiki_upsert_file"];

/// Represents the pruned trajectory data shown through reasoning views.
///
/// Deserializing this type from historical generated trajectory JSON projects
/// the full producer payload down to reasoning text, derived page-write facts,
/// and citation attribution. Unknown producer metadata is ignored.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReasoningTrajectoryFile {
    /// Citation attribution retained for user-facing source inspection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<Citations>,
    /// Ordered pruned reasoning entries under a stable trajectory UUID.
    pub trajectory: ReasoningTrajectory,
}

impl<'de> Deserialize<'de> for ReasoningTrajectoryFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let generated = GeneratedTrajectoryFile::deserialize(deserializer)?;
        Ok(generated.project())
    }
}

/// Holds the durable identity and pruned reasoning entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReasoningTrajectory {
    /// Stable UUID assigned to this trajectory.
    pub id: Uuid,
    /// User-visible reasoning entries and derived page-write markers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<ReasoningEntry>,
}

/// One displayable reasoning step, optionally marking pages written at that step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReasoningEntry {
    /// Trimmed reasoning text shown to users, when this step has any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// Wiki pages written by this step.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writes: Vec<ReasoningWrite>,
}

impl<'de> Deserialize<'de> for ReasoningEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = GeneratedReasoningEntry::deserialize(deserializer)?;
        Ok(ReasoningEntry::new_normalized(
            raw.reasoning.as_deref(),
            raw.writes,
        ))
    }
}

impl ReasoningEntry {
    fn new_normalized<I>(reasoning: Option<&str>, writes: I) -> Self
    where
        I: IntoIterator<Item = ReasoningWrite>,
    {
        ReasoningEntry {
            reasoning: normalize_reasoning(reasoning),
            writes: dedupe_writes(writes),
        }
    }

    pub(crate) fn normalized(&self) -> Option<Self> {
        ReasoningEntry::new_normalized(self.reasoning.as_deref(), self.writes.iter().cloned())
            .into_non_empty()
    }

    fn into_non_empty(self) -> Option<Self> {
        (self.reasoning.is_some() || !self.writes.is_empty()).then_some(self)
    }
}

/// A derived fact that a reasoning step wrote a wiki page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningWrite {
    /// Wiki page slug written by the step. The empty string is the wiki root.
    pub slug: String,
}

/// Captures citation attribution retained for user-facing inspection.
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

/// Decode a reasoning trajectory file from JSON bytes.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the bytes are not valid JSON for a
/// [`ReasoningTrajectoryFile`].
pub fn parse_generate_trajectory_bytes(
    bytes: &[u8],
) -> Result<ReasoningTrajectoryFile, serde_json::Error> {
    serde_json::from_slice(bytes)
}

#[derive(Debug, Deserialize)]
struct GeneratedTrajectoryFile {
    citations: Option<Citations>,
    trajectory: GeneratedTrajectory,
}

#[derive(Debug, Deserialize)]
struct GeneratedReasoningEntry {
    reasoning: Option<String>,
    #[serde(default)]
    writes: Vec<ReasoningWrite>,
}

impl GeneratedTrajectoryFile {
    fn project(self) -> ReasoningTrajectoryFile {
        ReasoningTrajectoryFile {
            citations: self.citations,
            trajectory: ReasoningTrajectory {
                id: self.trajectory.id,
                entries: project_entries(self.trajectory.actions_and_observations),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
struct GeneratedTrajectory {
    id: Uuid,
    #[serde(default, alias = "entries")]
    actions_and_observations: Vec<GeneratedEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum GeneratedEntry {
    Action(GeneratedAction),
    Observation(GeneratedObservation),
    Reasoning(ReasoningEntry),
}

#[derive(Debug, Deserialize)]
struct GeneratedAction {
    tools: Vec<GeneratedTool>,
    params: Vec<Value>,
    reasoning: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeneratedObservation {
    tool_metadata: Vec<Option<GeneratedToolCallMetadata>>,
}

#[derive(Debug, Deserialize)]
struct GeneratedTool {
    tool_schema: GeneratedToolSchema,
}

#[derive(Debug, Deserialize)]
struct GeneratedToolSchema {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GeneratedToolCallMetadata {
    skipped_due_to_handoff: Option<bool>,
    slug: Option<String>,
}

fn project_entries(entries: Vec<GeneratedEntry>) -> Vec<ReasoningEntry> {
    let mut out = Vec::new();

    for (index, entry) in entries.iter().enumerate() {
        let GeneratedEntry::Action(action) = entry else {
            if let GeneratedEntry::Reasoning(entry) = entry {
                if let Some(entry) = entry.normalized() {
                    out.push(entry);
                }
            }
            continue;
        };
        let observation = entries.get(index + 1).and_then(|entry| match entry {
            GeneratedEntry::Observation(observation) => Some(observation),
            GeneratedEntry::Action(_) | GeneratedEntry::Reasoning(_) => None,
        });

        if let Some(entry) = ReasoningEntry::new_normalized(
            action.reasoning.as_deref(),
            action_writes(action, observation),
        )
        .into_non_empty()
        {
            out.push(entry);
        }
    }

    out
}

fn action_writes(
    action: &GeneratedAction,
    observation: Option<&GeneratedObservation>,
) -> Vec<ReasoningWrite> {
    let mut writes = Vec::new();
    for call in 0..action.tools.len() {
        let tool_name = action.tools[call].tool_schema.name.as_str();
        if !WIKI_WRITE_TOOLS.contains(&tool_name) {
            continue;
        }

        let metadata = observation
            .and_then(|observation| observation.tool_metadata.get(call))
            .and_then(Option::as_ref);
        if metadata
            .and_then(|metadata| metadata.skipped_due_to_handoff)
            .unwrap_or(false)
        {
            continue;
        }

        let slug = param_slug(action.params.get(call))
            .or_else(|| metadata.and_then(|metadata| metadata.slug.as_deref()));
        let Some(slug) = slug else {
            continue;
        };
        writes.push(ReasoningWrite {
            slug: slug.to_string(),
        });
    }
    writes
}

fn param_slug(params: Option<&Value>) -> Option<&str> {
    params?.get("slug").and_then(Value::as_str)
}

fn normalize_reasoning(reasoning: Option<&str>) -> Option<String> {
    reasoning
        .map(str::trim)
        .filter(|reasoning| !reasoning.is_empty())
        .map(str::to_string)
}

fn dedupe_writes<I>(writes: I) -> Vec<ReasoningWrite>
where
    I: IntoIterator<Item = ReasoningWrite>,
{
    let mut out = Vec::new();
    for write in writes {
        if out
            .iter()
            .any(|existing: &ReasoningWrite| existing.slug == write.slug)
        {
            continue;
        }
        out.push(write);
    }
    out
}
