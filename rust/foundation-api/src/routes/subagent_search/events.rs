//! Typed model of the deep-research agent's event schema.
//!
//! [`AgentEvent`] is what we parse *from* the deep-research dependency;
//! [`SubagentSearchEvent`] is what we emit *to* our callers. Both share the
//! `action`/`observation` payload structs so re-emitting a step event is a
//! faithful, typed round-trip rather than an opaque passthrough.

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// Inbound: events parsed from the deep-research dependency
// ---------------------------------------------------------------------------

/// One step event from the deep-research agent (`{"type": …, "data": {…}}`).
/// Unrecognized event types and unparseable lines become
/// [`AgentEvent::Unknown`] and are dropped rather than forwarded blindly.
#[derive(Debug)]
pub(crate) enum AgentEvent {
    Action(ActionData),
    Observation(ObservationData),
    Usage(UsageData),
    Done,
    Error(ErrorData),
    Unknown,
}

impl AgentEvent {
    /// Classifies a raw event JSON line by its `type`, deserializing the
    /// payload. Any malformed payload (or unknown type) degrades to
    /// [`AgentEvent::Unknown`].
    pub(crate) fn parse(raw: &str) -> Self {
        let Ok(value) = serde_json::from_str::<Value>(raw) else {
            return AgentEvent::Unknown;
        };
        let data = || value.get("data").cloned().unwrap_or(Value::Null);
        match value.get("type").and_then(Value::as_str) {
            Some("action") => from_data(data()).map_or(AgentEvent::Unknown, AgentEvent::Action),
            Some("observation") => {
                from_data(data()).map_or(AgentEvent::Unknown, AgentEvent::Observation)
            }
            Some("usage") => from_data(data()).map_or(AgentEvent::Unknown, AgentEvent::Usage),
            Some("done") => AgentEvent::Done,
            Some("error") => from_data(data()).map_or(AgentEvent::Unknown, AgentEvent::Error),
            _ => AgentEvent::Unknown,
        }
    }
}

/// Deserializes an event's `data` object into a typed payload, returning `None`
/// if it doesn't match the expected shape.
fn from_data<T: for<'de> Deserialize<'de>>(data: Value) -> Option<T> {
    serde_json::from_value(data).ok()
}

/// The `data` of an `action` event. `tools` and `params` are position-aligned
/// arrays (the `i`th param belongs to the `i`th tool).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ActionData {
    #[serde(default)]
    pub tools: Vec<ToolRef>,
    #[serde(default)]
    pub params: Vec<Value>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
}

/// A tool referenced in an `action` event.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ToolRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The `data` of an `observation` event.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ObservationData {
    #[serde(default)]
    pub sources: Vec<String>,
}

/// The `data` of an `error` event the agent emits when its loop fails.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ErrorData {
    pub message: String,
}

/// The `data` of a `usage` event emitted by the deep-research dependency.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct UsageData {
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

impl ActionData {
    /// The `text` of this action's last `user_text` tool, if any — the agent
    /// "speaking" to the user.
    pub(crate) fn user_text(&self) -> Option<&str> {
        self.tools
            .iter()
            .zip(&self.params)
            .rev()
            .find(|(tool, _)| tool.name == "user_text")
            .and_then(|(_, param)| param.get("text").and_then(Value::as_str))
    }

    /// True if every tool in this action is `user_text` (a pure "answer" step).
    /// We surface the answer only via the structured `result`, so these are not
    /// re-emitted as progress.
    pub(crate) fn is_answer_only(&self) -> bool {
        !self.tools.is_empty() && self.tools.iter().all(|tool| tool.name == "user_text")
    }
}

// ---------------------------------------------------------------------------
// Outbound: events we emit to our callers
// ---------------------------------------------------------------------------

/// The events `/api/subagent_search` emits. Step events mirror the agent's
/// `action`/`observation`; `result` carries the final answer parsed into
/// structured documents; `done` terminates the stream.
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
pub(crate) enum SubagentSearchEvent {
    Action(ActionData),
    Observation(ObservationData),
    Result { documents: Vec<RankedDocument> },
    Done,
}

/// A single document the subagent ranked, parsed out of the terminal answer's
/// `<Document id=…><Justification>…</Justification></Document>` block.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RankedDocument {
    pub id: String,
    pub justification: String,
}

/// Failure to turn a subagent stream into structured results. An answer that
/// parses to zero documents is *not* an error — it is a valid empty result.
#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum SubagentResultError {
    /// The upstream request or byte stream failed.
    #[error("{0}")]
    Stream(String),
    /// The upstream agent emitted an explicit error event.
    #[error("subagent stream failed: {0}")]
    Upstream(String),
    /// The byte stream ended without an explicit `done` or `error` event.
    #[error("subagent stream ended without a terminal event")]
    MissingTerminalEvent,
}

/// The final structured outcome of a deep-research run.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SubagentSearchResult {
    pub documents: Vec<RankedDocument>,
    pub usage: Option<UsageData>,
}

/// Matches one `<Document id=…><Justification>…</Justification></Document>`
/// block. `id` may be unquoted or single/double-quoted; the justification is
/// captured non-greedily and may span lines (`(?is)` = case-insensitive +
/// dot-matches-newline).
static DOCUMENT_BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?is)<Document\s+id=["']?([^"'>\s]+)["']?\s*>\s*<Justification>\s*(.*?)\s*</Justification>\s*</Document>"#,
    )
    // SAFETY(hammadb): the pattern is a compile-time constant validated by the
    // unit tests, so compilation cannot fail at runtime.
    .expect("DOCUMENT_BLOCK regex is valid")
});

/// Parses the agent's free-text final answer into ranked documents, preserving
/// the order they appear (most relevant first, per the prompt). Justification
/// whitespace is collapsed to a single space.
pub(crate) fn parse_ranked_documents(answer: &str) -> Vec<RankedDocument> {
    DOCUMENT_BLOCK
        .captures_iter(answer)
        .map(|caps| RankedDocument {
            id: caps[1].to_string(),
            justification: caps[2].split_whitespace().collect::<Vec<_>>().join(" "),
        })
        .collect()
}
