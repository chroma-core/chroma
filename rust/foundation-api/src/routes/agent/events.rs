//! The SSE event schema `/api/agent` emits, plus the projections from the
//! agent's trajectory types into it.
//!
//! [`AgentSseEvent`] is the outbound contract: one JSON object per SSE frame,
//! tagged by `type` with its payload under `data`. The `action_*` /
//! `observation_*` helpers project the agent's [`Action`]/[`Observation`]
//! values (from `chroma-agent`) into these events, so the stream driver in the
//! parent module stays a thin loop.

use chroma_agent::{Action, ActionItem, Observation, ObservationItem};
use serde::Serialize;
use serde_json::Value;

/// The events `/api/agent` emits, one JSON object per SSE frame.
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
pub(crate) enum AgentSseEvent {
    /// One inference step: the model's reasoning, any user-facing text, and the
    /// tool calls it requested this turn.
    Action {
        #[serde(skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        text: Vec<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        calls: Vec<AgentToolCall>,
    },
    /// The results of the tool calls from the preceding action.
    Observation { results: Vec<AgentToolResult> },
    /// Terminal event: the agent finished, with its final user-facing answer.
    Done { final_text: String },
    /// The run failed mid-flight; carries a human-readable message.
    Error { message: String },
}

/// A single tool call requested by the model in an [`AgentSseEvent::Action`].
#[derive(Debug, Serialize)]
pub(crate) struct AgentToolCall {
    pub id: String,
    pub name: String,
    pub params: Value,
}

/// A single tool result in an [`AgentSseEvent::Observation`].
#[derive(Debug, Serialize)]
pub(crate) struct AgentToolResult {
    pub call_id: String,
    pub text: String,
    pub is_error: bool,
}

/// Projects an [`Action`] into the action event we emit: its reasoning text,
/// the user-facing `SendUserText` items, and the tool calls.
pub(crate) fn action_event(action: &Action) -> AgentSseEvent {
    let reasoning = action.reasoning.as_ref().map(|r| r.text.clone());
    let mut text = Vec::new();
    let mut calls = Vec::new();
    for item in &action.items {
        match item {
            ActionItem::SendUserText(t) => text.push(t.clone()),
            ActionItem::Call(call) => calls.push(AgentToolCall {
                id: call.id.clone(),
                name: call.name.clone(),
                params: call.params.clone(),
            }),
        }
    }
    AgentSseEvent::Action {
        reasoning,
        text,
        calls,
    }
}

/// Projects an [`Observation`] into the observation event we emit. Only tool
/// results are emitted; the initial user observation is never surfaced here.
pub(crate) fn observation_event(observation: &Observation) -> AgentSseEvent {
    let results = observation
        .items
        .iter()
        .filter_map(|item| match item {
            ObservationItem::ToolResult {
                call_id,
                text,
                is_error,
                ..
            } => Some(AgentToolResult {
                call_id: call_id.clone(),
                text: text.clone(),
                is_error: *is_error,
            }),
            ObservationItem::User(_) => None,
        })
        .collect();
    AgentSseEvent::Observation { results }
}

/// The user-facing text of an action (its `SendUserText` items joined), used to
/// track the agent's terminal answer.
pub(crate) fn action_text(action: &Action) -> String {
    action
        .items
        .iter()
        .filter_map(|item| match item {
            ActionItem::SendUserText(t) => Some(t.as_str()),
            ActionItem::Call(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
