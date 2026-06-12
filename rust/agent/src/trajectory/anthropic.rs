//! Anthropic Messages API rendering for a [`Trajectory`].
//!
//! Mirrors the Python `Trajectory.to_anthropic_format`: each [`Action`] becomes
//! an `assistant` message (optional `thinking` block, then `text` and
//! `tool_use` blocks) and each [`Observation`] becomes a `user` message
//! (`text` and `tool_result` blocks).

use serde_json::{json, Value};

use super::{Action, ActionItem, Entry, Observation, ObservationItem, Trajectory};

/// Render `trajectory` into the Anthropic Messages API `messages` array.
pub(super) fn to_messages(trajectory: &Trajectory) -> Value {
    let messages: Vec<Value> = trajectory
        .entries
        .iter()
        .map(|entry| match entry {
            Entry::Action(action) => action_to_message(action),
            Entry::Observation(observation) => observation_to_message(observation),
        })
        .collect();
    Value::Array(messages)
}

fn action_to_message(action: &Action) -> Value {
    let mut content: Vec<Value> = Vec::new();
    if let Some(reasoning) = &action.reasoning {
        content.push(json!({
            "type": "thinking",
            "thinking": reasoning.text,
            "signature": reasoning.signature,
        }));
    }
    for item in &action.items {
        match item {
            ActionItem::SendUserText(text) => content.push(json!({ "type": "text", "text": text })),
            ActionItem::Call(call) => content.push(json!({
                "type": "tool_use",
                "id": call.id,
                "name": call.name,
                "input": call.params,
            })),
        }
    }
    json!({ "role": "assistant", "content": content })
}

fn observation_to_message(observation: &Observation) -> Value {
    let mut content: Vec<Value> = Vec::new();
    for item in &observation.items {
        match item {
            ObservationItem::User(text) => content.push(json!({ "type": "text", "text": text })),
            ObservationItem::ToolResult {
                call_id,
                text,
                is_error,
                ..
            } => content.push(json!({
                "type": "tool_result",
                "tool_use_id": call_id,
                "content": [{ "type": "text", "text": text }],
                "is_error": is_error,
            })),
        }
    }
    json!({ "role": "user", "content": content })
}
