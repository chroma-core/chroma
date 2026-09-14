//! Renders a failed request as a log-safe copy: the full structure and
//! provider knobs are kept, while prompts, retrieved text, reasoning and
//! tool parameters are replaced with their sizes.

use serde_json::{json, Value};

use super::ANTHROPIC_VERSION;

/// Retain the complete request structure and provider knobs while replacing
/// prompts, retrieved text, model-generated reasoning, and tool parameters
/// with sizes. This is safe to put in an error log and is enough to map an
/// Anthropic validation path back to the failed content block.
pub(super) fn request_diagnostics(body: &Value, beta_header: Option<&str>) -> Value {
    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .map(|messages| messages.iter().map(message_diagnostics).collect::<Vec<_>>())
        .unwrap_or_default();
    let tools = body
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| tools.iter().map(tool_diagnostics).collect::<Vec<_>>())
        .unwrap_or_default();

    json!({
        "headers": {
            "anthropic-version": ANTHROPIC_VERSION,
            "anthropic-beta": beta_header,
        },
        "body": {
            "model": body.get("model"),
            "max_tokens": body.get("max_tokens"),
            "temperature": body.get("temperature"),
            "thinking": body.get("thinking"),
            "cache_control": body.get("cache_control"),
            "system": body.get("system").map(text_diagnostics),
            "tools": tools,
            "messages": messages,
        },
    })
}

fn message_diagnostics(message: &Value) -> Value {
    let content = message
        .get("content")
        .and_then(Value::as_array)
        .map(|blocks| blocks.iter().map(content_diagnostics).collect::<Vec<_>>())
        .unwrap_or_default();
    json!({ "role": message.get("role"), "content": content })
}

fn content_diagnostics(content: &Value) -> Value {
    match content.get("type").and_then(Value::as_str) {
        Some("text") => json!({
            "type": "text",
            "text": content.get("text").map(text_diagnostics),
        }),
        Some("thinking") => json!({
            "type": "thinking",
            "thinking": content.get("thinking").map(text_diagnostics),
            "signature_present": content.get("signature").is_some_and(|value| !value.is_null()),
        }),
        Some("tool_use") => json!({
            "type": "tool_use",
            "id": content.get("id"),
            "name": content.get("name"),
            "input": content.get("input").map(value_diagnostics),
        }),
        Some("tool_result") => json!({
            "type": "tool_result",
            "tool_use_id": content.get("tool_use_id"),
            "is_error": content.get("is_error"),
            "content": content.get("content").map(value_diagnostics),
        }),
        block_type => json!({
            "type": block_type,
            "value": value_diagnostics(content),
        }),
    }
}

fn tool_diagnostics(tool: &Value) -> Value {
    json!({
        "name": tool.get("name"),
        "description": tool.get("description").map(text_diagnostics),
        "input_schema": tool.get("input_schema").map(value_diagnostics),
    })
}

fn text_diagnostics(value: &Value) -> Value {
    match value.as_str() {
        Some(text) => json!({
            "redacted": true,
            "chars": text.chars().count(),
            "bytes": text.len(),
        }),
        None => value_diagnostics(value),
    }
}

fn value_diagnostics(value: &Value) -> Value {
    json!({
        "redacted": true,
        "json_bytes": serde_json::to_vec(value).map_or(0, |value| value.len()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_diagnostics_preserve_shape_and_redact_content() {
        let request = json!({
            "model": "claude-sonnet-4-5-20250929",
            "max_tokens": 4096,
            "temperature": 1.0,
            "thinking": { "type": "enabled", "budget_tokens": 6000 },
            "cache_control": { "type": "ephemeral" },
            "system": "private system prompt",
            "tools": [{
                "name": "search",
                "description": "Search private docs",
                "input_schema": { "type": "object" },
            }],
            "messages": [
                { "role": "user", "content": [{ "type": "text", "text": "private question" }] },
                { "role": "assistant", "content": [
                    { "type": "thinking", "thinking": "private reasoning", "signature": "secret-signature" },
                    { "type": "tool_use", "id": "toolu_1", "name": "search", "input": { "query": "private query" } },
                ] },
                { "role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_1",
                    "content": [{ "type": "text", "text": "private result" }],
                    "is_error": false,
                }] },
            ],
        });

        let diagnostics = request_diagnostics(&request, Some("interleaved-thinking-2025-05-14"));
        assert_eq!(
            diagnostics,
            json!({
                "headers": {
                    "anthropic-version": "2023-06-01",
                    "anthropic-beta": "interleaved-thinking-2025-05-14",
                },
                "body": {
                    "model": "claude-sonnet-4-5-20250929",
                    "max_tokens": 4096,
                    "temperature": 1.0,
                    "thinking": { "type": "enabled", "budget_tokens": 6000 },
                    "cache_control": { "type": "ephemeral" },
                    "system": { "redacted": true, "chars": 21, "bytes": 21 },
                    "tools": [{
                        "name": "search",
                        "description": { "redacted": true, "chars": 19, "bytes": 19 },
                        "input_schema": { "redacted": true, "json_bytes": 17 },
                    }],
                    "messages": [
                        {
                            "role": "user",
                            "content": [{
                                "type": "text",
                                "text": { "redacted": true, "chars": 16, "bytes": 16 },
                            }],
                        },
                        {
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "thinking",
                                    "thinking": {
                                        "redacted": true,
                                        "chars": 17,
                                        "bytes": 17,
                                    },
                                    "signature_present": true,
                                },
                                {
                                    "type": "tool_use",
                                    "id": "toolu_1",
                                    "name": "search",
                                    "input": { "redacted": true, "json_bytes": 25 },
                                },
                            ],
                        },
                        {
                            "role": "user",
                            "content": [{
                                "type": "tool_result",
                                "tool_use_id": "toolu_1",
                                "is_error": false,
                                "content": { "redacted": true, "json_bytes": 41 },
                            }],
                        },
                    ],
                },
            })
        );
    }
}
