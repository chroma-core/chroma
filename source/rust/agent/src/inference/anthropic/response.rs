//! Parses an Anthropic Messages API response into an [`Action`] and the
//! token counts that back billing.
//!
//! Kept free of I/O so it can be tested without network access.

use serde_json::Value;

use super::AnthropicModel;
use crate::error::AgentError;
use crate::inference::InferenceUsage;
use crate::tool::ToolSet;
use crate::trajectory::{Action, ActionBuilder, Call, Reasoning};

pub(super) fn parse_anthropic_usage(
    response: &Value,
    model: AnthropicModel,
) -> Option<InferenceUsage> {
    let usage = response.get("usage")?;
    let input_tokens = usage.get("input_tokens")?.as_u64()?;
    let output_tokens = usage.get("output_tokens")?.as_u64()?;
    let cache_read_tokens = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_write_tokens = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(InferenceUsage {
        model: model.id().to_string(),
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
    })
}

/// Parse an Anthropic Messages response body into an [`Action`].
///
/// Iterates the `content` blocks in order: `thinking` -> [`Reasoning`],
/// `text` -> [`crate::ActionItem::SendUserText`], `tool_use` -> [`Call`] (the
/// name is validated against `toolset`). `redacted_thinking` is rejected, like
/// the Python original. Returns `None` when there is no actionable content.
pub(super) fn parse_anthropic_response(
    response: &Value,
    toolset: &ToolSet,
) -> Result<Option<Action>, AgentError> {
    let content = response
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AgentError::Unsupported("Anthropic response missing `content` array".to_string())
        })?;

    let mut builder = ActionBuilder::new();
    for block in content {
        let block_type = block
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| AgentError::Unsupported("content block missing `type`".to_string()))?;

        match block_type {
            "thinking" => {
                let text = block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let signature = block
                    .get("signature")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                builder.set_reasoning(Reasoning { text, signature });
            }
            "text" => {
                let text = block
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                builder.push_send_user_text(text);
            }
            "tool_use" => {
                let name = block.get("name").and_then(Value::as_str).ok_or_else(|| {
                    AgentError::Unsupported("tool_use block missing `name`".to_string())
                })?;
                if toolset.get(name).is_none() {
                    return Err(AgentError::UnknownTool(name.to_string()));
                }
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let params = block.get("input").cloned().unwrap_or(Value::Null);
                builder.push_call(Call {
                    name: name.to_string(),
                    params,
                    id,
                });
            }
            "redacted_thinking" => {
                return Err(AgentError::Unsupported(
                    "redacted thinking is not supported".to_string(),
                ));
            }
            other => {
                return Err(AgentError::Unsupported(format!(
                    "unsupported content block type: {other}"
                )));
            }
        }
    }

    if builder.is_empty() {
        return Ok(None);
    }
    Ok(Some(builder.build()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::anthropic::tests::weather_toolset;
    use crate::trajectory::ActionItem;
    use serde_json::json;

    #[test]
    fn parses_content_blocks_into_action() {
        let toolset = weather_toolset();
        let response = json!({
            "content": [
                { "type": "thinking", "thinking": "I should check the weather.", "signature": "sig-1" },
                { "type": "text", "text": "Let me look that up." },
                { "type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": { "location": "Paris" } }
            ]
        });

        let action = parse_anthropic_response(&response, &toolset)
            .expect("parse")
            .expect("action");

        let reasoning = action.reasoning.as_ref().expect("reasoning");
        assert_eq!(reasoning.text, "I should check the weather.");
        assert_eq!(reasoning.signature.as_deref(), Some("sig-1"));

        assert_eq!(action.items.len(), 2);
        match &action.items[0] {
            ActionItem::SendUserText(text) => assert_eq!(text, "Let me look that up."),
            other => panic!("expected SendUserText, got {other:?}"),
        }
        match &action.items[1] {
            ActionItem::Call(call) => {
                assert_eq!(call.name, "get_weather");
                assert_eq!(call.id, "toolu_1");
                assert_eq!(call.params["location"], "Paris");
            }
            other => panic!("expected Call, got {other:?}"),
        }
    }

    #[test]
    fn empty_content_yields_no_action() {
        let toolset = weather_toolset();
        let response = json!({ "content": [] });
        assert!(parse_anthropic_response(&response, &toolset)
            .expect("parse")
            .is_none());
    }

    #[test]
    fn unknown_tool_errors() {
        let toolset = weather_toolset();
        let response = json!({
            "content": [
                { "type": "tool_use", "id": "x", "name": "not_a_tool", "input": {} }
            ]
        });
        let err = parse_anthropic_response(&response, &toolset).expect_err("should error");
        assert!(matches!(err, AgentError::UnknownTool(name) if name == "not_a_tool"));
    }

    #[test]
    fn redacted_thinking_is_unsupported() {
        let toolset = weather_toolset();
        let response = json!({
            "content": [ { "type": "redacted_thinking", "data": "..." } ]
        });
        let err = parse_anthropic_response(&response, &toolset).expect_err("should error");
        assert!(matches!(err, AgentError::Unsupported(_)));
    }
}
