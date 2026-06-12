//! Anthropic Messages API inference model (non-streaming, thinking enabled).
//!
//! Response parsing is split into a pure [`parse_anthropic_response`] helper so
//! it can be tested without network access.

use async_trait::async_trait;
use serde_json::{json, Value};

use super::{AgentInferenceModel, InferenceContext};
use crate::error::AgentError;
use crate::provider::ProviderFormat;
use crate::tool::ToolSet;
use crate::trajectory::{Action, ActionBuilder, Call, Reasoning};

const DEFAULT_MAX_TOKENS: u32 = 4096;
const DEFAULT_THINKING_BUDGET: u32 = 6000;
const DEFAULT_TEMPERATURE: f64 = 1.0;
const ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const ANTHROPIC_BETA: &str = "interleaved-thinking-2025-05-14";

/// Known Anthropic model snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicModel {
    /// `claude-opus-4-5-20251101`
    Opus4_5,
    /// `claude-sonnet-4-5-20250929`
    Sonnet4_5,
}

impl AnthropicModel {
    /// The API model identifier sent on the wire.
    pub fn id(self) -> &'static str {
        match self {
            AnthropicModel::Opus4_5 => "claude-opus-4-5-20251101",
            AnthropicModel::Sonnet4_5 => "claude-sonnet-4-5-20250929",
        }
    }
}

/// Anthropic Messages API inference model.
pub struct AnthropicAgentInferenceModel {
    client: reqwest::Client,
    api_key: String,
    model: AnthropicModel,
    max_tokens: u32,
    temperature: f64,
    thinking_budget: u32,
}

impl AnthropicAgentInferenceModel {
    /// Construct with the given API key and model.
    pub fn new(api_key: impl Into<String>, model: AnthropicModel) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model,
            max_tokens: DEFAULT_MAX_TOKENS,
            temperature: DEFAULT_TEMPERATURE,
            thinking_budget: DEFAULT_THINKING_BUDGET,
        }
    }

    /// Construct from the `ANTHROPIC_API_KEY` environment variable.
    pub fn from_env(model: AnthropicModel) -> Result<Self, AgentError> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .map_err(|_| AgentError::Config("ANTHROPIC_API_KEY is not set".to_string()))?;
        Ok(Self::new(api_key, model))
    }

    fn request_body(&self, ctx: &InferenceContext<'_>) -> Value {
        json!({
            "model": self.model.id(),
            "max_tokens": ctx.max_tokens.unwrap_or(self.max_tokens),
            "temperature": self.temperature,
            "thinking": { "type": "enabled", "budget_tokens": self.thinking_budget },
            "tools": ctx.toolset.get_formats(ProviderFormat::Anthropic),
            "messages": ctx.trajectory.to_provider_format(ProviderFormat::Anthropic),
        })
    }
}

#[async_trait]
impl AgentInferenceModel for AnthropicAgentInferenceModel {
    async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        let response: Value = self
            .client
            .post(format!("{ANTHROPIC_BASE_URL}/v1/messages"))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("anthropic-beta", ANTHROPIC_BETA)
            .json(&self.request_body(ctx))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        parse_anthropic_response(&response, ctx.toolset)
    }
}

/// Parse an Anthropic Messages response body into an [`Action`].
///
/// Iterates the `content` blocks in order: `thinking` -> [`Reasoning`],
/// `text` -> [`crate::ActionItem::SendUserText`], `tool_use` -> [`Call`] (the
/// name is validated against `toolset`). `redacted_thinking` is rejected, like
/// the Python original. Returns `None` when there is no actionable content.
fn parse_anthropic_response(
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
    use crate::tools::weather::GetWeatherTool;
    use crate::trajectory::{ActionItem, ObservationBuilder, TrajectoryBuilder};

    fn weather_toolset() -> ToolSet {
        let mut toolset = ToolSet::new();
        toolset.add(GetWeatherTool);
        toolset
    }

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

    #[tokio::test]
    #[ignore = "requires ANTHROPIC_API_KEY and network access"]
    async fn live_infer_requests_weather_tool() {
        let model = AnthropicAgentInferenceModel::from_env(AnthropicModel::Sonnet4_5)
            .expect("ANTHROPIC_API_KEY");
        let toolset = weather_toolset();

        let mut builder = TrajectoryBuilder::new();
        let mut prompt = ObservationBuilder::new();
        prompt.push_user("What's the weather in Paris? Use the get_weather tool.");
        builder.push_observation(prompt.build());

        let ctx = InferenceContext {
            trajectory: builder.build(),
            toolset: &toolset,
            max_tokens: None,
        };

        let action = model
            .infer(&ctx)
            .await
            .expect("infer succeeds")
            .expect("an action");
        assert!(
            action
                .items
                .iter()
                .any(|item| matches!(item, ActionItem::Call(_))),
            "expected the model to call a tool"
        );
    }
}
