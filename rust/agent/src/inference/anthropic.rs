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

const ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Opt-in feature flags sent in the `anthropic-beta` header.
///
/// The header is a comma-separated list, so several betas can be enabled at
/// once (see [`AnthropicAgentInferenceModel::with_betas`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicBeta {
    /// Allow `thinking` blocks to interleave with `tool_use`
    /// (`interleaved-thinking-2025-05-14`). Pairs with the `thinking` config in
    /// [`AnthropicAgentInferenceModel::request_body`].
    InterleavedThinking,
}

impl AnthropicBeta {
    /// The flag token as it appears in the `anthropic-beta` header.
    pub fn id(self) -> &'static str {
        match self {
            AnthropicBeta::InterleavedThinking => "interleaved-thinking-2025-05-14",
        }
    }
}

/// The set of `anthropic-beta` flags enabled on a request.
///
/// [`Default`] enables interleaved thinking, which pairs with the always-on
/// `thinking` config; an empty set omits the header entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnthropicBetas(pub Vec<AnthropicBeta>);

impl Default for AnthropicBetas {
    fn default() -> Self {
        Self(vec![AnthropicBeta::InterleavedThinking])
    }
}

impl AnthropicBetas {
    /// Render the comma-separated `anthropic-beta` header value, or `None` when
    /// no betas are enabled (in which case the header should be omitted).
    fn header_value(&self) -> Option<String> {
        if self.0.is_empty() {
            return None;
        }
        Some(
            self.0
                .iter()
                .map(|beta| beta.id())
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

impl From<Vec<AnthropicBeta>> for AnthropicBetas {
    fn from(betas: Vec<AnthropicBeta>) -> Self {
        Self(betas)
    }
}

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

/// Tunable Messages API request knobs, separated from the required api key and
/// model so they can carry sensible defaults via [`Default`].
#[derive(Debug, Clone, PartialEq)]
pub struct AnthropicRequestConfig {
    /// Default max output tokens (an [`InferenceContext`] may override per call).
    pub max_tokens: u32,
    pub temperature: f64,
    /// Token budget for the always-on `thinking` block.
    pub thinking_budget: u32,
    /// `anthropic-beta` feature flags to enable.
    pub betas: AnthropicBetas,
}

impl Default for AnthropicRequestConfig {
    fn default() -> Self {
        Self {
            max_tokens: 4096,
            temperature: 1.0,
            thinking_budget: 6000,
            betas: AnthropicBetas::default(),
        }
    }
}

/// Anthropic Messages API inference model.
pub struct AnthropicAgentInferenceModel {
    client: reqwest::Client,
    api_key: String,
    model: AnthropicModel,
    config: AnthropicRequestConfig,
}

impl AnthropicAgentInferenceModel {
    /// Construct with the given API key and model, using the default
    /// [`AnthropicRequestConfig`] (interleaved thinking enabled).
    pub fn new(api_key: impl Into<String>, model: AnthropicModel) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model,
            config: AnthropicRequestConfig::default(),
        }
    }

    /// Construct from the `ANTHROPIC_API_KEY` environment variable.
    pub fn from_env(model: AnthropicModel) -> Result<Self, AgentError> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .map_err(|_| AgentError::Config("ANTHROPIC_API_KEY is not set".to_string()))?;
        Ok(Self::new(api_key, model))
    }

    /// Reuse a shared [`reqwest::Client`] instead of the per-instance one built
    /// by [`new`](Self::new). Cloning a client shares its connection pool, so a
    /// caller that builds a model per request can avoid spawning a fresh pool
    /// each time.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Replace the request config (max tokens, temperature, thinking budget,
    /// betas).
    pub fn with_config(mut self, config: AnthropicRequestConfig) -> Self {
        self.config = config;
        self
    }

    /// Replace the enabled `anthropic-beta` feature flags (empty disables the
    /// header entirely).
    pub fn with_betas(mut self, betas: impl Into<AnthropicBetas>) -> Self {
        self.config.betas = betas.into();
        self
    }

    fn request_body(&self, ctx: &InferenceContext<'_>) -> Value {
        let mut body = json!({
            "model": self.model.id(),
            "max_tokens": ctx.max_tokens.unwrap_or(self.config.max_tokens),
            "temperature": self.config.temperature,
            "thinking": { "type": "enabled", "budget_tokens": self.config.thinking_budget },
            "tools": ctx.toolset.get_formats(ProviderFormat::Anthropic),
            "messages": ctx.trajectory.to_provider_format(ProviderFormat::Anthropic),
        });

        // Anthropic takes the system prompt as a top-level field; omit it when
        // unset rather than sending `null`.
        if let Some(system) = &ctx.system {
            body["system"] = json!(system);
        }

        body
    }
}

#[async_trait]
impl AgentInferenceModel for AnthropicAgentInferenceModel {
    async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        let mut request = self
            .client
            .post(format!("{ANTHROPIC_BASE_URL}/v1/messages"))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&self.request_body(ctx));

        if let Some(betas) = self.config.betas.header_value() {
            request = request.header("anthropic-beta", betas);
        }

        let response: Value = request.send().await?.error_for_status()?.json().await?;

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
    fn beta_header_value_renders_and_omits() {
        assert_eq!(
            AnthropicBetas::default().header_value().as_deref(),
            Some("interleaved-thinking-2025-05-14")
        );
        assert_eq!(
            AnthropicBetas(vec![
                AnthropicBeta::InterleavedThinking,
                AnthropicBeta::InterleavedThinking,
            ])
            .header_value()
            .as_deref(),
            Some("interleaved-thinking-2025-05-14,interleaved-thinking-2025-05-14")
        );
        assert_eq!(AnthropicBetas(vec![]).header_value(), None);
    }

    #[test]
    fn request_body_includes_system_only_when_set() {
        let model = AnthropicAgentInferenceModel::new("test-key", AnthropicModel::Sonnet4_5);
        let toolset = weather_toolset();
        let trajectory = {
            let mut builder = TrajectoryBuilder::new();
            let mut obs = ObservationBuilder::new();
            obs.push_user("hi");
            builder.push_observation(obs.build());
            builder.build()
        };

        let ctx = InferenceContext {
            trajectory: trajectory.clone(),
            toolset: &toolset,
            max_tokens: None,
            system: None,
        };
        assert!(model.request_body(&ctx).get("system").is_none());

        let ctx = InferenceContext {
            trajectory,
            toolset: &toolset,
            max_tokens: None,
            system: Some("Be terse.".to_string()),
        };
        assert_eq!(model.request_body(&ctx)["system"], json!("Be terse."));
    }

    #[test]
    fn with_client_yields_a_usable_model() {
        let shared = reqwest::Client::new();
        let model = AnthropicAgentInferenceModel::new("test-key", AnthropicModel::Opus4_5)
            .with_client(shared.clone());
        let toolset = weather_toolset();
        let ctx = InferenceContext {
            trajectory: TrajectoryBuilder::new().build(),
            toolset: &toolset,
            max_tokens: None,
            system: None,
        };
        assert_eq!(
            model.request_body(&ctx)["model"],
            json!("claude-opus-4-5-20251101")
        );
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
        prompt.push_user("What's the weather in Paris?");
        builder.push_observation(prompt.build());

        // Exercise the system-prompt wire path end-to-end: steer tool use via
        // the system prompt rather than the user turn.
        let ctx = InferenceContext {
            trajectory: builder.build(),
            toolset: &toolset,
            max_tokens: None,
            system: Some(
                "You are a weather assistant. Always call the get_weather tool to answer \
                 weather questions."
                    .to_string(),
            ),
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
