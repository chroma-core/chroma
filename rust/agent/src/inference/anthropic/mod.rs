//! Anthropic Messages API inference model (non-streaming, thinking enabled).
//!
//! Split by request lifecycle: [`config`] holds the caller-chosen model,
//! beta flags and knobs, [`request`] builds the Messages API body,
//! [`response`] parses what comes back, and [`diagnostics`] renders a
//! redacted copy of a failed request for logs.

mod config;
mod diagnostics;
mod request;
mod response;

pub use config::{
    AnthropicBeta, AnthropicBetas, AnthropicModel, AnthropicRequestConfig, UnknownAnthropicModel,
};

use async_trait::async_trait;
use serde_json::Value;

use self::diagnostics::request_diagnostics;
use self::response::{parse_anthropic_response, parse_anthropic_usage};
use super::{AgentInferenceModel, InferenceContext, InferenceStep};
use crate::error::AgentError;
use crate::trajectory::Action;

const ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2023-06-01";

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
}

#[async_trait]
impl AgentInferenceModel for AnthropicAgentInferenceModel {
    async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        Ok(self.infer_with_usage(ctx).await?.action)
    }

    async fn infer_with_usage(
        &self,
        ctx: &InferenceContext<'_>,
    ) -> Result<InferenceStep, AgentError> {
        let request_body = self.request_body(ctx);
        let beta_header = self.config.betas.header_value();
        let mut request = self
            .client
            .post(format!("{ANTHROPIC_BASE_URL}/v1/messages"))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&request_body);

        if let Some(betas) = beta_header.as_deref() {
            request = request.header("anthropic-beta", betas);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let request_id = response
                .headers()
                .get("request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let response_body = response
                .text()
                .await
                .unwrap_or_else(|error| format!("<failed to read response body: {error}>"));
            return Err(AgentError::ProviderResponse {
                provider: "Anthropic",
                status,
                request_id,
                response_body,
                request: request_diagnostics(&request_body, beta_header.as_deref()).to_string(),
            });
        }
        let response: Value = response.json().await?;

        Ok(InferenceStep {
            action: parse_anthropic_response(&response, ctx.toolset)?,
            usage: parse_anthropic_usage(&response, self.model),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::ToolSet;
    use crate::tools::weather::GetWeatherTool;
    use crate::trajectory::{ActionItem, ObservationBuilder, TrajectoryBuilder};
    use serde_json::json;

    pub(super) fn weather_toolset() -> ToolSet {
        let mut toolset = ToolSet::new();
        toolset.add(GetWeatherTool);
        toolset
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
