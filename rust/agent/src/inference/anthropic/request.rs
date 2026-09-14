//! Builds the Messages API request body from an [`InferenceContext`].

use serde_json::{json, Value};

use super::AnthropicAgentInferenceModel;
use crate::inference::InferenceContext;
use crate::provider::ProviderFormat;

impl AnthropicAgentInferenceModel {
    pub(super) fn request_body(&self, ctx: &InferenceContext<'_>) -> Value {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::anthropic::tests::weather_toolset;
    use crate::inference::anthropic::AnthropicModel;
    use crate::trajectory::{ObservationBuilder, TrajectoryBuilder};

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
}
