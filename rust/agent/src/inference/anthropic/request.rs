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
            // Automatic prompt caching. Anthropic puts the breakpoint on the
            // last cacheable block and advances it as the conversation grows,
            // so each iteration reads the previous iteration's context back at
            // cache-read price instead of re-paying full input price for it.
            //
            // The agent re-sends the whole conversation every iteration, which
            // is exactly the shape this is for: a request-level marker leaves
            // all four block-level breakpoints free, needs no bookkeeping as
            // the trajectory grows, and skips blocks that cannot carry a
            // marker (`thinking`) on its own.
            //
            // `ephemeral` is the 5-minute TTL, so the tool execution between
            // two iterations has to finish inside it to hit the cache.
            "cache_control": { "type": "ephemeral" },
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

    fn context_trajectory() -> crate::trajectory::Trajectory {
        let mut builder = TrajectoryBuilder::new();
        let mut obs = ObservationBuilder::new();
        obs.push_user("hi");
        builder.push_observation(obs.build());
        builder.build()
    }

    #[test]
    fn request_body_includes_system_only_when_set() {
        let model = AnthropicAgentInferenceModel::new("test-key", AnthropicModel::Sonnet4_5);
        let toolset = weather_toolset();
        let trajectory = context_trajectory();

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

    /// The breakpoint is requested once at the top level; no content block
    /// carries one, which is what leaves all four block-level slots free.
    #[test]
    fn request_body_requests_automatic_caching() {
        let model = AnthropicAgentInferenceModel::new("test-key", AnthropicModel::Sonnet4_5);
        let toolset = weather_toolset();
        let ctx = InferenceContext {
            trajectory: context_trajectory(),
            toolset: &toolset,
            max_tokens: None,
            system: Some("Be terse.".to_string()),
        };

        let body = model.request_body(&ctx);

        assert_eq!(body["cache_control"], json!({ "type": "ephemeral" }));

        let tools = body["tools"].as_array().expect("tools array");
        assert!(tools.iter().all(|tool| tool.get("cache_control").is_none()));

        let messages = body["messages"].as_array().expect("messages array");
        for message in messages {
            let content = message["content"].as_array().expect("content array");
            assert!(content.iter().all(|b| b.get("cache_control").is_none()));
        }
    }

    /// An empty trajectory still asks for caching; Anthropic skips it when no
    /// block is eligible rather than rejecting the request.
    #[test]
    fn request_body_requests_caching_on_an_empty_trajectory() {
        let model = AnthropicAgentInferenceModel::new("test-key", AnthropicModel::Sonnet4_5);
        let toolset = weather_toolset();
        let ctx = InferenceContext {
            trajectory: TrajectoryBuilder::new().build(),
            toolset: &toolset,
            max_tokens: None,
            system: Some("Be terse.".to_string()),
        };

        let body = model.request_body(&ctx);

        assert_eq!(body["messages"], json!([]));
        assert_eq!(body["cache_control"], json!({ "type": "ephemeral" }));
    }
}
