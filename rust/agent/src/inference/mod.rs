//! Inference models: turn a [`Trajectory`] into the next [`Action`].
//!
//! [`AgentInferenceModel`] is the provider-agnostic seam; concrete provider
//! implementations live in submodules (see [`anthropic`]). Only Anthropic is
//! supported for now.

mod anthropic;

pub use anthropic::{
    AnthropicAgentInferenceModel, AnthropicBeta, AnthropicBetas, AnthropicModel,
    AnthropicRequestConfig, UnknownAnthropicModel,
};

use async_trait::async_trait;

use crate::error::AgentError;
use crate::tool::ToolSet;
use crate::trajectory::{Action, Trajectory};

/// Token usage reported by an inference provider for a single model call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceUsage {
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

/// The next action plus any provider usage emitted while producing it.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceStep {
    pub action: Option<Action>,
    pub usage: Option<InferenceUsage>,
}

/// Everything an [`AgentInferenceModel`] needs to produce the next action.
///
/// `trajectory` is the (possibly masked) view to send to the model; `toolset`
/// supplies tool schemas and validates tool names in the response.
pub struct InferenceContext<'a> {
    pub trajectory: Trajectory,
    pub toolset: &'a ToolSet,
    /// Per-call override for the model's default max output tokens.
    pub max_tokens: Option<u32>,
    /// System prompt to send with this call. Owned by the agent definition
    /// (see [`crate::Agent::with_system_prompt`]) and seeded before behaviors
    /// run, so an [`crate::AgentBehavior::prepare_for_inference`] hook may
    /// override it. The provider decides how to render it on the wire.
    pub system: Option<String>,
}

/// Produces the next [`Action`] from an [`InferenceContext`], or `None` when the
/// model returned nothing actionable.
#[async_trait]
pub trait AgentInferenceModel: Send + Sync {
    async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError>;

    async fn infer_with_usage(
        &self,
        ctx: &InferenceContext<'_>,
    ) -> Result<InferenceStep, AgentError> {
        Ok(InferenceStep {
            action: self.infer(ctx).await?,
            usage: None,
        })
    }
}
