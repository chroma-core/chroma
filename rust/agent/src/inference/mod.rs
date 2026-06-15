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
}
