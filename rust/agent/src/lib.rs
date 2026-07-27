//! `chroma-agent`: a provider-agnostic agent core ported from the Python
//! search-agent research framework.
//!
//! This crate provides the [`ProviderFormat`] dispatch seam, the crate-wide
//! [`AgentError`] type, the tool abstraction ([`Tool`] / [`DynTool`] /
//! [`ToolSet`]) with a dummy [`GetWeatherTool`], the [`Trajectory`] record,
//! inference models ([`AgentInferenceModel`] / [`AnthropicAgentInferenceModel`]),
//! and the [`Agent`] state-machine driver with composable [`AgentBehavior`]
//! hooks.

mod agent;
mod error;
mod inference;
mod provider;
mod tool;
pub mod tools;
mod trajectory;

pub use agent::{Agent, AgentBehavior, ToolErrorPolicy};
pub use error::AgentError;
pub use inference::{
    AgentInferenceModel, AnthropicAgentInferenceModel, AnthropicBeta, AnthropicBetas,
    AnthropicModel, AnthropicRequestConfig, InferenceContext, InferenceStep, InferenceUsage,
    UnknownAnthropicModel,
};
pub use provider::ProviderFormat;
pub use tool::{DynTool, Tool, ToolCallMetadata, ToolSet};
pub use tools::weather::{GetWeatherTool, TemperatureUnit};
pub use trajectory::{
    Action, ActionBuilder, ActionItem, Call, Entry, Observation, ObservationBuilder,
    ObservationItem, Reasoning, Trajectory, TrajectoryBuilder,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_error_displays() {
        let err = AgentError::UnknownTool("get_weather".to_string());
        assert_eq!(err.to_string(), "unknown tool: get_weather");

        let err = AgentError::ToolRuntimeParamsTypeMismatch {
            tool: "get_weather".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "runtime params type mismatch for tool `get_weather`"
        );
    }
}
