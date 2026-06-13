//! `chroma-agent`: a provider-agnostic agent core ported from the Python
//! search-agent research framework.
//!
//! This milestone lands the crate scaffold: the [`ProviderFormat`] dispatch
//! seam and the crate-wide [`AgentError`] type. The tool abstraction,
//! trajectory, inference models, and the agent driver land in subsequent PRs
//! (see `plans/`).

mod error;
mod provider;

pub use error::AgentError;
pub use provider::ProviderFormat;

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
