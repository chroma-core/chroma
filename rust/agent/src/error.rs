//! Error types for the agent crate.

use thiserror::Error;

/// Errors surfaced by the agent core, tools, and inference models.
///
/// This is the crate-wide error skeleton; variants are filled in as the tool,
/// trajectory, inference, and driver layers land in subsequent milestones.
#[derive(Debug, Error)]
pub enum AgentError {
    /// Failed to (de)serialize JSON, e.g. decoding model-supplied tool params.
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    /// The model referenced a tool name that is not registered in the toolset.
    #[error("unknown tool: {0}")]
    UnknownTool(String),

    /// Harness-supplied runtime params did not downcast to the tool's declared
    /// `RuntimeParams` type.
    #[error("runtime params type mismatch for tool `{tool}`")]
    ToolRuntimeParamsTypeMismatch { tool: String },

    /// A requested provider format or operation is not yet supported.
    #[error("unsupported: {0}")]
    Unsupported(String),
}
