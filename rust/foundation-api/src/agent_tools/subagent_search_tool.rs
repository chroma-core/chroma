//! `subagent_search` agent tool: delegates deep research to the external
//! "context-1" agent and returns its ranked documents as text.
//!
//! Wraps [`subagent_search_text`], which runs the deep-research stream to
//! completion and renders the ranked answer; the tool just carries the
//! per-request connection state (shared HTTP client, endpoint URL, and Chroma
//! creds) resolved by the `/api/agent` handler.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use chroma_agent::{AgentError, Tool, ToolCallMetadata};

use crate::routes::subagent_search::{subagent_search_text, SubagentSearchCreds};

/// Model-supplied parameters for [`SubagentSearchTool`].
#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct SubagentSearchToolParams {
    /// The research question to hand to the deep-research subagent.
    pub query: String,
}

/// A deep-research tool bound to one request's endpoint, Chroma creds, and UI
/// origin (used to stamp page links on ranked documents).
pub(crate) struct SubagentSearchTool {
    http: reqwest::Client,
    url: String,
    creds: SubagentSearchCreds,
    ui_origin: Option<String>,
}

impl SubagentSearchTool {
    pub(crate) fn new(
        http: reqwest::Client,
        url: String,
        creds: SubagentSearchCreds,
        ui_origin: Option<String>,
    ) -> Self {
        Self {
            http,
            url,
            creds,
            ui_origin,
        }
    }
}

#[async_trait]
impl Tool for SubagentSearchTool {
    type ModelSuppliedParams = SubagentSearchToolParams;
    type RuntimeParams = ();

    fn name(&self) -> &str {
        "subagent_search"
    }

    fn description(&self) -> &str {
        "Delegate an open-ended research question to a deep-research subagent \
         that explores the knowledge base over multiple steps and returns a \
         ranked set of supporting documents. Prefer this for broad or \
         multi-part questions; use `search` for a single targeted lookup."
    }

    async fn call(
        &self,
        params: Self::ModelSuppliedParams,
        _runtime: Self::RuntimeParams,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        let text = subagent_search_text(
            self.http.clone(),
            self.url.clone(),
            self.creds.clone(),
            params.query,
            self.ui_origin.as_deref(),
        )
        .await
        .map_err(|err| AgentError::Tool(err.to_string()))?;

        Ok((text, None))
    }
}
