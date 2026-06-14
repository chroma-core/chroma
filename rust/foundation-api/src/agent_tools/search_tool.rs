//! `search` agent tool: hybrid dense+sparse retrieval over the wiki collection.
//!
//! Wraps the same [`run_hybrid_search`] core the `POST /api/search` route uses,
//! so the agent and the bare route share one retrieval implementation. The
//! per-request state (the resolved collection, the caller's token, and an
//! embedder) is captured as struct fields when the `/api/agent` handler builds
//! the toolset; the model only supplies the query and an optional limit.

use async_trait::async_trait;
use chroma::ChromaCollection;
use chroma_types::operator::SearchRecord;
use schemars::JsonSchema;
use serde::Deserialize;

use chroma_agent::{AgentError, Tool, ToolCallMetadata};

use crate::routes::search::{default_limit, run_hybrid_search};
use crate::wiki::embed::WikiEmbedder;

/// Model-supplied parameters for [`SearchTool`].
#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct SearchToolParams {
    /// The search query. Embedded into dense + sparse vectors before querying.
    pub query: String,
    /// Maximum number of hits to return. Defaults to the route default when
    /// omitted.
    #[serde(default)]
    pub limit: Option<u32>,
}

/// A retrieval tool bound to one request's collection, token, and embedder.
pub(crate) struct SearchTool {
    collection: ChromaCollection,
    embedder: WikiEmbedder,
    token: String,
}

impl SearchTool {
    pub(crate) fn new(collection: ChromaCollection, embedder: WikiEmbedder, token: String) -> Self {
        Self {
            collection,
            embedder,
            token,
        }
    }
}

#[async_trait]
impl Tool for SearchTool {
    type ModelSuppliedParams = SearchToolParams;
    type RuntimeParams = ();

    fn name(&self) -> &str {
        "search"
    }

    fn description(&self) -> &str {
        "Search the knowledge base for documents relevant to a query. Runs a \
         hybrid dense+sparse retrieval and returns the most relevant documents \
         with their ids and scores."
    }

    async fn call(
        &self,
        params: Self::ModelSuppliedParams,
        _runtime: Self::RuntimeParams,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        let limit = params.limit.unwrap_or_else(default_limit);
        let hits = run_hybrid_search(
            &self.collection,
            &self.embedder,
            &self.token,
            &params.query,
            limit,
        )
        .await
        .map_err(|err| AgentError::Tool(err.to_string()))?;

        Ok((format_hits(&hits), None))
    }
}

/// Renders search hits into a numbered text block the model can read: each hit
/// is its id (+ score) followed by the document text.
fn format_hits(hits: &[SearchRecord]) -> String {
    if hits.is_empty() {
        return "No results found.".to_string();
    }
    hits.iter()
        .enumerate()
        .map(|(i, hit)| {
            let score = hit
                .score
                .map(|s| format!(" (score {s:.4})"))
                .unwrap_or_default();
            let document = hit.document.as_deref().unwrap_or("");
            format!("[{}] {}{}\n{}", i + 1, hit.id, score, document)
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(id: &str, document: Option<&str>, score: Option<f32>) -> SearchRecord {
        SearchRecord {
            id: id.to_string(),
            document: document.map(str::to_string),
            embedding: None,
            score,
            metadata: None,
        }
    }

    #[test]
    fn formats_hits_with_id_score_and_document() {
        let hits = vec![
            hit("doc-a", Some("alpha body"), Some(0.9123)),
            hit("doc-b", Some("beta body"), None),
        ];
        let text = format_hits(&hits);
        assert!(text.contains("[1] doc-a (score 0.9123)"));
        assert!(text.contains("alpha body"));
        assert!(text.contains("[2] doc-b"));
        assert!(text.contains("beta body"));
    }

    #[test]
    fn formats_empty_hits_as_no_results() {
        assert_eq!(format_hits(&[]), "No results found.");
    }
}
