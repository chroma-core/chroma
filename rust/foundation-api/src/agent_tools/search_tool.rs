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
use chroma_types::MetadataValue;
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
         hybrid dense+sparse retrieval and returns the most relevant documents, \
         each with its source page title, slug, and score."
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

/// Reads a string-valued metadata field off a hit, if present.
fn meta_str<'a>(hit: &'a SearchRecord, key: &str) -> Option<&'a str> {
    match hit.metadata.as_ref()?.get(key) {
        Some(MetadataValue::Str(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Renders search hits into a numbered text block the model can read: each hit
/// is its source page (title + `slug:` line) and score, followed by the
/// document text. The `slug:` line gives the model a real page to cite — the
/// raw record id is `{slug}-{chunk_id}`, so the slug, not the id, is what links
/// resolve against. Falls back to the id for records lacking the metadata.
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
            let slug = meta_str(hit, "slug").unwrap_or(&hit.id);
            let title = meta_str(hit, "title").unwrap_or(slug);
            format!("[{}] {title}{score}\nslug: {slug}\n{document}", i + 1)
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::Metadata;

    fn hit(id: &str, document: Option<&str>, score: Option<f32>) -> SearchRecord {
        SearchRecord {
            id: id.to_string(),
            document: document.map(str::to_string),
            embedding: None,
            score,
            metadata: None,
        }
    }

    fn hit_with_page(id: &str, slug: &str, title: &str, document: &str) -> SearchRecord {
        let mut metadata = Metadata::new();
        metadata.insert("slug".to_string(), MetadataValue::Str(slug.to_string()));
        metadata.insert("title".to_string(), MetadataValue::Str(title.to_string()));
        SearchRecord {
            id: id.to_string(),
            document: Some(document.to_string()),
            embedding: None,
            score: Some(0.5),
            metadata: Some(metadata),
        }
    }

    #[test]
    fn falls_back_to_id_when_metadata_absent() {
        let hits = vec![
            hit("doc-a", Some("alpha body"), Some(0.9123)),
            hit("doc-b", Some("beta body"), None),
        ];
        let text = format_hits(&hits);
        assert!(text.contains("[1] doc-a (score 0.9123)"));
        assert!(text.contains("slug: doc-a"));
        assert!(text.contains("alpha body"));
        assert!(text.contains("[2] doc-b"));
        assert!(text.contains("beta body"));
    }

    #[test]
    fn surfaces_page_title_and_slug_from_metadata() {
        let hits = vec![hit_with_page(
            "getting-started-3",
            "getting-started",
            "Getting Started",
            "body text",
        )];
        let text = format_hits(&hits);
        // The model cites the page title and slug, not the opaque chunk id.
        assert!(text.contains("[1] Getting Started (score 0.5000)"));
        assert!(text.contains("slug: getting-started"));
        assert!(text.contains("body text"));
        assert!(!text.contains("getting-started-3"));
    }

    #[test]
    fn formats_empty_hits_as_no_results() {
        assert_eq!(format_hits(&[]), "No results found.");
    }
}
