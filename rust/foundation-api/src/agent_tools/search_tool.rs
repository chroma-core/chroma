//! `search` agent tool: hybrid dense+sparse retrieval over the wiki collection.
//!
//! Wraps the same [`run_hybrid_search`] core the `POST /api/search` route uses,
//! so the agent and the bare route share one retrieval implementation. The
//! per-request state (the resolved collection, the caller's token, and an
//! embedder) is captured as struct fields when the `/api/agent` handler builds
//! the toolset; the model only supplies the query and an optional limit.

use async_trait::async_trait;
use chroma::types::GroupBy;
use chroma::ChromaCollection;
use chroma_types::operator::SearchRecord;
use schemars::JsonSchema;
use serde::Deserialize;

use chroma_agent::{AgentError, Tool, ToolCallMetadata};

use crate::routes::links::page_url;
use crate::routes::search::{default_limit, run_hybrid_search};
use crate::wiki::chunking::ChunkRecordId;
use crate::wiki::embed::WikiEmbedder;
use crate::wiki::page::meta_str;

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

/// A retrieval tool bound to one request's collection, token, embedder,
/// tenant, and UI origin (used to stamp page links on hits).
pub(crate) struct SearchTool {
    collection: ChromaCollection,
    embedder: WikiEmbedder,
    token: String,
    tenant: String,
    ui_origin: Option<String>,
}

impl SearchTool {
    pub(crate) fn new(
        collection: ChromaCollection,
        embedder: WikiEmbedder,
        token: String,
        tenant: String,
        ui_origin: Option<String>,
    ) -> Self {
        Self {
            collection,
            embedder,
            token,
            tenant,
            ui_origin,
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
         with their ids, page slugs, page links, page titles, and scores."
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
            GroupBy::default(),
        )
        .await
        .map_err(|err| AgentError::Tool(err.to_string()))?;

        Ok((
            format_hits(&hits, self.ui_origin.as_deref(), &self.tenant),
            None,
        ))
    }
}

/// Renders search hits into a numbered text block the model can read: each hit
/// is its id, page slug, `url=` page link, and `title=` (when present), and
/// score, followed by the document text. The url is stamped from the UI origin
/// and the title read from hit metadata so the model cites pages without
/// constructing links or guessing titles itself. The slug comes from hit
/// metadata, falling back to the chunk record id so a hit missing the
/// metadata field is still linkable (matching the subagent_search path).
fn format_hits(hits: &[SearchRecord], ui_origin: Option<&str>, tenant: &str) -> String {
    if hits.is_empty() {
        return "No results found.".to_string();
    }
    hits.iter()
        .enumerate()
        .map(|(i, hit)| {
            let slug = hit
                .metadata
                .as_ref()
                .and_then(|meta| meta_str(meta, "slug"))
                .or_else(|| ChunkRecordId::slug_from_id(&hit.id).map(str::to_string));
            let url = slug
                .as_deref()
                .and_then(|slug| page_url(ui_origin, tenant, slug))
                .map(|url| format!(" url={url}"))
                .unwrap_or_default();
            let slug = slug.map(|slug| format!(" slug={slug}")).unwrap_or_default();
            let title = hit
                .metadata
                .as_ref()
                .and_then(|meta| meta_str(meta, "title"))
                .map(|title| {
                    // Keep the quoted field intact: flatten line breaks and
                    // escape embedded quotes so an unusual title can't split
                    // the hit line or close the field early.
                    let title = title.replace(['\n', '\r'], " ").replace('"', "\\\"");
                    format!(" title=\"{title}\"")
                })
                .unwrap_or_default();
            let score = hit
                .score
                .map(|s| format!(" (score {s:.4})"))
                .unwrap_or_default();
            let document = hit.document.as_deref().unwrap_or("");
            format!(
                "[{}] {}{}{}{}{}\n{}",
                i + 1,
                hit.id,
                slug,
                url,
                title,
                score,
                document
            )
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

    fn slugged(id: &str, slug: &str) -> SearchRecord {
        use chroma_types::{Metadata, MetadataValue};
        let mut record = hit(id, Some("body"), Some(0.5));
        let mut meta = Metadata::new();
        meta.insert("slug".to_string(), MetadataValue::Str(slug.to_string()));
        record.metadata = Some(meta);
        record
    }

    #[test]
    fn formats_hits_with_id_score_and_document() {
        let hits = vec![
            hit("doc-a", Some("alpha body"), Some(0.9123)),
            hit("doc-b", Some("beta body"), None),
        ];
        let text = format_hits(&hits, None, "t-1");
        assert!(text.contains("[1] doc-a (score 0.9123)"));
        assert!(text.contains("alpha body"));
        assert!(text.contains("[2] doc-b"));
        assert!(text.contains("beta body"));
    }

    #[test]
    fn formats_hits_surfaces_slug_when_present() {
        let text = format_hits(&[slugged("onboarding-0", "onboarding")], None, "t-1");
        assert!(text.contains("slug=onboarding"), "got: {text}");
        // Without a UI origin there is no url to stamp.
        assert!(!text.contains("url="), "got: {text}");
    }

    #[test]
    fn formats_hits_stamps_url_when_origin_configured() {
        let text = format_hits(
            &[slugged("onboarding-0", "onboarding")],
            Some("https://wiki.example.com"),
            "t-1",
        );
        assert!(
            text.contains(
                " url=https://wiki.example.com/~/page-redirect?tenant_uuid=t-1&slug=onboarding"
            ),
            "got: {text}"
        );
    }

    #[test]
    fn formats_hits_surfaces_title_when_present() {
        use chroma_types::MetadataValue;
        let mut record = slugged("onboarding-0", "onboarding");
        record.metadata.as_mut().expect("metadata").insert(
            "title".to_string(),
            MetadataValue::Str("Engineering Onboarding".to_string()),
        );

        let text = format_hits(&[record], None, "t-1");
        assert!(
            text.contains(" title=\"Engineering Onboarding\""),
            "got: {text}"
        );
    }

    #[test]
    fn formats_hits_escapes_quotes_and_newlines_in_title() {
        use chroma_types::MetadataValue;
        let mut record = slugged("onboarding-0", "onboarding");
        record.metadata.as_mut().expect("metadata").insert(
            "title".to_string(),
            MetadataValue::Str("A \"quoted\"\ntitle".to_string()),
        );

        let text = format_hits(&[record], None, "t-1");
        assert!(
            text.contains(" title=\"A \\\"quoted\\\" title\""),
            "got: {text}"
        );
    }

    #[test]
    fn formats_hits_derives_slug_from_chunk_id_without_metadata() {
        // No metadata at all, but the id is a canonical chunk id — the slug
        // (and url) still resolve, matching the subagent_search path.
        let text = format_hits(
            &[hit("getting-started-3", Some("body"), None)],
            Some("https://wiki.example.com"),
            "t-1",
        );
        assert!(text.contains("slug=getting-started"), "got: {text}");
        assert!(
            text.contains(
                " url=https://wiki.example.com/~/page-redirect?tenant_uuid=t-1&slug=getting-started"
            ),
            "got: {text}"
        );
    }

    #[test]
    fn formats_empty_hits_as_no_results() {
        assert_eq!(format_hits(&[], None, "t-1"), "No results found.");
    }
}
