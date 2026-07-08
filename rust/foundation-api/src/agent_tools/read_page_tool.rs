//! `read_page` agent tool: reconstruct a single wiki page in full by slug.
//!
//! Wraps the same [`read_page_from_collection`] core the `POST /api/read-page`
//! route and the `read_page` MCP tool use, so every surface reassembles pages
//! the same way. The per-request state (the resolved collection, the tenant,
//! and the configured UI origin used to stamp page links) is captured as struct
//! fields when the `/api/agent` handler builds the toolset; the model only
//! supplies the slug it wants to read, typically one surfaced by a prior
//! `search` result.

use async_trait::async_trait;
use chroma::ChromaCollection;
use schemars::JsonSchema;
use serde::Deserialize;

use chroma_agent::{AgentError, Tool, ToolCallMetadata};

use crate::routes::read_page::{read_page_from_collection, FoundationPage};

/// Model-supplied parameters for [`ReadPageTool`].
#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct ReadPageToolParams {
    /// Slug of the wiki page to read in full, as reported by a `search` hit
    /// (`slug=...`).
    pub slug: String,
}

/// A page-read tool bound to one request's collection, tenant, and UI origin.
pub(crate) struct ReadPageTool {
    collection: ChromaCollection,
    tenant: String,
    ui_origin: Option<String>,
}

impl ReadPageTool {
    pub(crate) fn new(
        collection: ChromaCollection,
        tenant: String,
        ui_origin: Option<String>,
    ) -> Self {
        Self {
            collection,
            tenant,
            ui_origin,
        }
    }
}

#[async_trait]
impl Tool for ReadPageTool {
    type ModelSuppliedParams = ReadPageToolParams;
    type RuntimeParams = ();

    fn name(&self) -> &str {
        "read_page"
    }

    fn description(&self) -> &str {
        "Read a single knowledge-base page in full by its slug (as reported by a \
         `search` hit's `slug=`), returning its title, categories, link, and \
         complete markdown content. Use this to pull the source material behind \
         a search hit so you can read and cite it directly."
    }

    async fn call(
        &self,
        params: Self::ModelSuppliedParams,
        _runtime: Self::RuntimeParams,
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        let page = read_page_from_collection(
            &self.collection,
            &self.tenant,
            self.ui_origin.as_deref(),
            &params.slug,
        )
        .await
        .map_err(|err| AgentError::Tool(err.to_string()))?;

        match page {
            Some(page) => Ok((format_page(&page), None)),
            None => Ok((format!("No page found for slug '{}'.", params.slug), None)),
        }
    }
}

/// Renders a full page into a text block the model can read and cite: a header
/// of title / slug / link / categories, followed by the complete markdown.
fn format_page(page: &FoundationPage) -> String {
    let mut header = format!("Title: {}\nSlug: {}", page.title, page.slug);
    if let Some(url) = &page.url {
        header.push_str(&format!("\nURL: {url}"));
    }
    if !page.categories.is_empty() {
        header.push_str(&format!("\nCategories: {}", page.categories.join(", ")));
    }
    format!("{header}\n\n{}", page.content)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(url: Option<&str>, categories: Vec<&str>) -> FoundationPage {
        FoundationPage {
            slug: "onboarding".to_string(),
            title: "Onboarding".to_string(),
            categories: categories.into_iter().map(str::to_string).collect(),
            source_ids: Vec::new(),
            version: 7,
            updated_at: Some(1700),
            content: "# Welcome\nBody text.".to_string(),
            url: url.map(str::to_string),
        }
    }

    #[test]
    fn formats_page_with_header_and_content() {
        let text = format_page(&page(
            Some("https://wiki.example.com/~/page-redirect?slug=onboarding"),
            vec!["eng"],
        ));
        assert!(text.contains("Title: Onboarding"));
        assert!(text.contains("Slug: onboarding"));
        assert!(text.contains("URL: https://wiki.example.com/~/page-redirect?slug=onboarding"));
        assert!(text.contains("Categories: eng"));
        assert!(text.contains("# Welcome\nBody text."));
    }

    #[test]
    fn formats_page_omits_url_and_categories_when_absent() {
        let text = format_page(&page(None, vec![]));
        assert!(!text.contains("URL:"));
        assert!(!text.contains("Categories:"));
        assert!(text.contains("Title: Onboarding"));
        assert!(text.contains("Body text."));
    }
}
