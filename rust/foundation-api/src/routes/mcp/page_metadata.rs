//! Batch page-metadata lookup backing the MCP `ask_foundation` tool.
//!
//! `ask_foundation` needs the title, categories, and web url of many pages at
//! once (to enrich the deep-research subagent's ranked pages). Rather than
//! reconstructing each page in full via [`run_read_page`](super::super::read_page::run_read_page),
//! this resolves all of them with a single filtered `search` over the wiki
//! collection.

use axum::http::HeaderMap;
use chroma::types::{Key, SearchPayload, SearchResponse};
use chroma_types::{
    MetadataComparison, MetadataExpression, MetadataSetValue, MetadataValue, PrimitiveOperator,
    SetOperator, Where,
};

use crate::routes::caller_token;
use crate::routes::links::page_redirect_url;
use crate::routes::read_page::ReadPageError;
use crate::server::FoundationApiServer;
use crate::wiki::page::{meta_str, meta_str_array};

/// Slim page metadata — title, categories, and web `url`, with no reconstructed
/// content. Resolved for a batch of slugs by [`run_read_pages_metadata`].
#[derive(Debug)]
pub(crate) struct PageMetadata {
    pub slug: String,
    pub title: String,
    pub categories: Vec<String>,
    pub url: Option<String>,
}

/// Resolves the metadata (title, categories, url) of many pages in a *single*
/// filtered search, rather than reconstructing each page in full. This is what
/// the MCP `ask_foundation` tool uses to enrich the subagent's ranked pages,
/// instead of one [`run_read_page`](crate::routes::read_page::run_read_page) per
/// slug.
///
/// The filter is `slug IN {slugs} AND chunk_id == 0`: every page has a chunk-0
/// record (the title chunk) and stamps the same per-page metadata on every
/// chunk, so chunk 0 alone yields one metadata record per page. Slugs with no
/// matching page are simply absent from the result, which callers key by slug.
/// Each `url` is stamped from the configured `foundation_ui_origin` (left `None`
/// when the origin is unset).
pub(crate) async fn run_read_pages_metadata(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    slugs: &[String],
) -> Result<Vec<PageMetadata>, ReadPageError> {
    if slugs.is_empty() {
        return Ok(Vec::new());
    }

    let wiki_client = server
        .wiki_client
        .as_ref()
        .ok_or(ReadPageError::RouteDisabled)?;
    let token = caller_token(headers).ok_or(ReadPageError::MissingToken)?;
    let collection = wiki_client.wiki_collection(tenant, token).await?;

    let where_filter = Where::conjunction([
        Where::Metadata(MetadataExpression {
            key: "slug".to_string(),
            comparison: MetadataComparison::Set(
                SetOperator::In,
                MetadataSetValue::Str(slugs.to_vec()),
            ),
        }),
        Where::Metadata(MetadataExpression {
            key: "chunk_id".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Int(0),
            ),
        }),
    ]);

    // No rank; metadata only (we need title/categories, not chunk content).
    let payload = SearchPayload::default()
        .r#where(where_filter)
        .limit(None, 0)
        .select([Key::Metadata]);

    let response = collection
        .search(vec![payload])
        .await
        .map_err(ReadPageError::Query)?;

    Ok(page_metadata_from_response(
        response,
        server.config.foundation.foundation_ui_origin.as_deref(),
        tenant,
    ))
}

/// Maps the chunk-0 records of a batch metadata search into [`PageMetadata`],
/// stamping each `url` from `origin`. The per-field outer `Vec` is indexed by
/// payload; we send exactly one payload, so we read row 0. Records with no
/// `slug` are dropped. Pure (no I/O) so it is unit-testable.
fn page_metadata_from_response(
    response: SearchResponse,
    origin: Option<&str>,
    tenant: &str,
) -> Vec<PageMetadata> {
    response
        .metadatas
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .filter_map(|meta| {
            let slug = meta_str(&meta, "slug")?;
            let title = meta_str(&meta, "title").unwrap_or_else(|| slug.clone());
            let categories = meta_str_array(&meta, "categories");
            let url = origin.and_then(|origin| page_redirect_url(origin, tenant, &slug));
            Some(PageMetadata {
                slug,
                title,
                categories,
                url,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::Metadata;

    /// A chunk-0 metadata record carrying the per-page fields, plus `slug`.
    fn page_meta(slug: &str) -> Metadata {
        let mut meta = Metadata::new();
        meta.insert("chunk_id".to_string(), MetadataValue::Int(0));
        meta.insert("slug".to_string(), MetadataValue::Str(slug.to_string()));
        meta.insert(
            "title".to_string(),
            MetadataValue::Str("My Page".to_string()),
        );
        meta.insert(
            "categories".to_string(),
            MetadataValue::StringArray(vec!["eng".to_string()]),
        );
        meta
    }

    /// A chunk-0 record with no `slug` — dropped by the mapping.
    fn slugless_meta() -> Metadata {
        let mut meta = Metadata::new();
        meta.insert("chunk_id".to_string(), MetadataValue::Int(0));
        meta.insert(
            "title".to_string(),
            MetadataValue::Str("Orphan".to_string()),
        );
        meta
    }

    #[test]
    fn page_metadata_from_response_maps_rows_stamps_urls_and_drops_slugless() {
        let response = SearchResponse {
            ids: vec![vec!["alpha-0".to_string(), "beta-0".to_string()]],
            documents: vec![None],
            embeddings: vec![None],
            // The second record carries no `slug`, so it is dropped.
            metadatas: vec![Some(vec![Some(page_meta("alpha")), Some(slugless_meta())])],
            scores: vec![None],
            select: vec![vec![]],
        };

        let pages =
            page_metadata_from_response(response, Some("https://wiki.example.com"), "tenant-1");

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].slug, "alpha");
        assert_eq!(pages[0].title, "My Page");
        assert_eq!(pages[0].categories, vec!["eng".to_string()]);
        assert_eq!(
            pages[0].url.as_deref(),
            Some("https://wiki.example.com/~/page-redirect?tenant_uuid=tenant-1&slug=alpha")
        );
    }

    #[test]
    fn page_metadata_from_response_leaves_url_none_without_origin() {
        let response = SearchResponse {
            ids: vec![vec!["alpha-0".to_string()]],
            documents: vec![None],
            embeddings: vec![None],
            metadatas: vec![Some(vec![Some(page_meta("alpha"))])],
            scores: vec![None],
            select: vec![vec![]],
        };

        let pages = page_metadata_from_response(response, None, "tenant-1");
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].url, None);
    }
}
