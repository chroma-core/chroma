//! `POST /api/read-page` — reconstruct a single wiki page in full from its
//! chunks, keyed by `slug`.
//!
//! Search returns chunk-level hits; this route reassembles a whole page: fetch
//! every chunk for the slug, order them by `chunk_id`, and join their
//! documents. The per-page metadata (title, categories, …) is stamped
//! identically on every chunk, so it is read off the head chunk. Like the other
//! wiki routes it proxies to the FE through
//! [`WikiClient`](crate::wiki::WikiClient), which enforces auth, quota,
//! metering, and billing.

use crate::routes::links::page_redirect_url;
use crate::routes::{caller_token, whoami::whoami_and_authorize};
use crate::wiki::page::{meta_int, meta_str, meta_str_array};
use crate::wiki::WikiClientError;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::{extract::State, http::HeaderMap, Json};
use chroma::client::ChromaHttpClientError;
use chroma::types::{Key, SearchPayload, SearchResponse};
use chroma::ChromaCollection;
use chroma_error::{ChromaError, ChromaValidationError, ErrorCodes};
use chroma_types::{
    Metadata, MetadataComparison, MetadataExpression, MetadataValue, PrimitiveOperator, Where,
};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Request body for `POST /api/read-page`.
#[derive(Debug, Deserialize, Validate)]
pub struct ReadPageRequest {
    /// Slug of the wiki page to reconstruct in full.
    #[validate(length(min = 1, message = "slug must not be empty"))]
    pub slug: String,
}

/// A full wiki page reconstructed from its chunks, returned by the `read_page`
/// tool and `POST /api/read-page`. Carries only the fields the agent needs.
///
/// `url` is a redirect link (built from the tenant UUID and slug) that the
/// configured web origin resolves to the page.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FoundationPage {
    pub slug: String,
    pub title: String,
    pub categories: Vec<String>,
    pub updated_at: Option<i64>,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Errors raised while reconstructing a wiki page (after validation).
#[derive(Debug, thiserror::Error)]
pub enum ReadPageError {
    /// `frontend_ingress_url` is unset, so the wiki client was never built.
    #[error("wiki read is not configured")]
    RouteDisabled,
    /// The caller's request carried no usable `x-chroma-token`.
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    /// Resolving the wiki collection through the proxy failed.
    #[error(transparent)]
    Resolve(#[from] WikiClientError),
    /// The proxied `/search` call to the FE failed.
    #[error("chroma search failed: {0}")]
    Query(ChromaHttpClientError),
    /// No chunks exist for the requested page slug.
    #[error("page not found")]
    PageNotFound,
}

impl ChromaError for ReadPageError {
    fn code(&self) -> ErrorCodes {
        match self {
            ReadPageError::RouteDisabled => ErrorCodes::Internal,
            ReadPageError::MissingToken => ErrorCodes::InvalidArgument,
            ReadPageError::Resolve(err) => err.code(),
            ReadPageError::Query(_) => ErrorCodes::Internal,
            ReadPageError::PageNotFound => ErrorCodes::NotFound,
        }
    }
}

/// `POST /api/read-page` handler. Reconstructs a single wiki page in full from
/// its chunks, keyed by `slug`. Returns 404 when the page does not exist.
pub async fn foundation_read_page(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<ReadPageRequest>,
) -> Result<Json<FoundationPage>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;

    let _guard =
        server.scorecard_request(&["op:foundation_read_page", &format!("tenant:{tenant}")])?;

    request.validate().map_err(ChromaValidationError::from)?;

    let page = run_read_page(&server, &headers, &tenant, &request.slug)
        .await?
        .ok_or(ReadPageError::PageNotFound)?;
    Ok(Json(page))
}

/// Resolves the wiki collection and reconstructs the full page for `slug`,
/// returning `None` when no such page exists. Stamps the page's `url` from the
/// configured `foundation_ui_origin` (left `None` when the origin is unset).
pub(crate) async fn run_read_page(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    slug: &str,
) -> Result<Option<FoundationPage>, ReadPageError> {
    let wiki_client = server
        .wiki_client
        .as_ref()
        .ok_or(ReadPageError::RouteDisabled)?;
    let token = caller_token(headers).ok_or(ReadPageError::MissingToken)?;
    let collection = wiki_client.wiki_collection(tenant, token).await?;

    let mut page = read_full_page(&collection, slug).await?;
    if let (Some(page), Some(origin)) = (&mut page, &server.config.foundation.foundation_ui_origin)
    {
        page.url = page_redirect_url(origin, tenant, &page.slug);
    }
    Ok(page)
}

/// Reconstructs a single page by fetching all chunks for `slug`, ordering them
/// by `chunk_id`, and joining their documents. Returns `None` when the slug has
/// no chunks.
///
/// This is a filter-only `search` (no rank) rather than a `get`: the two go
/// through different authorization paths on the FE, and the MCP token is
/// granted the `search` capability — `get` comes back 403.
async fn read_full_page(
    collection: &ChromaCollection,
    slug: &str,
) -> Result<Option<FoundationPage>, ReadPageError> {
    let where_slug = Where::Metadata(MetadataExpression {
        key: "slug".to_string(),
        comparison: MetadataComparison::Primitive(
            PrimitiveOperator::Equal,
            MetadataValue::Str(slug.to_string()),
        ),
    });

    // No rank and no limit: this is a pure metadata filter that returns every
    // chunk of the page, which `assemble_page` then orders by `chunk_id`.
    let payload = SearchPayload::default()
        .r#where(where_slug)
        .limit(None, 0)
        .select([Key::Document, Key::Metadata]);

    let response = collection
        .search(vec![payload])
        .await
        .map_err(ReadPageError::Query)?;

    Ok(assemble_page(slug, chunks_from_response(response)))
}

/// Flattens a single-payload [`SearchResponse`] into `(document, metadata)`
/// chunk pairs. The per-field outer `Vec` is indexed by payload; we send exactly
/// one payload, so we read row 0. Chunks whose metadata is absent are dropped
/// (assembly keys everything off metadata). Pure (no I/O) so it is unit-testable.
fn chunks_from_response(response: SearchResponse) -> Vec<(String, Metadata)> {
    let documents = response
        .documents
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default();
    let metadatas = response
        .metadatas
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default();

    documents
        .into_iter()
        .zip(metadatas)
        .filter_map(|(doc, meta)| Some((doc.unwrap_or_default(), meta?)))
        .collect()
}

/// Orders the chunks of a single page by `chunk_id`, joins their documents into
/// the full markdown, and reads the per-page metadata off the head chunk. Pure
/// (no I/O) so the ordering/join behaviour is unit-testable. Returns `None`
/// when there are no chunks.
fn assemble_page(slug: &str, mut chunks: Vec<(String, Metadata)>) -> Option<FoundationPage> {
    if chunks.is_empty() {
        return None;
    }

    chunks.sort_by_key(|(_, meta)| meta_int(meta, "chunk_id").unwrap_or(0));

    let content: String = chunks.iter().map(|(doc, _)| doc.as_str()).collect();
    let head = &chunks[0].1;

    Some(FoundationPage {
        slug: slug.to_string(),
        title: meta_str(head, "title").unwrap_or_else(|| slug.to_string()),
        categories: meta_str_array(head, "categories"),
        updated_at: meta_int(head, "updated_at"),
        content,
        url: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk_meta(chunk_id: i64) -> Metadata {
        let mut meta = Metadata::new();
        meta.insert("chunk_id".to_string(), MetadataValue::Int(chunk_id));
        meta.insert(
            "title".to_string(),
            MetadataValue::Str("My Page".to_string()),
        );
        meta.insert("updated_at".to_string(), MetadataValue::Int(1700));
        meta.insert(
            "categories".to_string(),
            MetadataValue::StringArray(vec!["eng".to_string()]),
        );
        meta
    }

    #[test]
    fn assemble_page_orders_chunks_and_joins_content() {
        // Deliberately out of order: assembly must sort by chunk_id before join.
        let chunks = vec![
            ("world".to_string(), chunk_meta(1)),
            ("hello ".to_string(), chunk_meta(0)),
        ];

        let page = assemble_page("my-page", chunks).expect("page");

        assert_eq!(page.slug, "my-page");
        assert_eq!(page.title, "My Page");
        assert_eq!(page.categories, vec!["eng".to_string()]);
        assert_eq!(page.updated_at, Some(1700));
        // Chunks are joined with no separator.
        assert_eq!(page.content, "hello world");
    }

    #[test]
    fn assemble_page_falls_back_to_slug_when_title_missing() {
        let mut meta = Metadata::new();
        meta.insert("chunk_id".to_string(), MetadataValue::Int(0));
        let page = assemble_page("orphan", vec![("body".to_string(), meta)]).expect("page");

        assert_eq!(page.title, "orphan");
        assert!(page.categories.is_empty());
        assert_eq!(page.updated_at, None);
    }

    #[test]
    fn assemble_page_returns_none_without_chunks() {
        assert!(assemble_page("empty", Vec::new()).is_none());
    }

    #[test]
    fn chunks_from_response_reads_row_zero_and_drops_metaless_chunks() {
        let response = SearchResponse {
            ids: vec![vec!["a-0".to_string(), "a-1".to_string()]],
            documents: vec![Some(vec![
                Some("first".to_string()),
                Some("second".to_string()),
            ])],
            embeddings: vec![None],
            // Second chunk has no metadata, so it is dropped.
            metadatas: vec![Some(vec![Some(chunk_meta(0)), None])],
            scores: vec![None],
            select: vec![vec![]],
        };

        let chunks = chunks_from_response(response);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, "first");
    }

    #[test]
    fn chunks_from_response_empty_yields_no_chunks() {
        let response = SearchResponse {
            ids: vec![],
            documents: vec![],
            embeddings: vec![],
            metadatas: vec![],
            scores: vec![],
            select: vec![],
        };
        assert!(chunks_from_response(response).is_empty());
    }

    #[test]
    fn read_page_request_rejects_empty_slug() {
        let valid: ReadPageRequest =
            serde_json::from_value(serde_json::json!({ "slug": "my-page" })).expect("deserialize");
        assert!(valid.validate().is_ok());

        let empty: ReadPageRequest =
            serde_json::from_value(serde_json::json!({ "slug": "" })).expect("deserialize");
        assert!(empty.validate().is_err());
    }

    #[test]
    fn read_page_error_maps_to_http_codes() {
        assert_eq!(ReadPageError::PageNotFound.code(), ErrorCodes::NotFound);
        assert_eq!(
            ReadPageError::MissingToken.code(),
            ErrorCodes::InvalidArgument
        );
        assert_eq!(ReadPageError::RouteDisabled.code(), ErrorCodes::Internal);
    }
}
