//! `POST /api/upsert-page` — replace a wiki page's chunks.
//!
//! Acts as a Chroma *client*: it resolves the tenant's `wiki` collection through
//! the proxying [`WikiClient`], reads the `{slug}-0` chunk to learn whether the
//! page exists (and to preserve `created_at` / bump `version`), deletes every
//! chunk for the slug on update, re-chunks the new content, computes SPLADE
//! sparse vectors with the caller's token, and re-adds the chunks in batches —
//! letting the collection's schema-bound Qwen EF produce the dense vectors on
//! `add`. The FE enforces auth, quota, metering, and billing on every proxied
//! call.
//!
//! The replace is currently a non-atomic delete-then-add. Once the data plane
//! exposes an atomic put-if-absent / put-if-none (compare-and-set) op, the
//! delete-then-add below will be replaced with it to close the partial-write
//! window and the read-then-write race on a slug.

use super::whoami::whoami_and_authorize;
use crate::wiki::chunking::{chunk_content, chunk_id_for, title_from_content, ChunkingConfig};
use crate::wiki::client::is_not_found;
use crate::wiki::embed::WikiEmbedder;
use crate::wiki::page::{build_metadatas, kind_for};
use crate::wiki::{WikiClient, WikiClientError};
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::{extract::State, http::HeaderMap, Json};
use chroma::client::ChromaHttpClientError;
use chroma_error::{ChromaError, ChromaValidationError, ErrorCodes};
use chroma_types::{
    Include, IncludeList, Metadata, MetadataComparison, MetadataExpression, MetadataValue,
    PrimitiveOperator, Where,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use validator::{Validate, ValidationError};

/// HTTP header carrying the caller's Chroma Cloud token, forwarded to the FE on
/// every proxied call so authz/quota/billing key off the user.
const CHROMA_TOKEN_HEADER: &str = "x-chroma-token";

/// Max records per `add` request. Chroma Cloud's embedding service rejects
/// calls with more than this many docs, and large pages can exceed it, so adds
/// are sliced into batches of this size.
const ADD_BATCH_SIZE: usize = 100;

/// `^(?:[a-z0-9][a-z0-9-]*|category:[a-z0-9][a-z0-9-]*|)$` — the wiki slug
/// shape (empty root, lowercase alnum/hyphen, or a `category:<slug>`). The
/// empty alternative makes the root slug valid.
static SLUG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:[a-z0-9][a-z0-9-]*|category:[a-z0-9][a-z0-9-]*|)$")
        .expect("the wiki slug regex should be valid")
});

/// `^[a-z0-9][a-z0-9-]*$` — the category-name shape.
static CATEGORY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-z0-9][a-z0-9-]*$").expect("the category-name regex should be valid")
});

/// Request body for `POST /api/upsert-page`.
#[derive(Debug, Deserialize, Validate)]
pub struct UpsertPageRequest {
    /// Page slug. Empty string is the wiki root; otherwise lowercase
    /// alnum/hyphen, or `category:<slug>`.
    #[validate(custom(function = "validate_slug"))]
    pub slug: String,
    /// Full markdown content. The first non-blank line is the title.
    #[validate(length(min = 1, message = "content must not be empty"))]
    pub content: String,
    /// Citation ids (`<collection>:<record_id>`) for every source whose
    /// information appears in `content`. May be empty.
    #[validate(custom(function = "validate_source_ids"))]
    pub source_ids: Vec<String>,
    /// Optional category tags (lowercase alnum/hyphen); deduplicated and
    /// sorted before they are stamped onto the page.
    #[serde(default)]
    #[validate(custom(function = "validate_categories"))]
    pub categories: Vec<String>,
    /// Optional caller-supplied reason for this change. Deliberately not
    /// persisted on the page: the text can be large and may carry sensitive
    /// context, so we only log that a reason was supplied (not its content) as
    /// an audit signal.
    #[validate(length(max = 350, message = "reason must be at most 250 characters"))]
    pub reason: Option<String>,
    /// The version the caller expects to be replacing: the page's current
    /// `version` on an update, or `0` when the caller expects to create a new
    /// page.
    ///
    /// Required so clients adopt the optimistic-concurrency contract now, but
    /// intentionally *not* enforced yet. The replace is a non-atomic
    /// delete-then-add (see the module docs), so comparing here would be a racy
    /// check-then-act that gives false confidence. Once the data plane exposes
    /// an atomic compare-and-set, this becomes the precondition and a mismatch
    /// returns `409 Conflict`; until then it is accepted and ignored.
    pub expected_version: u32,
}

/// Response body for `POST /api/upsert-page`.
#[derive(Debug, Serialize)]
pub struct UpsertPageResponse {
    /// The page slug that was written.
    pub slug: String,
    /// `"added"` for a new page or `"updated"` for an existing one.
    pub op: String,
    /// Monotonic page version (1 for a new page, previous + 1 on update).
    pub version: u32,
    /// Page creation time, unix seconds.
    pub created_at: i64,
    /// Last write time, unix seconds.
    pub updated_at: i64,
    /// Number of chunks written for the page.
    pub num_chunks: usize,
}

/// Errors raised while running the upsert-page replace flow (after validation).
#[derive(Debug, thiserror::Error)]
pub enum UpsertPageError {
    /// `frontend_ingress_url` is unset, so the wiki client was never built.
    #[error("wiki record I/O is not configured")]
    RouteDisabled,
    /// The caller's request carried no usable `x-chroma-token`.
    #[error("missing or invalid {CHROMA_TOKEN_HEADER} header")]
    MissingToken,
    /// Resolving the wiki collection through the proxy failed.
    #[error(transparent)]
    Resolve(#[from] WikiClientError),
    /// Computing SPLADE sparse embeddings failed.
    #[error(transparent)]
    Embed(#[from] crate::wiki::embed::WikiEmbedError),
    /// A proxied record-I/O call (`get`/`delete`/`add`) to the FE failed.
    #[error("chroma record I/O failed: {0}")]
    RecordIo(ChromaHttpClientError),
}

impl ChromaError for UpsertPageError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpsertPageError::RouteDisabled => ErrorCodes::Internal,
            UpsertPageError::MissingToken => ErrorCodes::InvalidArgument,
            UpsertPageError::Resolve(err) => err.code(),
            UpsertPageError::Embed(err) => err.code(),
            UpsertPageError::RecordIo(_) => ErrorCodes::Internal,
        }
    }
}

/// `POST /api/upsert-page` handler.
pub async fn foundation_upsert_page(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<UpsertPageRequest>,
) -> Result<Json<UpsertPageResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;

    let _guard =
        server.scorecard_request(&["op:foundation_upsert_page", &format!("tenant:{tenant}")])?;

    request.validate().map_err(ChromaValidationError::from)?;
    let categories = normalize_categories(&request.categories);

    let response = upsert_page(&server, &headers, &tenant, &request, &categories).await?;
    Ok(Json(response))
}

/// Runs the replace flow against the proxied wiki collection.
async fn upsert_page(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    request: &UpsertPageRequest,
    categories: &[String],
) -> Result<UpsertPageResponse, UpsertPageError> {
    let slug = request.slug.as_str();
    let wiki_client = server
        .wiki_client
        .as_ref()
        .ok_or(UpsertPageError::RouteDisabled)?;
    let token = chroma_token(headers)?;

    // Resolve (cache-first) the wiki collection identity, then derive the
    // chunker from its metadata so writes match how the collection was created.
    let collection = wiki_client.wiki_collection(tenant, token).await?;
    let chunking = ChunkingConfig::from_collection_metadata(collection.metadata().as_ref());

    // Read chunk-0 to learn page existence + preserve created_at / bump version.
    let chunk0 = chunk_id_for(slug, 0);
    let existing = record_op(
        wiki_client,
        tenant,
        collection.get(
            Some(vec![chunk0]),
            None,
            None,
            None,
            Some(IncludeList(vec![Include::Metadata])),
        ),
    )
    .await?;
    // Page existence is driven by chunk-0's *presence* (its id is always
    // returned), not by whether it carries metadata: a chunk-0 row with no
    // metadata still means the page exists and its other chunks must be
    // cleared. Reading metadata separately lets the recovery branch below fire.
    let exists = !existing.ids.is_empty();
    let existing_meta = existing
        .metadatas
        .and_then(|metas| metas.into_iter().next().flatten());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0);
    let (op, created_at, version) = match (exists, &existing_meta) {
        (true, Some(meta)) => {
            // chunk-0 exists, so the page exists; an absent or non-integer
            // created_at / version means corrupt or pre-versioning metadata.
            // Recover (so the upsert still succeeds) but warn — this should
            // never happen for a page foundation-api wrote.
            let created_at = match meta.get("created_at").and_then(|v| i64::try_from(v).ok()) {
                Some(created_at) => created_at,
                None => {
                    tracing::warn!(
                        slug = %slug,
                        "existing wiki page has no integer created_at; using current time"
                    );
                    now
                }
            };
            let version = match meta.get("version").and_then(|v| i64::try_from(v).ok()) {
                Some(version) => version + 1,
                None => {
                    tracing::warn!(
                        slug = %slug,
                        "existing wiki page has no integer version; resetting to 1"
                    );
                    1
                }
            };
            ("updated", created_at, version)
        }
        (true, None) => {
            // chunk-0 exists but carries no metadata at all (corrupt or
            // pre-versioning row). Treat it as an update so the delete-by-slug
            // still clears any orphan chunks, but reset created_at / version
            // since there is nothing to recover.
            tracing::warn!(
                slug = %slug,
                "existing wiki chunk-0 has no metadata; treating as update with reset created_at/version"
            );
            ("updated", now, 1)
        }
        (false, _) => ("added", now, 1),
    };

    // Re-chunk + compute sparse vectors (dense is auto-embedded on `add`).
    let chunks = chunk_content(slug, &request.content, &chunking);
    let documents: Vec<&str> = chunks.iter().map(|chunk| chunk.text.as_str()).collect();
    let sparse = WikiEmbedder::new(None)
        .embed_sparse(token, &documents)
        .await?;

    let metadatas = build_metadatas(
        slug,
        &chunks,
        sparse,
        kind_for(slug),
        &title_from_content(&request.content),
        created_at,
        now,
        version,
        categories,
        &request.source_ids,
    );

    // delete-then-add is non-atomic (see the module docstring for the planned
    // compare-and-set replacement): a mid-flight failure can leave the page
    // partially removed, and there is no fencing against a concurrent upsert of
    // the same slug. We only delete when the page already exists so a brand-new
    // page is never briefly absent.
    if exists {
        let where_slug = Where::Metadata(MetadataExpression {
            key: "slug".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(slug.to_string()),
            ),
        });
        record_op(
            wiki_client,
            tenant,
            collection.delete(None, Some(where_slug), None),
        )
        .await?;
    }

    let ids: Vec<String> = chunks.iter().map(|chunk| chunk.id.clone()).collect();
    let num_chunks = ids.len();
    for start in (0..num_chunks).step_by(ADD_BATCH_SIZE) {
        let end = (start + ADD_BATCH_SIZE).min(num_chunks);
        let id_batch = ids[start..end].to_vec();
        let doc_batch: Vec<Option<String>> = chunks[start..end]
            .iter()
            .map(|chunk| Some(chunk.text.clone()))
            .collect();
        let meta_batch: Vec<Option<Metadata>> =
            metadatas[start..end].iter().cloned().map(Some).collect();
        record_op(
            wiki_client,
            tenant,
            // `embeddings = None` lets the collection's schema-bound Qwen EF
            // embed the documents on the FE using the forwarded token.
            collection.add(
                id_batch,
                None::<Vec<Vec<f32>>>,
                Some(doc_batch),
                None,
                Some(meta_batch),
            ),
        )
        .await?;
    }

    // `request.expected_version` is intentionally not checked here yet (see its
    // field docs): enforcing it on the non-atomic flow above would be racy.
    // `reason` is logged as a presence flag only — never its content, which can
    // be large and may leak sensitive context.
    tracing::info!(
        slug = %slug,
        op,
        version,
        num_chunks,
        expected_version = request.expected_version,
        reason_provided = request.reason.is_some(),
        "wiki upsert-page complete"
    );

    Ok(UpsertPageResponse {
        slug: slug.to_string(),
        op: op.to_string(),
        version: version as u32,
        created_at,
        updated_at: now,
        num_chunks,
    })
}

/// Awaits a proxied record-I/O call, invalidating the tenant's cached
/// collection identity on a `NotFound` (the cached id is stale because the
/// collection was recreated/forked).
///
/// This request still fails; the next one re-resolves the name to the new id.
/// We don't transparently retry in-request because a stale id is rare (it
/// requires the collection to be recreated within the cache TTL) and a `delete`
/// against the dead id 404s before any `add`, so failing can't half-write the
/// live collection — a client retry is cheaper than an in-request re-resolve +
/// re-embed.
async fn record_op<T, F>(
    wiki_client: &WikiClient,
    tenant: &str,
    fut: F,
) -> Result<T, UpsertPageError>
where
    F: Future<Output = Result<T, ChromaHttpClientError>>,
{
    fut.await.map_err(|err| {
        if is_not_found(&err) {
            wiki_client.invalidate(tenant);
        }
        UpsertPageError::RecordIo(err)
    })
}

/// Extracts the caller's Chroma token from the request headers.
fn chroma_token(headers: &HeaderMap) -> Result<&str, UpsertPageError> {
    headers
        .get(CHROMA_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|token| !token.is_empty())
        .ok_or(UpsertPageError::MissingToken)
}

/// Validates the slug against [`SLUG_RE`].
fn validate_slug(slug: &str) -> Result<(), ValidationError> {
    if SLUG_RE.is_match(slug) {
        Ok(())
    } else {
        Err(ValidationError::new("slug").with_message(
            format!("invalid slug '{slug}': must be lowercase alnum/hyphen, or 'category:<slug>'")
                .into(),
        ))
    }
}

/// Validates each source id is `<collection>:<record_id>` with a non-empty
/// collection. The record id (after the first `:`) may be empty (the wiki
/// root's citation id).
fn validate_source_ids(source_ids: &[String]) -> Result<(), ValidationError> {
    for source_id in source_ids {
        match source_id.split_once(':') {
            Some((collection, _record_id)) if !collection.is_empty() => {}
            _ => {
                return Err(ValidationError::new("source_ids").with_message(
                    format!(
                    "invalid source id '{source_id}': expected format '<collection>:<record_id>'"
                )
                    .into(),
                ))
            }
        }
    }
    Ok(())
}

/// Validates each category against [`CATEGORY_RE`].
fn validate_categories(categories: &[String]) -> Result<(), ValidationError> {
    for category in categories {
        if !CATEGORY_RE.is_match(category) {
            return Err(ValidationError::new("categories").with_message(
                format!("invalid category name '{category}': must be lowercase alnum/hyphen")
                    .into(),
            ));
        }
    }
    Ok(())
}

/// Deduplicates and lexicographically sorts the (already-validated) category
/// list.
fn normalize_categories(categories: &[String]) -> Vec<String> {
    categories
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        slug: &str,
        content: &str,
        source_ids: &[&str],
        categories: &[&str],
    ) -> UpsertPageRequest {
        UpsertPageRequest {
            slug: slug.to_string(),
            content: content.to_string(),
            source_ids: source_ids.iter().map(|s| s.to_string()).collect(),
            categories: categories.iter().map(|s| s.to_string()).collect(),
            reason: None,
            expected_version: 0,
        }
    }

    #[test]
    fn validate_slug_accepts_root_and_well_formed_slugs() {
        validate_slug("").unwrap();
        validate_slug("foo").unwrap();
        validate_slug("foo-bar-1").unwrap();
        validate_slug("category:foo-bar").unwrap();
        validate_slug("0").unwrap();
    }

    #[test]
    fn validate_slug_rejects_malformed_slugs() {
        for slug in [
            "Foo",          // uppercase
            "foo_bar",      // underscore
            "-foo",         // leading hyphen
            "foo:bar",      // colon outside category prefix
            "category:",    // empty category body
            "category:Foo", // uppercase category body
            " foo",         // leading space
        ] {
            assert!(
                validate_slug(slug).is_err(),
                "expected {slug:?} to be rejected"
            );
        }
    }

    #[test]
    fn validate_source_ids_checks_collection_and_separator() {
        validate_source_ids(&[]).unwrap();
        validate_source_ids(&["slack_master:C094_177.992".to_string()]).unwrap();
        // Empty record id is allowed (root page citation id).
        validate_source_ids(&["wiki_master:".to_string()]).unwrap();

        assert!(validate_source_ids(&["norecord".to_string()]).is_err());
        assert!(validate_source_ids(&[":record".to_string()]).is_err());
    }

    #[test]
    fn validate_categories_rejects_malformed_names() {
        validate_categories(&[]).unwrap();
        validate_categories(&["foo".to_string(), "bar-1".to_string()]).unwrap();
        assert!(validate_categories(&["Archive".to_string()]).is_err());
    }

    #[test]
    fn normalize_categories_dedups_and_sorts() {
        assert_eq!(
            normalize_categories(&["b".to_string(), "a".to_string(), "a".to_string()]),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(normalize_categories(&[]), Vec::<String>::new());
    }

    #[test]
    fn request_validate_rejects_empty_content() {
        assert!(request("foo", "", &["c:r"], &[]).validate().is_err());
    }

    #[test]
    fn request_validate_rejects_bad_fields() {
        assert!(request("Foo", "body", &[], &[]).validate().is_err());
        assert!(request("foo", "body", &["norecord"], &[])
            .validate()
            .is_err());
        assert!(request("foo", "body", &[], &["Archive"])
            .validate()
            .is_err());
    }

    #[test]
    fn request_validate_accepts_well_formed_request() {
        request("foo", "# Title\n\nBody", &["slack_master:abc"], &["z", "a"])
            .validate()
            .unwrap();
    }

    #[test]
    fn request_validate_bounds_reason_length() {
        let mut req = request("foo", "body", &[], &[]);
        req.reason = Some("a".repeat(2000));
        req.validate().unwrap();

        req.reason = Some("a".repeat(2001));
        assert!(req.validate().is_err());
    }
}
