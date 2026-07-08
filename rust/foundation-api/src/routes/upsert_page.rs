//! `POST /api/upsert-page` — replace a wiki page's chunks.
//!
//! Acts as a Chroma *client*: it resolves the tenant's `wiki` collection through
//! the proxying Foundation Chroma client, transactionally reads the existing
//! chunk set for the slug to preserve `created_at` / bump `version`, re-chunks
//! the new content, computes dense Qwen and SPLADE sparse vectors with the
//! caller's token, and atomically upserts the replacement chunks plus deletes
//! any stale chunk ids. The FE enforces auth, quota, metering, and billing on
//! every proxied call.

use crate::foundation_chroma::{is_not_found, FoundationChromaClient};
use crate::routes::{caller_token, whoami::whoami_and_authorize};
use crate::wiki::chunking::{chunk_content, title_from_content, ChunkRecordId, ChunkingConfig};
use crate::wiki::embed::WikiEmbedder;
use crate::wiki::page::{build_metadatas, kind_for};
use crate::wiki::WikiClientError;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::{extract::State, http::HeaderMap, Json};
use chroma::client::ChromaHttpClientError;
use chroma::ChromaCollection;
use chroma_error::{ChromaError, ChromaValidationError, ErrorCodes};
use chroma_types::{
    GetResponse, Include, IncludeList, Metadata, MetadataComparison, MetadataExpression,
    MetadataValue, PrimitiveOperator, UpdateMetadata, Where,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use validator::{Validate, ValidationError};

/// Max docs per dense embedding request. The replacement page is still staged
/// as one transactional `upsert`; this only keeps Qwen embedding calls bounded
/// and does not reduce the final commit payload size.
const DENSE_EMBED_BATCH_SIZE: usize = 100;

/// Max record writes in one conditional transaction. Replacement chunks are
/// upsert records; stale chunks are delete records.
const MAX_TRANSACTION_WRITES: usize = 300;

/// Read one past the write cap so an oversized existing page is detected
/// without reading arbitrarily many ids.
const PAGE_CHUNK_READ_LIMIT: u32 = MAX_TRANSACTION_WRITES as u32 + 1;

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
    #[validate(length(max = 350, message = "reason must be at most 350 characters"))]
    pub reason: Option<String>,
    /// The version the caller expects to be replacing: the page's current
    /// `version` on an update, or `0` when the caller expects to create a new
    /// page.
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
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    /// Resolving the wiki collection through the proxy failed.
    #[error(transparent)]
    Resolve(#[from] WikiClientError),
    /// Computing SPLADE sparse embeddings failed.
    #[error(transparent)]
    Embed(#[from] crate::wiki::embed::WikiEmbedError),
    /// Computing dense Qwen embeddings failed.
    #[error("dense wiki embedding failed: {0}")]
    DenseEmbed(ChromaHttpClientError),
    /// A proxied record-I/O call (`get`/`upsert`/`delete`/`commit`) to the FE failed.
    #[error("chroma record I/O failed: {0}")]
    RecordIo(ChromaHttpClientError),
    /// The caller edited a stale page version.
    #[error("page version conflict for '{slug}': expected {expected}, current {actual}")]
    VersionConflict {
        slug: String,
        expected: u32,
        actual: u32,
    },
    /// The replacement would exceed the transaction write cap.
    #[error(
        "wiki upsert would write at least {writes} records; transaction write limit is {limit}"
    )]
    TooManyTransactionWrites { writes: usize, limit: usize },
    /// The page version cannot be incremented.
    #[error("page version for '{slug}' is too large to increment")]
    VersionOverflow { slug: String },
}

impl ChromaError for UpsertPageError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpsertPageError::RouteDisabled => ErrorCodes::Internal,
            UpsertPageError::MissingToken => ErrorCodes::InvalidArgument,
            UpsertPageError::Resolve(err) => err.code(),
            UpsertPageError::Embed(err) => err.code(),
            UpsertPageError::DenseEmbed(_) => ErrorCodes::Internal,
            UpsertPageError::RecordIo(err) if is_conditional_conflict(err) => {
                ErrorCodes::FailedPrecondition
            }
            UpsertPageError::RecordIo(_) => ErrorCodes::Internal,
            UpsertPageError::VersionConflict { .. } => ErrorCodes::FailedPrecondition,
            UpsertPageError::TooManyTransactionWrites { .. } => ErrorCodes::ResourceExhausted,
            UpsertPageError::VersionOverflow { .. } => ErrorCodes::FailedPrecondition,
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
        .foundation_chroma_client
        .as_ref()
        .ok_or(UpsertPageError::RouteDisabled)?;
    let token = caller_token(headers).ok_or(UpsertPageError::MissingToken)?;

    // Resolve (cache-first) the wiki collection identity, then derive the
    // chunker from its metadata so writes match how the collection was created.
    let collection = wiki_client.wiki_collection(tenant, token).await?;
    let chunking = ChunkingConfig::from_collection_metadata(collection.metadata().as_ref());

    let mut txn = collection.conditional();
    let chunk0 = ChunkRecordId::new(slug, 0).to_string();
    record_op(
        wiki_client,
        tenant,
        txn.get(
            Some(vec![chunk0]),
            None,
            None,
            None,
            Some(IncludeList(vec![Include::Metadata])),
        ),
    )
    .await?;

    let existing = record_op(
        wiki_client,
        tenant,
        txn.get(
            None,
            Some(where_slug(slug)),
            Some(PAGE_CHUNK_READ_LIMIT),
            None,
            Some(IncludeList(vec![Include::Metadata])),
        ),
    )
    .await?;
    if existing.ids.len() > MAX_TRANSACTION_WRITES {
        return Err(UpsertPageError::TooManyTransactionWrites {
            writes: existing.ids.len(),
            limit: MAX_TRANSACTION_WRITES,
        });
    }

    let existing_ids = existing.ids.clone();
    let exists = !existing_ids.is_empty();
    let existing_meta = head_metadata(&existing);
    let actual_version = current_version(exists, existing_meta.as_ref());
    if request.expected_version != actual_version {
        return Err(UpsertPageError::VersionConflict {
            slug: slug.to_string(),
            expected: request.expected_version,
            actual: actual_version,
        });
    }

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
            let version =
                actual_version
                    .checked_add(1)
                    .ok_or_else(|| UpsertPageError::VersionOverflow {
                        slug: slug.to_string(),
                    })?;
            if meta_version(meta).is_none() {
                tracing::warn!(
                    slug = %slug,
                    "existing wiki page has no integer version; resetting to 1"
                );
            }
            ("updated", created_at, version)
        }
        (true, None) => {
            // chunk-0 exists but carries no metadata at all (corrupt or
            // pre-versioning row). Treat it as an update so stale chunks still
            // get cleared by id, but reset created_at / version since there is
            // nothing to recover.
            tracing::warn!(
                slug = %slug,
                "existing wiki chunk-0 has no metadata; treating as update with reset created_at/version"
            );
            ("updated", now, 1)
        }
        (false, _) => ("added", now, 1),
    };

    // Re-chunk and determine transaction size before doing embedding work.
    let chunks = chunk_content(slug, &request.content, &chunking);
    let ids: Vec<String> = chunks.iter().map(|chunk| chunk.id.clone()).collect();
    let num_chunks = ids.len();
    let replacement_ids: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let stale_ids: Vec<String> = existing_ids
        .into_iter()
        .filter(|id| !replacement_ids.contains(id.as_str()))
        .collect();
    enforce_transaction_write_limit(num_chunks, stale_ids.len())?;

    let documents: Vec<&str> = chunks.iter().map(|chunk| chunk.text.as_str()).collect();
    let sparse = WikiEmbedder::new(None)
        .embed_sparse(token, &documents)
        .await?;
    let dense = embed_dense_documents(&collection, &documents)
        .await
        .map_err(UpsertPageError::DenseEmbed)?;

    let metadatas = build_metadatas(
        slug,
        &chunks,
        sparse,
        kind_for(slug),
        &title_from_content(&request.content),
        created_at,
        now,
        i64::from(version),
        categories,
        &request.source_ids,
    );

    let doc_batch: Vec<Option<String>> = chunks
        .iter()
        .map(|chunk| Some(chunk.text.clone()))
        .collect();
    let meta_batch: Vec<Option<UpdateMetadata>> = metadatas
        .into_iter()
        .map(metadata_to_update_metadata)
        .map(Some)
        .collect();
    record_op(
        wiki_client,
        tenant,
        txn.upsert(
            ids.clone(),
            Some(dense),
            Some(doc_batch),
            None,
            Some(meta_batch),
        ),
    )
    .await?;

    if !stale_ids.is_empty() {
        record_op(wiki_client, tenant, txn.delete(stale_ids)).await?;
    }

    record_op(wiki_client, tenant, txn.commit()).await?;

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
        version,
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
/// requires the collection to be recreated within the cache TTL), and the
/// transaction will fail before commit; a client retry is cheaper than an
/// in-request re-resolve plus re-embed.
async fn record_op<T, F>(
    wiki_client: &FoundationChromaClient,
    tenant: &str,
    fut: F,
) -> Result<T, UpsertPageError>
where
    F: Future<Output = Result<T, ChromaHttpClientError>>,
{
    fut.await.map_err(|err| {
        if is_not_found(&err) {
            wiki_client.invalidate_wiki(tenant);
        }
        UpsertPageError::RecordIo(err)
    })
}

fn where_slug(slug: &str) -> Where {
    Where::Metadata(MetadataExpression {
        key: "slug".to_string(),
        comparison: MetadataComparison::Primitive(
            PrimitiveOperator::Equal,
            MetadataValue::Str(slug.to_string()),
        ),
    })
}

fn enforce_transaction_write_limit(
    replacement_writes: usize,
    stale_delete_writes: usize,
) -> Result<(), UpsertPageError> {
    let writes = replacement_writes + stale_delete_writes;
    if writes > MAX_TRANSACTION_WRITES {
        Err(UpsertPageError::TooManyTransactionWrites {
            writes,
            limit: MAX_TRANSACTION_WRITES,
        })
    } else {
        Ok(())
    }
}

async fn embed_dense_documents(
    collection: &ChromaCollection,
    documents: &[&str],
) -> Result<Vec<Vec<f32>>, ChromaHttpClientError> {
    let mut embeddings = Vec::with_capacity(documents.len());
    for batch in documents.chunks(DENSE_EMBED_BATCH_SIZE) {
        embeddings.extend(collection.embed_documents(batch).await?);
    }
    Ok(embeddings)
}

fn head_metadata(response: &GetResponse) -> Option<Metadata> {
    response
        .metadatas
        .as_ref()?
        .iter()
        .filter_map(|meta| meta.as_ref())
        .min_by_key(|meta| meta_int(meta, "chunk_id").unwrap_or(0))
        .cloned()
}

fn current_version(exists: bool, meta: Option<&Metadata>) -> u32 {
    if exists {
        meta.and_then(meta_version).unwrap_or(0)
    } else {
        0
    }
}

fn meta_version(meta: &Metadata) -> Option<u32> {
    meta_int(meta, "version").and_then(|version| u32::try_from(version).ok())
}

fn meta_int(meta: &Metadata, key: &str) -> Option<i64> {
    meta.get(key).and_then(|value| i64::try_from(value).ok())
}

fn metadata_to_update_metadata(metadata: Metadata) -> UpdateMetadata {
    metadata
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect()
}

fn is_conditional_conflict(err: &ChromaHttpClientError) -> bool {
    matches!(
        err,
        ChromaHttpClientError::ApiError(message, status)
            if *status == reqwest::StatusCode::PRECONDITION_FAILED
                || (*status == reqwest::StatusCode::CONFLICT
                    && (message.contains("conditional")
                        || message.contains("ConditionalWriteConflict")))
    )
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

    fn chunk_meta(chunk_id: i64, version: Option<i64>) -> Metadata {
        let mut meta = Metadata::new();
        meta.insert("chunk_id".to_string(), MetadataValue::Int(chunk_id));
        if let Some(version) = version {
            meta.insert("version".to_string(), MetadataValue::Int(version));
        }
        meta
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
    fn transaction_write_limit_counts_upserts_and_deletes() {
        enforce_transaction_write_limit(300, 0).unwrap();
        enforce_transaction_write_limit(1, 299).unwrap();

        let err = enforce_transaction_write_limit(300, 1).unwrap_err();
        assert!(matches!(
            err,
            UpsertPageError::TooManyTransactionWrites {
                writes: 301,
                limit: MAX_TRANSACTION_WRITES,
            }
        ));
        assert_eq!(err.code(), ErrorCodes::ResourceExhausted);
    }

    #[test]
    fn current_version_reads_metadata_and_defaults_absent_pages_to_zero() {
        let meta = chunk_meta(0, Some(7));

        assert_eq!(current_version(true, Some(&meta)), 7);
        assert_eq!(current_version(false, Some(&meta)), 0);
        assert_eq!(current_version(true, Some(&chunk_meta(0, None))), 0);
        assert_eq!(current_version(true, None), 0);
    }

    #[test]
    fn head_metadata_picks_lowest_chunk_id() {
        let response = GetResponse {
            ids: vec!["page-1".to_string(), "page-0".to_string()],
            embeddings: None,
            documents: None,
            uris: None,
            metadatas: Some(vec![
                Some(chunk_meta(1, Some(4))),
                Some(chunk_meta(0, Some(3))),
            ]),
            include: vec![Include::Metadata],
            occ_read_token: None,
        };

        let head = head_metadata(&response).expect("head metadata");

        assert_eq!(meta_int(&head, "chunk_id"), Some(0));
        assert_eq!(meta_version(&head), Some(3));
    }

    #[test]
    fn conditional_conflicts_map_to_failed_precondition() {
        let err = UpsertPageError::RecordIo(ChromaHttpClientError::ApiError(
            "ConditionalWriteConflictError".into(),
            reqwest::StatusCode::CONFLICT,
        ));
        assert_eq!(err.code(), ErrorCodes::FailedPrecondition);

        let err = UpsertPageError::RecordIo(ChromaHttpClientError::ApiError(
            "stale read".into(),
            reqwest::StatusCode::PRECONDITION_FAILED,
        ));
        assert_eq!(err.code(), ErrorCodes::FailedPrecondition);
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
        req.reason = Some("a".repeat(350));
        req.validate().unwrap();

        req.reason = Some("a".repeat(351));
        assert!(req.validate().is_err());
    }
}
