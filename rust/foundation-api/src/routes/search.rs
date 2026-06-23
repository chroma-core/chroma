//! `POST /api/search` — hybrid dense+sparse search over the wiki collection.
//!
//! Embeds the query with the caller's token (dense Qwen + sparse SPLADE),
//! fuses the two `$knn` rankings with Reciprocal Rank Fusion, and returns the
//! top hits. Like the other wiki routes it proxies the actual query to the FE
//! through [`WikiClient`]; the FE enforces auth, quota, metering, and billing.
//!
//! The Chroma `/search` endpoint does not embed query text — the caller
//! supplies the dense and sparse query vectors, which is why both embedders run
//! client-side here before the query is issued.

use crate::routes::{caller_token, whoami::whoami_and_authorize};
use crate::wiki::embed::{WikiEmbedder, SPARSE_KEY};
use crate::wiki::WikiClientError;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::{extract::State, http::HeaderMap, Json};
use chroma::client::ChromaHttpClientError;
use chroma::types::{rrf, Key, QueryVector, RankExpr, SearchPayload, SearchResponse, SparseVector};
use chroma::ChromaCollection;
use chroma_error::{ChromaError, ChromaValidationError, ErrorCodes};
use chroma_types::operator::SearchRecord;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Default number of hits returned when the caller omits `limit`.
pub(crate) fn default_limit() -> u32 {
    10
}

/// Candidate pool size requested from each `$knn` arm before fusion. Larger
/// than the result `limit` so RRF has overlap to work with across the two
/// modalities.
const KNN_CANDIDATES: u32 = 100;

/// Upper bound on the requested `limit`. Fusion can never surface more than the
/// per-arm candidate pool, so clamp here to make that ceiling explicit rather
/// than letting callers ask for an arbitrarily large page.
const MAX_LIMIT: u32 = KNN_CANDIDATES;

/// The RRF `k` smoothing constant (the conventional default).
const RRF_K: u32 = 60;

/// Fallback rank assigned to a document that an arm did not retrieve (i.e. it
/// fell outside that arm's top-[`KNN_CANDIDATES`]). Using the candidate-pool
/// size treats a missing doc as if it ranked just past the cutoff, so it still
/// contributes to the fused score (union) instead of being dropped
/// (intersection). Ranks from the engine are 0-based, so this is strictly worse
/// than any retrieved rank.
const MISSING_ARM_RANK: f32 = KNN_CANDIDATES as f32;

/// Request body for `POST /api/search`.
#[derive(Debug, Deserialize, Validate)]
pub struct SearchRequest {
    /// The search query text. Embedded client-side into dense + sparse vectors.
    #[validate(length(min = 1, message = "query must not be empty"))]
    pub query: String,
    /// Maximum number of hits to return. Defaults to [`default_limit`]; must be
    /// at least 1 and is clamped down to [`MAX_LIMIT`].
    #[validate(range(min = 1, message = "limit must be at least 1"))]
    #[serde(default = "default_limit")]
    pub limit: u32,
}

/// Response body for `POST /api/search`.
#[derive(Debug, Serialize)]
pub struct SearchResponseBody {
    /// Hits in descending score order.
    pub hits: Vec<SearchRecord>,
}

/// Errors raised while running the hybrid search flow (after validation).
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// `frontend_ingress_url` is unset, so the wiki client was never built.
    #[error("wiki search is not configured")]
    RouteDisabled,
    /// The caller's request carried no usable `x-chroma-token`.
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    /// Resolving the wiki collection through the proxy failed.
    #[error(transparent)]
    Resolve(#[from] WikiClientError),
    /// Computing the sparse (SPLADE) query embedding failed.
    #[error(transparent)]
    Embed(#[from] crate::wiki::embed::WikiEmbedError),
    /// Computing the dense query embedding via the collection's schema-derived
    /// embedding function failed.
    #[error("dense query embedding failed: {0}")]
    DenseEmbed(ChromaHttpClientError),
    /// The query produced no embedding vector (empty embedder response).
    #[error("query produced no embedding vector")]
    EmptyEmbedding,
    /// Building the RRF rank expression failed (e.g. weight/rank mismatch).
    #[error("failed to build search ranking: {0}")]
    Rank(String),
    /// The proxied `/search` call to the FE failed.
    #[error("chroma search failed: {0}")]
    Query(ChromaHttpClientError),
}

impl ChromaError for SearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            SearchError::RouteDisabled => ErrorCodes::Internal,
            SearchError::MissingToken => ErrorCodes::InvalidArgument,
            SearchError::Resolve(err) => err.code(),
            SearchError::Embed(err) => err.code(),
            SearchError::DenseEmbed(_) => ErrorCodes::Internal,
            SearchError::EmptyEmbedding => ErrorCodes::Internal,
            SearchError::Rank(_) => ErrorCodes::Internal,
            SearchError::Query(_) => ErrorCodes::Internal,
        }
    }
}

/// `POST /api/search` handler.
pub async fn foundation_search(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponseBody>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;

    let _guard =
        server.scorecard_request(&["op:foundation_search", &format!("tenant:{tenant}")])?;

    request.validate().map_err(ChromaValidationError::from)?;

    let hits = run_search(&server, &headers, &tenant, &request).await?;
    Ok(Json(SearchResponseBody { hits }))
}

/// Resolves the wiki collection then runs the hybrid search core.
pub(crate) async fn run_search(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    request: &SearchRequest,
) -> Result<Vec<SearchRecord>, SearchError> {
    let wiki_client = server
        .wiki_client
        .as_ref()
        .ok_or(SearchError::RouteDisabled)?;
    let token = caller_token(headers).ok_or(SearchError::MissingToken)?;
    let collection = wiki_client.wiki_collection(tenant, token).await?;
    let embedder = WikiEmbedder::new(None);

    run_hybrid_search(&collection, &embedder, token, &request.query, request.limit).await
}

/// Embeds the query, issues a single RRF-ranked hybrid search, and maps the
/// response into [`SearchRecord`]s.
///
/// The dense vector comes from the collection's own schema-derived embedding
/// function (`embed_query`), so it always matches the EF the documents were
/// embedded with — no config to keep in sync here. The sparse vector is SPLADE,
/// which is not part of the dense EF, so it is computed separately.
pub(crate) async fn run_hybrid_search(
    collection: &ChromaCollection,
    embedder: &WikiEmbedder,
    token: &str,
    query: &str,
    limit: u32,
) -> Result<Vec<SearchRecord>, SearchError> {
    let dense_fut = async {
        collection
            .embed_query(&[query])
            .await
            .map_err(SearchError::DenseEmbed)?
            .into_iter()
            .next()
            .ok_or(SearchError::EmptyEmbedding)
    };
    let sparse_fut = async {
        embedder
            .embed_sparse(token, &[query])
            .await?
            .into_iter()
            .next()
            .ok_or(SearchError::EmptyEmbedding)
    };
    let (dense, sparse) = tokio::try_join!(dense_fut, sparse_fut)?;

    let payload = build_hybrid_search_payload(dense, sparse, limit.min(MAX_LIMIT))?;
    let response = collection
        .search(vec![payload])
        .await
        .map_err(SearchError::Query)?;
    Ok(search_response_to_hits(response))
}

/// Builds the single-query RRF hybrid [`SearchPayload`]: a dense `$knn` over
/// `#embedding` fused with a sparse `$knn` over [`SPARSE_KEY`], both with
/// `return_rank: true` (required for RRF). Pure (no I/O) so it is unit-testable.
///
/// Each arm sets `default` to [`MISSING_ARM_RANK`] rather than `None`: with
/// `None` on both arms the rank engine fuses by *intersection* (a doc must be in
/// both arms' top-`KNN_CANDIDATES` to survive), which would drop strong
/// single-modality hits. A finite default makes fusion a *union*, treating a doc
/// absent from an arm as ranked just past that arm's candidate cutoff.
fn build_hybrid_search_payload(
    dense: Vec<f32>,
    sparse: SparseVector,
    limit: u32,
) -> Result<SearchPayload, SearchError> {
    let dense_knn = RankExpr::Knn {
        query: QueryVector::Dense(dense),
        key: Key::Embedding,
        limit: KNN_CANDIDATES,
        default: Some(MISSING_ARM_RANK),
        return_rank: true,
    };
    let sparse_knn = RankExpr::Knn {
        query: QueryVector::Sparse(sparse),
        key: Key::field(SPARSE_KEY),
        limit: KNN_CANDIDATES,
        default: Some(MISSING_ARM_RANK),
        return_rank: true,
    };
    // Equal weights across the two modalities; no normalization.
    let rank = rrf(vec![dense_knn, sparse_knn], Some(RRF_K), None, false)
        .map_err(|err| SearchError::Rank(err.to_string()))?;

    Ok(SearchPayload::default()
        .rank(rank)
        .limit(Some(limit), 0)
        .select([Key::Document, Key::Score, Key::Metadata]))
}

/// Flattens a single-payload [`SearchResponse`] into [`SearchRecord`]s. The
/// per-field outer `Vec` is indexed by payload; we send exactly one payload, so
/// we read row 0. `embedding` is left `None` since we don't select embeddings.
fn search_response_to_hits(response: SearchResponse) -> Vec<SearchRecord> {
    let Some(ids) = response.ids.into_iter().next() else {
        return Vec::new();
    };
    let documents = response.documents.into_iter().next().flatten();
    let scores = response.scores.into_iter().next().flatten();
    let metadatas = response.metadatas.into_iter().next().flatten();

    ids.into_iter()
        .enumerate()
        .map(|(i, id)| SearchRecord {
            id,
            document: documents.as_ref().and_then(|d| d.get(i).cloned().flatten()),
            embedding: None,
            score: scores.as_ref().and_then(|s| s.get(i).copied().flatten()),
            metadata: metadatas.as_ref().and_then(|m| m.get(i).cloned().flatten()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma::types::SearchResponse;
    use serde_json::Value;

    fn collect_knn_objects(value: &Value, out: &mut Vec<Value>) {
        match value {
            Value::Object(map) => {
                if let Some(knn) = map.get("$knn") {
                    out.push(knn.clone());
                }
                for v in map.values() {
                    collect_knn_objects(v, out);
                }
            }
            Value::Array(items) => {
                for v in items {
                    collect_knn_objects(v, out);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn hybrid_payload_has_two_knn_arms_with_return_rank() {
        let sparse = SparseVector::new(vec![1, 5], vec![0.4, 0.7]).expect("sparse");
        let payload = build_hybrid_search_payload(vec![0.1, 0.2, 0.3], sparse, 7).expect("payload");

        let json = serde_json::to_value(&payload).expect("serialize payload");
        let mut knns = Vec::new();
        collect_knn_objects(&json, &mut knns);

        // Exactly the dense + sparse arms.
        assert_eq!(knns.len(), 2, "expected two $knn nodes, got: {json}");

        // Both arms must return rank for RRF to fuse them.
        assert!(knns
            .iter()
            .all(|knn| knn["return_rank"] == Value::Bool(true)));

        // Both arms must carry a finite default so fusion is a union (missing
        // docs filled), not an intersection (missing docs dropped).
        assert!(
            knns.iter().all(|knn| knn["default"].is_number()),
            "each $knn arm needs a numeric default for union fusion, got: {json}"
        );

        // One arm targets the dense embedding, the other the sparse key.
        let keys: Vec<&Value> = knns.iter().map(|knn| &knn["key"]).collect();
        assert!(
            keys.iter()
                .any(|k| *k == &Value::String(SPARSE_KEY.to_string())),
            "expected a $knn over the sparse key, got keys: {keys:?}"
        );
    }

    #[test]
    fn maps_search_response_rows_into_hits() {
        let response = SearchResponse {
            ids: vec![vec!["a-0".to_string(), "b-0".to_string()]],
            documents: vec![Some(vec![Some("doc a".to_string()), None])],
            embeddings: vec![None],
            metadatas: vec![None],
            scores: vec![Some(vec![Some(0.9), Some(0.4)])],
            select: vec![vec![]],
        };

        let hits = search_response_to_hits(response);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "a-0");
        assert_eq!(hits[0].document.as_deref(), Some("doc a"));
        assert_eq!(hits[0].score, Some(0.9));
        assert_eq!(hits[1].id, "b-0");
        assert_eq!(hits[1].document, None);
        assert_eq!(hits[1].score, Some(0.4));
    }

    #[test]
    fn request_validation_rejects_empty_query_and_zero_limit() {
        use validator::Validate;

        // Omitted limit falls back to the default and validates.
        let defaulted: SearchRequest =
            serde_json::from_value(serde_json::json!({ "query": "hi" })).expect("deserialize");
        assert_eq!(defaulted.limit, default_limit());
        assert!(defaulted.validate().is_ok());

        let empty_query: SearchRequest =
            serde_json::from_value(serde_json::json!({ "query": "", "limit": 5 }))
                .expect("deserialize");
        assert!(empty_query.validate().is_err());

        let zero_limit: SearchRequest =
            serde_json::from_value(serde_json::json!({ "query": "hi", "limit": 0 }))
                .expect("deserialize");
        assert!(zero_limit.validate().is_err());
    }

    #[test]
    fn maps_empty_response_to_no_hits() {
        let response = SearchResponse {
            ids: vec![],
            documents: vec![],
            embeddings: vec![],
            metadatas: vec![],
            scores: vec![],
            select: vec![],
        };
        assert!(search_response_to_hits(response).is_empty());
    }
}
