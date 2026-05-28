use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::{AttachFunctionError, SysDb};
use chroma_types::{
    Collection, CollectionUuid, CreateDatabaseError, DatabaseName, IndexConfig, KnnIndex, Metadata,
    MetadataValue, Schema, SparseIndexAlgorithm, SparseVectorIndexConfig,
    CHROMA_GROUP_CHUNK_SIBLINGS_KEY,
};
use frontend_core::collection_ops::{
    plan_create_collection, supported_segment_types, ExecutorKind, TenantFeatureFlags,
};
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use super::whoami::whoami_and_authorize;
use crate::{
    auth::AuthzAction, config::FoundationConfig, errors::ServerError, server::FoundationApiServer,
};

#[derive(Serialize)]
pub struct FoundationInitResponse {
    pub tenant: String,
    pub database: String,
    pub database_id: String,
    pub wiki_collection_id: String,
    pub wiki_revisions_collection_id: String,
    /// Source collection name -> id for each ensured source collection
    /// (slack, notion, …). Each carries the chunk-sibling grouping flag.
    pub source_collection_ids: std::collections::HashMap<String, String>,
}

/// `POST /api/init` — idempotent bootstrap for a team's Foundation
/// workspace. Ensures the configured Foundation database and the wiki +
/// wiki_revisions collections (names overridable via
/// `CHROMA_FOUNDATION__*` env vars) exist in the tenant resolved from the
/// auth context. Safe to call repeatedly.
pub async fn foundation_init(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
) -> Result<Json<FoundationInitResponse>, ServerError> {
    let tenant = whoami_and_authorize(&*server.auth, &headers, AuthzAction::InitFoundation)
        .await?
        .tenant;

    let _guard =
        server.scorecard_request(&["op:foundation_init", &format!("tenant:{}", tenant)])?;

    let foundation_cfg = &server.config.foundation;
    let db_name = DatabaseName::new(&foundation_cfg.database_name)
        .ok_or(FoundationInitError::DatabaseNameTooShort)?;

    let mut sysdb = server.sysdb.clone();
    let database_id = ensure_database(&mut sysdb, db_name.clone(), tenant.clone()).await?;

    // Wiki collections are the attached function's *output*; they don't
    // need chunk-sibling grouping (no end-of-job marker is read from them).
    let wiki = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.wiki_collection,
        None,
    )
    .await?;
    let wiki_revisions = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.wiki_revisions_collection,
        None,
    )
    .await?;

    // Source collections are the attached function's *input*. They carry
    // the chunk-sibling grouping flag so a job's chunk records stay in one
    // partition and the trailing end-of-job marker on `{base}-0` is
    // observed after every sibling chunk (ADR 0001 §6). Each gets the
    // server-side function attached, with the wiki collection as output —
    // mirroring the foundation CLI POC (chroma-core/foundation #97).
    let mut source_collection_ids = HashMap::new();
    for source_name in &foundation_cfg.source_collections {
        let source = ensure_collection(
            &mut sysdb,
            tenant.clone(),
            db_name.clone(),
            source_name,
            Some(group_chunk_siblings_metadata()),
        )
        .await?;
        ensure_attached_function(
            &mut sysdb,
            tenant.clone(),
            foundation_cfg.database_name.clone(),
            source.collection_id,
            source_name,
            foundation_cfg,
        )
        .await?;
        source_collection_ids.insert(source_name.clone(), source.collection_id.to_string());
    }

    Ok(Json(FoundationInitResponse {
        tenant,
        database: foundation_cfg.database_name.clone(),
        database_id: database_id.to_string(),
        wiki_collection_id: wiki.collection_id.to_string(),
        wiki_revisions_collection_id: wiki_revisions.collection_id.to_string(),
        source_collection_ids,
    }))
}

/// Collection metadata that opts a source collection into chunk-sibling
/// grouping during compaction/partitioning (see
/// [`chroma_types::CHROMA_GROUP_CHUNK_SIBLINGS_KEY`]).
fn group_chunk_siblings_metadata() -> Metadata {
    let mut metadata = HashMap::new();
    metadata.insert(
        CHROMA_GROUP_CHUNK_SIBLINGS_KEY.to_string(),
        MetadataValue::Bool(true),
    );
    metadata
}

/// Raised when `/init` needs the attached-function endpoint URL but the
/// deployment never configured `foundation.function_endpoint_url`. Surfaced
/// as a 500 so a misconfigured deploy fails loudly instead of attaching the
/// function with a missing/placeholder endpoint.
#[derive(Debug, thiserror::Error)]
#[error("foundation.function_endpoint_url is not configured")]
struct MissingFunctionEndpointUrl;

impl ChromaError for MissingFunctionEndpointUrl {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Idempotently attach the foundation function to a source collection,
/// mirroring the CLI POC (chroma-core/foundation #97): the function reads
/// the source collection and writes synthesized content to the wiki
/// collection. The attachment name is `{source}_to_wiki`; `params` carry
/// the modal `endpoint_url` plus `source_collection` / `source_kind`.
///
/// `/init` is safe to call repeatedly, so an already-attached function
/// (`AlreadyExists` or `CollectionAlreadyHasFunction`) is treated as
/// success rather than an error.
async fn ensure_attached_function(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: String,
    input_collection_id: CollectionUuid,
    source_name: &str,
    cfg: &FoundationConfig,
) -> Result<(), ServerError> {
    let attachment_name = format!("{source_name}_to_wiki");
    let endpoint_url = cfg
        .function_endpoint_url
        .as_ref()
        .ok_or(MissingFunctionEndpointUrl)?;
    let params = serde_json::json!({
        "endpoint_url": endpoint_url,
        "source_collection": source_name,
        "source_kind": source_name,
    });
    match sysdb
        .create_attached_function(
            attachment_name,
            cfg.function_name.clone(),
            input_collection_id,
            cfg.wiki_collection.clone(),
            params,
            tenant,
            database_name,
            cfg.min_records_for_invocation,
        )
        .await
    {
        Ok(_) => Ok(()),
        // Idempotent: the function is already attached to this collection.
        Err(AttachFunctionError::AlreadyExists(_))
        | Err(AttachFunctionError::CollectionAlreadyHasFunction(_)) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug, thiserror::Error)]
enum FoundationInitError {
    #[error("Configured foundation database name is shorter than the 3-character minimum")]
    DatabaseNameTooShort,
}

impl ChromaError for FoundationInitError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

async fn ensure_database(
    sysdb: &mut SysDb,
    database_name: DatabaseName,
    tenant: String,
) -> Result<Uuid, ServerError> {
    match sysdb
        .create_database(Uuid::new_v4(), database_name.clone(), tenant.clone())
        .await
    {
        Ok(_) | Err(CreateDatabaseError::AlreadyExists(_)) => {}
        Err(e) => return Err(e.into()),
    }
    let db = sysdb.get_database(database_name, tenant).await?;
    Ok(db.id)
}

/// SysDb's `create_collection` takes a `get_or_create: bool`. When true, an
/// existing collection with the same (tenant, database, name) is returned
/// instead of failing with `AlreadyExists` — atomic idempotency in one round
/// trip, so we don't need the try-then-fallback dance we use for databases.
const GET_OR_CREATE: bool = true;

/// Build the [`Schema`] used for Foundation collections. Adds a
/// SPLADE-compatible sparse vector index so the server-side mutation
/// writer has a field to land sparse embeddings in.
fn foundation_collection_schema() -> Schema {
    Schema::new_default(KnnIndex::Hnsw)
        .create_index(
            Some("sparse_embedding"),
            IndexConfig::SparseVector(SparseVectorIndexConfig {
                embedding_function: None,
                source_key: None,
                bm25: Some(false),
                // TODO: Change this to MaxScore
                algorithm: SparseIndexAlgorithm::Wand,
            }),
        )
        .expect("static schema construction should never fail")
}

/// Plan a fresh distributed-mode collection with the shared
/// `frontend_core::collection_ops` planner and hand it to sysdb. The
/// planner reconciles the Foundation schema (sparse vector index for
/// SPLADE) with the default config, picks the right segment types for
/// distributed mode, and emits everything sysdb needs.
async fn ensure_collection(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: DatabaseName,
    collection_name: &str,
    metadata: Option<Metadata>,
) -> Result<Collection, ServerError> {
    let schema = foundation_collection_schema();
    let plan = plan_create_collection(
        None,
        Some(schema),
        ExecutorKind::Distributed,
        &supported_segment_types(ExecutorKind::Distributed),
        true,
        KnnIndex::Spann,
        TenantFeatureFlags::default(),
    )?;
    let collection = sysdb
        .create_collection(
            tenant,
            database_name,
            plan.collection_id,
            collection_name.to_string(),
            plan.segments,
            plan.configuration,
            plan.schema,
            metadata,
            // NOTE(hammadb): Foundation uses Qwen0.6B by default which is 1024 dims
            Some(1024),
            GET_OR_CREATE,
        )
        .await?;
    Ok(collection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::SegmentType;

    #[test]
    fn foundation_schema_has_sparse_vector_index() {
        let schema = foundation_collection_schema();
        assert!(
            schema.is_sparse_index_enabled(),
            "schema must have a sparse vector index for SPLADE embeddings"
        );
    }

    #[test]
    fn foundation_schema_sparse_key_is_sparse_embedding() {
        let schema = foundation_collection_schema();
        let sparse_vt = schema
            .keys
            .get("sparse_embedding")
            .expect("schema must have a 'sparse_embedding' key override");
        let idx = sparse_vt
            .sparse_vector
            .as_ref()
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .expect("'sparse_embedding' key must have a sparse_vector_index");
        assert!(idx.enabled, "sparse_vector_index must be enabled");
        assert_eq!(idx.config.bm25, Some(false));
    }

    #[test]
    fn foundation_plan_produces_schema_and_segments() {
        let schema = foundation_collection_schema();
        let plan = plan_create_collection(
            None,
            Some(schema),
            ExecutorKind::Distributed,
            &supported_segment_types(ExecutorKind::Distributed),
            true,
            KnnIndex::Spann,
            TenantFeatureFlags::default(),
        )
        .expect("planning with foundation schema must succeed");

        assert!(
            plan.schema.is_some(),
            "plan must carry a reconciled schema when enable_schema=true"
        );
        assert!(
            !plan.segments.is_empty(),
            "plan must produce at least one segment"
        );
        let reconciled = plan.schema.as_ref().unwrap();
        assert!(
            reconciled.is_sparse_index_enabled(),
            "reconciled schema must preserve the sparse vector index"
        );
        assert!(
            plan.segments
                .iter()
                .any(|s| s.r#type == SegmentType::Spann || s.r#type == SegmentType::QuantizedSpann),
            "plan must include a SPANN vector segment, got: {:?}",
            plan.segments.iter().map(|s| &s.r#type).collect::<Vec<_>>()
        );
    }
}
