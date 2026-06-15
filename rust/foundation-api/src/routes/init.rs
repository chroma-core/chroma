use super::whoami::whoami_and_authorize;
use crate::{
    auth::AuthzAction, config::FoundationConfig, errors::ServerError, server::FoundationApiServer,
    wiki::WikiClientError,
};
use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_types::{
    Collection, CollectionUuid, CreateDatabaseError, DatabaseName, EmbeddingFunctionConfiguration,
    EmbeddingFunctionNewConfiguration, IndexConfig, KnnIndex, Metadata, MetadataValue, Schema,
    SparseIndexAlgorithm, SparseVectorIndexConfig, UpdateMetadata,
    CHROMA_GROUP_CHUNK_SIBLINGS_KEY, DOCUMENT_KEY,
};
use frontend_core::{
    attached_function_ops,
    collection_ops::{
        plan_create_collection, supported_segment_types, ExecutorKind, TenantFeatureFlags,
    },
    foundation::source_kind_for_collection_name,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Serialize)]
pub struct FoundationInitResponse {
    pub tenant: String,
    pub user_id: String,
    pub database: String,
    pub database_id: String,
    pub wiki_collection_id: String,
    pub wiki_revisions_collection_id: String,
    pub currents_collection_id: String,
    pub file_uploads_collection_id: String,
    pub agent_sessions_collection_id: String,
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
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::InitFoundation).await?;
    let tenant = identity.tenant;
    let user_id = identity.user_id;

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
        // NOTE(hammadb): Foundation uses Qwen0.6B by default which is 1024 dims
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;
    let wiki_revisions = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.wiki_revisions_collection,
        None,
        Some(1),
        CollectionEmbeddingFunctions::default(),
    )
    .await?;
    let currents = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.currents_collection,
        None,
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;

    maybe_seed_currents_collection(
        &server,
        &headers,
        &tenant,
        &foundation_cfg.currents_collection,
    )
    .await?;

    // Attach revision_history to the wiki collection so every mutation is
    // archived into wiki_revisions automatically on compaction.
    ensure_revision_history_function(
        &mut sysdb,
        tenant.clone(),
        wiki.collection_id,
        foundation_cfg,
    )
    .await?;

    // Private (per-user) collections — namespaced by user_id so each team
    // member gets their own isolated collection for uploads and traces.
    let file_uploads_name = format!("{}_{}", foundation_cfg.file_uploads_collection, user_id);
    let file_uploads = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &file_uploads_name,
        None,
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;

    let agent_sessions_name = format!("{}_{}", foundation_cfg.agent_sessions_collection, user_id);
    let agent_sessions = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &agent_sessions_name,
        None,
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;

    // Source collections are the attached function's *input*. They carry
    // the chunk-sibling grouping flag so a job's chunk records stay in one
    // partition and the trailing end-of-job marker on `{base}-0` is
    // observed after every sibling chunk (ADR 0001 §6). All sources share
    // one async attached function, and extra sources are added via
    // `add_input()`.
    let mut source_collection_ids = HashMap::new();
    let mut source_collections = Vec::new();
    for source_name in &foundation_cfg.source_collections {
        let source = ensure_collection(
            &mut sysdb,
            tenant.clone(),
            db_name.clone(),
            source_name,
            Some(group_chunk_siblings_metadata()),
            Some(1024),
            CollectionEmbeddingFunctions::default(),
        )
        .await?;
        source_collections.push((source_name.clone(), source.collection_id));
        source_collection_ids.insert(source_name.clone(), source.collection_id.to_string());
    }

    if let Some((base_source_name, base_source_id)) = source_collections.first() {
        ensure_attached_function(
            &mut sysdb,
            tenant.clone(),
            *base_source_id,
            base_source_name,
            foundation_cfg,
        )
        .await?;

        for (_, source_collection_id) in source_collections.iter().skip(1) {
            attached_function_ops::add_attached_function_input(
                &mut sysdb,
                foundation_attached_function_name(),
                *base_source_id,
                *source_collection_id,
                db_name.clone(),
            )
            .await?;
        }
    }

    Ok(Json(FoundationInitResponse {
        tenant,
        user_id,
        database: foundation_cfg.database_name.clone(),
        database_id: database_id.to_string(),
        wiki_collection_id: wiki.collection_id.to_string(),
        wiki_revisions_collection_id: wiki_revisions.collection_id.to_string(),
        currents_collection_id: currents.collection_id.to_string(),
        file_uploads_collection_id: file_uploads.collection_id.to_string(),
        agent_sessions_collection_id: agent_sessions.collection_id.to_string(),
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

/// Idempotently create the shared foundation function on the base source
/// collection. Additional source collections are attached later via
/// `add_input()`. Params carry the modal endpoint and the base source kind,
/// matching the existing Foundation function contract.
///
/// `/init` is safe to call repeatedly — the shared helper treats an
/// already-existing function as a no-op (`created = false`).
async fn ensure_attached_function(
    sysdb: &mut SysDb,
    tenant: String,
    input_collection_id: CollectionUuid,
    base_source_name: &str,
    cfg: &FoundationConfig,
) -> Result<(), ServerError> {
    let endpoint_url = cfg
        .function_endpoint_url
        .as_ref()
        .ok_or(MissingFunctionEndpointUrl)?;
    let source_kind = source_kind_for_collection_name(base_source_name)?;
    let params = serde_json::json!({
        "endpoint_url": endpoint_url,
        "source_collection": base_source_name,
        "source_kind": source_kind,
    });
    let output_schema = Schema::new_record_only();
    attached_function_ops::create_attached_function(
        sysdb,
        foundation_attached_function_name(),
        cfg.function_name.clone(),
        input_collection_id,
        cfg.wiki_collection.clone(),
        params,
        tenant,
        cfg.database_name.clone(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;
    Ok(())
}

fn foundation_attached_function_name() -> String {
    "foundation_sources_to_wiki".to_string()
}

/// Attach the built-in `revision_history` function to the wiki
/// collection so every upsert/delete is archived into the wiki_revisions
/// collection on compaction.
async fn ensure_revision_history_function(
    sysdb: &mut SysDb,
    tenant: String,
    wiki_collection_id: CollectionUuid,
    cfg: &FoundationConfig,
) -> Result<(), ServerError> {
    let params = serde_json::json!({
        "version_key": "version",
    });
    let output_schema = Schema::new_record_only();
    attached_function_ops::create_attached_function(
        sysdb,
        "wiki_revision_history".to_string(),
        "revision_history".to_string(),
        wiki_collection_id,
        cfg.wiki_revisions_collection.clone(),
        params,
        tenant,
        cfg.database_name.clone(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum FoundationInitError {
    #[error("Configured foundation database name is shorter than the 3-character minimum")]
    DatabaseNameTooShort,
    #[error("foundation frontend_ingress_url is not configured")]
    FrontendIngressUrlMissing,
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    #[error(transparent)]
    WikiClient(#[from] WikiClientError),
    #[error("chroma record I/O failed: {0}")]
    RecordIo(chroma::client::ChromaHttpClientError),
}

impl ChromaError for FoundationInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            FoundationInitError::DatabaseNameTooShort | FoundationInitError::MissingToken => {
                ErrorCodes::InvalidArgument
            }
            FoundationInitError::FrontendIngressUrlMissing
            | FoundationInitError::WikiClient(_)
            | FoundationInitError::RecordIo(_) => ErrorCodes::Internal,
        }
    }
}

const CHROMA_TOKEN_HEADER: &str = "x-chroma-token";

fn chroma_token(headers: &HeaderMap) -> Result<&str, FoundationInitError> {
    headers
        .get(CHROMA_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|token| !token.is_empty())
        .ok_or(FoundationInitError::MissingToken)
}

async fn ensure_currents_collection(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    collection_name: &str,
) -> Result<(), ServerError> {
    let wiki_client = server
        .wiki_client
        .as_ref()
        .ok_or(FoundationInitError::FrontendIngressUrlMissing)?;
    let token = chroma_token(headers)?;
    let collection = wiki_client
        .get_collection_by_name(tenant, token, collection_name)
        .await?;
    let records = mock_currents_records();

    let ids: Vec<String> = records.iter().map(|record| record.id.clone()).collect();
    let documents: Vec<Option<String>> = records
        .iter()
        .map(|record| Some(record.document.clone()))
        .collect();
    let metadatas: Vec<Option<UpdateMetadata>> = records
        .into_iter()
        .map(|record| {
            Some(
                record
                    .metadata
                    .into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect(),
            )
        })
        .collect();
    collection
        .upsert(
            ids,
            None::<Vec<Vec<f32>>>,
            Some(documents),
            None,
            Some(metadatas),
        )
        .await
        .map_err(FoundationInitError::RecordIo)?;
    Ok(())
}

async fn maybe_seed_currents_collection(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    collection_name: &str,
) -> Result<(), ServerError> {
    if server.wiki_client.is_none() {
        tracing::info!(
            collection_name = collection_name,
            "skipping currents mock seed because frontend_ingress_url is not configured"
        );
        return Ok(());
    }

    if chroma_token(headers).is_err() {
        tracing::info!(
            collection_name = collection_name,
            "skipping currents mock seed because request carried no x-chroma-token"
        );
        return Ok(());
    }

    ensure_currents_collection(server, headers, tenant, collection_name).await
}

struct MockCurrentRecord {
    id: String,
    document: String,
    metadata: Metadata,
}

#[derive(Debug, Deserialize)]
struct MockCurrentFixture {
    tilegroup_id: String,
    label: String,
    headline: String,
    summary: String,
    tiles: Vec<MockTileFixture>,
}

#[derive(Debug, Deserialize)]
struct MockTileFixture {
    slug: String,
    title: String,
    role: String,
    blurb: String,
}

fn mock_currents_records() -> Vec<MockCurrentRecord> {
    let fixture = include_str!("mock_currents.json");
    let tilegroups: Vec<MockCurrentFixture> =
        serde_json::from_str(fixture).expect("mock_currents.json must be valid");
    tilegroups.into_iter().map(mock_current_record).collect()
}

fn mock_current_record(fixture: MockCurrentFixture) -> MockCurrentRecord {
    let MockCurrentFixture {
        tilegroup_id,
        label,
        headline,
        summary,
        tiles,
    } = fixture;
    let page_slugs: Vec<String> = tiles.iter().map(|tile| tile.slug.clone()).collect();
    let tile_roles: Vec<String> = tiles.iter().map(|tile| tile.role.clone()).collect();
    let mut metadata = Metadata::new();
    metadata.insert(
        "tilegroup_id".to_string(),
        MetadataValue::Str(tilegroup_id.clone()),
    );
    metadata.insert("label".to_string(), MetadataValue::Str(label.clone()));
    metadata.insert("headline".to_string(), MetadataValue::Str(headline.clone()));
    metadata.insert("summary".to_string(), MetadataValue::Str(summary.clone()));
    metadata.insert(
        "tile_count".to_string(),
        MetadataValue::Int(tiles.len() as i64),
    );
    metadata.insert(
        "page_slugs".to_string(),
        MetadataValue::StringArray(page_slugs),
    );
    metadata.insert(
        "tile_roles".to_string(),
        MetadataValue::StringArray(tile_roles),
    );
    for (idx, tile) in tiles.iter().enumerate() {
        let key = format!("tile_{:02}_json", idx + 1);
        let value = serde_json::json!({
            "order": idx + 1,
            "slug": tile.slug,
            "title": tile.title,
            "role": tile.role,
            "blurb": tile.blurb,
        });
        metadata.insert(key, MetadataValue::Str(value.to_string()));
    }

    let tiles_document = tiles
        .iter()
        .enumerate()
        .map(|(idx, tile)| {
            format!(
                "Tile {}\nSlug: {}\nTitle: {}\nRole: {}\nBlurb: {}",
                idx + 1,
                tile.slug,
                tile.title,
                tile.role,
                tile.blurb
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let document = format!("{}: {}. {}\n\n{}", label, headline, summary, tiles_document);

    MockCurrentRecord {
        id: format!("tilegroup:{tilegroup_id}"),
        document,
        metadata,
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

/// The Chroma Cloud Qwen3-Embedding-0.6B known embedding function,
/// serialized exactly as the `chroma-cloud-qwen` embedding function expects
/// (see `schemas/embedding_functions/chroma-cloud-qwen.json` and the
/// Python/Rust implementations). This is the dense model Foundation uses by
/// default; the wiki collection is 1024-dimensional to match it.
fn qwen_embedding_function() -> EmbeddingFunctionConfiguration {
    EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
        name: "chroma-cloud-qwen".to_string(),
        config: serde_json::json!({
            "api_key_env_var": "CHROMA_API_KEY",
            "model": "Qwen/Qwen3-Embedding-0.6B",
            // `generic_retrieval` is the general-knowledge task the
            // chroma sync pipeline uses by default. It must be set (not null)
            // for the instruction below to actually be applied at query time.
            "task": "generic_retrieval",
            "instructions": {
                "generic_retrieval": {
                    "documents": "",
                    "query": "Retrieve semantically similar text",
                }
            },
        }),
    })
}

/// The Chroma Cloud SPLADE sparse embedding function
fn splade_embedding_function() -> EmbeddingFunctionConfiguration {
    EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
        name: "chroma-cloud-splade".to_string(),
        config: serde_json::json!({
            "model": "prithivida/Splade_PP_en_v1",
            "api_key_env_var": "CHROMA_API_KEY",
        }),
    })
}

/// Dense + sparse embedding functions to register on a Foundation
/// collection. A supplied function makes Chroma auto-embed that modality
/// from the document server-side; `None` leaves the corresponding index
/// EF-less (the writer supplies vectors).
#[derive(Default)]
struct CollectionEmbeddingFunctions {
    dense: Option<EmbeddingFunctionConfiguration>,
    sparse: Option<EmbeddingFunctionConfiguration>,
}

/// Build the [`Schema`] used for Foundation collections. Adds a
/// `sparse_embedding` sparse vector index for SPLADE.
///
/// The dense function is set on the dense vector index (defaults +
/// `#embedding`); the sparse function is set on the sparse index. Mirrors
/// the hosted-chroma file-upload `build_collection_schema`.
fn foundation_collection_schema(embedding_functions: CollectionEmbeddingFunctions) -> Schema {
    let CollectionEmbeddingFunctions { dense, sparse } = embedding_functions;
    // Both branches default the dense vector index to SPANN — what the
    // distributed frontend uses by default — and the planner is also given
    // `KnnIndex::Spann` in `ensure_collection`. When an embedding function
    // is supplied, `default_with_embedding_function` is the schema-native
    // way to set it on both the schema defaults and the `#embedding` key.
    let base = match dense {
        Some(dense) => Schema::default_with_embedding_function(dense),
        None => Schema::new_default(KnnIndex::Spann),
    };
    // Auto-embed sparse vectors from the document only when a sparse EF is
    // supplied; otherwise leave `source_key` unset alongside the EF.
    let sparse_source_key = sparse.as_ref().map(|_| DOCUMENT_KEY.to_string());
    base.create_index(
        Some("sparse_embedding"),
        IndexConfig::SparseVector(SparseVectorIndexConfig {
            embedding_function: sparse,
            source_key: sparse_source_key,
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
    dimension: Option<i32>,
    embedding_functions: CollectionEmbeddingFunctions,
) -> Result<Collection, ServerError> {
    let schema = foundation_collection_schema(embedding_functions);
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
            dimension,
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
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        assert!(
            schema.is_sparse_index_enabled(),
            "schema must have a sparse vector index for SPLADE embeddings"
        );
    }

    #[test]
    fn foundation_schema_sparse_key_is_sparse_embedding() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
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
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
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

    #[test]
    fn qwen_embedding_function_matches_known_serialization() {
        let EmbeddingFunctionConfiguration::Known(known) = qwen_embedding_function() else {
            panic!("Qwen embedding function must be a known embedding function");
        };
        assert_eq!(known.name, "chroma-cloud-qwen");
        assert_eq!(
            known.config,
            serde_json::json!({
                "api_key_env_var": "CHROMA_API_KEY",
                "model": "Qwen/Qwen3-Embedding-0.6B",
                "task": "generic_retrieval",
                "instructions": {
                    "generic_retrieval": {
                        "documents": "",
                        "query": "Retrieve semantically similar text",
                    }
                },
            })
        );
    }

    #[test]
    fn splade_embedding_function_matches_file_upload_serialization() {
        let EmbeddingFunctionConfiguration::Known(known) = splade_embedding_function() else {
            panic!("SPLADE embedding function must be a known embedding function");
        };
        assert_eq!(known.name, "chroma-cloud-splade");
        assert_eq!(
            known.config,
            serde_json::json!({
                "model": "prithivida/Splade_PP_en_v1",
                "api_key_env_var": "CHROMA_API_KEY",
            })
        );
    }

    #[test]
    fn auto_embed_schema_sets_splade_sparse_function() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        });
        let sparse_idx = schema
            .keys
            .get("sparse_embedding")
            .and_then(|vt| vt.sparse_vector.as_ref())
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .expect("auto-embed schema must have a sparse_embedding index");
        assert_eq!(
            sparse_idx.config.embedding_function,
            Some(splade_embedding_function())
        );
        assert_eq!(sparse_idx.config.source_key, Some("#document".to_string()));
    }

    #[test]
    fn no_dense_ef_leaves_sparse_function_unset() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        let sparse_idx = schema
            .keys
            .get("sparse_embedding")
            .and_then(|vt| vt.sparse_vector.as_ref())
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .expect("schema must have a sparse_embedding index");
        assert_eq!(sparse_idx.config.embedding_function, None);
        assert_eq!(sparse_idx.config.source_key, None);
    }

    #[test]
    fn foundation_schema_sets_dense_embedding_function() {
        let ef = qwen_embedding_function();
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions {
            dense: Some(ef.clone()),
            sparse: Some(splade_embedding_function()),
        });

        let defaults_ef = schema
            .defaults
            .float_list
            .as_ref()
            .and_then(|fl| fl.vector_index.as_ref())
            .expect("schema defaults must carry a dense vector index")
            .config
            .embedding_function
            .clone();
        assert_eq!(defaults_ef, Some(ef.clone()));

        let embedding_ef = schema
            .keys
            .get("#embedding")
            .and_then(|vt| vt.float_list.as_ref())
            .and_then(|fl| fl.vector_index.as_ref())
            .expect("#embedding key must carry a dense vector index")
            .config
            .embedding_function
            .clone();
        assert_eq!(embedding_ef, Some(ef));
    }

    #[test]
    fn foundation_schema_without_embedding_function_leaves_it_unset() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        let defaults_ef = schema
            .defaults
            .float_list
            .as_ref()
            .and_then(|fl| fl.vector_index.as_ref())
            .expect("schema defaults must carry a dense vector index")
            .config
            .embedding_function
            .clone();
        assert_eq!(defaults_ef, None);
    }
}
