use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_types::{Collection, CreateDatabaseError, DatabaseName, KnnIndex};
use frontend_core::collection_ops::{
    plan_create_collection, supported_segment_types, ExecutorKind, TenantFeatureFlags,
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    auth::{AuthzAction, AuthzResource},
    errors::ServerError,
    server::FoundationApiServer,
};

#[derive(Serialize)]
pub struct FoundationInitResponse {
    pub tenant: String,
    pub database: String,
    pub database_id: String,
    pub wiki_collection_id: String,
    pub wiki_revisions_collection_id: String,
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
    let identity = server
        .auth
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateDatabase,
            AuthzResource {
                tenant: None,
                database: None,
                collection: None,
            },
        )
        .await?;
    let tenant = identity.tenant.clone();

    let _guard =
        server.scorecard_request(&["op:foundation_init", &format!("tenant:{}", tenant)])?;

    let foundation_cfg = &server.config.foundation;
    let db_name = DatabaseName::new(&foundation_cfg.database_name)
        .ok_or(FoundationInitError::DatabaseNameTooShort)?;

    let mut sysdb = server.sysdb.clone();
    let database_id = ensure_database(&mut sysdb, db_name.clone(), tenant.clone()).await?;
    let wiki = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.wiki_collection,
    )
    .await?;
    let wiki_revisions = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name,
        &foundation_cfg.wiki_revisions_collection,
    )
    .await?;

    Ok(Json(FoundationInitResponse {
        tenant,
        database: foundation_cfg.database_name.clone(),
        database_id: database_id.to_string(),
        wiki_collection_id: wiki.collection_id.to_string(),
        wiki_revisions_collection_id: wiki_revisions.collection_id.to_string(),
    }))
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

/// Plan a fresh distributed-mode collection with the shared
/// `frontend_core::collection_ops` planner and hand it to sysdb. Foundation-api
/// has no user-supplied schema/config and (today) no per-tenant feature
/// flags, so most planner inputs are defaults. Sharing the planner keeps
/// us in lock-step with chroma-frontend on segment-type dispatch.
async fn ensure_collection(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: DatabaseName,
    collection_name: &str,
) -> Result<Collection, ServerError> {
    let plan = plan_create_collection(
        None,
        None,
        ExecutorKind::Distributed,
        &supported_segment_types(ExecutorKind::Distributed),
        false,
        KnnIndex::Hnsw,
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
            None,
            None,
            GET_OR_CREATE,
        )
        .await?;
    Ok(collection)
}
