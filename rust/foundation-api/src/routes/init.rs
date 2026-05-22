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
    auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource},
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

/// `POST /api/foundation/init` — idempotent bootstrap for a team's Foundation
/// workspace. Ensures the configured Foundation database and the wiki +
/// wiki_revisions collections (names overridable via
/// `CHROMA_FOUNDATION__*` env vars) exist in the tenant resolved from the
/// auth context. Safe to call repeatedly.
pub async fn foundation_init(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
) -> Result<Json<FoundationInitResponse>, ServerError> {
    let tenant = whoami_and_authorize(&*server.auth, &headers, AuthzAction::CreateDatabase).await?;

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

/// Resolve the caller's tenant from the auth token, then check that the
/// caller is allowed to perform `action` against that tenant.
///
/// The two-step shape exists because the Cloud `authenticate_and_authorize`
/// impl enforces `resource.tenant == user_identity.tenant` (returns 403 on
/// mismatch, including `resource.tenant == None`). The Noop impl ignores
/// resource entirely, which is why a single-call handler that passed
/// `tenant: None` looked fine in tests but 403'd under Cloud auth.
async fn whoami_and_authorize(
    auth: &dyn AuthenticateAndAuthorize,
    headers: &HeaderMap,
    action: AuthzAction,
) -> Result<String, AuthError> {
    let identity = auth.get_user_identity(headers).await?;
    let tenant = identity.tenant.clone();
    auth.authenticate_and_authorize(
        headers,
        action,
        AuthzResource {
            tenant: Some(tenant.clone()),
            database: None,
            collection: None,
        },
    )
    .await?;
    Ok(tenant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_api_types::GetUserIdentityResponse;
    use std::collections::HashSet;
    use std::future::{ready, Future};
    use std::pin::Pin;
    use std::sync::Mutex;

    /// Fake `AuthenticateAndAuthorize` that returns a fixed tenant from
    /// `get_user_identity` and records the `AuthzResource` passed to
    /// `authenticate_and_authorize` so tests can assert on it.
    struct FakeAuth {
        tenant: String,
        captured_resource: Mutex<Option<AuthzResource>>,
    }

    impl FakeAuth {
        fn new(tenant: &str) -> Self {
            Self {
                tenant: tenant.to_string(),
                captured_resource: Mutex::new(None),
            }
        }

        fn identity(&self) -> GetUserIdentityResponse {
            GetUserIdentityResponse {
                user_id: String::new(),
                tenant: self.tenant.clone(),
                databases: HashSet::new(),
            }
        }
    }

    impl AuthenticateAndAuthorize for FakeAuth {
        fn authenticate_and_authorize(
            &self,
            _headers: &axum::http::HeaderMap,
            _action: AuthzAction,
            resource: AuthzResource,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            *self.captured_resource.lock().unwrap() = Some(resource);
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }

        fn authenticate_and_authorize_collection(
            &self,
            _headers: &axum::http::HeaderMap,
            _action: AuthzAction,
            _resource: AuthzResource,
            _collection: chroma_types::Collection,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }

        fn get_user_identity(
            &self,
            _headers: &axum::http::HeaderMap,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }
    }

    #[tokio::test]
    async fn whoami_and_authorize_passes_resolved_tenant_to_authz() {
        let fake = FakeAuth::new("team_abc");
        let headers = HeaderMap::new();

        let tenant = whoami_and_authorize(&fake, &headers, AuthzAction::CreateDatabase)
            .await
            .expect("auth should succeed");

        assert_eq!(tenant, "team_abc");
        let captured = fake
            .captured_resource
            .lock()
            .unwrap()
            .clone()
            .expect("authenticate_and_authorize should have been called");
        // Regression: before this fix the handler passed `tenant: None`,
        // which the Cloud authz impl always rejects with 403.
        assert_eq!(captured.tenant, Some("team_abc".to_string()));
        assert_eq!(captured.database, None);
        assert_eq!(captured.collection, None);
    }
}
