//! Product catalog routes. Identity comes from the Foundation product's
//! registry; collections describe its storage and never establish its identity.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use chroma_types::DatabaseName;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::init::{
    provision_foundation, FoundationInitError, FoundationInitParams, FoundationInitResponse,
};
use super::whoami::{
    authenticate_path_tenant, authorize_scope, validate_foundation_name, ScopePolicy,
};
use super::{caller_token, FoundationScope};
use crate::{
    auth::AuthzAction,
    errors::ServerError,
    registry::{FoundationRecord, FoundationState, RegistryError, ReserveFoundation},
    server::FoundationApiServer,
};

#[derive(Debug, Deserialize)]
pub struct TenantPath {
    pub tenant: String,
}
#[derive(Debug, Deserialize)]
pub struct FoundationPath {
    pub tenant: String,
    #[serde(rename = "foundation")]
    pub name: String,
}
#[derive(Debug, Deserialize)]
pub struct CreateFoundationRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ListFoundationParams {
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}
fn default_limit() -> u32 {
    100
}

#[derive(Debug, Serialize)]
pub struct ListFoundationsResponse {
    pub foundations: Vec<FoundationRecord>,
    pub next_offset: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct DescribeFoundationResponse {
    #[serde(flatten)]
    pub foundation: FoundationRecord,
    /// Provisioning completion belongs to the product record. Missing wiki
    /// storage is a separate health signal and does not erase this identity.
    pub provisioned: bool,
    pub storage_available: bool,
}

/// Create a Foundation at `POST /api/tenants/{tenant}/foundations`.
pub async fn foundation_create(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<TenantPath>,
    Query(params): Query<FoundationInitParams>,
    Json(request): Json<CreateFoundationRequest>,
) -> Result<Json<FoundationInitResponse>, ServerError> {
    let scope = FoundationScope {
        tenant: Some(path.tenant),
        foundation: Some(request.name),
    };
    let (tenant, database, identity) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::CreateDatabase,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::Required,
    )
    .await?;
    authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::InitFoundation,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::Required,
    )
    .await?;
    let _guard =
        server.scorecard_request(&["op:foundation_create", &format!("tenant:{tenant}")])?;
    let db_name = DatabaseName::new(&database).ok_or(FoundationInitError::DatabaseNameTooShort)?;
    Ok(Json(
        provision_foundation(
            &server,
            &headers,
            tenant,
            identity.user_id,
            db_name,
            params.mock_wiki,
            false,
        )
        .await?,
    ))
}

/// List Foundations at `GET /api/tenants/{tenant}/foundations`.
pub async fn foundation_list(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<TenantPath>,
    Query(params): Query<ListFoundationParams>,
) -> Result<Json<ListFoundationsResponse>, ServerError> {
    authenticate_path_tenant(&*server.auth, &headers, &path.tenant).await?;
    if params.limit == 0 || params.limit > 100 {
        return Err(
            RegistryError::InvalidArgument("limit must be between 1 and 100".into()).into(),
        );
    }
    let _guard =
        server.scorecard_request(&["op:foundation_list", &format!("tenant:{}", path.tenant)])?;
    // The product catalog applies canonical caller permissions before paging.
    // The identity response's union of database claims is not an access filter.
    let page = server
        .foundation_registry
        .list(&headers, &path.tenant, params.limit, params.offset)
        .await?;
    for record in &page.foundations {
        validate_record(record, &path.tenant, &record.name)?;
        if record.state == FoundationState::Deleted {
            return Err(
                RegistryError::Unavailable("catalog returned a deleted Foundation".into()).into(),
            );
        }
    }
    if page.foundations.len() > params.limit as usize
        || page.next_offset.is_some_and(|next| next <= params.offset)
    {
        return Err(
            RegistryError::Unavailable("catalog returned invalid pagination".into()).into(),
        );
    }
    Ok(Json(ListFoundationsResponse {
        foundations: page.foundations,
        next_offset: page.next_offset,
    }))
}

/// Describe `GET /api/tenants/{tenant}/foundations/{foundation}`.
pub async fn foundation_describe(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<FoundationPath>,
) -> Result<Json<DescribeFoundationResponse>, ServerError> {
    let scope = FoundationScope {
        tenant: Some(path.tenant),
        foundation: Some(path.name),
    };
    let (tenant, name, _) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::ViewFoundation,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::Required,
    )
    .await?;
    let record = server
        .foundation_registry
        .get(&headers, &tenant, &name)
        .await?;
    validate_record(&record, &tenant, &name)?;
    let storage_available = storage_matches(&server, &headers, &record).await?;
    Ok(Json(DescribeFoundationResponse {
        provisioned: record.state == FoundationState::Ready,
        foundation: record,
        storage_available,
    }))
}

pub(crate) fn validate_record(
    record: &FoundationRecord,
    tenant: &str,
    name: &str,
) -> Result<(), RegistryError> {
    if record.tenant != tenant
        || record.name != name
        || record.id.is_nil()
        || record.database_id.is_nil()
    {
        return Err(RegistryError::Conflict);
    }
    validate_foundation_name(&record.name).map_err(RegistryError::InvalidArgument)?;
    if record.state == FoundationState::Deleted {
        return Err(RegistryError::NotFound);
    }
    Ok(())
}

/// Only a confirmed absence or a different UUID makes storage unavailable.
/// Authorization failures and transport failures remain errors.
async fn storage_matches(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    record: &FoundationRecord,
) -> Result<bool, ServerError> {
    let chroma = server
        .foundation_chroma_client
        .as_ref()
        .ok_or(RegistryError::Unconfigured)?;
    let token = caller_token(headers).ok_or(RegistryError::Unauthorized)?;
    match chroma.database(&record.tenant, &record.name, token).await {
        Ok(stored) => Ok(stored.name == record.name
            && Uuid::parse_str(&stored.id).ok() == Some(record.database_id)),
        Err(error) if error.is_not_found() => Ok(false),
        Err(error) if error.is_refused() => Err(RegistryError::Forbidden.into()),
        Err(error) => Err(error.into()),
    }
}

/// Every memory operation resolves the product identity before using storage.
/// A reused database name can never pass the immutable UUID check.
pub(crate) async fn require_ready_foundation(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    name: &str,
) -> Result<(), ServerError> {
    let record = server
        .foundation_registry
        .get(headers, tenant, name)
        .await?;
    validate_record(&record, tenant, name)?;
    if record.state != FoundationState::Ready {
        return Err(RegistryError::NotReady.into());
    }
    if !storage_matches(server, headers, &record).await? {
        return Err(RegistryError::NotReady.into());
    }
    Ok(())
}

pub(crate) async fn reserve_for_provisioning(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    name: &str,
    adopt_default: bool,
) -> Result<FoundationRecord, ServerError> {
    let database_id = if adopt_default {
        if name != server.config.foundation.database_name {
            return Err(RegistryError::Conflict.into());
        }
        let chroma = server
            .foundation_chroma_client
            .as_ref()
            .ok_or(RegistryError::Unconfigured)?;
        let token = caller_token(headers).ok_or(RegistryError::Unauthorized)?;
        match chroma.database(tenant, name, token).await {
            Ok(stored) if stored.name == name => {
                Some(Uuid::parse_str(&stored.id).map_err(|_| RegistryError::Conflict)?)
            }
            Ok(_) => return Err(RegistryError::Conflict.into()),
            Err(error) if error.is_not_found() => None,
            Err(error) if error.is_refused() => return Err(RegistryError::Forbidden.into()),
            Err(error) => return Err(error.into()),
        }
    } else {
        None
    };
    let record = server
        .foundation_registry
        .reserve(
            headers,
            ReserveFoundation {
                tenant: tenant.into(),
                name: name.into(),
                database_id,
            },
        )
        .await?;
    validate_record(&record, tenant, name)?;
    if database_id.is_some_and(|id| id != record.database_id) {
        return Err(RegistryError::Conflict.into());
    }
    match record.state {
        FoundationState::Provisioning => {}
        FoundationState::Ready => {
            if !storage_matches(server, headers, &record).await? {
                return Err(RegistryError::NotReady.into());
            }
        }
        _ => return Err(RegistryError::NotReady.into()),
    }
    Ok(record)
}

#[cfg(test)]
pub(super) mod tests;
