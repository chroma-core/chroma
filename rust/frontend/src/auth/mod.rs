use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::future::{ready, Future};
use std::pin::Pin;

use axum::http::HeaderMap;
use axum::http::StatusCode;

use chroma_api_types::GetUserIdentityResponse;
use chroma_types::Collection;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzAction {
    Reset,
    CreateTenant,
    GetTenant,
    UpdateTenant,
    CreateDatabase,
    GetDatabase,
    DeleteDatabase,
    ListDatabases,
    ListCollections,
    CountCollections,
    CreateCollection,
    GetOrCreateCollection,
    GetCollection,
    GetCollectionByCrn,
    UpdateCollection,
    DeleteCollection,
    ForkCollection,
    Add,
    Delete,
    Get,
    Query,
    Count,
    Update,
    Upsert,
    Search,
    CreateAttachedFunction,
    RemoveAttachedFunction,
}

impl Display for AuthzAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AuthzAction::Reset => write!(f, "system:reset"),
            AuthzAction::CreateTenant => write!(f, "tenant:create_tenant"),
            AuthzAction::GetTenant => write!(f, "tenant:get_tenant"),
            AuthzAction::UpdateTenant => write!(f, "tenant:update_tenant"),
            AuthzAction::CreateDatabase => write!(f, "db:create_database"),
            AuthzAction::GetDatabase => write!(f, "db:get_database"),
            AuthzAction::DeleteDatabase => write!(f, "db:delete_database"),
            AuthzAction::ListDatabases => write!(f, "db:list_databases"),
            AuthzAction::ListCollections => write!(f, "db:list_collections"),
            AuthzAction::CountCollections => write!(f, "db:count_collections"),
            AuthzAction::CreateCollection => write!(f, "db:create_collection"),
            AuthzAction::GetOrCreateCollection => write!(f, "db:get_or_create_collection"),
            AuthzAction::GetCollection => write!(f, "collection:get_collection"),
            AuthzAction::GetCollectionByCrn => write!(f, "collection:get_collection_by_crn"),
            AuthzAction::UpdateCollection => write!(f, "collection:update_collection"),
            AuthzAction::DeleteCollection => write!(f, "collection:delete_collection"),
            AuthzAction::ForkCollection => write!(f, "collection:fork_collection"),
            AuthzAction::Add => write!(f, "collection:add"),
            AuthzAction::Delete => write!(f, "collection:delete"),
            AuthzAction::Get => write!(f, "collection:get"),
            AuthzAction::Query => write!(f, "collection:query"),
            AuthzAction::Count => write!(f, "collection:count"),
            AuthzAction::Update => write!(f, "collection:update"),
            AuthzAction::Upsert => write!(f, "collection:upsert"),
            AuthzAction::Search => write!(f, "collection:search"),
            AuthzAction::CreateAttachedFunction => write!(f, "collection:create_attached_function"),
            AuthzAction::RemoveAttachedFunction => write!(f, "collection:remove_attached_function"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AuthzResource {
    pub tenant: Option<String>,
    pub database: Option<String>,
    pub collection: Option<String>,
}

#[derive(thiserror::Error, Debug)]
#[error("Permission denied.")]
pub struct AuthError(pub StatusCode);

impl chroma_error::ChromaError for AuthError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self.0 {
            StatusCode::UNAUTHORIZED => chroma_error::ErrorCodes::Unauthenticated,
            StatusCode::FORBIDDEN => chroma_error::ErrorCodes::PermissionDenied,
            _ => chroma_error::ErrorCodes::Internal,
        }
    }
}

pub trait AuthenticateAndAuthorize: Send + Sync {
    fn authenticate_and_authorize(
        &self,
        _headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>;

    fn authenticate_and_authorize_collection(
        &self,
        _headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
        _collection: Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>;

    fn get_user_identity(
        &self,
        _headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>;
}

fn default_identity() -> GetUserIdentityResponse {
    GetUserIdentityResponse {
        user_id: String::new(),
        tenant: "default_tenant".to_string(),
        databases: HashSet::from(["default_database".to_string()]),
    }
}

impl AuthenticateAndAuthorize for () {
    fn authenticate_and_authorize(
        &self,
        _headers: &HeaderMap,
        _action: AuthzAction,
        _resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(Ok::<GetUserIdentityResponse, AuthError>(
            default_identity(),
        )))
    }

    fn authenticate_and_authorize_collection(
        &self,
        _headers: &HeaderMap,
        _action: AuthzAction,
        _resource: AuthzResource,
        _collection: Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(Ok::<GetUserIdentityResponse, AuthError>(
            default_identity(),
        )))
    }

    fn get_user_identity(
        &self,
        _headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(Ok::<GetUserIdentityResponse, AuthError>(
            default_identity(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthzAction, AuthzResource};
    use serde_json::{json, to_value};

    #[test]
    fn test_serialize_authz_action() {
        let action = AuthzAction::DeleteCollection;
        let json = to_value(action).unwrap();
        assert_eq!(json, json!("delete_collection"));
    }

    #[test]
    fn test_serialize_authz_resource_with_all_fields() {
        let resource = AuthzResource {
            tenant: Some("my_tenant".to_string()),
            database: Some("my_db".to_string()),
            collection: Some("my_collection".to_string()),
        };

        let json = to_value(&resource).unwrap();
        assert_eq!(
            json,
            json!({
                "tenant": "my_tenant",
                "database": "my_db",
                "collection": "my_collection"
            })
        );
    }

    #[test]
    fn test_serialize_authz_resource_with_none_fields() {
        let resource = AuthzResource {
            tenant: None,
            database: Some("db_only".to_string()),
            collection: None,
        };

        let json = to_value(&resource).unwrap();
        assert_eq!(
            json,
            json!({
                "tenant": null,
                "database": "db_only",
                "collection": null
            })
        );
    }
}
