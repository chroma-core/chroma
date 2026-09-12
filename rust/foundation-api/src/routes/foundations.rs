//! Create, list and describe the Foundations one tenant holds.
//!
//! A Foundation is a Chroma database that holds a collection named `wiki`, so
//! these routes are database operations dressed as Foundation ones: create
//! provisions a database and everything a Foundation is made of, list reports
//! the tenant's databases that hold a Foundation, and describe reports what one
//! of them holds.
//!
//! Holding the wiki collection is the whole predicate, and list and describe
//! both decide by it, which is what keeps the two from disagreeing about a
//! name: a name the listing reports describes as provisioned, and a name it
//! omits describes as not.
//!
//! List and describe ask the frontend, carrying the caller's own token, so the
//! frontend's authorization decides which Foundations a caller sees. Describe
//! needs the get-database permission on top of the Foundation one, and holding
//! a Foundation permission does not imply it: a credential may be granted the
//! view-Foundation action and no database action at all, and a credential that
//! is granted the get-database action may carry it for the database the
//! configured default Foundation lives in alone. Such a credential describes
//! that Foundation and is refused another.
//!
//! Provisioning is the one path in this service that writes the system
//! database directly. It stays that way because the frontend exposes no
//! attached-function routes and a Foundation is made of collections and the
//! function that fills them, and the cost is that it bypasses the database
//! quota the frontend's create-database handler enforces.
//!
//! All three live under the static path segment `foundations`, which is why no
//! Foundation may be named that.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::DatabaseName;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::init::{
    provision_foundation, FoundationInitError, FoundationInitParams, FoundationInitResponse,
};
use super::whoami::{
    authenticate_path_tenant, authorize_scope, validate_foundation_name, ScopePolicy,
};
use super::{caller_token, FoundationScope};
use crate::{
    auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource},
    errors::ServerError,
    foundation_chroma::{FoundationChromaClient, FoundationChromaClientError},
    server::FoundationApiServer,
};

/// The tenant named in the path of the create and list routes.
#[derive(Debug, Deserialize)]
pub struct TenantPath {
    pub tenant: String,
}

/// The tenant and Foundation named in the path of the describe route.
#[derive(Debug, Deserialize)]
pub struct FoundationPath {
    pub tenant: String,
    pub name: String,
}

/// Request body for `POST /api/f/{tenant}/foundations`.
#[derive(Debug, Deserialize)]
pub struct CreateFoundationRequest {
    /// Name of the Foundation, which becomes the name of its database.
    pub name: String,
}

/// One Foundation in a tenant's listing.
///
/// A listing carries names. The id of the database a Foundation is comes from
/// describe, which reads the database record: reading it here would cost one
/// frontend call per Foundation and the get-database permission, which a caller
/// that may list Foundations need not hold.
#[derive(Debug, Serialize)]
pub struct FoundationSummary {
    pub name: String,
}

/// Answer to `GET /api/f/{tenant}/foundations`, ordered by name.
#[derive(Debug, Serialize)]
pub struct ListFoundationsResponse {
    pub foundations: Vec<FoundationSummary>,
}

/// Answer to `GET /api/f/{tenant}/foundations/{name}`.
#[derive(Debug, Serialize)]
pub struct DescribeFoundationResponse {
    pub tenant: String,
    pub name: String,
    pub database_id: String,
    /// Whether this database holds a Foundation at all.
    ///
    /// A tenant's key can address any database it owns, and a database becomes
    /// a Foundation only once it holds the wiki collection. False therefore
    /// means "this database exists and is yours, and it is not a Foundation" —
    /// a distinct answer from the 404 a database that does not exist gets.
    pub provisioned: bool,
}

/// Why a Foundation could not be read through the frontend.
#[derive(Debug, thiserror::Error)]
enum FoundationReadError {
    /// `frontend_ingress_url` is unset, so no client to the frontend was ever
    /// built and these routes have nothing to read through.
    #[error("foundation read is not configured")]
    RouteDisabled,
    /// The request carried no usable `x-chroma-token`. These routes read as the
    /// caller and never as the service, so without the caller's token there is
    /// nothing to read with.
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    /// The frontend refused a read this route made on the caller's behalf.
    ///
    /// Describe reads a database record as well as a collection, and the
    /// permission for the first is separate from the Foundation permission, so
    /// a caller that may view a Foundation can still be refused here. The
    /// refusal is reported as one, because a caller that reads this as a server
    /// fault goes looking for a fault there is none of.
    #[error("the caller's key does not carry the permission this read needs")]
    Refused,
    /// The frontend answered with a database other than the one the request
    /// named. Reporting it would answer a request addressed to one Foundation
    /// with another Foundation's name and id.
    #[error("requested database '{requested}' but the frontend answered with '{answered}'")]
    ForeignDatabase { requested: String, answered: String },
}

impl ChromaError for FoundationReadError {
    fn code(&self) -> ErrorCodes {
        match self {
            FoundationReadError::RouteDisabled => ErrorCodes::Internal,
            FoundationReadError::MissingToken => ErrorCodes::InvalidArgument,
            FoundationReadError::Refused => ErrorCodes::PermissionDenied,
            FoundationReadError::ForeignDatabase { .. } => ErrorCodes::Internal,
        }
    }
}

/// `POST /api/f/{tenant}/foundations` — provision a Foundation the caller
/// names.
///
/// Idempotent: every step of provisioning is get-or-create, so creating a
/// Foundation that already exists answers with the same ids and reports
/// `already_initialized: true` rather than a conflict.
#[tracing::instrument(name = "foundation_create", skip_all, err(Display))]
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
    let default_database = &server.config.foundation.database_name;

    // Two permissions, checked separately, because neither alone is enough.
    // The create-database permission is the one that is scoped to a database, so
    // it is what confines a key to the names it may claim; a Foundation
    // permission names no database and so accepts any name. The Foundation
    // permission is what says this key may build a Foundation at all. Checking
    // only the first would let any key that can make a database make a
    // Foundation; checking only the second would let a key holding one
    // Foundation permission mint databases under any name in its tenant. A warm
    // authorization cache serves the second check without a second network
    // round trip.
    //
    // Neither check counts the databases the tenant already holds. That quota
    // is enforced by the frontend's create-database handler, which this route
    // does not go through.
    let (tenant, database, identity) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::CreateDatabase,
        &scope,
        default_database,
        ScopePolicy::Required,
    )
    .await?;
    authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::InitFoundation,
        &scope,
        default_database,
        ScopePolicy::Required,
    )
    .await?;

    // The Foundation belongs to the tenant in the path; the owner of the
    // private per-member collections is whoever called.
    let user_id = identity.user_id;
    let _guard =
        server.scorecard_request(&["op:foundation_create", &format!("tenant:{}", tenant)])?;

    let db_name = DatabaseName::new(&database).ok_or(FoundationInitError::DatabaseNameTooShort)?;
    tracing::info!(
        tenant = %tenant,
        user_id = %user_id,
        database = %database,
        mock_wiki = params.mock_wiki,
        "foundation create starting"
    );

    Ok(Json(
        provision_foundation(&server, tenant, user_id, db_name, params.mock_wiki).await?,
    ))
}

/// `GET /api/f/{tenant}/foundations` — the Foundations in a tenant that the
/// caller may view.
///
/// Invariants:
/// 1. Every name reported passes two separate questions. The frontend answered
///    that the wiki collection is there for the caller's own token, and the
///    caller holds the view-Foundation permission against that database. The
///    first says the Foundation exists and the caller's data-plane claims reach
///    it; the second holds listing to the permission every other Foundation
///    read requires. The check names the database it decides, so a claim
///    carrying a database narrows to that one and a claim carrying none is
///    accepted for every database in the tenant. What separates one Foundation
///    from another for a claim that carries none is the data-plane claim the
///    frontend checks, which is why the first question is asked of the frontend
///    rather than answered here.
/// 2. A key that carries the whole tenant settles the candidates in one call
///    however many databases the tenant holds, because one search finds every
///    wiki collection in the tenant and each answer names the database holding
///    it.
/// 3. A key confined to databases is refused that search, and the refusal is
///    the only thing that sends this route down the per-database path. The set
///    it asks about is the set the key's own permissions name, which is small
///    by construction. A refusal earned by a key that names no database is
///    refused onward rather than answered with that empty set, because such a
///    key is confined to nothing and the refusal therefore means something
///    other than confinement.
/// 4. A refused permission check drops that one name. A listing reports the
///    Foundations the caller may view, so a Foundation it may not view is
///    absent from the answer rather than a refusal of the whole request.
/// 5. The search names no limit, which leaves the count to the frontend: a
///    deployment that enforces quotas answers at most the tenant's
///    list-collections limit. That limit is the ceiling on how many Foundations
///    one tenant can hold and still list all of them in one call.
#[tracing::instrument(name = "foundation_list", skip_all, err(Display))]
pub async fn foundation_list(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<TenantPath>,
) -> Result<Json<ListFoundationsResponse>, ServerError> {
    let identity = authenticate_path_tenant(&*server.auth, &headers, &path.tenant).await?;
    let tenant = path.tenant;
    let _guard =
        server.scorecard_request(&["op:foundation_list", &format!("tenant:{}", tenant)])?;

    let chroma = server
        .foundation_chroma_client
        .as_ref()
        .ok_or(FoundationReadError::RouteDisabled)?;
    let token = caller_token(&headers).ok_or(FoundationReadError::MissingToken)?;
    let wiki_collection = &server.config.foundation.wiki_collection;

    let candidates = match chroma
        .databases_holding(&tenant, token, wiki_collection)
        .await
    {
        Ok(databases) => databases,
        // A refusal is the frontend saying this key names a database of its
        // own, which is the one condition the per-database path answers. Every
        // other failure — a token the frontend will not accept, a frontend that
        // cannot answer, a connection that never opened — says nothing about
        // the key and is raised, because narrowing the search on it would
        // answer an outage with a short listing that reads as complete.
        Err(error) if error.is_refused() => {
            // A refusal says the key is confined to databases of its own. A key
            // whose permissions name none is confined to nothing, so a refusal
            // it earns cannot mean that, and answering it with the empty set
            // the key names would report "this tenant holds no Foundations" on
            // the strength of a refusal nobody can account for.
            if identity.databases.is_empty() {
                return Err(FoundationReadError::Refused.into());
            }
            databases_the_key_names_holding(
                chroma,
                &identity.databases,
                &tenant,
                token,
                wiki_collection,
            )
            .await?
        }
        Err(error) => return Err(error.into()),
    };

    let mut foundations = Vec::new();
    for name in candidates {
        if may_view(&*server.auth, &headers, &tenant, &name).await? {
            foundations.push(FoundationSummary { name });
        }
    }
    foundations.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(Json(ListFoundationsResponse { foundations }))
}

/// The databases among `named` that hold `collection_name`, asked one at a
/// time.
///
/// Invariants:
/// 1. `named` is the caller's reach — the database names its permissions carry
///    — so this costs one call per database the key names however many
///    Foundations the tenant holds.
/// 2. A database that answers holds the collection. One the frontend reports as
///    missing is not a Foundation, and one the frontend refuses is not this
///    caller's to see; both are left out of the answer.
/// 3. Every other failure is raised, so a frontend that cannot answer reads as
///    an error rather than as a tenant holding no Foundations.
/// 4. A name that is not a legal Foundation name is skipped without asking,
///    because it is interpolated into a frontend URL and no Foundation was ever
///    created under such a name.
async fn databases_the_key_names_holding(
    chroma: &FoundationChromaClient,
    named: &HashSet<String>,
    tenant: &str,
    token: &str,
    collection_name: &str,
) -> Result<Vec<String>, FoundationChromaClientError> {
    let mut holding = Vec::new();
    for database in named {
        // The name is interpolated into a frontend URL, so it has to be one
        // this service would accept as a Foundation name: a name carrying a
        // path separator addresses a different route than it spells. A name
        // create would refuse is a name no Foundation here was built under, so
        // nothing real is hidden by skipping it.
        if validate_foundation_name(database).is_err() {
            continue;
        }
        match chroma
            .uncached_collection(tenant, database, token, collection_name)
            .await
        {
            Ok(_) => holding.push(database.clone()),
            Err(error) if error.is_not_found() || error.is_refused() => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(holding)
}

/// Whether the caller may view the Foundation that `database` is.
///
/// Invariant: a refusal answers `false` and every other failure is raised. The
/// two are different answers — one says the caller may not see this Foundation,
/// the other says nothing could be decided — and reading a failure as a refusal
/// would drop a Foundation from a listing that claims to be complete.
async fn may_view(
    auth: &dyn AuthenticateAndAuthorize,
    headers: &HeaderMap,
    tenant: &str,
    database: &str,
) -> Result<bool, AuthError> {
    match auth
        .authenticate_and_authorize(
            headers,
            AuthzAction::ViewFoundation,
            AuthzResource {
                tenant: Some(tenant.to_string()),
                database: Some(database.to_string()),
                collection: None,
            },
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.0 == StatusCode::FORBIDDEN => Ok(false),
        Err(error) => Err(error),
    }
}

/// `GET /api/f/{tenant}/foundations/{name}` — what one Foundation holds.
///
/// Invariants:
/// 1. The caller is authorized against the named database before anything is
///    read, so every answer is one the caller's view-Foundation permission
///    covers.
/// 2. A tenant that holds no database under this name answers 404. A database
///    that exists and holds no wiki collection answers 200 with
///    `provisioned: false`, so a caller can tell a name nobody has used from a
///    name taken by something that is not a Foundation.
/// 3. Both reads go to the frontend with the caller's token, and the collection
///    read bypasses the collection cache. That cache is keyed on a tenant, a
///    database and a collection name and on no credential, so an entry one
///    caller's token populated would otherwise answer another caller's question
///    about a collection its own token may not reach.
/// 4. A read the frontend refuses answers as a refusal. Reading the database
///    record needs a permission the Foundation permission does not imply, so a
///    caller can clear the check above and be refused below, and that is an
///    answer about the caller's key rather than a fault to go looking for. The
///    frontend decides that before it looks the database up, so a name the
///    caller may not read answers the same whether or not it exists.
/// 5. The database the frontend answers with is the one the request named. A
///    record under another name would put that name and its id in an answer the
///    caller addressed elsewhere, so it is refused rather than reported.
#[tracing::instrument(name = "foundation_describe", skip_all, err(Display))]
pub async fn foundation_describe(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<FoundationPath>,
) -> Result<Json<DescribeFoundationResponse>, ServerError> {
    let scope = FoundationScope {
        tenant: Some(path.tenant),
        foundation: Some(path.name),
    };
    let (tenant, database, _identity) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::ViewFoundation,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::Required,
    )
    .await?;
    let _guard =
        server.scorecard_request(&["op:foundation_describe", &format!("tenant:{}", tenant)])?;

    let chroma = server
        .foundation_chroma_client
        .as_ref()
        .ok_or(FoundationReadError::RouteDisabled)?;
    let token = caller_token(&headers).ok_or(FoundationReadError::MissingToken)?;

    let stored = match chroma.database(&tenant, &database, token).await {
        Ok(stored) => stored,
        Err(error) if error.is_not_found() => {
            return Err(FoundationInitError::FoundationNotFound { name: database }.into())
        }
        Err(error) if error.is_refused() => return Err(FoundationReadError::Refused.into()),
        Err(error) => return Err(error.into()),
    };
    if stored.name != database {
        return Err(FoundationReadError::ForeignDatabase {
            requested: database,
            answered: stored.name,
        }
        .into());
    }

    let provisioned = match chroma
        .uncached_wiki_collection(&tenant, &database, token)
        .await
    {
        Ok(_) => true,
        Err(error) if error.is_not_found() => false,
        Err(error) if error.is_refused() => return Err(FoundationReadError::Refused.into()),
        Err(error) => return Err(error.into()),
    };

    Ok(Json(DescribeFoundationResponse {
        tenant,
        name: stored.name,
        database_id: stored.id,
        provisioned,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FoundationApiConfig;
    use crate::routes::test_auth::{expect_ok, server_with_config, FakeAuth};
    use crate::routes::CHROMA_TOKEN_HEADER;
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_types::{Collection, CollectionUuid};
    use httpmock::{Mock, MockServer};
    use std::sync::Arc;

    const TENANT: &str = "team_1";
    const WIKI: &str = "wiki";

    fn tenant_path() -> Path<TenantPath> {
        Path(TenantPath {
            tenant: TENANT.to_string(),
        })
    }

    fn foundation_path(name: &str) -> Path<FoundationPath> {
        Path(FoundationPath {
            tenant: TENANT.to_string(),
            name: name.to_string(),
        })
    }

    /// Request headers carrying the caller's token. Every read here goes to the
    /// frontend as the caller, so a request without one is refused before it
    /// reaches the frontend.
    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CHROMA_TOKEN_HEADER, "ck-token".parse().expect("ascii"));
        headers
    }

    /// A server whose frontend is `mock_server` and whose system database is
    /// empty, because neither list nor describe reads one.
    fn server_on(mock_server: &MockServer, auth: Arc<FakeAuth>) -> FoundationApiServer {
        let mut config = FoundationApiConfig::default();
        config.foundation.frontend_ingress_url = Some(mock_server.base_url());
        server_with_config(config, auth, SysDb::Test(TestSysDb::new()))
    }

    fn collection_in(name: &str, database: &str) -> Collection {
        Collection {
            collection_id: CollectionUuid::new(),
            name: name.to_string(),
            tenant: TENANT.to_string(),
            database: database.to_string(),
            ..Default::default()
        }
    }

    /// The tenant-wide search, answering with the wiki collection of each named
    /// database.
    async fn search_answers<'a>(mock_server: &'a MockServer, databases: &[&str]) -> Mock<'a> {
        let body = serde_json::to_value(
            databases
                .iter()
                .map(|database| collection_in(WIKI, database))
                .collect::<Vec<_>>(),
        )
        .expect("collections should serialize");
        mock_server
            .mock_async(move |when, then| {
                when.method("GET")
                    .path(format!("/api/v2/tenants/{TENANT}/collections"))
                    .query_param("name", WIKI);
                then.status(200).json_body(body.clone());
            })
            .await
    }

    /// The tenant-wide search, answering the way it answers a key confined to
    /// one database.
    async fn search_refuses(mock_server: &MockServer) -> Mock<'_> {
        search_fails_with(mock_server, 403).await
    }

    async fn search_fails_with(mock_server: &MockServer, status: u16) -> Mock<'_> {
        mock_server
            .mock_async(move |when, then| {
                when.method("GET")
                    .path(format!("/api/v2/tenants/{TENANT}/collections"))
                    .query_param("name", WIKI);
                then.status(status).json_body(serde_json::json!({
                    "error": "AuthError",
                    "message": "denied",
                }));
            })
            .await
    }

    /// The per-database collection lookup, answering with the collection when
    /// `present` and with a 404 otherwise.
    async fn collection_lookup<'a>(
        mock_server: &'a MockServer,
        database: &str,
        present: bool,
    ) -> Mock<'a> {
        let path = format!("/api/v2/tenants/{TENANT}/databases/{database}/collections/{WIKI}");
        let body = serde_json::to_value(collection_in(WIKI, database))
            .expect("a collection should serialize");
        mock_server
            .mock_async(move |when, then| {
                when.method("GET").path(path.clone());
                if present {
                    then.status(200).json_body(body.clone());
                } else {
                    then.status(404).json_body(serde_json::json!({
                        "error": "NotFoundError",
                        "message": "collection not found",
                    }));
                }
            })
            .await
    }

    /// The database lookup, answering with a record when `present` and with a
    /// 404 otherwise.
    async fn database_lookup<'a>(
        mock_server: &'a MockServer,
        database: &str,
        present: bool,
    ) -> Mock<'a> {
        let path = format!("/api/v2/tenants/{TENANT}/databases/{database}");
        let body = serde_json::json!({
            "id": "8f1c0a3e-0b6d-4a2f-9a1e-2f0c6d4b8a11",
            "name": database,
            "tenant": TENANT,
        });
        mock_server
            .mock_async(move |when, then| {
                when.method("GET").path(path.clone());
                if present {
                    then.status(200).json_body(body.clone());
                } else {
                    then.status(404).json_body(serde_json::json!({
                        "error": "NotFoundError",
                        "message": "database not found",
                    }));
                }
            })
            .await
    }

    /// A mock matching every request, so a test can assert a route answered
    /// without reaching the frontend.
    async fn any_request(mock_server: &MockServer) -> Mock<'_> {
        mock_server
            .mock_async(|when, then| {
                when.any_request();
                then.status(500);
            })
            .await
    }

    fn names(listed: &ListFoundationsResponse) -> Vec<&str> {
        listed
            .foundations
            .iter()
            .map(|foundation| foundation.name.as_str())
            .collect()
    }

    #[tokio::test]
    async fn create_authorizes_the_new_name_under_both_permissions() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let server = server_on(&mock_server, fake.clone());

        // No function endpoint is configured, so provisioning stops right after
        // both permission checks. What the route asked for is what is under
        // test here, not what it went on to build.
        let refused = foundation_create(
            HeaderMap::new(),
            State(server),
            tenant_path(),
            Query(FoundationInitParams::default()),
            Json(CreateFoundationRequest {
                name: "wiki_team".to_string(),
            }),
        )
        .await;
        assert!(
            refused.is_err(),
            "an unconfigured function endpoint should stop provisioning"
        );

        let authorizations = fake.authorizations();
        assert_eq!(
            authorizations.len(),
            2,
            "create must check both the create-database and the Foundation permission"
        );
        assert_eq!(authorizations[0].0, AuthzAction::CreateDatabase);
        assert_eq!(authorizations[1].0, AuthzAction::InitFoundation);
        for (action, resource) in &authorizations {
            assert_eq!(resource.tenant.as_deref(), Some(TENANT), "{action}");
            // Both checks name the database being created, not the configured
            // default one, so a key scoped away from this name is refused.
            assert_eq!(resource.database.as_deref(), Some("wiki_team"), "{action}");
            assert_eq!(resource.collection, None, "{action}");
        }
        // The tenant is in the path, so it is never looked up.
        assert_eq!(fake.identity_calls(), 0);
    }

    #[tokio::test]
    async fn create_refuses_an_illegal_name_before_authorizing() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let server = server_on(&mock_server, fake.clone());

        let refused = foundation_create(
            HeaderMap::new(),
            State(server),
            tenant_path(),
            Query(FoundationInitParams::default()),
            Json(CreateFoundationRequest {
                name: "foundations".to_string(),
            }),
        )
        .await;

        match refused {
            Ok(_) => panic!("the reserved name must not be creatable"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::InvalidArgument),
        }
        assert_eq!(fake.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn list_reports_every_database_the_tenant_wide_search_answers() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let search = search_answers(&mock_server, &["beta", "alpha"]).await;
        let server = server_on(&mock_server, fake);

        let listed = expect_ok(
            foundation_list(headers(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        assert_eq!(names(&listed), vec!["alpha", "beta"]);
        // One call settles the whole tenant, and the answer is ordered by name
        // whatever order the frontend answered in.
        assert_eq!(search.calls(), 1);
    }

    #[tokio::test]
    async fn list_authorizes_view_foundation_against_each_name() {
        // Listing requires the permission every other Foundation read requires,
        // asked against the database each Foundation is.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _search = search_answers(&mock_server, &["alpha", "beta"]).await;
        let server = server_on(&mock_server, fake.clone());

        let _listed = expect_ok(
            foundation_list(headers(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        let authorizations = fake.authorizations();
        assert_eq!(authorizations.len(), 2, "one check per candidate");
        let mut named: Vec<String> = authorizations
            .iter()
            .map(|(action, resource)| {
                assert_eq!(*action, AuthzAction::ViewFoundation);
                assert_eq!(resource.tenant.as_deref(), Some(TENANT));
                assert_eq!(resource.collection, None);
                resource
                    .database
                    .clone()
                    .expect("the check must name the Foundation it decides")
            })
            .collect();
        named.sort();
        assert_eq!(named, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[tokio::test]
    async fn list_drops_a_name_the_caller_may_not_view() {
        // A key may reach a database in the data plane and hold no Foundation
        // permission on it. The listing reports what the caller may view, so
        // such a name is absent rather than refusing the whole request.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT).refusing_databases(&["beta"]));
        let mock_server = MockServer::start_async().await;
        let _search = search_answers(&mock_server, &["alpha", "beta"]).await;
        let server = server_on(&mock_server, fake.clone());

        let listed = expect_ok(
            foundation_list(headers(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        assert_eq!(names(&listed), vec!["alpha"]);
        // Both names were candidates and both were checked, so the one that is
        // missing was dropped by its check rather than never asked about.
        assert_eq!(fake.authorizations().len(), 2);
    }

    #[tokio::test]
    async fn list_falls_back_to_the_databases_the_key_names_when_the_search_is_refused() {
        // A key confined to one database is refused the tenant-wide search,
        // and that refusal is the whole signal to ask about the databases the
        // key's own permissions name.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["beta"]));
        let mock_server = MockServer::start_async().await;
        let search = search_refuses(&mock_server).await;
        let beta = collection_lookup(&mock_server, "beta", true).await;
        let server = server_on(&mock_server, fake);

        let listed = expect_ok(
            foundation_list(headers(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        assert_eq!(names(&listed), vec!["beta"]);
        assert_eq!(search.calls(), 1);
        assert_eq!(beta.calls(), 1);
    }

    #[tokio::test]
    async fn list_keeps_only_the_named_databases_that_hold_a_wiki() {
        // The key names two databases and only one of them is a Foundation. A
        // database the frontend reports as holding no wiki collection is not
        // one, whatever else it holds.
        let fake =
            Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["beta", "orders"]));
        let mock_server = MockServer::start_async().await;
        let _search = search_refuses(&mock_server).await;
        let _beta = collection_lookup(&mock_server, "beta", true).await;
        let orders = collection_lookup(&mock_server, "orders", false).await;
        let server = server_on(&mock_server, fake);

        let listed = expect_ok(
            foundation_list(headers(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        assert_eq!(names(&listed), vec!["beta"]);
        assert_eq!(orders.calls(), 1);
    }

    #[tokio::test]
    async fn list_never_asks_about_a_name_that_could_not_be_a_foundation() {
        // A database name reaches the frontend inside a URL, and a name
        // carrying a path separator addresses a route other than the one it
        // spells. No Foundation exists under a name create would refuse, so the
        // question is never asked.
        let fake =
            Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["beta/../other"]));
        let mock_server = MockServer::start_async().await;
        let _search = search_refuses(&mock_server).await;
        let frontend = any_request(&mock_server).await;
        let server = server_on(&mock_server, fake);

        let listed = expect_ok(
            foundation_list(headers(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        assert!(listed.foundations.is_empty());
        // The search is answered by its own mock, so anything reaching this one
        // is a request built from the name above.
        assert_eq!(frontend.calls(), 0);
    }

    #[tokio::test]
    async fn list_raises_a_search_failure_that_is_not_a_refusal() {
        // Only a refusal narrows the question. A token the frontend will not
        // accept is a failure, and answering it with the databases the key
        // names would report a short listing as though it were complete.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["beta"]));
        let mock_server = MockServer::start_async().await;
        let search = search_fails_with(&mock_server, 401).await;
        let fallback = collection_lookup(&mock_server, "beta", true).await;
        let server = server_on(&mock_server, fake);

        let failed = foundation_list(headers(), State(server), tenant_path()).await;

        assert!(
            failed.is_err(),
            "a failure that is not a refusal must not be answered with a listing"
        );
        assert_eq!(
            search.calls(),
            1,
            "the failure under test has to be the one the search answered"
        );
        assert_eq!(
            fallback.calls(),
            0,
            "the per-database path must not run on a failure"
        );
    }

    #[tokio::test]
    async fn list_raises_a_refusal_a_key_naming_no_database_can_not_explain() {
        // The per-database path answers one condition: the key is confined to
        // databases of its own. A key whose permissions name none is confined to
        // nothing, so a refusal it earns means something else — a frontend that
        // could not settle the caller's quota answers the same 403 — and
        // answering it with that key's empty set would report the tenant as
        // holding no Foundations.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let search = search_refuses(&mock_server).await;
        let server = server_on(&mock_server, fake);

        let refused = foundation_list(headers(), State(server), tenant_path()).await;

        match refused {
            Ok(_) => panic!("an unexplained refusal must not answer with a listing"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::PermissionDenied),
        }
        assert_eq!(search.calls(), 1);
    }

    #[tokio::test]
    async fn list_asks_the_frontend_on_every_fallback_probe() {
        // The per-database probe answers whether this caller may see a
        // collection, so it reads past the collection cache for the same reason
        // describe does: the cache key names no credential.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["beta"]));
        let mock_server = MockServer::start_async().await;
        let _search = search_refuses(&mock_server).await;
        let beta = collection_lookup(&mock_server, "beta", true).await;
        let server = server_on(&mock_server, fake);

        for _ in 0..2 {
            let listed = expect_ok(
                foundation_list(headers(), State(server.clone()), tenant_path()).await,
                "listing should succeed",
            );
            assert_eq!(names(&listed), vec!["beta"]);
        }

        assert_eq!(beta.calls(), 2);
    }

    #[tokio::test]
    async fn list_refuses_a_tenant_the_caller_does_not_own() {
        let fake = Arc::new(FakeAuth::new("user_1", "team_other"));
        let mock_server = MockServer::start_async().await;
        let frontend = any_request(&mock_server).await;
        let server = server_on(&mock_server, fake);

        let refused = foundation_list(headers(), State(server), tenant_path()).await;

        match refused {
            Ok(_) => panic!("a tenant the caller does not own must not list"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::PermissionDenied),
        }
        // The refusal is settled here, so nothing about the other tenant is
        // ever asked for.
        assert_eq!(frontend.calls(), 0);
    }

    #[tokio::test]
    async fn list_refuses_a_request_carrying_no_token() {
        // These routes read as the caller. Without the caller's token there is
        // no credential to read with, and reading as the service is what this
        // route exists to stop doing.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let frontend = any_request(&mock_server).await;
        let server = server_on(&mock_server, fake);

        let refused = foundation_list(HeaderMap::new(), State(server), tenant_path()).await;

        match refused {
            Ok(_) => panic!("a request with no token must not list"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::InvalidArgument),
        }
        assert_eq!(frontend.calls(), 0);
    }

    #[tokio::test]
    async fn list_and_describe_agree_about_a_database_that_holds_no_wiki() {
        // The two routes answer one question, so a name absent from the listing
        // must be a name describe calls unprovisioned.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _search = search_answers(&mock_server, &[]).await;
        let _database = database_lookup(&mock_server, "customer_db", true).await;
        let _collection = collection_lookup(&mock_server, "customer_db", false).await;
        let server = server_on(&mock_server, fake);

        let listed = expect_ok(
            foundation_list(headers(), State(server.clone()), tenant_path()).await,
            "listing should succeed",
        );
        assert!(listed.foundations.is_empty());

        let described = expect_ok(
            foundation_describe(headers(), State(server), foundation_path("customer_db")).await,
            "describing a database the tenant holds should succeed",
        );
        assert!(!described.provisioned);
        assert_eq!(_database.calls(), 1);
        assert_eq!(_collection.calls(), 1);
    }

    #[tokio::test]
    async fn describe_authorizes_the_named_foundation() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _database = database_lookup(&mock_server, "wiki_team", true).await;
        let _collection = collection_lookup(&mock_server, "wiki_team", true).await;
        let server = server_on(&mock_server, fake.clone());

        let _described = expect_ok(
            foundation_describe(headers(), State(server), foundation_path("wiki_team")).await,
            "describing a database the tenant holds should succeed",
        );

        assert_eq!(fake.captured_action(), AuthzAction::ViewFoundation);
        let resource = fake.captured_resource();
        assert_eq!(resource.tenant.as_deref(), Some(TENANT));
        assert_eq!(resource.database.as_deref(), Some("wiki_team"));
    }

    #[tokio::test]
    async fn describe_refuses_a_caller_the_permission_check_refuses() {
        let fake = Arc::new(FakeAuth::refusing(StatusCode::FORBIDDEN));
        let mock_server = MockServer::start_async().await;
        let frontend = any_request(&mock_server).await;
        let server = server_on(&mock_server, fake);

        let refused =
            foundation_describe(headers(), State(server), foundation_path("wiki_team")).await;

        match refused {
            Ok(_) => panic!("a refused caller must not describe"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::PermissionDenied),
        }
        // Nothing is read before the permission is settled, so a refused caller
        // learns nothing about the name it asked for.
        assert_eq!(frontend.calls(), 0);
    }

    #[tokio::test]
    async fn describe_answers_not_found_for_a_name_the_tenant_does_not_hold() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _database = database_lookup(&mock_server, "wiki_team", false).await;
        let collection = collection_lookup(&mock_server, "wiki_team", true).await;
        let server = server_on(&mock_server, fake);

        let refused =
            foundation_describe(headers(), State(server), foundation_path("wiki_team")).await;

        match refused {
            Ok(_) => panic!("a database the tenant does not hold must not describe"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::NotFound),
        }
        assert_eq!(
            collection.calls(),
            0,
            "a database that does not exist holds nothing to ask about"
        );
    }

    #[tokio::test]
    async fn describe_reports_a_provisioned_foundation() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _database = database_lookup(&mock_server, "wiki_team", true).await;
        let _collection = collection_lookup(&mock_server, "wiki_team", true).await;
        let server = server_on(&mock_server, fake);

        let described = expect_ok(
            foundation_describe(headers(), State(server), foundation_path("wiki_team")).await,
            "describing a provisioned Foundation should succeed",
        );

        assert!(described.provisioned);
        assert_eq!(described.tenant, TENANT);
        assert_eq!(described.name, "wiki_team");
        assert_eq!(
            described.database_id,
            "8f1c0a3e-0b6d-4a2f-9a1e-2f0c6d4b8a11"
        );
    }

    #[tokio::test]
    async fn describe_separates_a_plain_database_from_a_provisioned_foundation() {
        // A tenant's key can address any database it owns, so describe has to
        // answer for a database that is not a Foundation rather than pretend it
        // is absent.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _database = database_lookup(&mock_server, "plain_db", true).await;
        let _collection = collection_lookup(&mock_server, "plain_db", false).await;
        let server = server_on(&mock_server, fake);

        let described = expect_ok(
            foundation_describe(headers(), State(server), foundation_path("plain_db")).await,
            "describing a database the tenant holds should succeed",
        );

        assert!(!described.provisioned);
        assert_eq!(described.name, "plain_db");
        // The answer is the frontend's, so the collection has to have been
        // asked about rather than assumed absent.
        assert_eq!(_collection.calls(), 1);
    }

    #[tokio::test]
    async fn describe_refuses_a_database_record_under_another_name() {
        // The answer carries a name and an id. A record under a name the
        // request did not address would report another Foundation's identity to
        // a caller that asked about this one.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _database = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(format!("/api/v2/tenants/{TENANT}/databases/wiki_team"));
                then.status(200).json_body(serde_json::json!({
                    "id": "8f1c0a3e-0b6d-4a2f-9a1e-2f0c6d4b8a11",
                    "name": "other_foundation",
                    "tenant": TENANT,
                }));
            })
            .await;
        let collection = collection_lookup(&mock_server, "wiki_team", true).await;
        let server = server_on(&mock_server, fake);

        let refused =
            foundation_describe(headers(), State(server), foundation_path("wiki_team")).await;

        match refused {
            Ok(_) => panic!("a record under another name must not be reported"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::Internal),
        }
        assert_eq!(
            collection.calls(),
            0,
            "nothing more is read once the record is refused"
        );
    }

    #[tokio::test]
    async fn describe_asks_the_frontend_on_every_call() {
        // The collection cache is keyed on no credential, so an entry the first
        // call left would answer the second one whoever made it. Describe reads
        // past the cache, which is what makes two calls cost two lookups.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let _database = database_lookup(&mock_server, "wiki_team", true).await;
        let collection = collection_lookup(&mock_server, "wiki_team", true).await;
        let server = server_on(&mock_server, fake);

        for _ in 0..2 {
            let described = expect_ok(
                foundation_describe(
                    headers(),
                    State(server.clone()),
                    foundation_path("wiki_team"),
                )
                .await,
                "describing a provisioned Foundation should succeed",
            );
            assert!(described.provisioned);
        }

        assert_eq!(collection.calls(), 2);
    }

    #[tokio::test]
    async fn describe_reports_a_refused_database_read_as_a_refusal() {
        // Reading the database record needs a permission the Foundation
        // permission does not imply, so this is an answer about the caller's
        // key. A missing database is an absent Foundation; a refused read is
        // not, and reporting it as a server fault sends a reader looking for a
        // fault there is none of.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mock_server = MockServer::start_async().await;
        let database = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(format!("/api/v2/tenants/{TENANT}/databases/wiki_team"));
                then.status(403).json_body(serde_json::json!({
                    "error": "AuthError",
                    "message": "denied",
                }));
            })
            .await;
        let server = server_on(&mock_server, fake);

        let failed =
            foundation_describe(headers(), State(server), foundation_path("wiki_team")).await;

        match failed {
            Ok(_) => panic!("a refused database read must not answer as a Foundation"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::PermissionDenied),
        }
        assert_eq!(database.calls(), 1);
    }
}
