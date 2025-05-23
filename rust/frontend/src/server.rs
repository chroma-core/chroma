use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header::HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router, ServiceExt,
};
use chroma_system::System;
use chroma_types::{
    AddCollectionRecordsResponse, ChecklistResponse, Collection, CollectionConfiguration,
    CollectionMetadataUpdate, CollectionUuid, CountCollectionsRequest, CountCollectionsResponse,
    CountRequest, CountResponse, CreateCollectionRequest, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    DeleteCollectionRecordsResponse, DeleteDatabaseRequest, DeleteDatabaseResponse,
    GetCollectionRequest, GetDatabaseRequest, GetDatabaseResponse, GetRequest, GetResponse,
    GetTenantRequest, GetTenantResponse, GetUserIdentityResponse, HeartbeatResponse, IncludeList,
    InternalCollectionConfiguration, ListCollectionsRequest, ListCollectionsResponse,
    ListDatabasesRequest, ListDatabasesResponse, Metadata, QueryRequest, QueryResponse,
    UpdateCollectionConfiguration, UpdateCollectionRecordsResponse, UpdateCollectionResponse,
    UpdateMetadata, UpsertCollectionRecordsResponse,
};
use chroma_types::{ForkCollectionResponse, RawWhereFields};
use mdac::{Rule, Scorecard, ScorecardTicket};
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{select, signal};
use tower_http::cors::CorsLayer;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa::ToSchema;
use utoipa::{Modify, OpenApi};
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::{
    ac::AdmissionControlledService,
    auth::{AuthenticateAndAuthorize, AuthzAction, AuthzResource},
    config::FrontendServerConfig,
    quota::{Action, QuotaEnforcer, QuotaPayload},
    server_middleware::{always_json_errors_middleware, default_json_content_type_middleware},
    tower_tracing::add_tracing_middleware,
    traced_json::TracedJson,
    types::errors::{ErrorResponse, ServerError, ValidationError},
    Frontend,
};

struct ScorecardGuard {
    scorecard: Arc<Scorecard<'static>>,
    ticket: Option<ScorecardTicket>,
}

impl Drop for ScorecardGuard {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.scorecard.untrack(ticket);
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Too many requests; backoff and try again")]
struct RateLimitError;

impl chroma_error::ChromaError for RateLimitError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::ResourceExhausted
    }
}

async fn graceful_shutdown(system: System) {
    select! {
        // Kubernetes will send SIGTERM to stop the pod gracefully
        // TODO: add more signal handling
        _ = signal::ctrl_c() => {
            system.stop().await;
            system.join().await;
        },
    }
}

pub struct Metrics {
    healthcheck: Counter<u64>,
    heartbeat: Counter<u64>,
    pre_flight_checks: Counter<u64>,
    reset: Counter<u64>,
    version: Counter<u64>,
    get_user_identity: Counter<u64>,
    create_tenant: Counter<u64>,
    get_tenant: Counter<u64>,
    list_databases: Counter<u64>,
    create_database: Counter<u64>,
    get_database: Counter<u64>,
    delete_database: Counter<u64>,
    create_collection: Counter<u64>,
    list_collections: Counter<u64>,
    count_collections: Counter<u64>,
    get_collection: Counter<u64>,
    update_collection: Counter<u64>,
    delete_collection: Counter<u64>,
    fork_collection: Counter<u64>,
    collection_add: Counter<u64>,
    collection_update: Counter<u64>,
    collection_upsert: Counter<u64>,
    collection_delete: Counter<u64>,
    collection_count: Counter<u64>,
    collection_get: Counter<u64>,
    collection_query: Counter<u64>,
}

impl Metrics {
    pub fn new(meter: Meter) -> Metrics {
        Metrics {
            healthcheck: meter.u64_counter("healthcheck").build(),
            heartbeat: meter.u64_counter("heartbeat").build(),
            pre_flight_checks: meter.u64_counter("pre_flight_checks").build(),
            reset: meter.u64_counter("reset").build(),
            version: meter.u64_counter("version").build(),
            get_user_identity: meter.u64_counter("get_user_identity").build(),
            create_tenant: meter.u64_counter("create_tenant").build(),
            get_tenant: meter.u64_counter("get_tenant").build(),
            list_databases: meter.u64_counter("list_databases").build(),
            create_database: meter.u64_counter("create_database").build(),
            get_database: meter.u64_counter("get_database").build(),
            delete_database: meter.u64_counter("delete_database").build(),
            create_collection: meter.u64_counter("create_collection").build(),
            list_collections: meter.u64_counter("list_collections").build(),
            count_collections: meter.u64_counter("count_collections").build(),
            get_collection: meter.u64_counter("get_collection").build(),
            update_collection: meter.u64_counter("update_collection").build(),
            delete_collection: meter.u64_counter("delete_collection").build(),
            fork_collection: meter.u64_counter("fork_collection").build(),
            collection_add: meter.u64_counter("collection_add").build(),
            collection_update: meter.u64_counter("collection_update").build(),
            collection_upsert: meter.u64_counter("collection_upsert").build(),
            collection_delete: meter.u64_counter("collection_delete").build(),
            collection_count: meter.u64_counter("collection_count").build(),
            collection_get: meter.u64_counter("collection_get").build(),
            collection_query: meter.u64_counter("collection_query").build(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FrontendServer {
    config: FrontendServerConfig,
    frontend: Frontend,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
    metrics: Arc<Metrics>,
    auth: Arc<dyn AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    system: System,
}

impl FrontendServer {
    pub fn new(
        config: FrontendServerConfig,
        frontend: Frontend,
        rules: Vec<Rule>,
        auth: Arc<dyn AuthenticateAndAuthorize>,
        quota_enforcer: Arc<dyn QuotaEnforcer>,
        system: System,
    ) -> FrontendServer {
        // NOTE(rescrv):  Assume statically no more than 128 threads because we won't deploy on
        // hardware with that many threads anytime soon for frontends, if ever.
        let scorecard_enabled = Arc::new(AtomicBool::new(config.scorecard_enabled));
        // SAFETY(rescrv):  This is safe because 128 is non-zero.
        let scorecard = Arc::new(Scorecard::new(&(), rules, 128.try_into().unwrap()));
        let metrics = Arc::new(Metrics::new(global::meter("chroma")));
        FrontendServer {
            config,
            frontend,
            scorecard_enabled,
            scorecard,
            metrics,
            auth,
            quota_enforcer,
            system,
        }
    }

    pub async fn run(self) {
        let system = self.system.clone();

        let FrontendServerConfig {
            port,
            listen_address,
            max_payload_size_bytes,
            circuit_breaker,
            cors_allow_origins,
            ..
        } = self.config.clone();

        let (docs_router, docs_api) =
            OpenApiRouter::with_openapi(ApiDoc::openapi()).split_for_parts();

        let docs_router = docs_router.merge(SwaggerUi::new("/docs").url("/openapi.json", docs_api));

        let app = Router::new()
            // `GET /` goes to `root`
            .route(
                "/api/v1/{*any}",
                get(v1_deprecation_notice)
                    .put(v1_deprecation_notice)
                    .patch(v1_deprecation_notice)
                    .delete(v1_deprecation_notice)
                    .head(v1_deprecation_notice)
                    .options(v1_deprecation_notice),
            )
            .route("/api/v2", get(heartbeat))
            .route("/api/v2/healthcheck", get(healthcheck))
            .route("/api/v2/heartbeat", get(heartbeat))
            .route("/api/v2/pre-flight-checks", get(pre_flight_checks))
            .route("/api/v2/reset", post(reset))
            .route("/api/v2/version", get(version))
            .route("/api/v2/auth/identity", get(get_user_identity))
            .route("/api/v2/tenants", post(create_tenant))
            .route("/api/v2/tenants/{tenant_name}", get(get_tenant))
            .route(
                "/api/v2/tenants/{tenant}/databases",
                get(list_databases).post(create_database),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}",
                get(get_database).delete(delete_database),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections",
                post(create_collection).get(list_collections),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections_count",
                get(count_collections),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
                get(get_collection)
                    .put(update_collection)
                    .delete(delete_collection),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/fork",
                post(fork_collection),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/add",
                post(collection_add),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/update",
                post(collection_update),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/upsert",
                post(collection_upsert),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/delete",
                post(collection_delete),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/count",
                get(collection_count),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/get",
                post(collection_get),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/query",
                post(collection_query),
            )
            .merge(docs_router)
            .with_state(self)
            .layer(DefaultBodyLimit::max(max_payload_size_bytes))
            .layer(axum::middleware::from_fn(
                default_json_content_type_middleware,
            ))
            .layer(axum::middleware::from_fn(always_json_errors_middleware));

        let mut app = add_tracing_middleware(app);

        if let Some(cors_allow_origins) = cors_allow_origins {
            let origins = cors_allow_origins
                .into_iter()
                .map(|origin| {
                    origin
                        .parse()
                        .unwrap_or_else(|_| panic!("Invalid origin: {}", origin))
                })
                .collect::<Vec<_>>();

            let mut cors_builder = CorsLayer::new()
                .allow_headers(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any);
            if origins.len() == 1 && origins[0] == "*" {
                cors_builder = cors_builder.allow_origin(tower_http::cors::Any);
            } else {
                cors_builder = cors_builder.allow_origin(origins);
            }

            app = app.layer(cors_builder);
        }

        // TODO: tracing
        let addr = format!("{}:{}", listen_address, port);
        println!("Listening on {addr}");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        if circuit_breaker.enabled() {
            let service = AdmissionControlledService::new(circuit_breaker, app);
            axum::serve(listener, service.into_make_service())
                .with_graceful_shutdown(graceful_shutdown(system))
                .await
                .unwrap();
        } else {
            axum::serve(listener, app)
                .with_graceful_shutdown(graceful_shutdown(system))
                .await
                .unwrap();
        };
    }

    fn scorecard_request(
        &self,
        tags: &[&str],
    ) -> Result<ScorecardGuard, Box<dyn chroma_error::ChromaError>> {
        if self.scorecard_enabled.load(Ordering::Relaxed) {
            self.scorecard
                .track(tags)
                .map(|ticket| ScorecardGuard {
                    scorecard: Arc::clone(&self.scorecard),
                    ticket: Some(ticket),
                })
                .ok_or_else(|| Box::new(RateLimitError) as _)
        } else {
            Ok(ScorecardGuard {
                scorecard: Arc::clone(&self.scorecard),
                ticket: None,
            })
        }
    }
}

impl FrontendServer {
    async fn authenticate_and_authorize(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Result<(), ServerError> {
        Ok(self
            .auth
            .authenticate_and_authorize(headers, action, resource)
            .await?)
    }

    // This is used to authenticate API operations that are collection-specific.
    // We need to send additional collection info to the auth service.
    async fn authenticate_and_authorize_collection(
        &mut self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
        collection_id: CollectionUuid,
    ) -> Result<(), ServerError> {
        let collection = self.frontend.get_cached_collection(collection_id).await?;
        Ok(self
            .auth
            .authenticate_and_authorize_collection(headers, action, resource, collection)
            .await?)
    }
}

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

/// Health check endpoint that returns 200 if the server and executor are ready
#[utoipa::path(
    get,
    path = "/api/v2/healthcheck",
    responses(
        (status = 200, description = "Success", body = String, content_type = "application/json"),
        (status = 503, description = "Service Unavailable", body = ErrorResponse),
    )
)]
async fn healthcheck(State(server): State<FrontendServer>) -> impl IntoResponse {
    server.metrics.healthcheck.add(1, &[]);
    let res = server.frontend.healthcheck().await;
    let code = match res.get_status_code() {
        tonic::Code::Ok => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (code, Json(res))
}

/// Heartbeat endpoint that returns a nanosecond timestamp of the current time.
#[utoipa::path(
    get,
    path = "/api/v2/heartbeat",
    responses(
        (status = 200, description = "Success", body = HeartbeatResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn heartbeat(
    State(server): State<FrontendServer>,
) -> Result<Json<HeartbeatResponse>, ServerError> {
    server.metrics.heartbeat.add(1, &[]);
    Ok(Json(server.frontend.heartbeat().await?))
}

/// Pre-flight checks endpoint reporting basic readiness info.
#[utoipa::path(
    get,
    path = "/api/v2/pre-flight-checks",
    responses(
        (status = 200, description = "Pre flight checks", body = ChecklistResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn pre_flight_checks(
    State(server): State<FrontendServer>,
) -> Result<Json<ChecklistResponse>, ServerError> {
    server.metrics.pre_flight_checks.add(1, &[]);
    Ok(Json(ChecklistResponse {
        max_batch_size: server.frontend.clone().get_max_batch_size(),
    }))
}

/// Reset endpoint allowing authorized users to reset the database.
#[utoipa::path(
    post,
    path = "/api/v2/reset",
    responses(
        (status = 200, description = "Reset successful", body = bool),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn reset(
    headers: HeaderMap,
    State(mut server): State<FrontendServer>,
) -> Result<Json<bool>, ServerError> {
    server.metrics.reset.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Reset,
            AuthzResource {
                tenant: None,
                database: None,
                collection: None,
            },
        )
        .await?;
    server.frontend.reset().await?;
    Ok(Json(true))
}

/// Returns the version of the server.
#[utoipa::path(
    get,
    path = "/api/v2/version",
    responses(
        (status = 200, description = "Get server version", body = String)
    )
)]
async fn version(State(server): State<FrontendServer>) -> Json<String> {
    server.metrics.version.add(1, &[]);
    // TODO: Decide on how to handle versioning across python / rust frontend
    // for now return a hardcoded version
    Json("1.0.0".to_string())
}

/// Retrieves the current user's identity, tenant, and databases.
#[utoipa::path(
    get,
    path = "/api/v2/auth/identity",
    responses(
        (status = 200, description = "Get user identity", body = GetUserIdentityResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn get_user_identity(
    headers: HeaderMap,
    State(server): State<FrontendServer>,
) -> Result<Json<GetUserIdentityResponse>, ServerError> {
    server.metrics.get_user_identity.add(1, &[]);
    Ok(Json(server.auth.get_user_identity(&headers).await?))
}

#[derive(Deserialize, Debug, ToSchema)]
struct CreateTenantPayload {
    name: String,
}

/// Creates a new tenant.
#[utoipa::path(
    post,
    path = "/api/v2/tenants",
    request_body = CreateTenantPayload,
    responses(
        (status = 200, description = "Tenant created successfully", body = CreateTenantResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn create_tenant(
    headers: HeaderMap,
    State(mut server): State<FrontendServer>,
    Json(request): Json<CreateTenantPayload>,
) -> Result<Json<CreateTenantResponse>, ServerError> {
    server.metrics.create_tenant.add(1, &[]);
    tracing::info!(name: "create_tenant", tenant_name = %request.name);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateTenant,
            AuthzResource {
                tenant: Some(request.name.clone()),
                database: None,
                collection: None,
            },
        )
        .await?;
    let request = CreateTenantRequest::try_new(request.name)?;
    Ok(Json(server.frontend.create_tenant(request).await?))
}

/// Returns an existing tenant by name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_name}",
    params(
        ("tenant_name" = String, Path, description = "Tenant name or ID to retrieve")
    ),
    responses(
        (status = 200, description = "Tenant found", body = GetTenantResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Tenant not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn get_tenant(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetTenantResponse>, ServerError> {
    server.metrics.get_tenant.add(1, &[]);
    tracing::info!(name: "get_tenant", tenant_name = %name);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetTenant,
            AuthzResource {
                tenant: Some(name.clone()),
                database: None,
                collection: None,
            },
        )
        .await?;
    let request = GetTenantRequest::try_new(name)?;
    Ok(Json(server.frontend.get_tenant(request).await?))
}

#[derive(Deserialize, Serialize, ToSchema, Debug)]
pub struct CreateDatabasePayload {
    pub name: String,
}

/// Creates a new database for a given tenant.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases",
    request_body = CreateDatabasePayload,
    responses(
        (status = 200, description = "Database created successfully", body = CreateDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID to associate with the new database")
    )
)]
async fn create_database(
    headers: HeaderMap,
    Path(tenant): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(CreateDatabasePayload { name }): Json<CreateDatabasePayload>,
) -> Result<Json<CreateDatabaseResponse>, ServerError> {
    server
        .metrics
        .create_database
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "create_database", tenant_name = %tenant, database_name = %name);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateDatabase,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(name.clone()),
                collection: None,
            },
        )
        .await?;
    // Enforce quota.
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::CreateDatabase, tenant.clone(), api_token);
    quota_payload = quota_payload.with_collection_name(&name);
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard =
        server.scorecard_request(&["op:create_database", format!("tenant:{}", tenant).as_str()])?;
    let create_database_request = CreateDatabaseRequest::try_new(tenant, name)?;
    let res = server
        .frontend
        .create_database(create_database_request)
        .await?;
    Ok(Json(res))
}

#[derive(Deserialize, ToSchema, Debug)]
struct ListDatabasesParams {
    limit: Option<u32>,
    #[serde(default)]
    offset: u32,
}

/// Lists all databases for a given tenant.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases",
    responses(
        (status = 200, description = "List of databases", body = ListDatabasesResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID to list databases for"),
        ("limit" = Option<u32>, Query, description = "Limit for pagination"),
        ("offset" = Option<u32>, Query, description = "Offset for pagination")
    )
)]
async fn list_databases(
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Query(ListDatabasesParams { limit, offset }): Query<ListDatabasesParams>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListDatabasesResponse>, ServerError> {
    server
        .metrics
        .list_databases
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "list_databases", tenant_name = %tenant);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::ListDatabases,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: None,
                collection: None,
            },
        )
        .await?;
    let _guard =
        server.scorecard_request(&["op:list_databases", format!("tenant:{}", tenant).as_str()])?;

    let request = ListDatabasesRequest::try_new(tenant, limit, offset)?;
    Ok(Json(server.frontend.list_databases(request).await?))
}

/// Retrieves a specific database by name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}",
    responses(
        (status = 200, description = "Database retrieved successfully", body = GetDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Database not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Name of the database to retrieve")
    )
)]
async fn get_database(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponse>, ServerError> {
    server
        .metrics
        .get_database
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "get_database", tenant_name = %tenant, database_name = %database);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetDatabase,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: None,
            },
        )
        .await?;
    let _guard =
        server.scorecard_request(&["op:get_database", format!("tenant:{}", tenant).as_str()])?;
    let request = GetDatabaseRequest::try_new(tenant, database)?;
    let res = server.frontend.get_database(request).await?;
    Ok(Json(res))
}

/// Deletes a specific database.
#[utoipa::path(
    delete,
    path = "/api/v2/tenants/{tenant}/databases/{database}",
    responses(
        (status = 200, description = "Database deleted successfully", body = DeleteDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Database not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Name of the database to delete")
    )
)]
async fn delete_database(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<DeleteDatabaseResponse>, ServerError> {
    server
        .metrics
        .delete_database
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "delete_database", tenant_name = %tenant, database_name = %database);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::DeleteDatabase,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: None,
            },
        )
        .await?;
    let _guard =
        server.scorecard_request(&["op:delete_database", format!("tenant:{}", tenant).as_str()])?;
    let request = DeleteDatabaseRequest::try_new(tenant, database)?;
    Ok(Json(server.frontend.delete_database(request).await?))
}

#[derive(Deserialize, Debug)]
struct ListCollectionsParams {
    limit: Option<u32>,
    #[serde(default)]
    offset: u32,
}

/// Lists all collections in the specified database.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections",
    responses(
        (status = 200, description = "List of collections", body = ListCollectionsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name to list collections from"),
        ("limit" = Option<u32>, Query, description = "Limit for pagination"),
        ("offset" = Option<u32>, Query, description = "Offset for pagination")
    )
)]
async fn list_collections(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    Query(ListCollectionsParams { limit, offset }): Query<ListCollectionsParams>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListCollectionsResponse>, ServerError> {
    server
        .metrics
        .list_collections
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "list_collections", tenant_name = %tenant, database_name = %database, limit = ?limit, offset = ?offset);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::ListCollections,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: None,
            },
        )
        .await?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::ListCollections, tenant.clone(), api_token);
    if let Some(limit) = limit {
        quota_payload = quota_payload.with_limit(limit);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server
        .scorecard_request(&["op:list_collections", format!("tenant:{}", tenant).as_str()])?;
    let request = ListCollectionsRequest::try_new(tenant, database, limit, offset)?;
    Ok(Json(server.frontend.list_collections(request).await?))
}

/// Retrieves the total number of collections in a given database.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections_count",
    responses(
        (status = 200, description = "Count of collections", body = CountCollectionsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name to count collections from")
    )
)]
async fn count_collections(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountCollectionsResponse>, ServerError> {
    server
        .metrics
        .count_collections
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "count_collections", tenant_name = %tenant, database_name = %database);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CountCollections,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: None,
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:count_collections",
        format!("tenant:{}", tenant).as_str(),
    ])?;

    let request = CountCollectionsRequest::try_new(tenant, database)?;
    Ok(Json(server.frontend.count_collections(request).await?))
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct CreateCollectionPayload {
    pub name: String,
    pub configuration: Option<CollectionConfiguration>,
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub get_or_create: bool,
}

/// Creates a new collection under the specified database.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections",
    request_body = CreateCollectionPayload,
    responses(
        (status = 200, description = "Collection created successfully", body = Collection),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name containing the new collection")
    )
)]
async fn create_collection(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateCollectionPayload>,
) -> Result<Json<Collection>, ServerError> {
    server
        .metrics
        .create_collection
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "create_collection", tenant_name = %tenant, database_name = %database);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateCollection,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(payload.name.clone()),
            },
        )
        .await?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::CreateCollection, tenant.clone(), api_token);
    quota_payload = quota_payload.with_collection_name(&payload.name);
    if let Some(metadata) = &payload.metadata {
        quota_payload = quota_payload.with_create_collection_metadata(metadata);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:create_collection",
        format!("tenant:{}", tenant).as_str(),
    ])?;

    let payload_clone = payload.clone();

    let configuration = match payload_clone.configuration {
        Some(c) => Some(InternalCollectionConfiguration::try_from_config(
            c,
            server.config.frontend.default_knn_index,
        )?),
        None => Some(InternalCollectionConfiguration::try_from_config(
            CollectionConfiguration::default(),
            server.config.frontend.default_knn_index,
        )?),
    };

    let request = CreateCollectionRequest::try_new(
        tenant,
        database,
        payload.name,
        payload.metadata,
        configuration,
        payload.get_or_create,
    )?;
    let collection = server.frontend.create_collection(request).await?;

    Ok(Json(collection))
}

/// Retrieves a collection by ID or name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
    responses(
        (status = 200, description = "Collection found", body = Collection),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection")
    )
)]
async fn get_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    server
        .metrics
        .get_collection
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "get_collection", tenant_name = %tenant, database_name = %database, collection_name = %collection_name);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetCollection,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_name.clone()),
            },
        )
        .await?;
    let _guard =
        server.scorecard_request(&["op:get_collection", format!("tenant:{}", tenant).as_str()])?;
    let request = GetCollectionRequest::try_new(tenant, database, collection_name)?;
    let collection = server.frontend.get_collection(request).await?;
    Ok(Json(collection))
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct UpdateCollectionPayload {
    pub new_name: Option<String>,
    pub new_metadata: Option<UpdateMetadata>,
    pub new_configuration: Option<UpdateCollectionConfiguration>,
}

/// Updates an existing collection's name or metadata.
#[utoipa::path(
    put,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
    request_body = UpdateCollectionPayload,
    responses(
        (status = 200, description = "Collection updated successfully", body = UpdateCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection to update")
    )
)]
async fn update_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionPayload>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server.metrics.update_collection.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    tracing::info!(name: "update_collection", tenant_name = %tenant, database_name = %database, collection_id = %collection_id);
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::UpdateCollection,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::UpdateCollection, tenant.clone(), api_token);
    if let Some(new_name) = &payload.new_name {
        quota_payload = quota_payload.with_collection_new_name(new_name);
    }
    if let Some(new_metadata) = &payload.new_metadata {
        quota_payload = quota_payload.with_update_collection_metadata(new_metadata);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:update_collection",
        format!("tenant:{}", tenant).as_str(),
    ])?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;

    let request = chroma_types::UpdateCollectionRequest::try_new(
        collection_id,
        payload.new_name,
        payload
            .new_metadata
            .map(CollectionMetadataUpdate::UpdateMetadata),
        payload.new_configuration,
    )?;

    server.frontend.update_collection(request).await?;

    Ok(Json(UpdateCollectionResponse {}))
}

/// Deletes a collection in a given database.
#[utoipa::path(
    delete,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
    responses(
        (status = 200, description = "Collection deleted successfully", body = UpdateCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection to delete")
    )
)]
async fn delete_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server
        .metrics
        .delete_collection
        .add(1, &[KeyValue::new("tenant", tenant.clone())]);
    tracing::info!(name: "delete_collection", tenant_name = %tenant, database_name = %database);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::DeleteCollection,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_name.clone()),
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:delete_collection",
        format!("tenant:{}", tenant).as_str(),
    ])?;
    let request =
        chroma_types::DeleteCollectionRequest::try_new(tenant, database, collection_name)?;
    server.frontend.delete_collection(request).await?;

    Ok(Json(UpdateCollectionResponse {}))
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct ForkCollectionPayload {
    pub new_name: String,
}

/// Forks an existing collection.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/fork",
    request_body = ForkCollectionPayload,
    responses(
        (status = 200, description = "Collection forked successfully", body = ForkCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection to update")
    )
)]
async fn fork_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<ForkCollectionPayload>,
) -> Result<Json<ForkCollectionResponse>, ServerError> {
    server.metrics.fork_collection.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    tracing::info!(name: "fork_collection", tenant_name = %tenant, database_name = %database, collection_id = %collection_id);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::ForkCollection,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;

    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let mut quota_payload = QuotaPayload::new(Action::ForkCollection, tenant.clone(), api_token);
    quota_payload = quota_payload.with_collection_uuid(collection_id);
    quota_payload = quota_payload.with_collection_name(&payload.new_name);
    server.quota_enforcer.enforce(&quota_payload).await?;

    let _guard =
        server.scorecard_request(&["op:fork_collection", format!("tenant:{}", tenant).as_str()])?;

    let request = chroma_types::ForkCollectionRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.new_name,
    )?;

    Ok(Json(server.frontend.fork_collection(request).await?))
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct AddCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<Metadata>>>,
}

impl AddCollectionRecordsPayload {
    pub fn new(
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Self {
        Self {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        }
    }
}

/// Adds records to a collection.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/add",
    request_body = AddCollectionRecordsPayload,
    responses(
        (status = 201, description = "Collection added successfully", body = AddCollectionRecordsResponse),
        (status = 400, description = "Invalid data for collection addition")
    )
)]
async fn collection_add(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<AddCollectionRecordsPayload>,
) -> Result<(StatusCode, Json<AddCollectionRecordsResponse>), ServerError> {
    server.metrics.collection_add.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Add,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Add, tenant.clone(), api_token);
    quota_payload = quota_payload.with_ids(&payload.ids);
    if let Some(embeddings) = &payload.embeddings {
        quota_payload = quota_payload.with_add_embeddings(embeddings);
    }
    if let Some(metadatas) = &payload.metadatas {
        quota_payload = quota_payload.with_metadatas(metadatas);
    }
    if let Some(documents) = &payload.documents {
        quota_payload = quota_payload.with_documents(documents);
    }
    if let Some(uris) = &payload.uris {
        quota_payload = quota_payload.with_uris(uris);
    }
    quota_payload = quota_payload.with_collection_uuid(collection_id);
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    tracing::info!(name: "collection_add", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.len());
    let request = chroma_types::AddCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        payload.embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    let res = server.frontend.add(request).await?;

    Ok((StatusCode::CREATED, Json(res)))
}

#[derive(Deserialize, Debug, Clone, ToSchema, Serialize)]
pub struct UpdateCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Option<Vec<f32>>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

/// Updates records in a collection by ID.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/update",
    request_body = UpdateCollectionRecordsPayload,
    responses(
        (status = 200, description = "Collection updated successfully", body = UpdateCollectionRecordsResponse),
        (status = 404, description = "Collection not found")
    )
)]
async fn collection_update(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionRecordsPayload>,
) -> Result<Json<UpdateCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_update.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Update,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Update, tenant.clone(), api_token);
    quota_payload = quota_payload.with_ids(&payload.ids);
    if let Some(embeddings) = &payload.embeddings {
        quota_payload = quota_payload.with_update_embeddings(embeddings);
    }
    if let Some(metadatas) = &payload.metadatas {
        quota_payload = quota_payload.with_update_metadatas(metadatas);
    }
    if let Some(documents) = &payload.documents {
        quota_payload = quota_payload.with_documents(documents);
    }
    if let Some(uris) = &payload.uris {
        quota_payload = quota_payload.with_uris(uris);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    tracing::info!(name: "collection_update", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.len());
    let request = chroma_types::UpdateCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        payload.embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    Ok(Json(server.frontend.update(request).await?))
}

#[derive(Deserialize, Debug, Clone, ToSchema, Serialize)]
pub struct UpsertCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

/// Upserts records in a collection (create if not exists, otherwise update).
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/upsert",
    request_body = UpsertCollectionRecordsPayload,
    responses(
        (status = 200, description = "Records upserted successfully", body = UpsertCollectionRecordsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse),
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection ID"),
    )
)]
async fn collection_upsert(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpsertCollectionRecordsPayload>,
) -> Result<Json<UpsertCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_upsert.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Update,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Upsert, tenant.clone(), api_token);
    quota_payload = quota_payload.with_ids(&payload.ids);
    if let Some(embeddings) = &payload.embeddings {
        quota_payload = quota_payload.with_add_embeddings(embeddings);
    }
    if let Some(metadatas) = &payload.metadatas {
        quota_payload = quota_payload.with_update_metadatas(metadatas);
    }
    if let Some(documents) = &payload.documents {
        quota_payload = quota_payload.with_documents(documents);
    }
    if let Some(uris) = &payload.uris {
        quota_payload = quota_payload.with_uris(uris);
    }
    quota_payload = quota_payload.with_collection_uuid(collection_id);
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    tracing::info!(name: "collection_upsert", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.len());
    let request = chroma_types::UpsertCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        payload.embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    Ok(Json(server.frontend.upsert(request).await?))
}

#[derive(Deserialize, Debug, Clone, ToSchema, Serialize)]
pub struct DeleteCollectionRecordsPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
}

/// Deletes records in a collection. Can filter by IDs or metadata.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/delete",
    request_body = DeleteCollectionRecordsPayload,
    responses(
        (status = 200, description = "Records deleted successfully", body = DeleteCollectionRecordsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse),
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection ID"),
    )
)]
async fn collection_delete(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<DeleteCollectionRecordsPayload>,
) -> Result<Json<DeleteCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_delete.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Delete,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let r#where = payload.where_fields.parse()?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Delete, tenant.clone(), api_token);
    if let Some(ids) = &payload.ids {
        quota_payload = quota_payload.with_ids(ids);
    }
    if let Some(r#where) = &r#where {
        quota_payload = quota_payload.with_where(r#where);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;
    tracing::info!(name: "collection_delete", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.as_ref().map_or(0, |ids| ids.len()), has_where = r#where.is_some());
    let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        r#where,
    )?;

    server.frontend.delete(request).await?;

    Ok(Json(DeleteCollectionRecordsResponse {}))
}

/// Retrieves the number of records in a collection.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/count",
    responses(
        (status = 200, description = "Number of records in the collection", body = CountResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID for the collection"),
        ("database" = String, Path, description = "Database containing this collection"),
        ("collection_id" = String, Path, description = "Collection ID whose records are counted")
    )
)]
async fn collection_count(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountResponse>, ServerError> {
    server.metrics.collection_count.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    tracing::info!(
        name: "collection_count",
        tenant = tenant,
        database = database,
        collection_id = collection_id
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Count,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    let request = CountRequest::try_new(
        tenant,
        database,
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
    )?;

    Ok(Json(server.frontend.count(request).await?))
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct GetRequestPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
    limit: Option<u32>,
    offset: Option<u32>,
    #[serde(default = "IncludeList::default_get")]
    include: IncludeList,
}

/// Retrieves records from a collection by ID or metadata filter.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/get",
    request_body = GetRequestPayload,
    responses(
        (status = 200, description = "Records retrieved from the collection", body = GetResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name for the collection"),
        ("collection_id" = String, Path, description = "Collection ID to fetch records from")
    )
)]
async fn collection_get(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<GetRequestPayload>,
) -> Result<Json<GetResponse>, ServerError> {
    server.metrics.collection_get.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Get,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let parsed_where = payload.where_fields.parse()?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Get, tenant.clone(), api_token);
    if let Some(ids) = &payload.ids {
        quota_payload = quota_payload.with_ids(ids);
    }
    if let Some(r#where) = &parsed_where {
        quota_payload = quota_payload.with_where(r#where);
    }
    if let Some(limit) = payload.limit {
        quota_payload = quota_payload.with_limit(limit);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    tracing::info!(
        name: "collection_get",
        num_ids = payload.ids.as_ref().map_or(0, |ids| ids.len()),
        include = ?payload.include,
        has_where = parsed_where.is_some(),
    );
    let request = GetRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        parsed_where,
        payload.limit,
        payload.offset.unwrap_or(0),
        payload.include,
    )?;
    let res = server.frontend.get(request).await?;
    Ok(Json(res))
}

#[derive(Deserialize, Debug, Clone, Serialize, ToSchema)]
pub struct QueryRequestPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
    query_embeddings: Vec<Vec<f32>>,
    n_results: Option<u32>,
    #[serde(default = "IncludeList::default_query")]
    include: IncludeList,
}

/// Query a collection in a variety of ways, including vector search, metadata filtering, and full-text search
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/query",
    request_body = QueryRequestPayload,
    responses(
        (status = 200, description = "Records matching the query", body = QueryResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse),
    ),
    params(
        ("tenant" = String, Path, description = "Tenant ID"),
        ("database" = String, Path, description = "Database name containing the collection"),
        ("collection_id" = String, Path, description = "Collection ID to query"),
        ("limit" = Option<u32>, Query, description = "Limit for pagination"),
        ("offset" = Option<u32>, Query, description = "Offset for pagination")
    )
)]
async fn collection_query(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<QueryRequestPayload>,
) -> Result<Json<QueryResponse>, ServerError> {
    server.metrics.collection_query.add(
        1,
        &[
            KeyValue::new("tenant", tenant.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Query,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let parsed_where = payload.where_fields.parse()?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Query, tenant.clone(), api_token);
    if let Some(ids) = &payload.ids {
        quota_payload = quota_payload.with_ids(ids);
    }
    if let Some(r#where) = &parsed_where {
        quota_payload = quota_payload.with_where(r#where);
    }
    quota_payload = quota_payload.with_query_embeddings(&payload.query_embeddings);
    if let Some(n_results) = payload.n_results {
        quota_payload = quota_payload.with_n_results(n_results);
    }
    if let Some(ids) = &payload.ids {
        quota_payload = quota_payload.with_query_ids(ids);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    tracing::info!(
        name: "collection_query",
        num_ids = payload.ids.as_ref().map_or(0, |ids| ids.len()),
        num_embeddings = payload.query_embeddings.len(),
        include = ?payload.include,
        has_where = parsed_where.is_some(),
    );
    let request = QueryRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        parsed_where,
        payload.query_embeddings,
        payload.n_results.unwrap_or(10),
        payload.include,
    )?;

    let res = server.frontend.query(request).await?;

    Ok(Json(res))
}

async fn v1_deprecation_notice() -> Response {
    let err_response = ErrorResponse::new(
        "Unimplemented".to_string(),
        "The v1 API is deprecated. Please use /v2 apis".to_string(),
    );
    (StatusCode::GONE, Json(err_response)).into_response()
}

////////////////////////////////////////////////////////////
/// OpenAPI
////////////////////////////////////////////////////////////
struct ChromaTokenSecurityAddon;
impl Modify for ChromaTokenSecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        // NOTE(philipithomas) - This unwrap is usually safe, and will crash the service on initialization if it's not.
        let components = openapi
            .components
            .as_mut()
            .expect("It should be able to get components as mutable");
        components.add_security_scheme(
            "x-chroma-token",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("x-chroma-token"))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        healthcheck,
        heartbeat,
        pre_flight_checks,
        reset,
        version,
        get_user_identity,
        create_tenant,
        get_tenant,
        list_databases,
        create_database,
        get_database,
        delete_database,
        create_collection,
        list_collections,
        count_collections,
        get_collection,
        update_collection,
        delete_collection,
        fork_collection,
        collection_add,
        collection_update,
        collection_upsert,
        collection_delete,
        collection_count,
        collection_get,
        collection_query
    ),
    // Apply our new security scheme here
    modifiers(&ChromaTokenSecurityAddon)
)]
struct ApiDoc;

#[cfg(test)]
mod tests {
    use crate::{config::FrontendServerConfig, Frontend, FrontendServer};
    use chroma_config::{registry::Registry, Configurable};
    use chroma_system::System;
    use std::sync::Arc;

    async fn test_server() -> u16 {
        let registry = Registry::new();
        let system = System::new();

        let port = random_port::PortPicker::new().random(true).pick().unwrap();

        let mut config = FrontendServerConfig::single_node_default();
        config.port = port;

        let frontend = Frontend::try_from_config(&(config.clone().frontend, system), &registry)
            .await
            .unwrap();
        let app = FrontendServer::new(
            config,
            frontend,
            vec![],
            Arc::new(()),
            Arc::new(()),
            System::new(),
        );
        tokio::task::spawn(async move {
            app.run().await;
        });

        port
    }

    #[tokio::test]
    async fn test_cors() {
        let registry = Registry::new();
        let system = System::new();

        let port = random_port::PortPicker::new().pick().unwrap();

        let mut config = FrontendServerConfig::single_node_default();
        config.port = port;
        config.cors_allow_origins = Some(vec!["http://localhost:3000".to_string()]);

        let frontend = Frontend::try_from_config(&(config.clone().frontend, system), &registry)
            .await
            .unwrap();
        let app = FrontendServer::new(
            config,
            frontend,
            vec![],
            Arc::new(()),
            Arc::new(()),
            System::new(),
        );
        tokio::task::spawn(async move {
            app.run().await;
        });

        let client = reqwest::Client::new();
        let res = client
            .request(
                reqwest::Method::OPTIONS,
                format!("http://localhost:{}/api/v2/heartbeat", port),
            )
            .header("Origin", "http://localhost:3000")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let allow_origin = res.headers().get("Access-Control-Allow-Origin");
        assert_eq!(allow_origin.unwrap(), "http://localhost:3000");

        let allow_methods = res.headers().get("Access-Control-Allow-Methods");
        assert_eq!(allow_methods.unwrap(), "*");

        let allow_headers = res.headers().get("Access-Control-Allow-Headers");
        assert_eq!(allow_headers.unwrap(), "*");
    }

    #[tokio::test]
    async fn test_cors_wildcard() {
        let registry = Registry::new();
        let system = System::new();

        let port = random_port::PortPicker::new().pick().unwrap();

        let mut config = FrontendServerConfig::single_node_default();
        config.port = port;
        config.cors_allow_origins = Some(vec!["*".to_string()]);

        let frontend = Frontend::try_from_config(&(config.clone().frontend, system), &registry)
            .await
            .unwrap();
        let app = FrontendServer::new(
            config,
            frontend,
            vec![],
            Arc::new(()),
            Arc::new(()),
            System::new(),
        );
        tokio::task::spawn(async move {
            app.run().await;
        });

        let client = reqwest::Client::new();
        let res = client
            .request(
                reqwest::Method::OPTIONS,
                format!("http://localhost:{}/api/v2/heartbeat", port),
            )
            .header("Origin", "http://localhost:3000")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let allow_origin = res.headers().get("Access-Control-Allow-Origin");
        assert_eq!(allow_origin.unwrap(), "*");

        let allow_methods = res.headers().get("Access-Control-Allow-Methods");
        assert_eq!(allow_methods.unwrap(), "*");

        let allow_headers = res.headers().get("Access-Control-Allow-Headers");
        assert_eq!(allow_headers.unwrap(), "*");
    }

    #[tokio::test]
    async fn test_defaults_to_json_content_type() {
        let port = test_server().await;

        // We don't send a content-type header
        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://localhost:{}/api/v2/tenants", port))
            .body(serde_json::to_string(&serde_json::json!({ "name": "test" })).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn test_plaintext_error_conversion() {
        // By default, axum returns plaintext errors for some errors. This asserts that there's middleware to ensure all errors are returned as JSON.
        let port = test_server().await;

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://localhost:{}/api/v2/tenants", port))
            .header("content-type", "application/json")
            .body("{") // invalid JSON
            .send()
            .await
            .unwrap();

        // Should have returned JSON
        assert_eq!(
            res.headers().get("content-type").unwrap(),
            "application/json"
        );
        let response_json = res.json::<serde_json::Value>().await.unwrap();
        assert_eq!(
            response_json["error"],
            serde_json::Value::String("InvalidArgumentError".to_string())
        );
    }
}
