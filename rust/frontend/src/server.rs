use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header::HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
    Json, Router, ServiceExt,
};
use chroma_api_types::{ForkCollectionPayload, GetUserIdentityResponse, HeartbeatResponse};
use chroma_metering::{
    CollectionForkContext, CollectionReadContext, CollectionWriteContext, Enterable,
    ExternalCollectionReadContext, MeteredFutureExt, ReadAction, StartRequest, WriteAction,
};
use chroma_system::System;
use chroma_tracing::add_tracing_middleware;
use chroma_types::{
    decode_embeddings, maybe_decode_update_embeddings, validate_name, AddCollectionRecordsPayload,
    AddCollectionRecordsResponse, AttachFunctionRequest, AttachFunctionResponse, ChecklistResponse,
    Collection, CollectionConfiguration, CollectionMetadataUpdate, CollectionUuid,
    CountCollectionsRequest, CountCollectionsResponse, CountRequest, CountResponse,
    CreateCollectionPayload, CreateCollectionRequest, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse, DatabaseName,
    DeleteCollectionRecordsPayload, DeleteCollectionRecordsResponse, DeleteDatabaseRequest,
    DeleteDatabaseResponse, DetachFunctionRequest, DetachFunctionResponse, ForkCollectionResponse,
    GetAttachedFunctionResponse, GetCollectionByCrnRequest, GetCollectionRequest,
    GetDatabaseRequest, GetDatabaseResponse, GetRequest, GetRequestPayload, GetResponse,
    GetTenantRequest, GetTenantResponse, IndexStatusResponse, InternalCollectionConfiguration,
    InternalUpdateCollectionConfiguration, ListCollectionsRequest, ListCollectionsResponse,
    ListDatabasesRequest, ListDatabasesResponse, QueryRequest, QueryRequestPayload, QueryResponse,
    SearchRequest, SearchRequestPayload, SearchResponse, UpdateCollectionPayload,
    UpdateCollectionRecordsPayload, UpdateCollectionRecordsResponse, UpdateCollectionResponse,
    UpdateTenantRequest, UpdateTenantResponse, UpsertCollectionRecordsPayload,
    UpsertCollectionRecordsResponse,
};
use mdac::{Rule, Scorecard, ScorecardGuard};
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Meter};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{str::FromStr, time::Instant};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
#[cfg(windows)]
use tokio::signal::windows::ctrl_c;
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
    traced_json::TracedJson,
    types::errors::{ErrorResponse, ServerError, ValidationError},
    Frontend,
};

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Too many requests; backoff and try again")]
struct RateLimitError;

impl chroma_error::ChromaError for RateLimitError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::ResourceExhausted
    }
}

async fn graceful_shutdown(system: System) {
    #[cfg(unix)]
    {
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
                tracing::info!("Received SIGTERM, shutting down service");
            }
            Err(err) => {
                tracing::error!("Failed to create SIGTERM handler: {err}");
                return;
            }
        }
    }

    #[cfg(windows)]
    {
        match ctrl_c() {
            Ok(mut ctrl_c_signal) => {
                ctrl_c_signal.recv().await;
                tracing::info!("Received Ctrl+C, shutting down service");
            }
            Err(err) => {
                tracing::error!("Failed to create Ctrl+C handler: {err}");
                return;
            }
        }
    }

    system.stop().await;
    system.join().await;
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
    update_tenant: Counter<u64>,
    list_databases: Counter<u64>,
    create_database: Counter<u64>,
    get_database: Counter<u64>,
    delete_database: Counter<u64>,
    create_collection: Counter<u64>,
    list_collections: Counter<u64>,
    count_collections: Counter<u64>,
    get_collection: Counter<u64>,
    get_collection_by_crn: Counter<u64>,
    update_collection: Counter<u64>,
    delete_collection: Counter<u64>,
    fork_collection: Counter<u64>,
    collection_add: Counter<u64>,
    collection_update: Counter<u64>,
    collection_upsert: Counter<u64>,
    collection_delete: Counter<u64>,
    collection_count: Counter<u64>,
    collection_get: Counter<u64>,
    collection_index_status: Counter<u64>,
    collection_query: Counter<u64>,
    collection_search: Counter<u64>,
    attach_function: Counter<u64>,
    get_attached_function: Counter<u64>,
    detach_function: Counter<u64>,
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
            update_tenant: meter.u64_counter("update_tenant").build(),
            list_databases: meter.u64_counter("list_databases").build(),
            create_database: meter.u64_counter("create_database").build(),
            get_database: meter.u64_counter("get_database").build(),
            delete_database: meter.u64_counter("delete_database").build(),
            create_collection: meter.u64_counter("create_collection").build(),
            list_collections: meter.u64_counter("list_collections").build(),
            count_collections: meter.u64_counter("count_collections").build(),
            get_collection: meter.u64_counter("get_collection").build(),
            get_collection_by_crn: meter.u64_counter("get_collection_by_crn").build(),
            update_collection: meter.u64_counter("update_collection").build(),
            delete_collection: meter.u64_counter("delete_collection").build(),
            fork_collection: meter.u64_counter("fork_collection").build(),
            collection_add: meter.u64_counter("collection_add").build(),
            collection_update: meter.u64_counter("collection_update").build(),
            collection_upsert: meter.u64_counter("collection_upsert").build(),
            collection_delete: meter.u64_counter("collection_delete").build(),
            collection_count: meter.u64_counter("collection_count").build(),
            collection_get: meter.u64_counter("collection_get").build(),
            collection_index_status: meter.u64_counter("collection_index_status").build(),
            collection_query: meter.u64_counter("collection_query").build(),
            collection_search: meter.u64_counter("collection_search").build(),
            attach_function: meter.u64_counter("attach_function").build(),
            get_attached_function: meter.u64_counter("get_attached_function").build(),
            detach_function: meter.u64_counter("detach_function").build(),
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

    /// Accepts an optional `ready_tx` channel that emits the bound port when the server is ready.
    pub async fn run(self, ready_tx: Option<tokio::sync::oneshot::Sender<u16>>) {
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
            .route("/api/v2/collections/{crn}", get(get_collection_by_crn))
            .route("/api/v2/tenants", post(create_tenant))
            .route("/api/v2/tenants/{tenant_name}", get(get_tenant))
            .route("/api/v2/tenants/{tenant_name}", patch(update_tenant))
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
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/indexing_status",
                get(indexing_status),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/get",
                post(collection_get),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/query",
                post(collection_query),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/search",
                post(collection_search),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/functions/attach",
                post(attach_function),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/functions/{function_name}",
                get(get_attached_function),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/attached_functions/{name}/detach",
                post(detach_function),
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

        let addr = format!("{}:{}", listen_address, port);
        tracing::info!(%addr, "Frontend server listening on address");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_port = listener
            .local_addr()
            .expect("Failed to get local address of server")
            .port();
        if let Some(ready_tx) = ready_tx {
            ready_tx
                .send(bound_port)
                .expect("Failed to send bound port. Receiver has been dropped.");
        }
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
                .map(|ticket| ScorecardGuard::new(Arc::clone(&self.scorecard), Some(ticket)))
                .ok_or_else(|| Box::new(RateLimitError) as _)
        } else {
            Ok(ScorecardGuard::new(Arc::clone(&self.scorecard), None))
        }
    }
}

impl FrontendServer {
    async fn authenticate_and_authorize(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Result<GetUserIdentityResponse, ServerError> {
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
        database_name: DatabaseName,
        collection_id: CollectionUuid,
    ) -> Result<GetUserIdentityResponse, ServerError> {
        let collection = self
            .frontend
            .get_cached_collection(database_name, collection_id)
            .await?;
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
/// Healthcheck
/// Returns the health status of the service.
#[utoipa::path(
    get,
    path = "/api/v2/healthcheck",
    summary = "Healthcheck",
    description = "Returns the health status of the service.",
    tag = "System",
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

/// Heartbeat
/// Returns a nanosecond timestamp of the current time.
#[utoipa::path(
    get,
    path = "/api/v2/heartbeat",
    summary = "Heartbeat",
    description = "Returns a nanosecond timestamp of the current time.",
    tag = "System",
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

/// Pre-flight checks
/// Returns basic readiness information.
#[utoipa::path(
    get,
    path = "/api/v2/pre-flight-checks",
    summary = "Pre-flight checks",
    description = "Returns basic readiness information.",
    tag = "System",
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
        supports_base64_encoding: true,
    }))
}

/// Reset database
/// Resets the database. Requires authorization.
#[utoipa::path(
    post,
    path = "/api/v2/reset",
    summary = "Reset database",
    description = "Resets the database. Requires authorization.",
    tag = "System",
    security(
        ("ApiKeyAuth" = [])
    ),
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

/// Get version
/// Returns the version of the server.
#[utoipa::path(
    get,
    path = "/api/v2/version",
    summary = "Get version",
    description = "Returns the version of the server.",
    tag = "System",
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

/// Get user identity
/// Returns the current user's identity, tenant, and databases.
#[utoipa::path(
    get,
    path = "/api/v2/auth/identity",
    summary = "Get user identity",
    description = "Returns the current user's identity, tenant, and databases.",
    tag = "Authentication",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "User identity", body = GetUserIdentityResponse),
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

/// Create tenant
/// Creates a new tenant.
#[utoipa::path(
    post,
    path = "/api/v2/tenants",
    summary = "Create tenant",
    description = "Creates a new tenant.",
    tag = "Tenant",
    security(
        ("ApiKeyAuth" = [])
    ),
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

/// Get tenant
/// Returns an existing tenant by name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_name}",
    summary = "Get tenant",
    description = "Returns an existing tenant by name.",
    tag = "Tenant",
    security(
        ("ApiKeyAuth" = [])
    ),
    params(
        ("tenant_name" = String, Path, description = "Tenant UUID")
    ),
    responses(
        (status = 200, description = "Tenant found", body = GetTenantResponse,
            example = json!({
                "name": "1e30d217-3d78-4f8c-b244-79381dc6a254",
                "resource_name": null
            })
        ),
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
pub struct UpdateTenantPayload {
    pub resource_name: String,
}

/// Update tenant
/// Updates an existing tenant by name.
#[utoipa::path(
    patch,
    path = "/api/v2/tenants/{tenant_name}",
    summary = "Update tenant",
    description = "Updates an existing tenant by name.",
    tag = "Tenant",
    security(
        ("ApiKeyAuth" = [])
    ),
    params(
        ("tenant_name" = String, Path, description = "Tenant UUID")
    ),
    request_body = UpdateTenantPayload,
    responses(
        (status = 200, description = "Tenant updated successfully", body = UpdateTenantResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Tenant not found", body = ErrorResponse),
        (status = 409, description = "Tenant resource name already set", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn update_tenant(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateTenantPayload>,
) -> Result<Json<UpdateTenantResponse>, ServerError> {
    server.metrics.update_tenant.add(1, &[]);
    tracing::info!(name: "update_tenant", tenant_name = %name);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::UpdateTenant,
            AuthzResource {
                tenant: Some(name.clone()),
                database: None,
                collection: None,
            },
        )
        .await?;
    let request = UpdateTenantRequest::try_new(name, payload.resource_name)?;
    Ok(Json(server.frontend.update_tenant(request).await?))
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct InvalidDatabaseError(String);

impl From<validator::ValidationError> for InvalidDatabaseError {
    fn from(err: validator::ValidationError) -> Self {
        let message = err
            .message
            .map(|m| m.to_string())
            .unwrap_or_else(|| "invalid database name".to_string());
        Self(message)
    }
}

impl chroma_error::ChromaError for InvalidDatabaseError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::InvalidArgument
    }
}

#[derive(Deserialize, Serialize, ToSchema, Debug)]
pub struct CreateDatabasePayload {
    pub name: String,
}

/// Create database
/// Creates a new database for a tenant.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases",
    summary = "Create database",
    description = "Creates a new database for a tenant.",
    tag = "Database",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = CreateDatabasePayload,
    responses(
        (status = 200, description = "Database created successfully", body = CreateDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID")
    )
)]
async fn create_database(
    headers: HeaderMap,
    Path(tenant): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(CreateDatabasePayload { name }): Json<CreateDatabasePayload>,
) -> Result<Json<CreateDatabaseResponse>, ServerError> {
    if let Err(err) = validate_name(&name) {
        return Err(InvalidDatabaseError::from(err).into());
    }
    server.metrics.create_database.add(1, &[]);
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
    // enforce scorecard
    let _guard =
        server.scorecard_request(&["op:create_database", format!("tenant:{}", tenant).as_str()])?;
    // Enforce quota.
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::CreateDatabase, tenant.clone(), api_token);
    quota_payload = quota_payload.with_collection_name(&name);
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;
    let database_name = DatabaseName::new(name).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let create_database_request = CreateDatabaseRequest::try_new(tenant, database_name)?;
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

/// List databases
/// Lists all databases for a tenant.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases",
    summary = "List databases",
    description = "Lists all databases for a tenant.",
    tag = "Database",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "List of databases", body = ListDatabasesResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("limit" = Option<u32>, Query, description = "Limit for pagination", minimum = 1, example = 10),
        ("offset" = Option<u32>, Query, description = "Offset for pagination", minimum = 0, example = 0)
    )
)]
async fn list_databases(
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Query(ListDatabasesParams { limit, offset }): Query<ListDatabasesParams>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListDatabasesResponse>, ServerError> {
    server.metrics.list_databases.add(1, &[]);
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

/// Get database
/// Returns a database by name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}",
    summary = "Get database",
    description = "Returns a database by name.",
    tag = "Database",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Database retrieved successfully", body = GetDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Database not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name")
    )
)]
async fn get_database(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponse>, ServerError> {
    server.metrics.get_database.add(1, &[]);
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
    let database_name = DatabaseName::new(database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let request = GetDatabaseRequest::try_new(tenant, database_name)?;
    let res = server.frontend.get_database(request).await?;
    Ok(Json(res))
}

/// Delete database
/// Deletes a database by name.
#[utoipa::path(
    delete,
    path = "/api/v2/tenants/{tenant}/databases/{database}",
    summary = "Delete database",
    description = "Deletes a database by name.",
    tag = "Database",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Database deleted successfully", body = DeleteDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Database not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name")
    )
)]
async fn delete_database(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<DeleteDatabaseResponse>, ServerError> {
    server.metrics.delete_database.add(1, &[]);
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

/// List collections
/// Lists all collections in a database.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections",
    summary = "List collections",
    description = "Lists all collections in a database.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "List of collections", body = ListCollectionsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("limit" = Option<u32>, Query, description = "Limit for pagination", minimum = 1, example = 10),
        ("offset" = Option<u32>, Query, description = "Offset for pagination", minimum = 0, example = 0)
    )
)]
async fn list_collections(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    Query(ListCollectionsParams { limit, offset }): Query<ListCollectionsParams>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListCollectionsResponse>, ServerError> {
    server.metrics.list_collections.add(1, &[]);
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
    let _guard = server
        .scorecard_request(&["op:list_collections", format!("tenant:{}", tenant).as_str()])?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());

    let mut quota_payload = QuotaPayload::new(Action::ListCollections, tenant.clone(), api_token);
    if let Some(provided_limit) = limit {
        quota_payload = quota_payload.with_limit(provided_limit);
    }

    let quota_overrides = server.quota_enforcer.enforce(&quota_payload).await?;

    let validated_limit = match quota_overrides {
        Some(overrides) => Some(overrides.limit),
        None => limit,
    };

    // TODO: Limit shouldn't be optional here
    let database_name = DatabaseName::new(&database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let request = ListCollectionsRequest::try_new(tenant, database_name, validated_limit, offset)?;
    Ok(Json(server.frontend.list_collections(request).await?))
}

/// Get number of collections
/// Returns the total number of collections in a database.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections_count",
    summary = "Get number of collections",
    description = "Returns the total number of collections in a database.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Count of collections", body = CountCollectionsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name")
    )
)]
async fn count_collections(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountCollectionsResponse>, ServerError> {
    server.metrics.count_collections.add(1, &[]);
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

    let database_name = DatabaseName::new(&database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let request = CountCollectionsRequest::try_new(tenant, database_name)?;
    Ok(Json(server.frontend.count_collections(request).await?))
}

/// Create collection
/// Creates a new collection in a database.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections",
    summary = "Create collection",
    description = "Creates a new collection in a database.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body(
        content = CreateCollectionPayload,
        description = "Collection creation payload",
        example = json!({
            "name": "my_collection",
            "schema": null,
            "configuration": null,
            "metadata": {"key": "value"},
            "get_or_create": false
        })
    ),
    responses(
        (status = 200, description = "Collection created successfully", body = Collection),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name")
    )
)]
async fn create_collection(
    headers: HeaderMap,
    Path((tenant, database)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateCollectionPayload>,
) -> Result<Json<Collection>, ServerError> {
    server.metrics.create_collection.add(1, &[]);
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
    let _guard = server.scorecard_request(&[
        "op:create_collection",
        format!("tenant:{}", tenant).as_str(),
    ])?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::CreateCollection, tenant.clone(), api_token);
    quota_payload = quota_payload.with_collection_name(&payload.name);
    if let Some(metadata) = &payload.metadata {
        quota_payload = quota_payload.with_create_collection_metadata(metadata);
    }
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    let payload_clone = payload.clone();

    let configuration = match payload_clone.configuration {
        Some(c) => Some(InternalCollectionConfiguration::try_from_config(
            c,
            server.config.frontend.default_knn_index,
            payload_clone.metadata,
        )?),
        None => Some(InternalCollectionConfiguration::try_from_config(
            CollectionConfiguration::default(),
            server.config.frontend.default_knn_index,
            payload_clone.metadata,
        )?),
    };

    let database_name = DatabaseName::new(database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let request = CreateCollectionRequest::try_new(
        tenant,
        database_name,
        payload.name,
        payload.metadata,
        configuration,
        payload.schema,
        payload.get_or_create,
    )?;
    let collection = server.frontend.create_collection(request).await?;

    Ok(Json(collection))
}

/// Get collection
/// Returns a collection by ID or name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
    summary = "Get collection",
    description = "Returns a collection by ID or name.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Collection found", body = Collection),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn get_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    server.metrics.get_collection.add(1, &[]);
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
    let database_name = DatabaseName::new(&database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let request = GetCollectionRequest::try_new(tenant, database_name, collection_name)?;
    let collection = server.frontend.get_collection(request).await?;
    Ok(Json(collection))
}

/// Get collection by CRN
/// Returns a collection by Chroma Resource Name.
#[utoipa::path(
    get,
    path = "/api/v2/collections/{crn}",
    summary = "Get collection by CRN",
    description = "Returns a collection by Chroma Resource Name.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Collection found", body = Collection),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("crn" = String, Path, description = "Chroma Resource Name", example = "my_tenant:my_database:my_collection")
    )
)]
async fn get_collection_by_crn(
    headers: HeaderMap,
    Path(crn): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    server.metrics.get_collection_by_crn.add(1, &[]);
    tracing::info!(name: "get_collection_by_crn", crn = %crn);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetCollectionByCrn,
            AuthzResource {
                tenant: None,
                database: None,
                collection: Some(crn.clone()),
            },
        )
        .await?;
    let request = GetCollectionByCrnRequest::try_new(crn)?;
    let collection = server.frontend.get_collection_by_crn(request).await?;
    Ok(Json(collection))
}

/// Update collection
/// Updates an existing collection's name or metadata.
#[utoipa::path(
    put,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
    summary = "Update collection",
    description = "Updates an existing collection's name or metadata.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body(
        content = UpdateCollectionPayload,
        description = "Collection update payload",
        example = json!({
            "new_name": "updated_collection_name",
            "new_metadata": {"key": "value"},
            "new_configuration": null
        })
    ),
    responses(
        (status = 200, description = "Collection updated successfully", body = UpdateCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn update_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionPayload>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server.metrics.update_collection.add(1, &[]);
    tracing::info!(name: "update_collection", tenant_name = %tenant, database_name = %database, collection_id = %collection_id);
    let database_name = DatabaseName::new(&database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::UpdateCollection,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            database_name.clone(),
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:update_collection",
        format!("tenant:{}", tenant).as_str(),
    ])?;
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
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;

    let configuration = match payload.new_configuration {
        Some(c) => Some(InternalUpdateCollectionConfiguration::try_from(c)?),
        None => None,
    };

    let request = chroma_types::UpdateCollectionRequest::try_new(
        Some(database_name),
        collection_id,
        payload.new_name,
        payload
            .new_metadata
            .map(CollectionMetadataUpdate::UpdateMetadata),
        configuration,
    )?;

    server.frontend.update_collection(request).await?;

    Ok(Json(UpdateCollectionResponse {}))
}

/// Delete collection
/// Deletes a collection in a database.
#[utoipa::path(
    delete,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}",
    summary = "Delete collection",
    description = "Deletes a collection in a database.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Collection deleted successfully", body = UpdateCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn delete_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server.metrics.delete_collection.add(1, &[]);
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

/// Fork collection
/// Creates a fork of an existing collection.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/fork",
    summary = "Fork collection",
    description = "Creates a fork of an existing collection.",
    tag = "Collection",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = ForkCollectionPayload,
    responses(
        (status = 200, description = "Collection forked successfully", body = ForkCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn fork_collection(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<ForkCollectionPayload>,
) -> Result<Json<ForkCollectionResponse>, ServerError> {
    server.metrics.fork_collection.add(1, &[]);
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
    let _guard = server.scorecard_request(&[
        "op:fork_collection",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let mut quota_payload = QuotaPayload::new(Action::ForkCollection, tenant.clone(), api_token);
    quota_payload = quota_payload.with_collection_uuid(collection_id);
    quota_payload = quota_payload.with_collection_name(&payload.new_name);
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    let metering_context_container =
        chroma_metering::create::<CollectionForkContext>(CollectionForkContext::new(
            tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
        ));

    let request = chroma_types::ForkCollectionRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.new_name,
    )?;

    Ok(Json(
        server
            .frontend
            .fork_collection(request)
            .meter(metering_context_container)
            .await?,
    ))
}

/// Add records
/// Adds records to a collection.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/add",
    summary = "Add records",
    description = "Adds records to a collection.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = AddCollectionRecordsPayload,
    responses(
        (status = 201, description = "Collection added successfully", body = AddCollectionRecordsResponse),
        (status = 400, description = "Invalid data for collection addition")
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
// NOTE(hammadb) collection_[add, upsert, update] can have large payloads, so we trace
// the individual method since the overall handler span includes buffering
// the body.
#[tracing::instrument(name = "collection_add", skip_all)]
async fn collection_add(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<AddCollectionRecordsPayload>,
) -> Result<(StatusCode, Json<AddCollectionRecordsResponse>), ServerError> {
    server.metrics.collection_add.add(1, &[]);
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Add,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Add, tenant.clone(), api_token);
    quota_payload = quota_payload.with_ids(&payload.ids);

    let payload_embeddings: Vec<Vec<f32>> = decode_embeddings(payload.embeddings)?;
    quota_payload = quota_payload.with_add_embeddings(&payload_embeddings);
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
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    let metering_context_container =
        chroma_metering::create::<CollectionWriteContext>(CollectionWriteContext::new(
            tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            WriteAction::Add,
        ));

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

    tracing::info!(name: "collection_add", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.len());
    let request = chroma_types::AddCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        payload_embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    let res = server
        .frontend
        .add(request)
        .meter(metering_context_container)
        .await?;

    Ok((StatusCode::CREATED, Json(res)))
}

/// Update records
/// Updates records in a collection by ID.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/update",
    summary = "Update records",
    description = "Updates records in a collection by ID.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = UpdateCollectionRecordsPayload,
    responses(
        (status = 200, description = "Collection updated successfully", body = UpdateCollectionRecordsResponse),
        (status = 404, description = "Collection not found")
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
// NOTE(hammadb) collection_[add, upsert, update] can have large payloads, so we trace
// the individual method since the overall handler span includes buffering
// the body.
#[tracing::instrument(name = "collection_update", skip_all)]
async fn collection_update(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<UpdateCollectionRecordsPayload>,
) -> Result<Json<UpdateCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_update.add(1, &[]);
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Update,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Update, tenant.clone(), api_token);
    quota_payload = quota_payload.with_ids(&payload.ids);
    let payload_embeddings: Option<Vec<Option<Vec<f32>>>> =
        maybe_decode_update_embeddings(payload.embeddings)?;
    if let Some(embeddings) = &payload_embeddings {
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
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    let metering_context_container =
        chroma_metering::create::<CollectionWriteContext>(CollectionWriteContext::new(
            tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            WriteAction::Update,
        ));

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

    tracing::info!(name: "collection_update", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.len());
    let request = chroma_types::UpdateCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        payload_embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    Ok(Json(
        server
            .frontend
            .update(request)
            .meter(metering_context_container)
            .await?,
    ))
}

/// Upsert records
/// Upserts records in a collection (create if not exists, otherwise update).
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/upsert",
    summary = "Upsert records",
    description = "Upserts records in a collection (create if not exists, otherwise update).",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = UpsertCollectionRecordsPayload,
    responses(
        (status = 200, description = "Records upserted successfully", body = UpsertCollectionRecordsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse),
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
    )
)]
// NOTE(hammadb) collection_[add, upsert, update] can have large payloads, so we trace
// the individual method since the overall handler span includes buffering
// the body.
#[tracing::instrument(name = "collection_upsert", skip_all)]
async fn collection_upsert(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<UpsertCollectionRecordsPayload>,
) -> Result<Json<UpsertCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_upsert.add(1, &[]);
    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Upsert,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Upsert, tenant.clone(), api_token);
    quota_payload = quota_payload.with_ids(&payload.ids);
    let payload_embeddings: Vec<Vec<f32>> = decode_embeddings(payload.embeddings)?;
    quota_payload = quota_payload.with_add_embeddings(&payload_embeddings);
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
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    let metering_context_container =
        chroma_metering::create::<CollectionWriteContext>(CollectionWriteContext::new(
            tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            WriteAction::Upsert,
        ));

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

    tracing::info!(name: "collection_upsert", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.len());
    let request = chroma_types::UpsertCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        payload_embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    Ok(Json(
        server
            .frontend
            .upsert(request)
            .meter(metering_context_container)
            .await?,
    ))
}

/// Delete records
/// Deletes records in a collection. Can filter by IDs or metadata.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/delete",
    summary = "Delete records",
    description = "Deletes records in a collection. Can filter by IDs or metadata.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = DeleteCollectionRecordsPayload,
    responses(
        (status = 200, description = "Records deleted successfully", body = DeleteCollectionRecordsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse),
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
    )
)]
async fn collection_delete(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<DeleteCollectionRecordsPayload>,
) -> Result<Json<DeleteCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_delete.add(1, &[]);
    let requester_identity = server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Delete,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;
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
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    // NOTE(c-gamble): This is a read context because read happens first on delete, then write.
    let metering_context_container =
        chroma_metering::create::<CollectionReadContext>(CollectionReadContext::new(
            requester_identity.tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            ReadAction::GetForDelete,
        ));

    tracing::info!(name: "collection_delete", tenant_name = %tenant, database_name = %database, collection_id = %collection_id, num_ids = %payload.ids.as_ref().map_or(0, |ids| ids.len()), has_where = r#where.is_some());
    let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
        tenant,
        database,
        collection_id,
        payload.ids,
        r#where,
    )?;

    Box::pin(
        server
            .frontend
            .delete(request)
            .meter(metering_context_container),
    )
    .await?;

    Ok(Json(DeleteCollectionRecordsResponse {}))
}

/// Get number of records
/// Returns the number of records in a collection.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/count",
    summary = "Get number of records",
    description = "Returns the number of records in a collection.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Number of records in the collection", body = CountResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn collection_count(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountResponse>, ServerError> {
    server.metrics.collection_count.add(1, &[]);
    tracing::info!(
        name: "collection_count",
        tenant = tenant,
        database = database,
        collection_id = collection_id
    );
    let requester_identity = server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Count,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
        format!("requester:{}", requester_identity.tenant).as_str(),
    ])?;

    // Create a metering context
    let metering_context_container = if requester_identity.tenant == tenant {
        chroma_metering::create::<CollectionReadContext>(CollectionReadContext::new(
            requester_identity.tenant.clone(),
            database.clone(),
            collection_id.clone(),
            ReadAction::Count,
        ))
    } else {
        chroma_metering::create::<ExternalCollectionReadContext>(
            ExternalCollectionReadContext::new(
                requester_identity.tenant.clone(),
                database.clone(),
                collection_id.clone(),
                ReadAction::Count,
            ),
        )
    };

    let request = CountRequest::try_new(
        tenant,
        database,
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
    )?;

    Ok(Json(
        server
            .frontend
            .count(request)
            .meter(metering_context_container)
            .await?,
    ))
}

/// Get indexing status
/// Returns the indexing status of a collection.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/indexing_status",
    summary = "Get indexing status",
    description = "Returns the indexing status of a collection.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Index status retrieved successfully", body = IndexStatusResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn indexing_status(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<IndexStatusResponse>, ServerError> {
    server.metrics.collection_index_status.add(1, &[]);
    tracing::info!(
        name: "index_status",
        tenant = tenant,
        database = database,
        collection_id = collection_id
    );

    let _guard = server.scorecard_request(&[
        "op:indexing_status",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ])?;

    server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Count,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;

    let metering_context_container =
        chroma_metering::create::<CollectionReadContext>(CollectionReadContext::new(
            tenant.clone(),
            database.clone(),
            collection_id.clone(),
            ReadAction::Query,
        ));

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let database_name = DatabaseName::new(database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;

    Ok(Json(
        server
            .frontend
            .indexing_status(database_name, collection_id)
            .meter(metering_context_container)
            .await?,
    ))
}

/// Get records
/// Returns records from a collection by ID or metadata filter.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/get",
    summary = "Get records",
    description = "Returns records from a collection by ID or metadata filter.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = GetRequestPayload,
    responses(
        (status = 200, description = "Records retrieved from the collection", body = GetResponse,
            example = json!({
                "ids": ["record1", "record2"],
                "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                "documents": ["Document 1", "Document 2"],
                "uris": null,
                "metadatas": [{"key": "value"}, {"key2": "value2"}],
                "include": ["documents", "metadatas", "embeddings"]
            })
        ),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn collection_get(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<GetRequestPayload>,
) -> Result<Json<GetResponse>, ServerError> {
    server.metrics.collection_get.add(1, &[]);
    let requester_identity = server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Get,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
        format!("requester:{}", requester_identity.tenant).as_str(),
    ])?;
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
    if let Some(provided_limit) = payload.limit {
        quota_payload = quota_payload.with_limit(provided_limit);
    }

    let quota_overrides = server.quota_enforcer.enforce(&quota_payload).await?;

    let validated_limit = match quota_overrides {
        Some(overrides) => Some(overrides.limit),
        None => payload.limit,
    };

    // Create a metering context
    let metering_context_container = if requester_identity.tenant == tenant {
        chroma_metering::create::<CollectionReadContext>(CollectionReadContext::new(
            requester_identity.tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            ReadAction::Get,
        ))
    } else {
        chroma_metering::create::<ExternalCollectionReadContext>(
            ExternalCollectionReadContext::new(
                requester_identity.tenant.clone(),
                database.clone(),
                collection_id.0.to_string(),
                ReadAction::Get,
            ),
        )
    };

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

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
        // TODO: Limit shouldn't be optional here
        validated_limit,
        payload.offset.unwrap_or(0),
        payload.include,
    )?;
    let res = Box::pin(
        server
            .frontend
            .get(request)
            .meter(metering_context_container),
    )
    .await?;
    Ok(Json(res))
}

/// Query collection
/// Queries a collection using dense vector search with metadata and full-text search filtering.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/query",
    summary = "Query collection",
    description = "Queries a collection using dense vector search with metadata and full-text search filtering.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body(
        content = QueryRequestPayload,
        description = "Query request payload",
        example = json!({
            "query_embeddings": [[0.1, 0.2, 0.3]],
            "n_results": 10,
            "include": ["documents", "metadatas", "distances"]
        })
    ),
    responses(
        (status = 200, description = "Records matching the query", body = QueryResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse),
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("limit" = Option<u32>, Query, description = "Limit for pagination", minimum = 1, example = 10),
        ("offset" = Option<u32>, Query, description = "Offset for pagination", minimum = 0, example = 0)
    )
)]

async fn collection_query(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<QueryRequestPayload>,
) -> Result<Json<QueryResponse>, ServerError> {
    server.metrics.collection_query.add(1, &[]);
    let requester_identity = server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Query,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
        format!("requester:{}", requester_identity.tenant).as_str(),
    ])?;
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
    let _ = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    let metering_context_container = if requester_identity.tenant == tenant {
        chroma_metering::create::<CollectionReadContext>(CollectionReadContext::new(
            requester_identity.tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            ReadAction::Query,
        ))
    } else {
        chroma_metering::create::<ExternalCollectionReadContext>(
            ExternalCollectionReadContext::new(
                requester_identity.tenant.clone(),
                database.clone(),
                collection_id.0.to_string(),
                ReadAction::Query,
            ),
        )
    };

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

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

    // pin the request since future exceeds size limit (16KB)
    // Box::pin is required to avoid stack overflow by moving future to heap
    let res = Box::pin(
        server
            .frontend
            .query(request)
            .meter(metering_context_container),
    )
    .await?;

    Ok(Json(res))
}

/// Search records
/// Searches records from a collection with dense, sparse, or hybrid vector search.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/search",
    summary = "Search records",
    description = "Searches records from a collection with dense, sparse, or hybrid vector search.",
    tag = "Record",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body(
        content = SearchRequestPayload,
        description = "Search request payload",
        example = json!({
            "searches": [{
                "query_embeddings": [[0.1, 0.2, 0.3]],
                "n_results": 10
            }],
            "read_level": "IndexAndWal"
        })
    ),
    responses(
        (status = 200, description = "Records searched from the collection", body = SearchResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn collection_search(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(payload): TracedJson<SearchRequestPayload>,
) -> Result<Json<SearchResponse>, ServerError> {
    server.metrics.collection_search.add(1, &[]);
    let requester_identity = server
        .authenticate_and_authorize_collection(
            &headers,
            AuthzAction::Search,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
            DatabaseName::new(&database).ok_or_else(|| {
                ValidationError::InvalidArgument(
                    "database name must be at least 3 characters".to_string(),
                )
            })?,
            CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let _guard = server.scorecard_request(&[
        // TODO: Make this a read operation once we stablize this
        // "op:read",
        "op:search",
        format!("tenant:{}", tenant).as_str(),
        format!("collection:{}", collection_id).as_str(),
        format!("requester:{}", requester_identity.tenant).as_str(),
    ])?;

    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());

    let quota_payload = QuotaPayload::new(Action::Search, tenant.clone(), api_token)
        .with_search_payloads(payload.searches.as_slice());
    let quota_override = server.quota_enforcer.enforce(&quota_payload).await?;

    // Create a metering context
    let metering_context_container = if requester_identity.tenant == tenant {
        chroma_metering::create::<CollectionReadContext>(CollectionReadContext::new(
            requester_identity.tenant.clone(),
            database.clone(),
            collection_id.0.to_string(),
            ReadAction::Search,
        ))
    } else {
        chroma_metering::create::<ExternalCollectionReadContext>(
            ExternalCollectionReadContext::new(
                requester_identity.tenant.clone(),
                database.clone(),
                collection_id.0.to_string(),
                ReadAction::Search,
            ),
        )
    };

    metering_context_container.enter();

    chroma_metering::with_current(|context| {
        context.start_request(Instant::now());
    });

    tracing::info!(
        name: "collection_search",
        num_queries = payload.searches.len(),
    );

    // Override limit by quota
    let mut searches = payload.searches;
    if let Some(quota_override) = quota_override {
        for payload in &mut searches {
            let override_limit = match payload.limit.limit {
                Some(limit) => quota_override.limit.min(limit),
                None => quota_override.limit,
            };
            payload.limit.limit = Some(override_limit);
        }
    }

    let request = SearchRequest::try_new(
        tenant,
        database,
        collection_id,
        searches,
        payload.read_level,
    )?;
    let res = server
        .frontend
        .search(request)
        .meter(metering_context_container)
        .await?;
    Ok(Json(res))
}

/// Attach function
/// Attaches a function to a collection.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/functions/attach",
    summary = "Attach function",
    description = "Attaches a function to a collection.",
    tag = "Function",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body(
        content = AttachFunctionRequest,
        description = "Function attachment request",
        example = json!({
            "name": "my_function",
            "function_id": "1e30d217-3d78-4f8c-b244-79381dc6a254",
            "output_collection": "output_collection_name",
            "params": {}
        })
    ),
    responses(
        (status = 200, description = " Function attached successfully", body = AttachFunctionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254")
    )
)]
async fn attach_function(
    headers: HeaderMap,
    Path((tenant, database, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(request): TracedJson<AttachFunctionRequest>,
) -> Result<Json<AttachFunctionResponse>, ServerError> {
    server.metrics.attach_function.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateAttachedFunction,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;

    let _guard = server.scorecard_request(&[
        "op:attach_function",
        format!("tenant:{}", tenant).as_str(),
        format!("database:{}", database).as_str(),
    ])?;

    let database_name = DatabaseName::new(database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let res = server
        .frontend
        .attach_function(tenant, database_name, collection_id, request)
        .await?;
    Ok(Json(res))
}

/// Get attached function
/// Returns an attached function by name.
#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/functions/{function_name}",
    summary = "Get attached function",
    description = "Returns an attached function by name.",
    tag = "Function",
    security(
        ("ApiKeyAuth" = [])
    ),
    responses(
        (status = 200, description = "Attached function retrieved successfully", body = GetAttachedFunctionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Attached function not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("function_name" = String, Path, description = "Function name")
    )
)]
async fn get_attached_function(
    headers: HeaderMap,
    Path((tenant, database, collection_id, function_name)): Path<(String, String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetAttachedFunctionResponse>, ServerError> {
    server.metrics.get_attached_function.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetCollection, // Using GetCollection as the auth action for getting attached functions
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;

    let _guard = server.scorecard_request(&[
        "op:get_attached_function",
        format!("tenant:{}", tenant).as_str(),
        format!("database:{}", database).as_str(),
    ])?;

    let database_name = DatabaseName::new(database).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let attached_function = server
        .frontend
        .get_attached_function(tenant, database_name, collection_id, function_name)
        .await?;
    let attached_function_api =
        chroma_types::AttachedFunctionApiResponse::from_attached_function(attached_function)?;
    Ok(Json(GetAttachedFunctionResponse {
        attached_function: attached_function_api,
    }))
}

/// Detach function
/// Detaches a function from a collection.
#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/attached_functions/{name}/detach",
    summary = "Detach function",
    description = "Detaches a function from a collection.",
    tag = "Function",
    security(
        ("ApiKeyAuth" = [])
    ),
    request_body = DetachFunctionRequest,
    responses(
        (status = 200, description = "Function detached successfully", body = DetachFunctionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant" = String, Path, description = "Tenant UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("database" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "Collection UUID", example = "1e30d217-3d78-4f8c-b244-79381dc6a254"),
        ("name" = String, Path, description = "Function name")
    )
)]
async fn detach_function(
    headers: HeaderMap,
    Path((tenant, database_name, collection_id, name)): Path<(String, String, String, String)>,
    State(mut server): State<FrontendServer>,
    TracedJson(request): TracedJson<DetachFunctionRequest>,
) -> Result<Json<DetachFunctionResponse>, ServerError> {
    server.metrics.detach_function.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::RemoveAttachedFunction,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;

    let _guard = server.scorecard_request(&[
        "op:detach_function",
        format!("tenant:{}", tenant).as_str(),
        format!("database:{}", database_name).as_str(),
    ])?;

    let database_name_typed = DatabaseName::new(database_name).ok_or_else(|| {
        ValidationError::InvalidArgument("database name must be at least 3 characters".to_string())
    })?;
    let res = server
        .frontend
        .detach_function(tenant, database_name_typed, collection_id, name, request)
        .await?;
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
            "ApiKeyAuth",
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
        update_tenant,
        list_databases,
        create_database,
        get_database,
        delete_database,
        create_collection,
        list_collections,
        count_collections,
        get_collection,
        get_collection_by_crn,
        update_collection,
        delete_collection,
        fork_collection,
        collection_add,
        collection_update,
        collection_upsert,
        collection_delete,
        collection_count,
        collection_get,
        collection_query,
        collection_search,
        attach_function,
        get_attached_function,
        detach_function,
        indexing_status,
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

    async fn test_server(mut config: FrontendServerConfig) -> u16 {
        let registry = Registry::new();
        let system = System::new();

        // Binding to port 0 will let the OS choose an available port. This avoids port conflicts when running tests in parallel.
        config.port = 0;

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

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn(async move {
            app.run(Some(ready_tx)).await;
        });

        // Wait for port
        ready_rx.await.unwrap()
    }

    #[tokio::test]
    async fn test_cors() {
        let mut config = FrontendServerConfig::single_node_default();
        config.cors_allow_origins = Some(vec!["http://localhost:8000".to_string()]);

        let port = test_server(config).await;

        let client = reqwest::Client::new();
        let res = client
            .request(
                reqwest::Method::OPTIONS,
                format!("http://localhost:{}/api/v2/heartbeat", port),
            )
            .header("Origin", "http://localhost:8000")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let allow_origin = res.headers().get("Access-Control-Allow-Origin");
        assert_eq!(allow_origin.unwrap(), "http://localhost:8000");

        let allow_methods = res.headers().get("Access-Control-Allow-Methods");
        assert_eq!(allow_methods.unwrap(), "*");

        let allow_headers = res.headers().get("Access-Control-Allow-Headers");
        assert_eq!(allow_headers.unwrap(), "*");
    }

    #[tokio::test]
    async fn test_cors_wildcard() {
        let mut config = FrontendServerConfig::single_node_default();
        config.cors_allow_origins = Some(vec!["*".to_string()]);

        let port = test_server(config).await;

        let client = reqwest::Client::new();
        let res = client
            .request(
                reqwest::Method::OPTIONS,
                format!("http://localhost:{}/api/v2/heartbeat", port),
            )
            .header("Origin", "http://localhost:8000")
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
        let port = test_server(FrontendServerConfig::single_node_default()).await;

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
        let port = test_server(FrontendServerConfig::single_node_default()).await;

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
