use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::header::HeaderMap,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router, ServiceExt,
};
use chroma_types::RawWhereFields;
use chroma_types::{
    AddCollectionRecordsResponse, ChecklistResponse, Collection, CollectionMetadataUpdate,
    CollectionUuid, CountCollectionsRequest, CountCollectionsResponse, CountRequest, CountResponse,
    CreateCollectionRequest, CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantRequest,
    CreateTenantResponse, DeleteCollectionRecordsResponse, DeleteDatabaseRequest,
    DeleteDatabaseResponse, GetCollectionRequest, GetDatabaseRequest, GetDatabaseResponse,
    GetRequest, GetResponse, GetTenantRequest, GetTenantResponse, GetUserIdentityResponse,
    HeartbeatResponse, IncludeList, ListCollectionsRequest, ListCollectionsResponse,
    ListDatabasesRequest, ListDatabasesResponse, Metadata, QueryRequest, QueryResponse,
    UpdateCollectionRecordsResponse, UpdateCollectionResponse, UpdateMetadata,
    UpsertCollectionRecordsResponse,
};
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
use utoipa::OpenApi;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::{
    ac::AdmissionControlledService,
    auth::{AuthenticateAndAuthorize, AuthzAction, AuthzResource},
    frontend::Frontend,
    quota::{Action, QuotaEnforcer, QuotaPayload},
    tower_tracing::add_tracing_middleware,
    types::errors::{ErrorResponse, ServerError, ValidationError},
    FrontendConfig,
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

pub struct Metrics {
    healthcheck: Counter<u64>,
    heartbeat: Counter<u64>,
    pre_flight_checks: Counter<u64>,
    reset: Counter<u64>,
    version: Counter<u64>,
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
    config: FrontendConfig,
    frontend: Frontend,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
    metrics: Arc<Metrics>,
    auth: Arc<dyn AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
}

impl FrontendServer {
    pub fn new(
        config: FrontendConfig,
        frontend: Frontend,
        rules: Vec<Rule>,
        auth: Arc<dyn AuthenticateAndAuthorize>,
        quota_enforcer: Arc<dyn QuotaEnforcer>,
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
        }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer) {
        let circuit_breaker_config = server.config.circuit_breaker.clone();

        // Build an OpenApiRouter with only the healthcheck endpoint
        let (docs_router, docs_api) =
            OpenApiRouter::with_openapi(ApiDoc::openapi()).split_for_parts();

        let docs_router = docs_router.merge(SwaggerUi::new("/docs").url("/openapi.json", docs_api));

        let app = Router::new()
            // `GET /` goes to `root`
            .route("/api/v1/{*any}", get(v1_deprecation_notice)
                                     .put(v1_deprecation_notice)
                                     .patch(v1_deprecation_notice)
                                     .delete(v1_deprecation_notice)
                                     .head(v1_deprecation_notice)
                                     .options(v1_deprecation_notice))
            .route("/api/v2/healthcheck", get(healthcheck))
            .route("/api/v2/heartbeat", get(heartbeat))
            .route("/api/v2/pre-flight-checks", get(pre_flight_checks))
            .route("/api/v2/reset", post(reset))
            .route("/api/v2/version", get(version))
            .route("/api/v2/auth/identity", get(get_user_identity))
            .route("/api/v2/tenants", post(create_tenant))
            .route("/api/v2/tenants/{tenant_name}", get(get_tenant))
            .route("/api/v2/tenants/{tenant_id}/databases", get(list_databases).post(create_database))
            .route("/api/v2/tenants/{tenant_id}/databases/{name}", get(get_database).delete(delete_database))
            .route(
                "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections",
               post(create_collection).get(list_collections),
            )
            .route(
                "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections_count",
                get(count_collections),
            )
            .route(
                "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}",
                get(get_collection).put(update_collection).delete(delete_collection),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database_name}/collections/{collection_id}/add",
                post(collection_add),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database_name}/collections/{collection_id}/update",
                post(collection_update),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database_name}/collections/{collection_id}/upsert",
                post(collection_upsert),
            )
            .route(
                "/api/v2/tenants/{tenant}/databases/{database_name}/collections/{collection_id}/delete",
                post(collection_delete),
            )
            .route(
                "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}/count",
                get(collection_count),
            )
            .route(
                "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}/get",
                post(collection_get),
            )
            .route(
                "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}/query",
                post(collection_query),
            )
            .merge(docs_router)
            .with_state(server)
            .layer(DefaultBodyLimit::max(6000000)); // TODO: add to server configuration
        let app = add_tracing_middleware(app);

        // TODO: configuration for this
        // TODO: tracing
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        if circuit_breaker_config.enabled() {
            let service = AdmissionControlledService::new(circuit_breaker_config, app);
            axum::serve(listener, service.into_make_service())
                .await
                .unwrap();
        } else {
            axum::serve(listener, app).await.unwrap();
        };
    }

    fn scorecard_request(&self, tags: &[&str]) -> Option<ScorecardGuard> {
        if self.scorecard_enabled.load(Ordering::Relaxed) {
            self.scorecard.track(tags).map(|ticket| ScorecardGuard {
                scorecard: Arc::clone(&self.scorecard),
                ticket: Some(ticket),
            })
        } else {
            Some(ScorecardGuard {
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
}

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

#[utoipa::path(
    get,
    path = "/api/v2/healthcheck",
    responses(
        (status = 200, description = "Success", body = str, content_type = "text/plain")
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

#[utoipa::path(
    get,
    path = "/api/v2/heartbeat",
    responses(
        (status = 200, description = "Success", body = HeartbeatResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn heartbeat(State(server): State<FrontendServer>) -> impl IntoResponse {
    server.metrics.heartbeat.add(1, &[]);
    match server.frontend.heartbeat().await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => {
            let error = ErrorResponse::new("HeartbeatError".to_string(), err.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

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
        max_batch_size: 100,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v2/reset",
    responses(
        (status = 200, description = "Reset successful", body = bool),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn reset(headers: HeaderMap, State(mut server): State<FrontendServer>) -> impl IntoResponse {
    server.metrics.reset.add(1, &[]);
    match server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Reset,
            AuthzResource {
                tenant: None,
                database: None,
                collection: None,
            },
        )
        .await
    {
        Err(auth_err) => {
            let error = ErrorResponse::new("AuthError".to_string(), auth_err.to_string());
            (StatusCode::UNAUTHORIZED, Json(error)).into_response()
        }
        Ok(_) => match server.frontend.reset().await {
            Ok(_) => (StatusCode::OK, Json(true)).into_response(),
            Err(reset_err) => {
                let error = ErrorResponse::new("ResetError".to_string(), reset_err.to_string());
                (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
            }
        },
    }
}

#[utoipa::path(
    get,
    path = "/api/v2/version",
    responses(
        (status = 200, description = "Get server version", body = String)
    )
)]
async fn version(State(server): State<FrontendServer>) -> &'static str {
    server.metrics.version.add(1, &[]);
    env!("CARGO_PKG_VERSION")
}

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
    server.metrics.version.add(1, &[]);
    Ok(Json(server.auth.get_user_identity(&headers).await?))
}

#[utoipa::path(
    post,
    path = "/api/v2/tenants",
    request_body = CreateTenantRequest,
    responses(
        (status = 200, description = "Tenant created successfully", body = CreateTenantResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn create_tenant(
    headers: HeaderMap,
    State(mut server): State<FrontendServer>,
    Json(request): Json<CreateTenantRequest>,
) -> Result<Json<CreateTenantResponse>, ServerError> {
    server.metrics.create_tenant.add(1, &[]);
    tracing::info!("Creating tenant [{}]", request.name);
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
    Ok(Json(server.frontend.create_tenant(request).await?))
}

#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_name}",
    params(
        ("tenant_name" = String, Path, description = "Tenant name or ID to retrieve")
    ),
    responses(
        (status = 200, description = "Tenant found", body = GetTenantResponse),
        (status = 404, description = "Tenant not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
async fn get_tenant(
    headers: HeaderMap,
    Path(name): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetTenantResponse>, ServerError> {
    server.metrics.get_tenant.add(1, &[]);
    tracing::info!("Getting tenant [{}]", name);
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
struct CreateDatabasePayload {
    name: String,
}

#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant_id}/databases",
    request_body = CreateDatabasePayload,
    responses(
        (status = 200, description = "Database created successfully", body = CreateDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID to associate with the new database")
    )
)]
async fn create_database(
    headers: HeaderMap,
    Path(tenant_id): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(CreateDatabasePayload { name }): Json<CreateDatabasePayload>,
) -> Result<Json<CreateDatabaseResponse>, ServerError> {
    server.metrics.create_database.add(1, &[]);
    tracing::info!("Creating database [{}] for tenant [{}]", name, tenant_id);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateDatabase,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
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
    let mut quota_payload = QuotaPayload::new(Action::CreateDatabase, tenant_id.clone(), api_token);
    quota_payload = quota_payload.with_collection_name(&name);
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:create_database",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let create_database_request = CreateDatabaseRequest::try_new(tenant_id, name)?;
    let res = server
        .frontend
        .create_database(create_database_request)
        .await?;
    Ok(Json(res))
}

#[derive(Deserialize, Serialize, ToSchema, Debug)]
struct ListDatabasesPayload {
    limit: Option<u32>,
    offset: u32,
}

#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_id}/databases",
    request_body = ListDatabasesPayload,
    responses(
        (status = 200, description = "List of databases", body = [ListDatabasesResponse]),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID to list databases for")
    )
)]
async fn list_databases(
    headers: HeaderMap,
    Path(tenant_id): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(ListDatabasesPayload { limit, offset }): Json<ListDatabasesPayload>,
) -> Result<Json<ListDatabasesResponse>, ServerError> {
    server.metrics.list_databases.add(1, &[]);
    tracing::info!("Listing database for tenant [{}]", tenant_id);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::ListDatabases,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: None,
                collection: None,
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:list_databases",
        format!("tenant:{}", tenant_id).as_str(),
    ]);

    let request = ListDatabasesRequest::try_new(tenant_id, limit, offset)?;
    Ok(Json(server.frontend.list_databases(request).await?))
}

#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}",
    responses(
        (status = 200, description = "Database retrieved successfully", body = GetDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Database not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Name of the database to retrieve")
    )
)]
async fn get_database(
    headers: HeaderMap,
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponse>, ServerError> {
    server.metrics.get_database.add(1, &[]);
    tracing::info!(
        "Getting database [{}] for tenant [{}]",
        database_name,
        tenant_id
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetDatabase,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: None,
            },
        )
        .await?;
    let _guard =
        server.scorecard_request(&["op:get_database", format!("tenant:{}", tenant_id).as_str()]);
    let request = GetDatabaseRequest::try_new(tenant_id, database_name)?;
    let res = server.frontend.get_database(request).await?;
    Ok(Json(res))
}

#[utoipa::path(
    delete,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}",
    responses(
        (status = 200, description = "Database deleted successfully", body = DeleteDatabaseResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Database not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Name of the database to delete")
    )
)]
async fn delete_database(
    headers: HeaderMap,
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<DeleteDatabaseResponse>, ServerError> {
    server.metrics.delete_database.add(1, &[]);
    tracing::info!(
        "Deleting database [{}] for tenant [{}]",
        database_name,
        tenant_id
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::DeleteDatabase,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: None,
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:delete_database",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request = DeleteDatabaseRequest::try_new(tenant_id, database_name)?;
    Ok(Json(server.frontend.delete_database(request).await?))
}

#[derive(Deserialize, Debug)]
struct ListCollectionsParams {
    limit: Option<u32>,
    #[serde(default)]
    offset: u32,
}

#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections",
    responses(
        (status = 200, description = "List of collections", body = [Collection]),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Database name to list collections from")
    )
)]
async fn list_collections(
    headers: HeaderMap,
    Path((tenant_id, database_name)): Path<(String, String)>,
    Query(ListCollectionsParams { limit, offset }): Query<ListCollectionsParams>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListCollectionsResponse>, ServerError> {
    server.metrics.list_collections.add(1, &[]);
    tracing::info!(
        "Listing collections in database [{}] for tenant [{}] with limit [{:?}] and offset [{:?}]",
        database_name,
        tenant_id,
        limit,
        offset
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::ListCollections,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: None,
            },
        )
        .await?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload =
        QuotaPayload::new(Action::ListCollections, tenant_id.clone(), api_token);
    if let Some(limit) = limit {
        quota_payload = quota_payload.with_limit(limit);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:list_collections",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request = ListCollectionsRequest::try_new(tenant_id, database_name, limit, offset)?;
    Ok(Json(server.frontend.list_collections(request).await?))
}

#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections_count",
    responses(
        (status = 200, description = "Count of collections", body = u32),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Database name to count collections from")
    )
)]
async fn count_collections(
    headers: HeaderMap,
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountCollectionsResponse>, ServerError> {
    server.metrics.count_collections.add(
        1,
        &[
            KeyValue::new("tenant_id", tenant_id.clone()),
            KeyValue::new("database_name", database_name.clone()),
        ],
    );
    tracing::info!(
        "Counting number of collections in database [{database_name}] for tenant [{tenant_id}]",
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CountCollections,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: None,
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:count_collections",
        format!("tenant:{}", tenant_id).as_str(),
    ]);

    let request = CountCollectionsRequest::try_new(tenant_id, database_name)?;
    Ok(Json(server.frontend.count_collections(request).await?))
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct CreateCollectionPayload {
    pub name: String,
    pub configuration: Option<serde_json::Value>,
    pub metadata: Option<Metadata>,
    pub get_or_create: bool,
}

#[utoipa::path(
    post,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections",
    request_body = CreateCollectionPayload,
    responses(
        (status = 200, description = "Collection created successfully", body = Collection),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Database name containing the new collection")
    )
)]
async fn create_collection(
    headers: HeaderMap,
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateCollectionPayload>,
) -> Result<Json<Collection>, ServerError> {
    server.metrics.create_collection.add(1, &[]);
    tracing::info!("Creating collection in database [{database_name}] for tenant [{tenant_id}]");
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::CreateCollection,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(payload.name.clone()),
            },
        )
        .await?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload =
        QuotaPayload::new(Action::CreateCollection, tenant_id.clone(), api_token);
    quota_payload = quota_payload.with_collection_name(&payload.name);
    if let Some(metadata) = &payload.metadata {
        quota_payload = quota_payload.with_create_collection_metadata(metadata);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:create_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request = CreateCollectionRequest::try_new(
        tenant_id,
        database_name,
        payload.name,
        payload.metadata,
        payload.configuration,
        payload.get_or_create,
    )?;
    let collection = server.frontend.create_collection(request).await?;

    Ok(Json(collection))
}

#[utoipa::path(
    get,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}",
    responses(
        (status = 200, description = "Collection found", body = Collection),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection")
    )
)]
async fn get_collection(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    server.metrics.get_collection.add(1, &[]);
    tracing::info!("Getting collection [{collection_name}] in database [{database_name}] for tenant [{tenant_id}]");
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::GetCollection,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_name.clone()),
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:get_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request = GetCollectionRequest::try_new(tenant_id, database_name, collection_name)?;
    let collection = server.frontend.get_collection(request).await?;
    Ok(Json(collection))
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
pub struct UpdateCollectionPayload {
    pub new_name: Option<String>,
    pub new_metadata: Option<UpdateMetadata>,
}

#[utoipa::path(
    put,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}",
    request_body = UpdateCollectionPayload,
    responses(
        (status = 200, description = "Collection updated successfully", body = UpdateCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection to update")
    )
)]
async fn update_collection(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionPayload>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server.metrics.update_collection.add(1, &[]);
    tracing::info!("Updating collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]");
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::UpdateCollection,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload =
        QuotaPayload::new(Action::UpdateCollection, tenant_id.clone(), api_token);
    if let Some(new_name) = &payload.new_name {
        quota_payload = quota_payload.with_collection_new_name(new_name);
    }
    if let Some(new_metadata) = &payload.new_metadata {
        quota_payload = quota_payload.with_update_collection_metadata(new_metadata);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:update_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;

    let request = chroma_types::UpdateCollectionRequest::try_new(
        collection_id,
        payload.new_name,
        payload
            .new_metadata
            .map(CollectionMetadataUpdate::UpdateMetadata),
    )?;

    server.frontend.update_collection(request).await?;

    Ok(Json(UpdateCollectionResponse {}))
}

#[utoipa::path(
    delete,
    path = "/api/v2/tenants/{tenant_id}/databases/{database_name}/collections/{collection_id}",
    responses(
        (status = 200, description = "Collection deleted successfully", body = UpdateCollectionResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    ),
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("database_name" = String, Path, description = "Database name"),
        ("collection_id" = String, Path, description = "UUID of the collection to delete")
    )
)]
async fn delete_collection(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server.metrics.delete_collection.add(1, &[]);
    tracing::info!("Deleting collection [{collection_name}] in database [{database_name}] for tenant [{tenant_id}]");
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::DeleteCollection,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_name.clone()),
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:delete_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request =
        chroma_types::DeleteCollectionRequest::try_new(tenant_id, database_name, collection_name)?;
    server.frontend.delete_collection(request).await?;

    Ok(Json(UpdateCollectionResponse {}))
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct AddCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<Metadata>>>,
}

#[utoipa::path(
    post,
    path = "/collection_add",
    request_body = AddCollectionRecordsPayload,
    responses(
        (status = 201, description = "Collection added successfully", body = AddCollectionRecordsResponse),
        (status = 400, description = "Invalid data for collection addition")
    )
)]
async fn collection_add(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<AddCollectionRecordsPayload>,
) -> Result<Json<AddCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_add.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Add,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Add, tenant_id.clone(), api_token);
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
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    server
        .frontend
        .validate_embedding(
            collection_id,
            payload.embeddings.as_ref(),
            true,
            |embedding| Some(embedding.len()),
        )
        .await?;

    let request = chroma_types::AddCollectionRecordsRequest::try_new(
        tenant_id,
        database_name,
        collection_id,
        payload.ids,
        payload.embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    let res = server.frontend.add(request).await?;

    Ok(Json(res))
}

#[derive(Deserialize, Debug, Clone, ToSchema)]
pub struct UpdateCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Option<Vec<f32>>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

#[utoipa::path(
    put,
    path = "/collection_update",
    request_body = UpdateCollectionRecordsPayload,
    responses(
        (status = 200, description = "Collection updated successfully"),
        (status = 404, description = "Collection not found")
    )
)]
async fn collection_update(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionRecordsPayload>,
) -> Result<Json<UpdateCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_update.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Update,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Update, tenant_id.clone(), api_token);
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
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    server
        .frontend
        .validate_embedding(
            collection_id,
            payload.embeddings.as_ref(),
            true,
            |embedding| embedding.as_ref().map(|e| e.len()),
        )
        .await?;

    let request = chroma_types::UpdateCollectionRecordsRequest::try_new(
        tenant_id,
        database_name,
        collection_id,
        payload.ids,
        payload.embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    Ok(Json(server.frontend.update(request).await?))
}

#[derive(Deserialize, Debug, Clone)]
pub struct UpsertCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

async fn collection_upsert(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpsertCollectionRecordsPayload>,
) -> Result<Json<UpsertCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_upsert.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Update,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Upsert, tenant_id.clone(), api_token);
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
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    server
        .frontend
        .validate_embedding(
            collection_id,
            payload.embeddings.as_ref(),
            true,
            |embedding| Some(embedding.len()),
        )
        .await?;

    let request = chroma_types::UpsertCollectionRecordsRequest::try_new(
        tenant_id,
        database_name,
        collection_id,
        payload.ids,
        payload.embeddings,
        payload.documents,
        payload.uris,
        payload.metadatas,
    )?;

    Ok(Json(server.frontend.upsert(request).await?))
}

#[derive(Deserialize, Debug, Clone)]
pub struct DeleteCollectionRecordsPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
}

async fn collection_delete(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<DeleteCollectionRecordsPayload>,
) -> Result<Json<DeleteCollectionRecordsResponse>, ServerError> {
    server.metrics.collection_delete.add(1, &[]);
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Delete,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let r#where = payload.where_fields.parse()?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Delete, tenant_id.clone(), api_token);
    if let Some(ids) = &payload.ids {
        quota_payload = quota_payload.with_ids(ids);
    }
    if let Some(r#where) = &r#where {
        quota_payload = quota_payload.with_where(r#where);
    }
    server.quota_enforcer.enforce(&quota_payload).await?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
        tenant_id,
        database_name,
        collection_id,
        payload.ids,
        r#where,
    )?;

    server.frontend.delete(request).await?;

    Ok(Json(DeleteCollectionRecordsResponse {}))
}

async fn collection_count(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountResponse>, ServerError> {
    server.metrics.collection_count.add(
        1,
        &[
            KeyValue::new("tenant_id", tenant_id.clone()),
            KeyValue::new("database_name", database_name.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    tracing::info!(
        "Counting number of records in collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]",
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Count,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    let request = CountRequest::try_new(
        tenant_id,
        database_name,
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?,
    )?;

    Ok(Json(server.frontend.count(request).await?))
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetRequestPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
    limit: Option<u32>,
    offset: Option<u32>,
    #[serde(default = "IncludeList::default_get")]
    include: IncludeList,
}

async fn collection_get(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<GetRequestPayload>,
) -> Result<Json<GetResponse>, ServerError> {
    server.metrics.collection_get.add(
        1,
        &[
            KeyValue::new("tenant_id", tenant_id.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Get,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let parsed_where = payload.where_fields.parse()?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Get, tenant_id.clone(), api_token);
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
    tracing::info!(
        "Getting records from collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]",
    );
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);
    let request = GetRequest::try_new(
        tenant_id,
        database_name,
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

#[derive(Deserialize, Debug, Clone)]
pub struct QueryRequestPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
    query_embeddings: Vec<Vec<f32>>,
    n_results: Option<u32>,
    #[serde(default = "IncludeList::default_query")]
    include: IncludeList,
}

async fn collection_query(
    headers: HeaderMap,
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<QueryRequestPayload>,
) -> Result<Json<QueryResponse>, ServerError> {
    server.metrics.collection_query.add(
        1,
        &[
            KeyValue::new("tenant_id", tenant_id.clone()),
            KeyValue::new("collection_id", collection_id.clone()),
        ],
    );
    server
        .authenticate_and_authorize(
            &headers,
            AuthzAction::Query,
            AuthzResource {
                tenant: Some(tenant_id.clone()),
                database: Some(database_name.clone()),
                collection: Some(collection_id.clone()),
            },
        )
        .await?;
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let parsed_where = payload.where_fields.parse()?;
    let api_token = headers
        .get("x-chroma-token")
        .map(|val| val.to_str().unwrap_or_default())
        .map(|val| val.to_string());
    let mut quota_payload = QuotaPayload::new(Action::Query, tenant_id.clone(), api_token);
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
    server.quota_enforcer.enforce(&quota_payload).await?;
    tracing::info!(
        "Querying records from collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]",
    );

    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);
    server
        .frontend
        .validate_embedding(
            collection_id,
            Some(&payload.query_embeddings),
            true,
            |embedding| Some(embedding.len()),
        )
        .await?;

    let request = QueryRequest::try_new(
        tenant_id,
        database_name,
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

#[derive(OpenApi)]
#[openapi(paths(
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
    collection_add,
    collection_update
))]
struct ApiDoc;
