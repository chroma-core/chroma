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
        let app = Router::new()
            // `GET /` goes to `root`
            .route("/api/v1/*any", get(v1_deprecation_notice).put(v1_deprecation_notice).patch(v1_deprecation_notice).delete(v1_deprecation_notice).head(v1_deprecation_notice).options(v1_deprecation_notice))
            .route("/api/v2/healthcheck", get(healthcheck))
            .route("/api/v2/heartbeat", get(heartbeat))
            .route("/api/v2/pre-flight-checks", get(pre_flight_checks))
            .route("/api/v2/reset", post(reset))
            .route("/api/v2/version", get(version))
            .route("/api/v2/auth/identity", get(get_user_identity))
            .route("/api/v2/tenants", post(create_tenant))
            .route("/api/v2/tenants/:tenant_name", get(get_tenant))
            .route("/api/v2/tenants/:tenant_id/databases", get(list_databases).post(create_database))
            .route("/api/v2/tenants/:tenant_id/databases/:name", get(get_database).delete(delete_database))
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections",
               post(create_collection).get(list_collections),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections_count",
                get(count_collections),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id",
                get(get_collection).put(update_collection).delete(delete_collection),
            )
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/add",
                post(collection_add),
            )
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/update",
                post(collection_update),
            )
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/upsert",
                post(collection_upsert),
            )
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/delete",
                post(collection_delete),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id/count",
                get(collection_count),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id/get",
                post(collection_get),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id/query",
                post(collection_query),
            )
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

async fn healthcheck(State(server): State<FrontendServer>) -> impl IntoResponse {
    server.metrics.healthcheck.add(1, &[]);
    let res = server.frontend.healthcheck().await;
    let code = match res.get_status_code() {
        tonic::Code::Ok => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };

    (code, Json(res))
}

async fn heartbeat(
    State(server): State<FrontendServer>,
) -> Result<Json<HeartbeatResponse>, ServerError> {
    server.metrics.heartbeat.add(1, &[]);
    Ok(Json(server.frontend.heartbeat().await?))
}

// Dummy implementation for now
async fn pre_flight_checks(
    State(server): State<FrontendServer>,
) -> Result<Json<ChecklistResponse>, ServerError> {
    server.metrics.pre_flight_checks.add(1, &[]);
    Ok(Json(ChecklistResponse {
        max_batch_size: server.frontend.clone().get_max_batch_size(),
    }))
}

async fn reset(State(mut server): State<FrontendServer>) -> Result<Json<bool>, ServerError> {
    server.metrics.reset.add(1, &[]);
    server.frontend.reset().await?;
    Ok(Json(true))
}

async fn version(State(server): State<FrontendServer>) -> &'static str {
    server.metrics.version.add(1, &[]);
    env!("CARGO_PKG_VERSION")
}

// TOOD: Dummy implementation for now
async fn get_user_identity(State(server): State<FrontendServer>) -> Json<GetUserIdentityResponse> {
    server.metrics.version.add(1, &[]);
    Json(GetUserIdentityResponse {
        user_id: String::new(),
        tenant: "default_tenant".to_string(),
        databases: vec!["default_database".to_string()],
    })
}

#[derive(Deserialize, Debug)]
struct CreateTenantPayload {
    name: String,
}

async fn create_tenant(
    headers: HeaderMap,
    State(mut server): State<FrontendServer>,
    Json(request): Json<CreateTenantPayload>,
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
    let request = CreateTenantRequest::try_new(request.name)?;
    Ok(Json(server.frontend.create_tenant(request).await?))
}

async fn get_tenant(
    Path(name): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetTenantResponse>, ServerError> {
    server.metrics.get_tenant.add(1, &[]);
    tracing::info!("Getting tenant [{}]", name);
    let request = GetTenantRequest::try_new(name)?;
    Ok(Json(server.frontend.get_tenant(request).await?))
}

#[derive(Deserialize, Debug)]
struct CreateDatabasePayload {
    name: String,
}

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

#[derive(Deserialize)]
struct ListDatabasesPayload {
    limit: Option<u32>,
    offset: u32,
}

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

#[derive(Deserialize)]
struct ListCollectionsParams {
    limit: Option<u32>,
    #[serde(default)]
    offset: u32,
}

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

async fn count_collections(
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountCollectionsResponse>, ServerError> {
    server.metrics.count_collections.add(1, &[]);
    tracing::info!(
        "Counting collections in database [{}] for tenant [{}]",
        database_name,
        tenant_id
    );
    let _guard = server.scorecard_request(&[
        "op:count_collections",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request = CountCollectionsRequest::try_new(tenant_id, database_name)?;
    Ok(Json(server.frontend.count_collections(request).await?))
}

#[derive(Deserialize, Debug, Clone)]
pub struct CreateCollectionPayload {
    pub name: String,
    pub configuration: Option<serde_json::Value>,
    pub metadata: Option<Metadata>,
    pub get_or_create: bool,
}

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

async fn get_collection(
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    server.metrics.get_collection.add(1, &[]);
    tracing::info!("Getting collection [{collection_name}] in database [{database_name}] for tenant [{tenant_id}]");
    let _guard = server.scorecard_request(&[
        "op:get_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request = GetCollectionRequest::try_new(tenant_id, database_name, collection_name)?;
    let collection = server.frontend.get_collection(request).await?;
    Ok(Json(collection))
}

#[derive(Deserialize, Debug, Clone)]
pub struct UpdateCollectionPayload {
    pub new_name: Option<String>,
    pub new_metadata: Option<UpdateMetadata>,
}

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

async fn delete_collection(
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    server.metrics.delete_collection.add(1, &[]);
    let _guard = server.scorecard_request(&[
        "op:delete_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let request =
        chroma_types::DeleteCollectionRequest::try_new(tenant_id, database_name, collection_name)?;
    server.frontend.delete_collection(request).await?;

    Ok(Json(UpdateCollectionResponse {}))
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<Metadata>>>,
}

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

#[derive(Deserialize, Debug, Clone)]
pub struct UpdateCollectionRecordsPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Option<Vec<f32>>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

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
