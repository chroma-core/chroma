use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::SystemTime;

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router, ServiceExt,
};
use biometrics::Collector;
use biometrics_prometheus::SlashMetrics;
use chroma_types::{
    operator::Filter, AddCollectionRecordsResponse, ChecklistResponse, Collection,
    CollectionMetadataUpdate, CollectionUuid, CountCollectionsRequest, CountCollectionsResponse,
    CountRequest, CountResponse, CreateCollectionRequest, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    DeleteCollectionRecordsResponse, DeleteDatabaseRequest, DeleteDatabaseResponse,
    GetCollectionRequest, GetDatabaseRequest, GetDatabaseResponse, GetRequest, GetResponse,
    GetTenantRequest, GetTenantResponse, GetUserIdentityResponse, HeartbeatResponse, IncludeList,
    ListCollectionsRequest, ListCollectionsResponse, ListDatabasesRequest, ListDatabasesResponse,
    Metadata, QueryRequest, QueryResponse, UpdateCollectionRecordsResponse,
    UpdateCollectionResponse, UpdateMetadata, UpsertCollectionRecordsResponse,
};
use mdac::{Rule, Scorecard, ScorecardTicket};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ac::AdmissionControlledService,
    frontend::Frontend,
    tower_tracing::add_tracing_middleware,
    types::{
        errors::{ServerError, ValidationError},
        where_parsing::RawWhereFields,
    },
    utils::{validate_name, validate_non_empty_filter, validate_non_empty_metadata},
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

///////////////////////////////////////////// counters /////////////////////////////////////////////

static ROOT: biometrics::Counter = biometrics::Counter::new("chroma__frontend__root");
static HEARTBEAT: biometrics::Counter = biometrics::Counter::new("chroma__frontend__heartbeat");
static PRE_FLIGHT_CHECKS: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__pre_flight_checks");
static RESET: biometrics::Counter = biometrics::Counter::new("chroma__frontend__reset");
static VERSION: biometrics::Counter = biometrics::Counter::new("chroma__frontend__version");
static GET_USER_IDENTITY: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__get_user_identity");
static CREATE_TENANT: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__create_tenant");
static GET_TENANT: biometrics::Counter = biometrics::Counter::new("chroma__frontend__get_tenant");
static LIST_DATABASES: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__list_databases");
static GET_DATABASE: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__get_database");
static CREATE_COLLECTION: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__create_collection");
static LIST_COLLECTIONS: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__list_collections");
static COUNT_COLLECTIONS: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__count_collections");
static GET_COLLECTION: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__get_collection");
static COLLECTION_ADD: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_add");
static COLLECTION_UPDATE: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_update");
static COLLECTION_UPSERT: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_upsert");
static COLLECTION_DELETE: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_delete");
static COLLECTION_COUNT: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_count");
static COLLECTION_GET: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_get");
static COLLECTION_QUERY: biometrics::Counter =
    biometrics::Counter::new("chroma__frontend__collection_query");

pub fn register_biometrics(collector: &Collector) {
    collector.register_counter(&ROOT);
    collector.register_counter(&HEARTBEAT);
    collector.register_counter(&PRE_FLIGHT_CHECKS);
    collector.register_counter(&RESET);
    collector.register_counter(&VERSION);
    collector.register_counter(&GET_USER_IDENTITY);
    collector.register_counter(&CREATE_TENANT);
    collector.register_counter(&GET_TENANT);
    collector.register_counter(&LIST_DATABASES);
    collector.register_counter(&GET_DATABASE);
    collector.register_counter(&CREATE_COLLECTION);
    collector.register_counter(&LIST_COLLECTIONS);
    collector.register_counter(&COUNT_COLLECTIONS);
    collector.register_counter(&GET_COLLECTION);
    collector.register_counter(&COLLECTION_ADD);
    collector.register_counter(&COLLECTION_UPDATE);
    collector.register_counter(&COLLECTION_UPSERT);
    collector.register_counter(&COLLECTION_DELETE);
    collector.register_counter(&COLLECTION_COUNT);
    collector.register_counter(&COLLECTION_GET);
    collector.register_counter(&COLLECTION_QUERY);
}

////////////////////////////////////////// FrontendServer //////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct FrontendServer {
    config: FrontendConfig,
    frontend: Frontend,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
}

impl FrontendServer {
    pub fn new(config: FrontendConfig, frontend: Frontend, rules: Vec<Rule>) -> FrontendServer {
        // NOTE(rescrv):  Assume statically no more than 128 threads because we won't deploy on
        // hardware with that many threads anytime soon for frontends, if ever.
        let scorecard_enabled = Arc::new(AtomicBool::new(config.scorecard_enabled));
        // SAFETY(rescrv):  This is safe because 128 is non-zero.
        let scorecard = Arc::new(Scorecard::new(&(), rules, 128.try_into().unwrap()));
        FrontendServer {
            config,
            frontend,
            scorecard_enabled,
            scorecard,
        }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer) {
        let circuit_breaker_config = server.config.circuit_breaker.clone();
        let app = Router::new()
            // `GET /` goes to `root`
            .route("/metrics", get(metrics))
            .route("/api/v2", get(root))
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

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

async fn metrics() -> String {
    let mut metrics = SlashMetrics::new();
    let collector = Collector::new();
    register_biometrics(&collector);
    let _ = collector.emit(
        &mut metrics,
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    );
    metrics.take()
}

// Dummy implementation for now
async fn root() -> &'static str {
    ROOT.click();
    "Chroma Rust Frontend"
}

async fn healthcheck(State(server): State<FrontendServer>) -> impl IntoResponse {
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
    HEARTBEAT.click();
    Ok(Json(server.frontend.heartbeat().await?))
}

// Dummy implementation for now
async fn pre_flight_checks() -> Result<Json<ChecklistResponse>, ServerError> {
    PRE_FLIGHT_CHECKS.click();
    Ok(Json(ChecklistResponse {
        max_batch_size: 100,
    }))
}

async fn reset(State(mut server): State<FrontendServer>) -> Result<Json<bool>, ServerError> {
    RESET.click();
    server.frontend.reset().await?;
    Ok(Json(true))
}

async fn version() -> &'static str {
    VERSION.click();
    env!("CARGO_PKG_VERSION")
}

// Dummy implementation for now
async fn get_user_identity() -> Json<GetUserIdentityResponse> {
    GET_USER_IDENTITY.click();
    Json(GetUserIdentityResponse {
        user_id: String::new(),
        tenant: "default_tenant".to_string(),
        databases: vec!["default_database".to_string()],
    })
}

async fn create_tenant(
    State(mut server): State<FrontendServer>,
    Json(request): Json<CreateTenantRequest>,
) -> Result<Json<CreateTenantResponse>, ServerError> {
    CREATE_TENANT.click();
    tracing::info!("Creating tenant [{}]", request.name);
    Ok(Json(server.frontend.create_tenant(request).await?))
}

async fn get_tenant(
    Path(name): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetTenantResponse>, ServerError> {
    GET_TENANT.click();
    tracing::info!("Getting tenant [{}]", name);
    Ok(Json(
        server
            .frontend
            .get_tenant(GetTenantRequest { name })
            .await?,
    ))
}

#[derive(Deserialize, Debug)]
struct CreateDatabasePayload {
    name: String,
}

async fn create_database(
    Path(tenant_id): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(CreateDatabasePayload { name }): Json<CreateDatabasePayload>,
) -> Result<Json<CreateDatabaseResponse>, ServerError> {
    tracing::info!("Creating database [{}] for tenant [{}]", name, tenant_id);
    let _guard = server.scorecard_request(&[
        "op:create_database",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let create_database_request = CreateDatabaseRequest {
        database_id: Uuid::new_v4(),
        tenant_id,
        database_name: name,
    };
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
    Path(tenant_id): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(ListDatabasesPayload { limit, offset }): Json<ListDatabasesPayload>,
) -> Result<Json<ListDatabasesResponse>, ServerError> {
    tracing::info!("Listing database for tenant [{}]", tenant_id);
    let _guard = server.scorecard_request(&[
        "op:list_databases",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    Ok(Json(
        server
            .frontend
            .list_databases(ListDatabasesRequest {
                tenant_id,
                limit,
                offset,
            })
            .await?,
    ))
}

async fn get_database(
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponse>, ServerError> {
    GET_DATABASE.click();
    tracing::info!(
        "Getting database [{}] for tenant [{}]",
        database_name,
        tenant_id
    );
    let _guard =
        server.scorecard_request(&["op:get_database", format!("tenant:{}", tenant_id).as_str()]);
    let res = server
        .frontend
        .get_database(GetDatabaseRequest {
            tenant_id,
            database_name,
        })
        .await?;
    Ok(Json(res))
}

async fn delete_database(
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<DeleteDatabaseResponse>, ServerError> {
    tracing::info!(
        "Deleting database [{}] for tenant [{}]",
        database_name,
        tenant_id
    );
    let _guard = server.scorecard_request(&[
        "op:delete_database",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    Ok(Json(
        server
            .frontend
            .delete_database(DeleteDatabaseRequest {
                tenant_id,
                database_name,
            })
            .await?,
    ))
}

#[derive(Deserialize)]
struct ListCollectionsParams {
    limit: Option<u32>,
    #[serde(default)]
    offset: u32,
}

async fn list_collections(
    Path((tenant_id, database_name)): Path<(String, String)>,
    Query(ListCollectionsParams { limit, offset }): Query<ListCollectionsParams>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListCollectionsResponse>, ServerError> {
    LIST_COLLECTIONS.click();
    tracing::info!(
        "Listing collections in database [{}] for tenant [{}] with limit [{:?}] and offset [{:?}]",
        database_name,
        tenant_id,
        limit,
        offset
    );
    let _guard = server.scorecard_request(&[
        "op:list_collections",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    Ok(Json(
        server
            .frontend
            .list_collections(ListCollectionsRequest {
                tenant_id,
                database_name,
                limit,
                offset,
            })
            .await?,
    ))
}

async fn count_collections(
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountCollectionsResponse>, ServerError> {
    COUNT_COLLECTIONS.click();
    tracing::info!(
        "Counting collections in database [{}] for tenant [{}]",
        database_name,
        tenant_id
    );
    let _guard = server.scorecard_request(&[
        "op:count_collections",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    Ok(Json(
        server
            .frontend
            .count_collections(CountCollectionsRequest {
                tenant_id,
                database_name,
            })
            .await?,
    ))
}

#[derive(Deserialize, Debug, Clone)]
pub struct CreateCollectionPayload {
    pub name: String,
    pub configuration: Option<serde_json::Value>,
    pub metadata: Option<Metadata>,
    pub get_or_create: bool,
}

async fn create_collection(
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateCollectionPayload>,
) -> Result<Json<Collection>, ServerError> {
    CREATE_COLLECTION.click();
    tracing::info!("Creating collection in database [{database_name}] for tenant [{tenant_id}]");
    let _guard = server.scorecard_request(&[
        "op:create_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    validate_name(&payload.name)?;
    if let Some(metadata) = payload.metadata.as_ref() {
        validate_non_empty_metadata(metadata)?;
    }
    let collection = server
        .frontend
        .create_collection(CreateCollectionRequest {
            name: payload.name,
            tenant_id,
            database_name,
            metadata: payload.metadata,
            configuration_json: payload.configuration,
            get_or_create: payload.get_or_create,
        })
        .await?;

    Ok(Json(collection))
}

async fn get_collection(
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    GET_COLLECTION.click();
    tracing::info!("Getting collection [{collection_name}] in database [{database_name}] for tenant [{tenant_id}]");
    let _guard = server.scorecard_request(&[
        "op:get_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    let collection = server
        .frontend
        .get_collection(GetCollectionRequest {
            tenant_id,
            database_name,
            collection_name,
        })
        .await?;
    Ok(Json(collection))
}

#[derive(Deserialize, Debug, Clone)]
pub struct UpdateCollectionPayload {
    pub new_name: Option<String>,
    pub new_metadata: Option<UpdateMetadata>,
}

async fn update_collection(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionPayload>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    tracing::info!("Updating collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]");
    let _guard = server.scorecard_request(&[
        "op:update_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    if let Some(name) = payload.new_name.as_ref() {
        validate_name(name)?;
    }
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;

    if let Some(metadata) = payload.new_metadata.as_ref() {
        validate_non_empty_metadata(metadata)?;
    }

    server
        .frontend
        .update_collection(chroma_types::UpdateCollectionRequest {
            collection_id,
            new_name: payload.new_name,
            new_metadata: payload
                .new_metadata
                .map(CollectionMetadataUpdate::UpdateMetadata),
        })
        .await?;

    Ok(Json(UpdateCollectionResponse {}))
}

async fn delete_collection(
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<UpdateCollectionResponse>, ServerError> {
    let _guard = server.scorecard_request(&[
        "op:delete_collection",
        format!("tenant:{}", tenant_id).as_str(),
    ]);
    server
        .frontend
        .delete_collection(chroma_types::DeleteCollectionRequest {
            tenant_id,
            database_name,
            collection_name,
        })
        .await?;

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
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<AddCollectionRecordsPayload>,
) -> Result<Json<AddCollectionRecordsResponse>, ServerError> {
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);
    COLLECTION_ADD.click();
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);

    server
        .frontend
        .validate_embedding(
            collection_id,
            payload.embeddings.as_ref(),
            true,
            |embedding| Some(embedding.len()),
        )
        .await?;

    let res = server
        .frontend
        .add(chroma_types::AddCollectionRecordsRequest {
            tenant_id,
            database_name,
            collection_id,
            ids: payload.ids,
            embeddings: payload.embeddings,
            documents: payload.documents,
            uris: payload.uris,
            metadatas: payload.metadatas,
        })
        .await?;

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
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpdateCollectionRecordsPayload>,
) -> Result<Json<UpdateCollectionRecordsResponse>, ServerError> {
    COLLECTION_UPDATE.click();
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
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

    Ok(Json(
        server
            .frontend
            .update(chroma_types::UpdateCollectionRecordsRequest {
                tenant_id,
                database_name,
                collection_id,
                ids: payload.ids,
                embeddings: payload.embeddings,
                documents: payload.documents,
                uris: payload.uris,
                metadatas: payload.metadatas,
            })
            .await?,
    ))
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
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<UpsertCollectionRecordsPayload>,
) -> Result<Json<UpsertCollectionRecordsResponse>, ServerError> {
    COLLECTION_UPSERT.click();
    let collection_id =
        CollectionUuid(Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?);
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

    Ok(Json(
        server
            .frontend
            .upsert(chroma_types::UpsertCollectionRecordsRequest {
                tenant_id,
                database_name,
                collection_id,
                ids: payload.ids,
                embeddings: payload.embeddings,
                documents: payload.documents,
                uris: payload.uris,
                metadatas: payload.metadatas,
            })
            .await?,
    ))
}

#[derive(Deserialize, Debug, Clone)]
pub struct DeleteCollectionRecordsPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
}

async fn collection_delete(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<DeleteCollectionRecordsPayload>,
) -> Result<Json<DeleteCollectionRecordsResponse>, ServerError> {
    COLLECTION_DELETE.click();
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    let _guard = server.scorecard_request(&[
        "op:write",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    let r#where = payload.where_fields.parse()?;

    validate_non_empty_filter(&Filter {
        query_ids: payload.ids.clone(),
        where_clause: r#where.clone(),
    })?;

    server
        .frontend
        .delete(chroma_types::DeleteCollectionRecordsRequest {
            tenant_id,
            database_name,
            collection_id,
            ids: payload.ids,
            r#where,
        })
        .await?;

    Ok(Json(DeleteCollectionRecordsResponse {}))
}

async fn collection_count(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountResponse>, ServerError> {
    tracing::info!(
        "Counting number of records in collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]",
    );
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);

    Ok(Json(
        server
            .frontend
            .count(CountRequest {
                tenant_id,
                database_name,
                collection_id: CollectionUuid::from_str(&collection_id)
                    .map_err(|_| ValidationError::CollectionId)?,
            })
            .await?,
    ))
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
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<GetRequestPayload>,
) -> Result<Json<GetResponse>, ServerError> {
    COLLECTION_GET.click();
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    tracing::info!(
        "Getting records from collection [{collection_id}] in database [{database_name}] for tenant [{tenant_id}]",
    );
    let _guard = server.scorecard_request(&[
        "op:read",
        format!("tenant:{}", tenant_id).as_str(),
        format!("collection:{}", collection_id).as_str(),
    ]);
    let res = server
        .frontend
        .get(GetRequest {
            tenant_id,
            database_name,
            collection_id,
            ids: payload.ids,
            r#where: payload.where_fields.parse()?,
            limit: payload.limit,
            offset: payload.offset.unwrap_or(0),
            include: payload.include,
        })
        .await?;
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
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<QueryRequestPayload>,
) -> Result<Json<QueryResponse>, ServerError> {
    COLLECTION_QUERY.click();
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
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

    let res = server
        .frontend
        .query(QueryRequest {
            tenant_id,
            database_name,
            collection_id,
            ids: payload.ids,
            r#where: payload.where_fields.parse()?,
            include: payload.include,
            embeddings: payload.query_embeddings,
            n_results: payload.n_results.unwrap_or(10),
        })
        .await?;

    Ok(Json(res))
}
