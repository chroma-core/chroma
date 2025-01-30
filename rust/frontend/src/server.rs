use std::str::FromStr;

use axum::{
    extract::{Path, State},
    routing::{delete, get, post},
    Json, Router, ServiceExt,
};
use chroma_types::{
    AddToCollectionResponse, ChecklistResponse, Collection, CollectionUuid, CountRequest,
    CountResponse, CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantRequest,
    CreateTenantResponse, DeleteDatabaseRequest, DeleteDatabaseResponse, GetCollectionRequest,
    GetDatabaseRequest, GetDatabaseResponse, GetRequest, GetResponse, GetTenantRequest,
    GetTenantResponse, GetUserIdentityResponse, IncludeList, ListDatabasesRequest,
    ListDatabasesResponse, Metadata, QueryRequest, QueryResponse,
};
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
    FrontendConfig,
};

#[derive(Clone)]
pub(crate) struct FrontendServer {
    config: FrontendConfig,
    frontend: Frontend,
}

impl FrontendServer {
    pub fn new(config: FrontendConfig, frontend: Frontend) -> FrontendServer {
        FrontendServer { config, frontend }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer) {
        let circuit_breaker_config = server.config.circuit_breaker.clone();
        let app = Router::new()
            // `GET /` goes to `root`
            .route("/api/v2", get(root))
            .route("/api/v2/heartbeat", get(heartbeat))
            .route("/api/v2/pre-flight-checks", get(pre_flight_checks))
            .route("/api/v2/reset", post(reset))
            .route("/api/v2/version", get(version))
            .route("/api/v2/auth/identity", get(get_user_identity))
            .route("/api/v2/tenants", post(create_tenant))
            .route("/api/v2/tenants/:tenant_name", get(get_tenant))
            .route("/api/v2/tenants/:tenant_id/databases", post(create_database))
            .route("/api/v2/tenants/:tenant_id/databases", get(list_databases))
            .route("/api/v2/tenants/:tenant_id/databases/:name", get(get_database))
            .route("/api/v2/tenants/:tenant_id/databases/:name", delete(delete_database))
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_name",
                get(get_collection),
            )
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/add",
                post(collection_add),
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
            .with_state(server);
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
}

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

// Dummy implementation for now
async fn root() -> &'static str {
    "Chroma Rust Frontend"
}

async fn heartbeat() -> &'static str {
    "<Heartbeat.wav>"
}

// Dummy implementation for now
async fn pre_flight_checks() -> Result<Json<ChecklistResponse>, ServerError> {
    Ok(Json(ChecklistResponse {
        max_batch_size: 100,
    }))
}

async fn reset(State(mut server): State<FrontendServer>) -> Result<(), ServerError> {
    server.frontend.reset().await?;
    Ok(())
}

async fn version() -> &'static str {
    "0.7.0-dev"
}

// Dummy implementation for now
async fn get_user_identity() -> Json<GetUserIdentityResponse> {
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
    tracing::info!("Creating tenant with name: {}", request.name);
    Ok(Json(server.frontend.create_tenant(request).await?))
}

async fn get_tenant(
    Path(name): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetTenantResponse>, ServerError> {
    tracing::info!("Getting tenant with name: {}", name);
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
    Json(payload): Json<CreateDatabasePayload>,
) -> Result<Json<CreateDatabaseResponse>, ServerError> {
    tracing::info!(
        "Creating database for tenant: {} and name: {}",
        tenant_id,
        payload.name
    );
    let create_database_request = CreateDatabaseRequest {
        database_id: Uuid::new_v4(),
        tenant_id,
        database_name: payload.name,
    };
    let res = server
        .frontend
        .create_database(create_database_request)
        .await?;
    Ok(Json(res))
}

async fn list_databases(
    Path(tenant_id): Path<String>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<ListDatabasesResponse>, ServerError> {
    tracing::info!("Listing database for tenant: {}", tenant_id);
    let list_databases = server
        .frontend
        .list_databases(ListDatabasesRequest { tenant_id });
    Ok(Json(list_databases.await?))
}

async fn get_database(
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponse>, ServerError> {
    tracing::info!(
        "Getting database for tenant: {} and name: {}",
        tenant_id,
        database_name
    );
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
        "Deleting database for tenant: {} and name: {}",
        tenant_id,
        database_name
    );
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

async fn get_collection(
    Path((tenant_id, database_name, collection_name)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<Collection>, ServerError> {
    tracing::info!("Getting collection for tenant [{tenant_id}], database [{database_name}], and collection name [{collection_name}]");
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

#[derive(Debug, Clone, Deserialize)]
pub struct QueryRequestPayload {
    ids: Option<Vec<String>>,
    #[serde(flatten)]
    where_fields: RawWhereFields,
    query_embeddings: Vec<Vec<f32>>,
    n_results: Option<u32>,
    include: IncludeList,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddToCollectionPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<String>>,
    uri: Option<Vec<String>>,
    metadatas: Option<Vec<Metadata>>,
}

async fn collection_add(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<AddToCollectionPayload>,
) -> Result<Json<AddToCollectionResponse>, ServerError> {
    let collection_id =
        Uuid::parse_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;

    let res = server
        .frontend
        .add(chroma_types::AddToCollectionRequest {
            tenant_id,
            database_name,
            collection_id,
            ids: payload.ids,
            embeddings: payload.embeddings,
            documents: payload.documents,
            uri: payload.uri,
            metadatas: payload.metadatas,
        })
        .await?;

    Ok(Json(res))
}

async fn collection_count(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<CountResponse>, ServerError> {
    tracing::info!(
        "Counting collection [{collection_id}] from tenant [{tenant_id}] and database [{database_name}]",
    );

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
    offset: u32,
    include: IncludeList,
}

async fn collection_get(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<GetRequestPayload>,
) -> Result<Json<GetResponse>, ServerError> {
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    tracing::info!(
        "Get collection [{collection_id}] from tenant [{tenant_id}] and database [{database_name}], with query parameters [{payload:?}]",
    );
    let res = server
        .frontend
        .get(GetRequest {
            tenant_id,
            database_name,
            collection_id,
            ids: payload.ids,
            r#where: payload.where_fields.parse()?,
            limit: payload.limit,
            offset: payload.offset,
            include: payload.include,
        })
        .await?;
    Ok(Json(res))
}

async fn collection_query(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<QueryRequestPayload>,
) -> Result<Json<QueryResponse>, ServerError> {
    let collection_id =
        CollectionUuid::from_str(&collection_id).map_err(|_| ValidationError::CollectionId)?;
    tracing::info!(
        "Querying collection [{collection_id}] from tenant [{tenant_id}] and database [{database_name}], with query parameters [{payload:?}]",
    );

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
