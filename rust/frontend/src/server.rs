use crate::ac::AdmissionControlledService;
use crate::errors::{ServerError, ValidationError};
use crate::frontend::Frontend;
use axum::ServiceExt;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};

use chroma_types::{CreateDatabaseRequest, Include, QueryResponse};
use mdac::CircuitBreakerConfig;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct FrontendServer {
    circuit_breaker_config: CircuitBreakerConfig,
    frontend: Frontend,
}

impl FrontendServer {
    pub fn new(circuit_breaker_config: CircuitBreakerConfig, frontend: Frontend) -> FrontendServer {
        let frontend = frontend;
        FrontendServer {
            circuit_breaker_config,
            frontend,
        }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer) {
        let circuit_breaker_config = server.circuit_breaker_config.clone();
        let app = Router::new()
            // `GET /` goes to `root`
            .route("/", get(root))
            .route("/api/v2/tenants/:tenant/databases", post(create_database))
            .route("/api/v2/tenants/:tenant/databases/:name", get(get_database))
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/query",
                post(query),
            )
            .with_state(server.clone());
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

    ////////////////////////// Method Implementations //////////////////////

    fn root(&self) -> &'static str {
        "Hello, World!"
    }
}

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

// Dummy implementation for now
async fn root(State(server): State<FrontendServer>) -> &'static str {
    server.root()
}

#[derive(Deserialize, Debug)]
struct CreateDatabasePayload {
    name: String,
}

async fn create_database(
    Path(tenant): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateDatabasePayload>,
) -> Result<(), ServerError> {
    tracing::info!(
        "Creating database for tenant: {} and name: {:?}",
        tenant,
        payload
    );
    let create_database_request = CreateDatabaseRequest {
        database_id: Uuid::new_v4(),
        tenant_id: tenant,
        database_name: payload.name,
    };
    server
        .frontend
        .create_database(create_database_request)
        .await?;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GetDatabaseResponsePayload {
    id: Uuid,
    name: String,
    tenant: String,
}

async fn get_database(
    Path((tenant, name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponsePayload>, ServerError> {
    tracing::info!("Getting database for tenant: {} and name: {}", tenant, name);
    let res = server
        .frontend
        .get_database(chroma_types::GetDatabaseRequest {
            tenant_id: tenant,
            database_name: name,
        })
        .await?;
    Ok(Json(GetDatabaseResponsePayload {
        id: res.database_id,
        name: res.database_name,
        tenant: res.tenant_id,
    }))
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WherePayload {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WhereDocumentPayload {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryRequestPayload {
    r#where: Option<WherePayload>,
    where_document: Option<WhereDocumentPayload>,
    query_embeddings: Vec<Vec<f32>>,
    n_results: Option<u32>,
    include: Vec<Include>,
}

async fn query(
    Path((tenant, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<QueryRequestPayload>,
) -> Result<Json<QueryResponse>, ServerError> {
    let collection_uuid =
        Uuid::parse_str(&collection_id).map_err(|_| ValidationError::InvalidCollectionId)?;
    tracing::info!(
        "Querying database for tenant: {}, db_name: {} and collection id: {}",
        tenant,
        database_name,
        collection_uuid
    );

    let res = server
        .frontend
        .query(chroma_types::QueryRequest {
            tenant_id: tenant,
            database_name,
            collection_id: collection_uuid,
            r#where: None,
            include: Vec::new(),
            embeddings: payload.query_embeddings,
            n_results: payload.n_results.unwrap_or(10),
        })
        .await?;

    Ok(Json(res))
}
