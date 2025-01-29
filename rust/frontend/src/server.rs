use crate::ac::AdmissionControlledService;
use crate::config::FrontendConfig;
use crate::frontend::Frontend;
use crate::tower_tracing::add_tracing_middleware;
use crate::types::errors::{ServerError, ValidationError};
use crate::types::where_parsing::{parse_where, parse_where_document};
use axum::ServiceExt;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use chroma_types::{
    AddToCollectionResponse, Collection, CollectionUuid, CompositeExpression, CountRequest,
    CountResponse, CreateDatabaseRequest, GetCollectionRequest, GetDatabaseRequest, GetRequest,
    GetResponse, GetTenantResponse, GetUserIdentityResponse, Include, IncludeList, Metadata,
    QueryRequest, QueryResponse, Where,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use uuid::Uuid;

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
            .route("/", get(root))
            .route("/api/v2/auth/identity", get(get_user_identity))
            .route("/api/v2/tenants/:tenant_id", get(get_tenant))
            .route("/api/v2/tenants/:tenant_id/databases", post(create_database))
            .route("/api/v2/tenants/:tenant_id/databases/:name", get(get_database))
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_name",
                get(get_collection),
            )
            .route(
                "/api/v2/tenants/:tenant/databases/:database_name/collections/:collection_id/add",
                post(add),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id/count",
                get(count),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id/get",
                post(collection_get),
            )
            .route(
                "/api/v2/tenants/:tenant_id/databases/:database_name/collections/:collection_id/query",
                post(query),
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

// Dummy implementation for now
async fn get_user_identity() -> Json<GetUserIdentityResponse> {
    Json(GetUserIdentityResponse {
        user_id: String::new(),
        tenant: "default_tenant".to_string(),
        databases: vec!["default_database".to_string()],
    })
}

// Dummy implementation for now
async fn get_tenant() -> Json<GetTenantResponse> {
    Json(GetTenantResponse {
        name: "default_tenant".to_string(),
    })
}

#[derive(Deserialize, Debug)]
struct CreateDatabasePayload {
    name: String,
}

async fn create_database(
    Path(tenant_id): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateDatabasePayload>,
) -> Result<(), ServerError> {
    tracing::info!(
        "Creating database for tenant: {} and name: {:?}",
        tenant_id,
        payload
    );
    let create_database_request = CreateDatabaseRequest {
        database_id: Uuid::new_v4(),
        tenant_id,
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
    Path((tenant_id, database_name)): Path<(String, String)>,
    State(mut server): State<FrontendServer>,
) -> Result<Json<GetDatabaseResponsePayload>, ServerError> {
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
    Ok(Json(GetDatabaseResponsePayload {
        id: res.database_id,
        name: res.database_name,
        tenant: res.tenant_id,
    }))
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

#[derive(Debug, Clone)]
pub struct QueryRequestPayload {
    ids: Option<Vec<String>>,
    r#where: Option<Where>,
    query_embeddings: Vec<Vec<f32>>,
    n_results: Option<u32>,
    include: IncludeList,
}

impl TryFrom<Value> for QueryRequestPayload {
    type Error = ValidationError;

    fn try_from(json_payload: Value) -> Result<Self, Self::Error> {
        let ids = match &json_payload["ids"] {
            Value::Null => None,
            Value::Array(uids) => Some(
                uids.iter()
                    .map(|id| {
                        id.as_str()
                            .ok_or(ValidationError::InvalidUserID)
                            .map(ToString::to_string)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(ValidationError::InvalidUserID),
        };
        let n_results = match &json_payload["n_results"] {
            Value::Null => None,
            Value::Number(n) => Some(n.as_u64().unwrap() as u32),
            _ => return Err(ValidationError::InvalidLimit),
        };
        let embeddings = json_payload["query_embeddings"]
            .as_array()
            .ok_or(ValidationError::InvalidEmbeddings)?
            .iter()
            .map(|inner_array| {
                inner_array
                    .as_array()
                    .ok_or(ValidationError::InvalidEmbeddings)
                    .and_then(|arr| {
                        arr.iter()
                            .map(|num| {
                                num.as_f64()
                                    .ok_or(ValidationError::InvalidEmbeddings)
                                    .map(|n| n as f32)
                            })
                            .collect::<Result<Vec<f32>, _>>()
                    })
            })
            .collect::<Result<Vec<Vec<f32>>, _>>()?;
        let mut where_clause = None;
        if !json_payload["where"].is_null() {
            let where_payload = &json_payload["where"];
            where_clause = Some(parse_where(where_payload)?);
        }
        let mut where_document_clause = None;
        if !json_payload["where_document"].is_null() {
            let where_document_payload = &json_payload["where_document"];
            where_document_clause = Some(parse_where_document(where_document_payload)?);
        }
        let combined_where = match where_clause {
            Some(where_clause) => match where_document_clause {
                Some(where_document_clause) => Some(Where::Composite(CompositeExpression {
                    operator: chroma_types::BooleanOperator::And,
                    children: vec![where_clause, where_document_clause],
                })),
                None => Some(where_clause),
            },
            None => where_document_clause,
        };
        // Parse includes.
        let include = match &json_payload["include"] {
            Value::Null => IncludeList::default_query(),
            Value::Array(arr) => {
                let mut include_list = IncludeList {
                    includes: Vec::new(),
                };
                for val in arr {
                    if !val.is_string() {
                        return Err(ValidationError::InvalidIncludeList);
                    }
                    let include_str = val.as_str().unwrap();
                    match include_str {
                        "distances" => include_list.includes.push(Include::Distance),
                        "documents" => include_list.includes.push(Include::Document),
                        "embeddings" => include_list.includes.push(Include::Embedding),
                        "metadatas" => include_list.includes.push(Include::Metadata),
                        "uris" => include_list.includes.push(Include::Uri),
                        _ => return Err(ValidationError::InvalidIncludeList),
                    }
                }
                include_list
            }
            _ => return Err(ValidationError::InvalidIncludeList),
        };

        Ok(QueryRequestPayload {
            ids,
            r#where: combined_where,
            query_embeddings: embeddings,
            n_results,
            include,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddToCollectionPayload {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<String>>,
    uri: Option<Vec<String>>,
    metadatas: Option<Vec<Metadata>>,
}

async fn add(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<AddToCollectionPayload>,
) -> Result<Json<AddToCollectionResponse>, ServerError> {
    let collection_id =
        Uuid::parse_str(&collection_id).map_err(|_| ValidationError::InvalidCollectionId)?;

    server
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

    Ok(Json(AddToCollectionResponse {}))
}

async fn count(
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
                    .map_err(|_| ValidationError::InvalidCollectionId)?,
            })
            .await?,
    ))
}

#[derive(Debug, Clone)]
pub struct GetRequestPayload {
    ids: Option<Vec<String>>,
    r#where: Option<Where>,
    limit: Option<u32>,
    offset: u32,
    include: IncludeList,
}

impl TryFrom<Value> for GetRequestPayload {
    type Error = ValidationError;

    fn try_from(json_payload: Value) -> Result<Self, Self::Error> {
        let ids = match &json_payload["ids"] {
            Value::Null => None,
            Value::Array(uids) => Some(
                uids.iter()
                    .map(|id| {
                        id.as_str()
                            .ok_or(ValidationError::InvalidUserID)
                            .map(ToString::to_string)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(ValidationError::InvalidUserID),
        };
        let limit = match &json_payload["limit"] {
            Value::Null => None,
            Value::Number(n) => Some(n.as_u64().unwrap() as u32),
            _ => return Err(ValidationError::InvalidLimit),
        };
        let offset = match &json_payload["offset"] {
            Value::Null => 0,
            Value::Number(n) => n.as_u64().unwrap() as u32,
            _ => return Err(ValidationError::InvalidLimit),
        };
        let mut where_clause = None;
        if !json_payload["where"].is_null() {
            let where_payload = &json_payload["where"];
            where_clause = Some(parse_where(where_payload)?);
        }
        let mut where_document_clause = None;
        if !json_payload["where_document"].is_null() {
            let where_document_payload = &json_payload["where_document"];
            where_document_clause = Some(parse_where_document(where_document_payload)?);
        }
        let combined_where = match where_clause {
            Some(where_clause) => match where_document_clause {
                Some(where_document_clause) => Some(Where::Composite(CompositeExpression {
                    operator: chroma_types::BooleanOperator::And,
                    children: vec![where_clause, where_document_clause],
                })),
                None => Some(where_clause),
            },
            None => where_document_clause,
        };
        // Parse includes.
        let include = match &json_payload["include"] {
            Value::Null => IncludeList::default_query(),
            Value::Array(arr) => {
                let mut include_list = IncludeList {
                    includes: Vec::new(),
                };
                for val in arr {
                    if !val.is_string() {
                        return Err(ValidationError::InvalidIncludeList);
                    }
                    let include_str = val.as_str().unwrap();
                    match include_str {
                        "distances" => include_list.includes.push(Include::Distance),
                        "documents" => include_list.includes.push(Include::Document),
                        "embeddings" => include_list.includes.push(Include::Embedding),
                        "metadatas" => include_list.includes.push(Include::Metadata),
                        "uris" => include_list.includes.push(Include::Uri),
                        _ => return Err(ValidationError::InvalidIncludeList),
                    }
                }
                include_list
            }
            _ => return Err(ValidationError::InvalidIncludeList),
        };

        Ok(GetRequestPayload {
            ids,
            r#where: combined_where,
            limit,
            offset,
            include,
        })
    }
}

async fn collection_get(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(json_payload): Json<Value>,
) -> Result<Json<GetResponse>, ServerError> {
    let collection_id = CollectionUuid::from_str(&collection_id)
        .map_err(|_| ValidationError::InvalidCollectionId)?;
    let payload = GetRequestPayload::try_from(json_payload)?;
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
            r#where: payload.r#where,
            limit: payload.limit,
            offset: payload.offset,
            include: payload.include,
        })
        .await?;
    Ok(Json(res))
}

async fn query(
    Path((tenant_id, database_name, collection_id)): Path<(String, String, String)>,
    State(mut server): State<FrontendServer>,
    Json(json_payload): Json<Value>,
) -> Result<Json<QueryResponse>, ServerError> {
    let collection_id = CollectionUuid::from_str(&collection_id)
        .map_err(|_| ValidationError::InvalidCollectionId)?;
    let payload = QueryRequestPayload::try_from(json_payload)?;
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
            r#where: payload.r#where,
            include: payload.include,
            embeddings: payload.query_embeddings,
            n_results: payload.n_results.unwrap_or(10),
        })
        .await?;

    Ok(Json(res))
}
