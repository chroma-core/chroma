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
use std::f32::consts::E;

use axum::{
    body::{self, Body, Bytes},
    extract::{FromRequest, FromRequestParts, Path, State},
    http::{request::Parts, Request, StatusCode},
    routing::{get, post},
    Json, Router,
};
use chroma_types::{
    operator::{KnnProjection, Projection},
    CompositeExpression, DocumentOperator, MetadataExpression, OperationRecord, PrimitiveOperator,
    Where,
};
use chroma_types::{CreateDatabaseError, CreateDatabaseRequest, Include, QueryResponse};
use figment::value;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Clone)]
pub struct QueryRequestPayload {
    r#where: Option<Where>,
    query_embeddings: Vec<Vec<f32>>,
    n_results: Option<u32>,
    include: Vec<Include>,
}

fn parse_where_document(json_payload: &Value) -> Result<Where, StatusCode> {
    let where_doc_payload = json_payload["where_document"]
        .as_object()
        .ok_or(StatusCode::BAD_REQUEST)?;
    if where_doc_payload.len() != 1 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let (key, value) = where_doc_payload.iter().next().unwrap();
    // Check if it is a composite expression.
    if key == "$and" {
        let logical_operator = chroma_types::BooleanOperator::And;
        // Check that the value is list type.
        let children = value.as_array().ok_or(StatusCode::BAD_REQUEST)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where_document(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    if key == "$or" {
        let logical_operator = chroma_types::BooleanOperator::Or;
        // Check that the value is list type.
        let children = value.as_array().ok_or(StatusCode::BAD_REQUEST)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where_document(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    if !value.is_string() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let value_str = value.as_str().unwrap();
    let operator_type;
    if key == "$contains" {
        operator_type = DocumentOperator::Contains;
    } else if key == "not_contains" {
        operator_type = DocumentOperator::NotContains;
    } else {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Where::Document(chroma_types::DocumentExpression {
        operator: operator_type,
        text: value_str.to_string(),
    }))
}

fn parse_where(json_payload: &Value) -> Result<Where, StatusCode> {
    let where_payload = json_payload["where"]
        .as_object()
        .ok_or(StatusCode::BAD_REQUEST)?;
    if where_payload.len() != 1 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let (key, value) = where_payload.iter().next().unwrap();
    // Check if it is a composite expression.
    if key == "$and" {
        let logical_operator = chroma_types::BooleanOperator::And;
        // Check that the value is list type.
        let children = value.as_array().ok_or(StatusCode::BAD_REQUEST)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    if key == "$or" {
        let logical_operator = chroma_types::BooleanOperator::Or;
        // Check that the value is list type.
        let children = value.as_array().ok_or(StatusCode::BAD_REQUEST)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    // At this point we know we're at a direct comparison. It can either
    // be of the form {"key": "value"} or {"key": {"$operator": "value"}}.
    if value.is_string() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Str(value.as_str().unwrap().to_string()),
            ),
        }));
    }
    if value.is_boolean() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Bool(value.as_bool().unwrap()),
            ),
        }));
    }
    if value.is_f64() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Float(value.as_f64().unwrap()),
            ),
        }));
    }
    if value.is_i64() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Int(value.as_i64().unwrap()),
            ),
        }));
    }
    if value.is_object() {
        let value_obj = value.as_object().unwrap();
        // value_obj should have exactly one key.
        if value_obj.len() != 1 {
            return Err(StatusCode::BAD_REQUEST);
        }
        let (operator, operand) = value_obj.iter().next().unwrap();
        if operand.is_array() {
            let set_operator;
            if operator == "$in" {
                set_operator = chroma_types::SetOperator::In;
            } else if operator == "$nin" {
                set_operator = chroma_types::SetOperator::NotIn;
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
            let operand = operand.as_array().unwrap();
            if operand.is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            if operand[0].is_string() {
                let operand_str = operand
                    .iter()
                    .map(|val| {
                        val.as_str()
                            .ok_or(StatusCode::BAD_REQUEST)
                            .map(|s| s.to_string())
                    })
                    .collect::<Result<Vec<String>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Str(operand_str),
                    ),
                }));
            }
            if operand[0].is_boolean() {
                let operand_bool = operand
                    .iter()
                    .map(|val| val.as_bool().ok_or(StatusCode::BAD_REQUEST))
                    .collect::<Result<Vec<bool>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Bool(operand_bool),
                    ),
                }));
            }
            if operand[0].is_f64() {
                let operand_f64 = operand
                    .iter()
                    .map(|val| val.as_f64().ok_or(StatusCode::BAD_REQUEST))
                    .collect::<Result<Vec<f64>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Float(operand_f64),
                    ),
                }));
            }
            if operand[0].is_i64() {
                let operand_i64 = operand
                    .iter()
                    .map(|val| val.as_i64().ok_or(StatusCode::BAD_REQUEST))
                    .collect::<Result<Vec<i64>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Int(operand_i64),
                    ),
                }));
            }
            return Err(StatusCode::BAD_REQUEST);
        }
        if operand.is_string() {
            let operand_str = operand.as_str().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Str(operand_str.to_string()),
                ),
            }));
        }
        if operand.is_boolean() {
            let operand_bool = operand.as_bool().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Bool(operand_bool),
                ),
            }));
        }
        if operand.is_f64() {
            let operand_f64 = operand.as_f64().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else if operator == "$lt" {
                operator_type = PrimitiveOperator::LessThan;
            } else if operator == "$lte" {
                operator_type = PrimitiveOperator::LessThanOrEqual;
            } else if operator == "$gt" {
                operator_type = PrimitiveOperator::GreaterThan;
            } else if operator == "$gte" {
                operator_type = PrimitiveOperator::GreaterThanOrEqual;
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Float(operand_f64),
                ),
            }));
        }
        if operand.is_i64() {
            let operand_i64 = operand.as_i64().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else if operator == "$lt" {
                operator_type = PrimitiveOperator::LessThan;
            } else if operator == "$lte" {
                operator_type = PrimitiveOperator::LessThanOrEqual;
            } else if operator == "$gt" {
                operator_type = PrimitiveOperator::GreaterThan;
            } else if operator == "$gte" {
                operator_type = PrimitiveOperator::GreaterThanOrEqual;
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Int(operand_i64),
                ),
            }));
        }
        return Err(StatusCode::BAD_REQUEST);
    }
    Err(StatusCode::BAD_REQUEST)
}

impl TryFrom<Value> for QueryRequestPayload {
    type Error = StatusCode;

    fn try_from(json_payload: Value) -> Result<Self, Self::Error> {
        let n_results = match &json_payload["n_results"] {
            Value::Null => None,
            Value::Number(n) => Some(n.as_u64().unwrap() as u32),
            _ => return Err(StatusCode::BAD_REQUEST),
        };
        let embeddings = json_payload["query_embeddings"]
            .as_array()
            .ok_or(StatusCode::BAD_REQUEST)?
            .iter()
            .map(|inner_array| {
                inner_array
                    .as_array()
                    .ok_or(StatusCode::BAD_REQUEST)
                    .and_then(|arr| {
                        arr.iter()
                            .map(|num| {
                                num.as_f64()
                                    .ok_or(StatusCode::BAD_REQUEST)
                                    .map(|n| n as f32)
                            })
                            .collect::<Result<Vec<f32>, _>>()
                    })
            })
            .collect::<Result<Vec<Vec<f32>>, _>>()?;

        Ok(QueryRequestPayload {
            r#where: None,
            query_embeddings: vec![],
            n_results: None,
            include: None,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryResponsePayload {}

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
    println!("{:?} Query json", json_payload);
    let payload =
        QueryRequestPayload::try_from(json_payload).map_err(|_| StatusCode::BAD_REQUEST)?;
    println!("{:?} Query payload", payload);

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
