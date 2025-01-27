use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

use crate::error::QueryConversionError;
use crate::Metadata;
use crate::{operator::KnnProjection, Where};

#[derive(Clone)]
pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Clone)]
pub struct CreateDatabaseResponse {}

#[derive(Error, Debug)]
pub enum CreateDatabaseError {
    #[error("Database already exists")]
    AlreadyExists,
    #[error("Failed to create database: {0}")]
    FailedToCreateDatabase(String),
}

impl ChromaError for CreateDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateDatabaseError::AlreadyExists => ErrorCodes::AlreadyExists,
            CreateDatabaseError::FailedToCreateDatabase(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone)]
pub struct GetDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Clone)]
pub struct QueryRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: Uuid,
    pub r#where: Option<Where>,
    pub include: KnnProjection,
    pub embeddings: Vec<Vec<f32>>,
    pub n_results: u32,
}

#[derive(Clone)]
// TODO(Sanket): Implement this
pub struct QueryResponse {
    ids: Vec<Vec<String>>,
    embeddings: Option<Vec<Vec<Vec<f32>>>>,
    documents: Option<Vec<Vec<String>>>,
    uri: Option<Vec<Vec<String>>>,
    metadatas: Option<Vec<Vec<Metadata>>>,
    distances: Option<Vec<Vec<f32>>>,
    // TODO(Sanket): Add the include field.
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncludePayload {
    pub document: bool,
    pub metadata: bool,
    pub uri: bool,
    pub embedding: bool,
    pub distance: bool,
    pub data: bool,
}

#[derive(Error, Debug)]
pub enum QueryError {}

impl ChromaError for QueryError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Clone)]
pub struct GetDatabaseResponse {
    pub database_id: Uuid,
    pub database_name: String,
    pub tenant_id: String,
}

#[derive(Error, Debug)]
pub enum GetDatabaseError {
    #[error("Database not found")]
    NotFound,
    #[error("Server sent empty response")]
    ResponseEmpty,
    #[error("Failed to parse database id")]
    IdParsingError,
    #[error("Failed to get database: {0}")]
    FailedToGetDatabase(String),
}

impl ChromaError for GetDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetDatabaseError::NotFound => ErrorCodes::NotFound,
            _ => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Error converting: {0}")]
    Conversion(#[from] QueryConversionError),
    #[error("Error from grpc: {0}")]
    Grpc(#[from] Status),
}
