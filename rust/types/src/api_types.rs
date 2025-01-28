use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

use crate::error::QueryConversionError;
use crate::operator::KnnBatchResult;
use crate::operator::KnnProjectionRecord;
use crate::operator::ProjectionRecord;
use crate::Metadata;
use crate::Where;

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
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for CreateDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateDatabaseError::AlreadyExists => ErrorCodes::AlreadyExists,
            CreateDatabaseError::FailedToCreateDatabase(_) => ErrorCodes::Internal,
            CreateDatabaseError::RateLimited => ErrorCodes::ResourceExhausted,
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
    pub embeddings: Vec<Vec<f32>>,
    pub n_results: u32,
    pub include: Vec<Include>,
}

#[derive(Clone, Deserialize, Serialize)]
// TODO(Sanket): Implement this
pub struct QueryResponse {
    ids: Vec<Vec<String>>,
    embeddings: Option<Vec<Vec<Vec<f32>>>>,
    documents: Option<Vec<Vec<String>>>,
    uri: Option<Vec<Vec<String>>>,
    metadatas: Option<Vec<Vec<Metadata>>>,
    distances: Option<Vec<Vec<f32>>>,
    include: Vec<Include>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Include {
    Distance,
    Document,
    Embedding,
    Metadata,
    Uri,
}

pub const CHROMA_KEY: &str = "chroma:";
pub const CHROMA_URI_KEY: &str = "chroma:uri";

impl From<(KnnBatchResult, Vec<Include>)> for QueryResponse {
    fn from((result_vec, include_vec): (KnnBatchResult, Vec<Include>)) -> Self {
        let mut res = Self {
            ids: Vec::new(),
            embeddings: include_vec
                .contains(&Include::Embedding)
                .then_some(Vec::new()),
            documents: include_vec
                .contains(&Include::Document)
                .then_some(Vec::new()),
            uri: include_vec.contains(&Include::Uri).then_some(Vec::new()),
            metadatas: include_vec
                .contains(&Include::Metadata)
                .then_some(Vec::new()),
            distances: include_vec
                .contains(&Include::Distance)
                .then_some(Vec::new()),
            include: include_vec,
        };
        for query_result in result_vec {
            let mut ids = Vec::new();
            let mut embeddings = Vec::new();
            let mut documents = Vec::new();
            let mut uris = Vec::new();
            let mut metadatas = Vec::new();
            let mut distances = Vec::new();
            for KnnProjectionRecord {
                record:
                    ProjectionRecord {
                        id,
                        document,
                        embedding,
                        mut metadata,
                    },
                distance,
            } in query_result.records
            {
                ids.push(id);
                if let Some(emb) = embedding {
                    embeddings.push(emb);
                }
                if let Some(doc) = document {
                    documents.push(doc);
                }
                if let Some(crate::MetadataValue::Str(uri)) = metadata
                    .as_mut()
                    .and_then(|meta| meta.remove(CHROMA_URI_KEY))
                {
                    uris.push(uri);
                }
                if let Some(meta) = metadata.map(|m| {
                    m.into_iter()
                        .filter_map(|(k, v)| (!k.starts_with(CHROMA_KEY)).then_some((k, v)))
                        .collect()
                }) {
                    metadatas.push(meta);
                }
                if let Some(dist) = distance {
                    distances.push(dist);
                }
            }
            res.ids.push(ids);
            if let Some(res_embs) = res.embeddings.as_mut() {
                res_embs.push(embeddings);
            }
            if let Some(res_docs) = res.documents.as_mut() {
                res_docs.push(documents);
            }
            if let Some(res_uri) = res.uri.as_mut() {
                res_uri.push(uris);
            }
            if let Some(res_metas) = res.metadatas.as_mut() {
                res_metas.push(metadatas);
            }
            if let Some(res_dists) = res.distances.as_mut() {
                res_dists.push(distances);
            }
        }
        res
    }
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Error getting collection and segments info from sysdb")]
    CollectionSegments,
    #[error("Error executing plan: {0}")]
    Executor(#[from] ExecutorError),
}

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
