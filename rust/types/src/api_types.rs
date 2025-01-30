use crate::error::QueryConversionError;
use crate::operator::GetResult;
use crate::operator::KnnBatchResult;
use crate::operator::KnnProjectionRecord;
use crate::operator::ProjectionRecord;
use crate::Collection;
use crate::CollectionUuid;
use crate::Metadata;
use crate::UpdateMetadata;
use crate::Where;
use chroma_config::assignment::rendezvous_hash::AssignmentError;
use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ResetError {
    #[error("Unable to reset cache")]
    Cache,
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for ResetError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Serialize)]
pub struct ChecklistResponse {
    pub max_batch_size: u32,
}

#[derive(Serialize)]
pub struct GetUserIdentityResponse {
    pub user_id: String,
    pub tenant: String,
    pub databases: Vec<String>,
}

#[derive(Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct CreateTenantResponse {}

#[derive(Debug, Error)]
pub enum CreateTenantError {
    #[error("Tenant already exists")]
    AlreadyExists,
    #[error("Rate limited")]
    RateLimited,
    #[error("Failed to create tenant: {0}")]
    SysDB(String),
}

impl ChromaError for CreateTenantError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateTenantError::AlreadyExists => ErrorCodes::AlreadyExists,
            CreateTenantError::SysDB(_) => ErrorCodes::Internal,
            CreateTenantError::RateLimited => ErrorCodes::ResourceExhausted,
        }
    }
}

pub struct GetTenantRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct GetTenantResponse {
    pub name: String,
}

#[derive(Debug, Error)]
pub enum GetTenantError {
    #[error("Server sent empty response")]
    ResponseEmpty,
    #[error("Rate limited")]
    RateLimited,
    #[error("Failed to get tenant: {0}")]
    SysDB(String),
}

impl ChromaError for GetTenantError {
    fn code(&self) -> ErrorCodes {
        todo!()
    }
}

pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Serialize)]
pub struct CreateDatabaseResponse {}

#[derive(Error, Debug)]
pub enum CreateDatabaseError {
    #[error("Database already exists")]
    AlreadyExists,
    #[error("Failed to create database: {0}")]
    SysDB(String),
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for CreateDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateDatabaseError::AlreadyExists => ErrorCodes::AlreadyExists,
            CreateDatabaseError::SysDB(_) => ErrorCodes::Internal,
            CreateDatabaseError::RateLimited => ErrorCodes::ResourceExhausted,
        }
    }
}

#[derive(Serialize)]
pub struct Database {
    pub id: Uuid,
    pub name: String,
    pub tenant: String,
}

pub struct ListDatabasesRequest {
    pub tenant_id: String,
    pub limit: Option<u32>,
    pub offset: u32,
}

pub type ListDatabasesResponse = Vec<Database>;

#[derive(Debug, Error)]
pub enum ListDatabasesError {
    #[error("Server sent empty response")]
    ResponseEmpty,
    #[error("Failed to parse database id")]
    IdParsingError,
    #[error("Failed to list database: {0}")]
    SysDB(String),
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for ListDatabasesError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

pub struct GetDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

pub type GetDatabaseResponse = Database;

#[derive(Error, Debug)]
pub enum GetDatabaseError {
    #[error("Database not found")]
    NotFound,
    #[error("Server sent empty response")]
    ResponseEmpty,
    #[error("Failed to parse database id")]
    IdParsingError,
    #[error("Failed to get database: {0}")]
    SysDB(String),
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for GetDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetDatabaseError::NotFound => ErrorCodes::NotFound,
            _ => ErrorCodes::Internal,
        }
    }
}

pub struct DeleteDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Serialize)]
pub struct DeleteDatabaseResponse {}

#[derive(Debug, Error)]
pub enum DeleteDatabaseError {
    #[error("Database not found")]
    NotFound,
    #[error("Server sent empty response")]
    ResponseEmpty,
    #[error("Failed to parse database id")]
    IdParsingError,
    #[error("Failed to delete database: {0}")]
    SysDB(String),
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for DeleteDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteDatabaseError::NotFound => ErrorCodes::NotFound,
            _ => ErrorCodes::Internal,
        }
    }
}

pub struct ListCollectionsRequest {
    pub tenant_id: String,
    pub database_name: String,
}

pub type ListCollectionsResponse = Vec<Collection>;

pub struct CountCollectionsRequest {
    pub tenant_id: String,
    pub database_name: String,
}

pub type CountCollectionsResponse = u32;

pub struct GetCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_name: String,
}

pub type GetCollectionResponse = Collection;

#[derive(Debug, Error)]
pub enum GetCollectionError {
    #[error("Collection not found")]
    NotFound,
    #[error("Error getting collection from sysdb {0}")]
    SysDB(String),
    #[error("Rate limited")]
    RateLimited,
}

impl ChromaError for GetCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionError::NotFound => ErrorCodes::NotFound,
            _ => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Deserialize)]
pub enum CollectionMetadataUpdate {
    ResetMetadata,
    UpdateMetadata(UpdateMetadata),
}

#[derive(Clone)]
pub struct UpdateCollectionRequest {
    pub collection_id: CollectionUuid,
    pub new_name: Option<String>,
    pub new_metadata: Option<CollectionMetadataUpdate>,
}

#[derive(Serialize)]
pub struct UpdateCollectionResponse {}

#[derive(Error, Debug)]
pub enum UpdateCollectionError {
    #[error("Could not update collection: {0}")]
    SysDB(String),
}

impl ChromaError for UpdateCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionError::SysDB(_) => ErrorCodes::Internal,
        }
    }
}

pub struct AddToCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: Uuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<String>>,
    pub uri: Option<Vec<String>>,
    pub metadatas: Option<Vec<Metadata>>,
}

#[derive(Serialize)]
pub struct AddToCollectionResponse {}

#[derive(Error, Debug)]
pub enum AddToCollectionError {
    #[error("Inconsistent number of IDs, embeddings, documents, URIs and metadatas")]
    InconsistentLength,
    #[error("Failed to push logs: {0}")]
    FailedToPushLogs(#[from] Box<dyn ChromaError>),
}

impl ChromaError for AddToCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            AddToCollectionError::InconsistentLength => ErrorCodes::InvalidArgument,
            AddToCollectionError::FailedToPushLogs(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Include {
    #[serde(rename = "distances")]
    Distance,
    #[serde(rename = "documents")]
    Document,
    #[serde(rename = "embeddings")]
    Embedding,
    #[serde(rename = "metadatas")]
    Metadata,
    #[serde(rename = "uris")]
    Uri,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncludeList(pub Vec<Include>);

impl IncludeList {
    pub fn default_query() -> Self {
        Self(vec![
            Include::Document,
            Include::Metadata,
            Include::Distance,
        ])
    }
    pub fn default_get() -> Self {
        Self(vec![Include::Document, Include::Metadata])
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct CountRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
}

pub type CountResponse = u32;

pub const CHROMA_KEY: &str = "chroma:";
pub const CHROMA_URI_KEY: &str = "chroma:uri";

#[derive(Clone)]
pub struct GetRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Option<Vec<String>>,
    pub r#where: Option<Where>,
    pub limit: Option<u32>,
    pub offset: u32,
    pub include: IncludeList,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct GetResponse {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<String>>,
    uri: Option<Vec<String>>,
    metadatas: Option<Vec<Value>>,
    include: Vec<Include>,
}

impl From<(GetResult, IncludeList)> for GetResponse {
    fn from((result_vec, IncludeList(include_vec)): (GetResult, IncludeList)) -> Self {
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
            include: include_vec,
        };
        for ProjectionRecord {
            id,
            document,
            embedding,
            mut metadata,
        } in result_vec.records
        {
            res.ids.push(id);
            if let (Some(emb), Some(embeddings)) = (embedding, res.embeddings.as_mut()) {
                embeddings.push(emb);
            }
            if let (Some(doc), Some(documents)) = (document, res.documents.as_mut()) {
                documents.push(doc);
            }
            if let (Some(crate::MetadataValue::Str(uri)), Some(uris)) = (
                metadata
                    .as_mut()
                    .and_then(|meta| meta.remove(CHROMA_URI_KEY)),
                res.uri.as_mut(),
            ) {
                uris.push(uri);
            }
            if let (Some(meta), Some(metadatas)) = (
                metadata.map(|m| {
                    Value::Object(
                        m.into_iter()
                            .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                            .map(|(k, v)| (k, v.into()))
                            .collect(),
                    )
                }),
                res.metadatas.as_mut(),
            ) {
                metadatas.push(meta);
            }
        }
        res
    }
}

#[derive(Clone)]
pub struct QueryRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Option<Vec<String>>,
    pub r#where: Option<Where>,
    pub embeddings: Vec<Vec<f32>>,
    pub n_results: u32,
    pub include: IncludeList,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct QueryResponse {
    ids: Vec<Vec<String>>,
    embeddings: Option<Vec<Vec<Vec<f32>>>>,
    documents: Option<Vec<Vec<String>>>,
    uri: Option<Vec<Vec<String>>>,
    metadatas: Option<Vec<Vec<Value>>>,
    distances: Option<Vec<Vec<f32>>>,
    include: Vec<Include>,
}

impl From<(KnnBatchResult, IncludeList)> for QueryResponse {
    fn from((result_vec, IncludeList(include_vec)): (KnnBatchResult, IncludeList)) -> Self {
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
                    Value::Object(
                        m.into_iter()
                            .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                            .map(|(k, v)| (k, v.into()))
                            .collect(),
                    )
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

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Error converting: {0}")]
    Conversion(#[from] QueryConversionError),
    #[error("Error from grpc: {0}")]
    Grpc(#[from] Status),
    #[error("Memberlist is empty")]
    EmptyMemberlist,
    #[error("Assignment error: {0}")]
    AssignmentError(#[from] AssignmentError),
    #[error("No client found for node: {0}")]
    NoClientFound(String),
}

impl ChromaError for ExecutorError {
    fn code(&self) -> ErrorCodes {
        match self {
            ExecutorError::Conversion(_) => ErrorCodes::InvalidArgument,
            ExecutorError::Grpc(_) => ErrorCodes::Internal,
            ExecutorError::EmptyMemberlist => ErrorCodes::Internal,
            ExecutorError::AssignmentError(_) => ErrorCodes::Internal,
            ExecutorError::NoClientFound(_) => ErrorCodes::Internal,
        }
    }
}
