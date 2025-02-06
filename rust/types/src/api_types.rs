use std::time::SystemTimeError;

use crate::error::QueryConversionError;
use crate::operator::GetResult;
use crate::operator::KnnBatchResult;
use crate::operator::KnnProjectionRecord;
use crate::operator::ProjectionRecord;
use crate::Collection;
use crate::CollectionConversionError;
use crate::CollectionUuid;
use crate::Metadata;
use crate::SegmentConversionError;
use crate::SegmentScopeConversionError;
use crate::UpdateMetadata;
use crate::Where;
use chroma_config::assignment::rendezvous_hash::AssignmentError;
use chroma_error::ChromaValidationError;
use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Error)]
pub enum GetSegmentsError {
    #[error("Could not parse segment")]
    SegmentConversion(#[from] SegmentConversionError),
    #[error("Unknown segment scope")]
    UnknownScope(#[from] SegmentScopeConversionError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

#[derive(Debug, Error)]
pub enum GetCollectionWithSegmentsError {
    #[error("Failed to convert proto collection")]
    CollectionConversionError(#[from] CollectionConversionError),
    #[error("Duplicate segment")]
    DuplicateSegment,
    #[error("Missing field: [{0}]")]
    Field(String),
    #[error("Failed to convert proto segment")]
    SegmentConversionError(#[from] SegmentConversionError),
    #[error("Failed to fetch")]
    FailedToGetSegments(#[from] tonic::Status),
    #[error("Failed to get segments")]
    GetSegmentsError(#[from] GetSegmentsError),
    #[error("Collection not found")]
    NotFound,
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

pub struct ResetResponse {}

#[derive(Debug, Error)]
pub enum ResetError {
    #[error(transparent)]
    Cache(Box<dyn ChromaError>),
    #[error(transparent)]
    Internal(#[from] Status),
    #[error("Reset is disabled by config")]
    NotAllowed,
}

impl ChromaError for ResetError {
    fn code(&self) -> ErrorCodes {
        match self {
            ResetError::Cache(err) => err.code(),
            ResetError::Internal(status) => status.code().into(),
            ResetError::NotAllowed => ErrorCodes::PermissionDenied,
        }
    }
}

#[derive(Serialize)]
pub struct ChecklistResponse {
    pub max_batch_size: u32,
}

#[derive(Serialize)]
pub struct HeartbeatResponse {
    #[serde(rename(serialize = "nanosecond heartbeat"))]
    pub nanosecond_heartbeat: u128,
}

#[derive(Debug, Error)]
pub enum HeartbeatError {
    #[error(transparent)]
    CouldNotGetTime(#[from] SystemTimeError),
}

impl ChromaError for HeartbeatError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
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
    #[error("Tenant [{0}] already exists")]
    AlreadyExists(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CreateTenantError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateTenantError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            CreateTenantError::Internal(err) => err.code(),
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Tenant [{0}] not found")]
    NotFound(String),
}

impl ChromaError for GetTenantError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetTenantError::Internal(err) => err.code(),
            GetTenantError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[non_exhaustive]
#[derive(Validate)]
pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub tenant_id: String,
    #[validate(length(min = 3))]
    pub database_name: String,
}

impl CreateDatabaseRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let database_id = Uuid::new_v4();
        let request = Self {
            database_id,
            tenant_id,
            database_name,
        };
        request
            .validate()
            .map_err(|err| ChromaValidationError::from(err))?;
        Ok(request)
    }
}

#[derive(Serialize)]
pub struct CreateDatabaseResponse {}

#[derive(Error, Debug)]
pub enum CreateDatabaseError {
    #[error("Database [{0}] already exists")]
    AlreadyExists(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CreateDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateDatabaseError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            CreateDatabaseError::Internal(status) => status.code(),
        }
    }
}

#[derive(Serialize, Debug)]
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Invalid database id [{0}]")]
    InvalidID(String),
}

impl ChromaError for ListDatabasesError {
    fn code(&self) -> ErrorCodes {
        match self {
            ListDatabasesError::Internal(status) => status.code(),
            ListDatabasesError::InvalidID(_) => ErrorCodes::InvalidArgument,
        }
    }
}

pub struct GetDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

pub type GetDatabaseResponse = Database;

#[derive(Error, Debug)]
pub enum GetDatabaseError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Invalid database id [{0}]")]
    InvalidID(String),
    #[error("Database [{0}] not found")]
    NotFound(String),
}

impl ChromaError for GetDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetDatabaseError::Internal(err) => err.code(),
            GetDatabaseError::InvalidID(_) => ErrorCodes::InvalidArgument,
            GetDatabaseError::NotFound(_) => ErrorCodes::NotFound,
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Invalid database id [{0}]")]
    InvalidID(String),
    #[error("Database [{0}] not found")]
    NotFound(String),
}

impl ChromaError for DeleteDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteDatabaseError::Internal(err) => err.code(),
            DeleteDatabaseError::InvalidID(_) => ErrorCodes::InvalidArgument,
            DeleteDatabaseError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

pub struct ListCollectionsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub limit: Option<u32>,
    pub offset: u32,
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
}

impl ChromaError for GetCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionError::Internal(err) => err.code(),
            GetCollectionError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[derive(Clone)]
pub struct CreateCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub name: String,
    pub metadata: Option<Metadata>,
    pub configuration_json: Option<Value>,
    pub get_or_create: bool,
}

pub type CreateCollectionResponse = Collection;

#[derive(Debug, Error)]
pub enum CreateCollectionError {
    #[error("Collection [{0}] already exists")]
    AlreadyExists(String),
    #[error("Database [{0}] does not exist")]
    DatabaseNotFound(String),
    #[error("Could not fetch collections: {0}")]
    Get(#[from] GetCollectionsError),
    #[error("Could not deserialize configuration: {0}")]
    Configuration(#[from] serde_json::Error),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CreateCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateCollectionError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            CreateCollectionError::DatabaseNotFound(_) => ErrorCodes::InvalidArgument,
            CreateCollectionError::Get(err) => err.code(),
            CreateCollectionError::Configuration(_) => ErrorCodes::Internal,
            CreateCollectionError::Internal(err) => err.code(),
        }
    }
}

#[derive(Debug, Error)]
pub enum GetCollectionsError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Could not deserialize configuration")]
    Configuration(#[from] serde_json::Error),
    #[error("Could not deserialize collection ID")]
    CollectionId(#[from] uuid::Error),
}

impl ChromaError for GetCollectionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsError::Internal(err) => err.code(),
            GetCollectionsError::Configuration(_) => ErrorCodes::Internal,
            GetCollectionsError::CollectionId(_) => ErrorCodes::Internal,
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for UpdateCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionError::Internal(err) => err.code(),
        }
    }
}

#[derive(Clone)]
pub struct DeleteCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_name: String,
}

#[derive(Serialize)]
pub struct DeleteCollectionResponse {}

#[derive(Error, Debug)]
pub enum DeleteCollectionError {
    #[error(transparent)]
    Get(#[from] GetCollectionError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for DeleteCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteCollectionError::Get(err) => err.code(),
            DeleteCollectionError::Internal(err) => err.code(),
        }
    }
}

#[derive(Debug)]
pub struct AddCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<Metadata>>>,
}

#[derive(Serialize)]
pub struct AddCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum AddCollectionRecordsError {
    #[error("Failed to get collection: {0}")]
    Collection(#[from] GetCollectionError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for AddCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            AddCollectionRecordsError::Collection(err) => err.code(),
            AddCollectionRecordsError::Internal(err) => err.code(),
        }
    }
}

pub struct UpdateCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Option<Vec<f32>>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

#[derive(Serialize)]
pub struct UpdateCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum UpdateCollectionRecordsError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for UpdateCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionRecordsError::Internal(err) => err.code(),
        }
    }
}

pub struct UpsertCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

#[derive(Serialize)]
pub struct UpsertCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum UpsertCollectionRecordsError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for UpsertCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpsertCollectionRecordsError::Internal(err) => err.code(),
        }
    }
}

#[derive(Clone)]
pub struct DeleteCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Option<Vec<String>>,
    pub r#where: Option<Where>,
}

#[derive(Serialize)]
pub struct DeleteCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum DeleteCollectionRecordsError {
    #[error("Failed to resolve records for deletion: {0}")]
    Get(#[from] ExecutorError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for DeleteCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteCollectionRecordsError::Get(err) => err.code(),
            DeleteCollectionRecordsError::Internal(err) => err.code(),
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
pub const CHROMA_DOCUMENT_KEY: &str = "chroma:document";
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

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct GetResponse {
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<Option<String>>>,
    uri: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<Metadata>>>,
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
            if let Some(documents) = res.documents.as_mut() {
                documents.push(document);
            }

            let uri = metadata.as_mut().and_then(|meta| {
                meta.remove(CHROMA_URI_KEY).and_then(|v| {
                    if let crate::MetadataValue::Str(uri) = v {
                        Some(uri)
                    } else {
                        None
                    }
                })
            });
            if let Some(uris) = res.uri.as_mut() {
                uris.push(uri);
            }

            let metadata = metadata.map(|m| {
                m.into_iter()
                    .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                    .collect()
            });
            if let Some(metadatas) = res.metadatas.as_mut() {
                metadatas.push(metadata);
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
    embeddings: Option<Vec<Vec<Option<Vec<f32>>>>>,
    documents: Option<Vec<Vec<Option<String>>>>,
    uri: Option<Vec<Vec<Option<String>>>>,
    metadatas: Option<Vec<Vec<Option<Metadata>>>>,
    distances: Option<Vec<Vec<Option<f32>>>>,
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
                embeddings.push(embedding);
                documents.push(document);

                let uri = metadata.as_mut().and_then(|meta| {
                    meta.remove(CHROMA_URI_KEY).and_then(|v| {
                        if let crate::MetadataValue::Str(uri) = v {
                            Some(uri)
                        } else {
                            None
                        }
                    })
                });
                uris.push(uri);

                let metadata = metadata.map(|m| {
                    m.into_iter()
                        .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                        .collect()
                });
                metadatas.push(metadata);

                distances.push(distance);
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
    #[error("Error executing plan: {0}")]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for QueryError {
    fn code(&self) -> ErrorCodes {
        match self {
            QueryError::Executor(e) => e.code(),
            QueryError::Internal(err) => err.code(),
        }
    }
}

#[derive(Serialize)]
pub struct HealthCheckResponse {
    pub is_executor_ready: bool,
}

impl HealthCheckResponse {
    pub fn get_status_code(&self) -> tonic::Code {
        if self.is_executor_ready {
            tonic::Code::Ok
        } else {
            tonic::Code::Unavailable
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Assignment error: {0}")]
    AssignmentError(#[from] AssignmentError),
    #[error("Error converting: {0}")]
    Conversion(#[from] QueryConversionError),
    #[error("Memberlist is empty")]
    EmptyMemberlist,
    #[error(transparent)]
    Grpc(#[from] Status),
    #[error("Inconsistent data")]
    InconsistentData,
    #[error("Internal error: {0}")]
    Internal(Box<dyn ChromaError>),
    #[error("No client found for node: {0}")]
    NoClientFound(String),
}

impl ChromaError for ExecutorError {
    fn code(&self) -> ErrorCodes {
        match self {
            ExecutorError::AssignmentError(_) => ErrorCodes::Internal,
            ExecutorError::Conversion(_) => ErrorCodes::InvalidArgument,
            ExecutorError::EmptyMemberlist => ErrorCodes::Internal,
            ExecutorError::Grpc(e) => e.code().into(),
            ExecutorError::InconsistentData => ErrorCodes::Internal,
            ExecutorError::Internal(e) => e.code(),
            ExecutorError::NoClientFound(_) => ErrorCodes::Internal,
        }
    }
}
